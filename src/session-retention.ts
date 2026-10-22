import { resolve } from "node:path";
import {
  loadSessionCoverage,
  sessionCoverageUnchanged,
  type SessionCoverage,
} from "./consolidation-source.ts";
import type { SQLiteMemoryStore } from "./memory-store.js";

export interface SessionRetentionAssessment {
  readonly sessionFile: string;
  readonly sessionId: string | null;
  readonly status: "processed" | "pending" | "unknown";
  readonly reason: "source-unavailable" | "progress-unavailable" | "checkpoint-changed" | "source-changed" | "duplicate-session-id" | null;
  readonly unprocessedSources: number | null;
  readonly pendingJobs: number | null;
  /** Every ancestor needed to resolve this session, including paths outside the input. */
  readonly dependsOn: readonly string[];
  /** Supplied sessions that still require this file, even if they are processed. */
  readonly requiredBy: readonly string[];
}

export interface SessionRetentionReport {
  readonly scope: "provided-session-files";
  /** Only describes dependency resolution for the supplied files, never global discovery. */
  readonly dependenciesComplete: boolean;
  readonly sessions: readonly SessionRetentionAssessment[];
}

type ProgressReader = Pick<SQLiteMemoryStore, "readConsolidationStatus">;

/**
 * Read-only coverage of current file contents and known fork dependencies.
 * A processed result is not a deletion permit: it establishes neither inactivity,
 * complete discovery, historical file integrity, nor a retention policy.
 * Results are snapshots; any future deletion must recheck under its own protection.
 */
export async function assessSessionRetention(
  sessionFiles: readonly string[],
  store: ProgressReader,
): Promise<SessionRetentionReport> {
  if (!Array.isArray(sessionFiles) || sessionFiles.length > 1024 ||
    [...sessionFiles].some(path => typeof path !== "string" || path.trim().length === 0 || path.includes("\0"))) {
    throw new TypeError("Session files must be an array of at most 1024 nonempty paths.");
  }
  const paths = [...new Set(sessionFiles.map(path => resolve(path)))];
  const coverage = new Map<string, SessionCoverage>();
  const sessions: SessionRetentionAssessment[] = [];
  let initial: ReturnType<ProgressReader["readConsolidationStatus"]> | undefined;
  try {
    initial = store.readConsolidationStatus([]);
  } catch {
    // File dependencies can still be inspected, but processing will be unknown.
  }
  const budget = { bytes: 0, entries: 0 };
  for (const path of paths) {
    try {
      coverage.set(path, await loadSessionCoverage(path, initial?.observations, budget));
    } catch {
      // A malformed/unstable source cannot be interpreted as an empty conversation.
    }
  }
  const sourceKeys = [...new Set([...coverage.values()].flatMap(source => source.sourceKeys))];
  let progress: ReturnType<ProgressReader["readConsolidationStatus"]> | undefined;
  try {
    progress = store.readConsolidationStatus(sourceKeys);
  } catch {
    // No persisted progress evidence means no completion claim.
  }
  const processed = new Set(progress?.processedSources);
  const identities = new Map<string, number>();
  for (const source of coverage.values()) {
    identities.set(source.sessionId, (identities.get(source.sessionId) ?? 0) + 1);
  }
  let dependenciesComplete = coverage.size === paths.length;
  for (const path of paths) {
    const source = coverage.get(path);
    const dependsOn = source ? [...source.fileVersions.keys()].filter(file => file !== path).sort() : [];
    const ownVersion = source?.fileVersions.get(path);
    // The version includes device and inode: directory aliases/hard links must
    // retain the same dependency even when their absolute path strings differ.
    const requiredBy = [...coverage].filter(([file, other]) => file !== path &&
      (other.fileVersions.has(path) || (ownVersion !== undefined && [...other.fileVersions.values()].includes(ownVersion))))
      .map(([file]) => file).sort();
    let reason: SessionRetentionAssessment["reason"] = null;
    if (!source) reason = "source-unavailable";
    else if (!await sessionCoverageUnchanged(source)) reason = "source-changed";
    else if ((identities.get(source.sessionId) ?? 0) > 1) reason = "duplicate-session-id";
    else if (!progress || !initial) reason = "progress-unavailable";
    else {
      const checkpoints = (state: typeof progress): string => JSON.stringify(state.observations.filter(observation =>
        observation.sessionId === source.sessionId || resolve(observation.sessionFile) === path));
      if (checkpoints(initial) !== checkpoints(progress)) reason = "checkpoint-changed";
    }
    if (reason === "source-unavailable" || reason === "source-changed" || reason === "duplicate-session-id") dependenciesComplete = false;
    const unprocessedSources = reason || !source ? null : source.sourceKeys.filter(key => !processed.has(key)).length;
    const pendingJobs = reason || !source || !progress ? null : progress.pendingJobs.filter(job =>
      job.sessionId === source.sessionId || (job.sessionFile !== null && resolve(job.sessionFile) === path),
    ).length;
    sessions.push({
      sessionFile: path,
      sessionId: source?.sessionId ?? null,
      status: reason ? "unknown" : (unprocessedSources !== 0 || pendingJobs !== 0) ? "pending" : "processed",
      reason,
      unprocessedSources,
      pendingJobs,
      dependsOn,
      requiredBy,
    });
  }
  return { scope: "provided-session-files", dependenciesComplete, sessions };
}
