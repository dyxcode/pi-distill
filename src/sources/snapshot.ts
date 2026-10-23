import {
  buildSessionProjection
} from "@earendil-works/pi-coding-agent";
import { resolve } from "node:path";
import { consumeAggregateBudget } from './budget.ts';
import { branchForLeaf, invalidSource, MAX_ENTRY_ID_LENGTH, MAX_SESSION_ID_LENGTH, validateBranch, validateContextEdits, validateSessionEntries, validateSessionHeader, validNonEmptyString } from './format.ts';
import { getProjection, projections } from './projection.ts';
import { loadSessionGraph, syncAncestry } from './reader.ts';
import type { AggregateBudget, ReadonlySessionManager, SessionGraph, SourceSnapshot } from './types.ts';
export function snapshotSession(manager: ReadonlySessionManager): SourceSnapshot {
  const sessionId = manager.getSessionId();
  const header = manager.getHeader();
  const sessionFile = manager.getSessionFile() ?? null;
  const cwd = manager.getCwd();
  const allEntries = manager.getEntries();
  const leafId = manager.getLeafId();
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  if (!consumeAggregateBudget(budget, [sessionId, sessionFile, cwd, header, allEntries], allEntries.length)) {
    return pendingSnapshot(sessionId, sessionFile, cwd, leafId,
      "Session snapshot exceeds the aggregate ancestry size or entry limit.");
  }

  validateSessionHeader({ ...header, id: sessionId, cwd });
  validateSessionEntries(allEntries);

  const branch = manager.getBranch();
  if (leafId === null && branch.length > 0) {
    throw invalidSource("Pi returned session entries without a leaf checkpoint");
  }
  if (leafId !== null) validateBranch(branch, leafId);

  // Pi's projection is the public implementation of context_edit semantics.
  // It also gives us effective content for message entries still in model
  // context; older compacted messages are handled by their append-only edits.
  const projection = manager.buildSessionProjection();
  const ancestry = syncAncestry(sessionId, allEntries, sessionFile ?? undefined, header?.parentSession, budget);
  const pendingReason = ancestry.unresolved
    ? "Fork ancestry could not be verified; reload the original session asynchronously before planning."
    : validateContextEdits(branch).reason;

  const snapshot: SourceSnapshot = {
    sessionId,
    sessionFile,
    cwd,
    leafId,
    entries: branch,
    ...(header?.parentSession ? { parentSession: header.parentSession } : {}),
    ancestry,
    ...(pendingReason ? { pending: true, pendingReason } : { pending: false }),
  };
  projections.set(snapshot, projection);
  return snapshot;
}

/** Read and validate a persisted Pi session without changing the source file. */
export async function loadSnapshot(sessionFile: string, leafId?: string | null): Promise<SourceSnapshot> {
  if (typeof sessionFile !== "string" || sessionFile.trim().length === 0) {
    throw invalidSource("Session file path must be a nonempty string.");
  }
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  const graph = await loadSessionGraph(resolve(sessionFile), new Set(), 0, budget);
  return snapshotFromGraph(graph, sessionFile, leafId);
}

/** Recover a saved compaction across every branch, including an inactive branch. */
export async function findCompactionRecord(
  sessionFile: string,
  sessionId: string,
  cwd: string,
  leafId: string | null,
): Promise<string | undefined> {
  if (typeof sessionFile !== "string" || sessionFile.trim().length === 0 ||
    !validNonEmptyString(sessionId, MAX_SESSION_ID_LENGTH) || typeof cwd !== "string" ||
    (leafId !== null && !validNonEmptyString(leafId, MAX_ENTRY_ID_LENGTH))) {
    throw invalidSource("Compaction recovery source identity is invalid.");
  }
  const graph = await loadSessionGraph(resolve(sessionFile), new Set(), 0, { bytes: 0, entries: 0 });
  if (graph.header.id !== sessionId || graph.header.cwd !== cwd || graph.pending || graph.parentSessionPending) {
    throw invalidSource("Compaction recovery requires the complete original session and ancestry.");
  }
  return graph.entries.find(entry => entry.type === "compaction" && entry.parentId === leafId)?.id;
}

export function snapshotFromGraph(
  graph: SessionGraph & { readonly parentSessionPending: boolean; },
  sessionFile: string,
  leafId?: string | null,
  budget?: AggregateBudget,
): SourceSnapshot {
  const selectedLeaf = leafId === undefined ? graph.entries.at(-1)?.id ?? null : leafId;
  if (selectedLeaf !== null && !graph.entriesById.has(selectedLeaf)) {
    return {
      sessionId: graph.header.id,
      sessionFile: resolve(sessionFile),
      cwd: graph.header.cwd,
      leafId: null,
      entries: [],
      pending: true,
      pendingReason: "Requested source checkpoint is not present in the complete session prefix.",
      ...(graph.header.parentSession ? { parentSession: graph.header.parentSession } : {}),
      ancestry: { origins: graph.origins, signatures: graph.signatures },
    };
  }

  const branch = selectedLeaf === null ? [] : branchForLeaf(graph.entriesById, selectedLeaf);
  if (budget && !consumeAggregateBudget(budget,
    [graph.header.id, resolve(sessionFile), graph.header.cwd, branch], branch.length)) {
    throw invalidSource("Session branch coverage exceeds the aggregate size or entry limit.");
  }
  const editStatus = validateContextEdits(branch);
  const projection = buildSessionProjection(branch, selectedLeaf, graph.entriesById);
  const pendingReason = graph.pending
    ? "Session file has an incomplete trailing JSONL record."
    : graph.parentSessionPending
      ? "Fork ancestry could not be verified completely."
      : editStatus.reason;

  const snapshot: SourceSnapshot = {
    sessionId: graph.header.id,
    sessionFile: resolve(sessionFile),
    cwd: graph.header.cwd,
    leafId: selectedLeaf,
    entries: branch,
    ...(graph.header.parentSession ? { parentSession: graph.header.parentSession } : {}),
    ancestry: { origins: graph.origins, signatures: graph.signatures, ...(graph.parentSessionPending ? { unresolved: true } : {}) },
    ...(pendingReason ? { pending: true, pendingReason } : { pending: false }),
  };
  projections.set(snapshot, projection);
  return snapshot;
}

/** Establish whether an empty plan is valid rather than a failed source parse. */
export function isValidSourceSnapshot(snapshot: SourceSnapshot): boolean {
  if (snapshot.pending || snapshot.ancestry?.unresolved) return false;
  if (!validNonEmptyString(snapshot.sessionId, MAX_SESSION_ID_LENGTH) || typeof snapshot.cwd !== "string" ||
    !Array.isArray(snapshot.entries)) return false;
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  if (!consumeAggregateBudget(budget, [snapshot.sessionId, snapshot.sessionFile, snapshot.cwd, snapshot.entries], snapshot.entries.length)) return false;
  if (snapshot.leafId === null) return snapshot.entries.length === 0;
  try {
    const branch = validateBranch(snapshot.entries, snapshot.leafId);
    if (validateContextEdits(branch).reason) return false;
    getProjection(snapshot);
    return true;
  } catch { return false; }
}

export function pendingSnapshot(
  sessionId: string,
  sessionFile: string | null,
  cwd: string,
  leafId: string | null,
  pendingReason: string,
): SourceSnapshot {
  return {
    sessionId,
    sessionFile,
    cwd,
    leafId,
    entries: [],
    pending: true,
    pendingReason,
    ancestry: { origins: new Map(), signatures: new Map(), unresolved: true },
  };
}
