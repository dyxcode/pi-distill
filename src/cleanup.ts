import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { closeSync, fsyncSync, lstatSync, openSync, unlinkSync } from "node:fs";
import type { SQLiteMemoryStore } from "./memory-store.js";
import { assessSessionRetention, type SessionRetentionAssessment } from "./session-retention.ts";
import type { WaitReason } from "./task-types.ts";

interface Options {
  readonly openStore: () => SQLiteMemoryStore;
  readonly hasStore?: () => boolean;
  readonly retryDelayMs?: number;
}

/** Independent local cleanup of fully processed, inactive managed sources. */
export class CleanupCoordinator {
  readonly #options: Options;
  #store: SQLiteMemoryStore | undefined;
  #working: Promise<void> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  #wakeAgain = false;
  #cursor: number | undefined;
  #legacyCursor: number | undefined;
  #claim: { id: string; owner: string } | undefined;

  constructor(options: Options) { this.#options = options; }

  /** Usage/source changes bypass a previous assessment's retry delay. */
  wake(): void {
    if (this.#closed) return;
    if (this.#working) { this.#wakeAgain = true; return; }
    this.#run(true);
  }

  async whenIdle(): Promise<void> {
    while (this.#working) await this.#working;
  }

  close(): void {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    if (this.#claim) {
      try { this.#store?.releaseCleanTask(this.#claim.id, this.#claim.owner); } catch { /* Lease expiry recovers a failed release. */ }
      this.#claim = undefined;
    }
    // A pending file read owns this connection until its continuation notices
    // closure; shutdown itself never waits for the read.
    if (!this.#working) { this.#store?.close(); this.#store = undefined; }
  }

  #run(force: boolean): void {
    if (this.#closed || this.#working) return;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#working = new Promise<void>(resolveWork => {
      setImmediate(() => { void this.#pass(force).catch(() => {}).finally(resolveWork); });
    }).finally(() => {
      this.#working = undefined;
      if (this.#closed) { this.#store?.close(); this.#store = undefined; return; }
      if (this.#wakeAgain) { this.#wakeAgain = false; this.#run(true); return; }
      this.#timer = setTimeout(() => this.#run(false), this.#options.retryDelayMs ?? 30_000);
      this.#timer.unref();
    });
  }

  async #pass(force: boolean): Promise<void> {
    if (this.#closed) return;
    if (!this.#store && this.#options.hasStore && !this.#options.hasStore()) return;
    const store = this.#store ??= this.#options.openStore();
    const reconciled = store.reconcileLegacyCleanTasks(32, this.#legacyCursor);
    this.#legacyCursor = reconciled.nextCursor;
    const page = store.listCleanTasks({ limit: 4,
      ...(this.#cursor === undefined ? {} : { cursor: this.#cursor }),
      ...(force ? {} : { now: Date.now() }),
    });
    this.#cursor = page.nextCursor;
    for (const task of page.items) {
      if (this.#closed) return;
      const owner = randomUUID();
      if (!store.claimCleanTask(task.id, owner, Date.now(), 60_000, force)) continue;
      this.#claim = { id: task.id, owner };
      let reason: WaitReason = "unavailable";
      const access = store.acquireSourceAccess();
      if (!access) { store.releaseCleanTask(task.id, owner); this.#claim = undefined; continue; }
      try {
        store.reconcileExitedSessionOwners();
        const version = store.retentionVersion();
        const receipt = store.getDeletion(task.sessionFile);
        if (receipt?.state === "deleting" && receipt.sessionId === task.sessionId && receipt.intentId === task.intentId && missing(task.sessionFile)) {
          if (!this.#closed && store.finishDeletion(task.id, owner, access, version, () => { if (!missing(task.sessionFile)) return false; syncParent(task.sessionFile); return true; })) {
            this.#claim = undefined;
            continue;
          }
        }
        // Page the entire managed catalog within the assessment's explicit cap.
        // An extra page at the cap means incomplete discovery, never permission.
        const known: string[] = [];
        let cursor: string | undefined;
        let complete = false;
        while (known.length <= 1024) {
          const paths = store.listKnownSessionFiles(128, cursor);
          known.push(...paths);
          if (paths.length < 128) { complete = true; break; }
          cursor = paths.at(-1);
        }
        if (!complete || known.length > 1024) { reason = "dependencies"; }
        else {
          const usages = store.listSessionUsages();
          const files = known.filter(path => {
            // A newly-created Pi session has a path before its first message is saved.
            // It is not an old source to assess or delete.
            const owners = usages.filter(item => item.sessionFile === path);
            return !(path !== task.sessionFile && owners.length > 0 &&
              !store.hasSessionSourceWork(path) && missing(path));
          });
          const report = await assessSessionRetention([...new Set([task.sessionFile, ...files])], store);
          const assessment = report.sessions.find(source => source.sessionFile === task.sessionFile);
          reason = assessment?.sessionId === task.sessionId ? assessmentReason(assessment, report.dependenciesComplete) : "unavailable";
          if (store.observedSessions().some(source => source.sessionId === task.sessionId || source.sessionFile === task.sessionFile)) {
            reason = "unprocessed";
          }
          if (!this.#closed && reason === "host_coordination") {
            const fileVersion = report.fileVersions.get(task.sessionFile);
            if (fileVersion &&
                store.prepareDeletion(task.id, owner, access, version, fileVersion)) {
              const readyVersion = store.retentionVersion();
              const done = store.finishDeletion(task.id, owner, access, readyVersion, () => {
                for (const [path, expected] of report.fileVersions) if (fileVersionAt(path) !== expected) return false;
                unlinkSync(task.sessionFile);
                syncParent(task.sessionFile);
                return true;
              });
              if (done) { this.#claim = undefined; continue; }
            }
            reason = "unavailable";
          }
        }
      } catch { reason = "unavailable"; /* Keep failed or changed evidence for a later wake. */ }
      finally { store.releaseSourceAccess(access); }
      if (this.#closed) return;
      const now = Date.now();
      store.recordCleanAssessment(task.id, owner, now, reason, now + (this.#options.retryDelayMs ?? 30_000));
      this.#claim = undefined;
    }
  }
}

function assessmentReason(source: SessionRetentionAssessment | undefined, dependenciesComplete: boolean): WaitReason {
  if (!source || source.status === "unknown") return "unavailable";
  if (source.status === "pending") return "unprocessed";
  if (!dependenciesComplete) return "dependencies";
  if (source.usage !== "released") return "usage";
  if (source.requiredBy.length > 0) return "dependencies";
  return "host_coordination";
}

function missing(path: string): boolean {
  try { lstatSync(path); return false; }
  catch (error) { return error instanceof Error && "code" in error && error.code === "ENOENT"; }
}

function fileVersionAt(path: string): string {
  const info = lstatSync(path, { bigint: true });
  if (!info.isFile()) throw new Error("Cleanup requires a regular source file.");
  return [info.dev, info.ino, info.size, info.mtimeNs, info.ctimeNs].join(":");
}

function syncParent(path: string): void {
  const fd = openSync(dirname(path), "r");
  try { fsyncSync(fd); } finally { closeSync(fd); }
}
