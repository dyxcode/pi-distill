import { dirname, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { closeSync, fsyncSync, lstatSync, openSync, unlinkSync } from "node:fs";
import type { SQLiteMemoryStore } from "./memory-store.js";
import { assessSessionRetention, type SessionRetentionAssessment } from "./session-retention.ts";
import { readSessionHeader } from "./session-header.ts";
import type { CleanTask, WaitReason } from "./task-types.ts";

const headerPageSize = 128;
const cleanPageSize = 4;
const maxSessionDepth = 128;

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
    // A pending bounded file read owns this connection until its continuation notices
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
    store.backfillSources(128);

    try {
      // Header discovery must use the same inter-process source reservation as a
      // retention assessment. A failed read leaves the registry unknown and blocks
      // deletion until a later bounded page can establish its parent edge.
      const headerAccess = store.acquireSourceAccess();
      if (!headerAccess) return;
      try { this.#discoverHeaders(store); }
      finally { store.releaseSourceAccess(headerAccess); }

      const legacyCursor = store.sources.readCursor("legacy");
      const reconciled = store.reconcileLegacyCleanTasks(32, legacyCursor ?? undefined);
      store.sources.writeCursor("legacy", reconciled.nextCursor ?? null);

      const cursor = store.sources.readCursor("clean");
      const page = store.listCleanTasks({ limit: cleanPageSize,
        ...(cursor === null ? {} : { cursor }),
        ...(force ? {} : { now: Date.now() }),
      });
      for (const task of page.items) {
        if (this.#closed) return;
        await this.#processTask(store, task, force);
      }
      if (!this.#closed) store.sources.writeCursor("clean", page.nextCursor ?? null);
    } finally {
      // Reclamation is independent of clean-task availability and header reads.
      // The store rechecks source references inside each bounded transaction.
      if (!this.#closed) store.collectMetadata(128);
    }
  }

  #discoverHeaders(store: SQLiteMemoryStore): void {
    const afterRowId = store.sources.readCursor("headers");
    const page = store.sources.listPage({
      limit: headerPageSize,
      ...(afterRowId === null ? {} : { afterRowId }),
    });
    for (const source of page.items) {
      if (this.#closed || source.deleted) continue;
      // The Pi manager supplied this header and dependency before the first
      // save. Pending close/trigger work does not invalidate that fact; once a
      // file appears, its physical header is validated and clears the reserve.
      if (source.reserved && missing(source.sessionFile)) continue;
      const header = readSessionHeader(source.sessionFile);
      if (header.state === "known" && header.sessionId === source.sessionId) {
        store.sources.recordHeader({
          sessionFile: source.sessionFile,
          sessionId: source.sessionId,
          fileVersion: header.fileVersion,
          ...(header.parentSession === undefined ? {} : { parentSession: header.parentSession }),
        });
      } else {
        store.sources.markUnknown({
          sessionFile: source.sessionFile,
          sessionId: source.sessionId,
        });
      }
    }
    // Page writes are idempotent. Advance only after every row was examined so
    // a crash repeats the page instead of silently skipping an unvisited row.
    store.sources.writeCursor("headers", page.nextCursor);
  }

  async #processTask(store: SQLiteMemoryStore, task: CleanTask, force: boolean): Promise<void> {
    const owner = randomUUID();
    if (!store.claimCleanTask(task.id, owner, Date.now(), 60_000, force)) return;
    this.#claim = { id: task.id, owner };
    let reason: WaitReason = "unavailable";
    const access = store.acquireSourceAccess();
    if (!access) {
      store.releaseCleanTask(task.id, owner);
      this.#claim = undefined;
      return;
    }
    try {
      store.reconcileExitedSessionOwners([task.sessionFile]);
      const receipt = store.getDeletion(task.sessionFile);
      if (receipt?.state === "deleting" && receipt.sessionId === task.sessionId &&
        receipt.intentId === task.intentId && missing(task.sessionFile)) {
        const recoveryScope = [task.sessionFile];
        const recoveryVersion = store.retentionVersion(recoveryScope);
        if (!this.#closed && store.finishDeletion(task.id, owner, access, recoveryVersion, () => {
          if (!missing(task.sessionFile)) return false;
          syncParent(task.sessionFile);
          return true;
        }, recoveryScope)) {
          this.#claim = undefined;
          return;
        }
      }
      // Dependency discovery and topology completeness must share a stable
      // baseline. A fork can be reserved by another process after the
      // dependency query; the scoped token alone would then include that new
      // topology and bless a scope that never examined it.
      const topologyVersion = store.sources.topologyVersion();
      const scope = this.#sourceScope(store, task);
      if (!scope) {
        reason = store.sources.get(task.sessionFile)?.headerState === "known" ? "dependencies" : "unavailable";
      } else {
        const topologyComplete = store.sources.isTopologyComplete();
        const version = store.retentionVersion(scope.paths);
        // The post-token check catches registrations during #sourceScope and
        // closes the gap between the topology query and the scoped snapshot.
        if (!topologyComplete || store.sources.topologyVersion() !== topologyVersion) {
          reason = "dependencies";
        } else {
          const report = await assessSessionRetention(scope.paths, store);
          const assessment = report.sessions.find(source => source.sessionFile === task.sessionFile);
          reason = assessmentReason(assessment, report.dependenciesComplete);
          if (store.hasUncapturedObservation(scope.paths)) reason = "unprocessed";
          if (scope.hasDependents) reason = "dependencies";

          if (!this.#closed && reason === "host_coordination") {
            const fileVersion = report.fileVersions.get(task.sessionFile);
            if (fileVersion) {
              // The same pre-assessment token fences the read, receipt, and unlink.
              if (store.prepareDeletion(task.id, owner, access, version, fileVersion, scope.paths)) {
                const done = store.finishDeletion(task.id, owner, access, version, () => {
                  for (const [path, expected] of report.fileVersions) {
                    if (fileVersionAt(path) !== expected) return false;
                  }
                  unlinkSync(task.sessionFile);
                  syncParent(task.sessionFile);
                  return true;
                }, scope.paths);
                if (done) { this.#claim = undefined; return; }
              }
              reason = "unavailable";
            }
          }
        }
      }
    } catch { reason = "unavailable"; /* Keep failed or changed evidence for a later wake. */ }
    finally { store.releaseSourceAccess(access); }
    if (this.#closed) return;
    const now = Date.now();
    store.recordCleanAssessment(task.id, owner, now, reason, now + (this.#options.retryDelayMs ?? 30_000));
    this.#claim = undefined;
  }

  #sourceScope(store: SQLiteMemoryStore, task: CleanTask): { readonly paths: readonly string[]; readonly hasDependents: boolean } | undefined {
    const target = store.sources.get(task.sessionFile);
    if (!target || target.deleted || target.sessionId !== task.sessionId || target.headerState !== "known") return undefined;
    const paths: string[] = [];
    const seen = new Set<string>();
    paths.push(target.sessionFile);
    seen.add(target.sessionFile);
    let parentPath = target.parentPath;
    while (parentPath !== null) {
      if (seen.has(parentPath) || paths.length >= maxSessionDepth) return undefined;
      seen.add(parentPath);
      paths.push(parentPath);
      const parent = store.sources.get(parentPath);
      if (parent) {
        if (parent.deleted || parent.headerState !== "known") return undefined;
        parentPath = parent.parentPath;
      } else {
        // A fork may reference a valid Pi source that predates extension
        // management. Read its bounded header as ancestry evidence, but never
        // make it a cleanup target merely because this child refers to it.
        const header = readSessionHeader(parentPath);
        if (header.state !== "known") return undefined;
        parentPath = header.parentSession === undefined
          ? null
          : resolve(dirname(parentPath), header.parentSession);
      }
    }
    return { paths, hasDependents: store.sources.hasDependents(task.sessionFile) };
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
