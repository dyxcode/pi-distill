import { dirname, resolve } from "node:path";
import { randomUUID } from "node:crypto";
import { closeSync, fsyncSync, lstatSync, openSync, unlinkSync } from "node:fs";
import type { SQLiteMemoryStore } from "./memory-store.js";
import { SourceWorkPending } from "./source-view.ts";
import { DistillFailure } from "./distill-failure.ts";
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

interface MaintenanceRound {
  readonly headerEnd: number;
  readonly legacyEnd: number;
  cleanEnd: number;
  headersDone: boolean;
  legacyDone: boolean;
  cleanDone: boolean;
  gcDone: boolean;
  forceClean: boolean;
  cleanAgain: boolean;
  cleanContended: boolean;
  cleanBuilding: boolean;
}
interface PassOutcome { ready: boolean; contended: boolean; progressed: boolean }
type TaskOutcome = "complete" | "building" | "waiting" | "contended";
const continuationMs = 100;
const pollMs = 30_000;
const maxContentionMs = 5_000;

/** Independent local cleanup of fully processed, inactive managed sources. */
export class CleanupCoordinator {
  readonly #options: Options;
  #store: SQLiteMemoryStore | undefined;
  #working: Promise<void> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  #round: MaintenanceRound | undefined;
  #contentionMs = continuationMs;
  #wakeAgain = false;
  #claim: { id: string; owner: string } | undefined;

  constructor(options: Options) { this.#options = options; }

  /** Usage/source changes bypass a previous assessment's retry delay. */
  wake(): void {
    if (this.#closed) return;
    this.#contentionMs = continuationMs;
    if (this.#working) { this.#wakeAgain = true; return; }
    this.#round = undefined;
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
    const result: PassOutcome = { ready: false, contended: false, progressed: false };
    this.#working = new Promise<void>(resolveWork => {
      setImmediate(() => { void this.#pass(force, result).catch(error => {
        // A failed pass is not proof of completion. Transient writers back off;
        // other storage/validation failures use normal polling. Durable cursors
        // and all eligibility checks remain authoritative.
        result.ready = false; result.contended = isContention(error);
        if (!result.contended) this.#round = undefined;
      }).finally(resolveWork); });
    }).finally(() => {
      this.#working = undefined;
      if (this.#closed) { this.#store?.close(); this.#store = undefined; return; }
      const wakeAgain = this.#wakeAgain;
      this.#wakeAgain = false;
      if (wakeAgain) this.#round = undefined;
      if (result.progressed || wakeAgain) this.#contentionMs = continuationMs;
      let delay = pollMs;
      if (result.ready || wakeAgain) delay = continuationMs;
      else if (result.contended) {
        delay = this.#contentionMs;
        this.#contentionMs = Math.min(maxContentionMs, this.#contentionMs * 2);
      } else this.#round = undefined;
      const retryForce = force && result.contended && !this.#round;
      this.#timer = setTimeout(() => this.#run(wakeAgain || retryForce), this.#options.retryDelayMs ?? delay);
      this.#timer.unref();
    });
  }

  async #pass(force: boolean, result: PassOutcome): Promise<void> {
    if (this.#closed) return;
    if (!this.#store && this.#options.hasStore && !this.#options.hasStore()) return;
    const store = this.#store ??= this.#options.openStore();
    if (this.#claim) {
      store.releaseCleanTask(this.#claim.id, this.#claim.owner);
      this.#claim = undefined;
    }
    const backfilled = store.backfillSources(128);
    const round = this.#round ??= {
      headerEnd: store.sources.highWater(), legacyEnd: store.maintenanceBoundary("legacy"),
      cleanEnd: store.maintenanceBoundary("clean"), headersDone: false, legacyDone: false,
      cleanDone: false, gcDone: false, forceClean: force, cleanAgain: false, cleanContended: false, cleanBuilding: false,
    };
    if (!backfilled) result.ready = true;

    if (!round.headersDone) {
      const access = store.acquireSourceAccess();
      if (!access) result.contended = true;
      else {
        try {
          const before = store.sources.topologyVersion();
          round.headersDone = this.#discoverHeaders(store, round.headerEnd);
          result.progressed = true;
          if (!round.headersDone) result.ready = true;
          if (store.sources.topologyVersion() !== before) round.cleanAgain = true;
        } finally { store.releaseSourceAccess(access); }
      }
    }
    if (!round.legacyDone) {
      const cursor = store.sources.readCursor("legacy");
      const page = store.reconcileLegacyCleanTasks(32, cursor ?? undefined, round.legacyEnd);
      store.sources.writeCursor("legacy", page.nextCursor ?? null);
      round.legacyDone = page.nextCursor === undefined;
      result.progressed = true;
      if (!round.legacyDone) result.ready = true;
      if (page.created) { round.cleanEnd = store.maintenanceBoundary("clean"); round.cleanAgain = true; }
    }
    if (round.cleanDone && round.cleanAgain) round.cleanDone = false;
    if (!round.cleanDone) {
      const cursor = store.sources.readCursor("clean");
      const page = store.listCleanTasks({ limit: cleanPageSize, throughCursor: round.cleanEnd,
        ...(cursor === null ? {} : { cursor }), ...(round.forceClean ? {} : { now: Date.now() }),
      });
      for (const task of page.items) {
        if (this.#closed) return;
        const outcome = await this.#processTask(store, task, round.forceClean);
        if (outcome === "contended") { result.contended = true; round.cleanContended = true; }
        else if (outcome !== "waiting") result.progressed = true;
        if (outcome === "building") { result.ready = true; round.cleanBuilding = true; }
        if (outcome === "complete") { round.cleanAgain = true; round.gcDone = false; }
      }
      if (this.#closed) return;
      store.sources.writeCursor("clean", page.nextCursor ?? null);
      if (page.nextCursor !== undefined) result.ready = true;
      else {
        round.cleanDone = !round.cleanAgain && !round.cleanContended && !round.cleanBuilding;
        // A lifecycle wake invalidates an older assessment even if access was
        // busy. Keep that request until the contended sweep can actually run.
        round.forceClean = round.cleanAgain || (round.forceClean && round.cleanContended);
        if (round.cleanAgain || round.cleanBuilding) result.ready = true;
        if (round.cleanContended) result.contended = true;
        round.cleanAgain = false; round.cleanContended = false; round.cleanBuilding = false;
      }
    }
    if (!round.gcDone && !this.#closed) {
      const gc = store.collectMetadata(128);
      if (gc.contended) result.contended = true;
      else {
        round.gcDone = !gc.hasMore;
        result.progressed = true;
        if (gc.hasMore) result.ready = true;
      }
    }
    if (round.headersDone && round.legacyDone && round.cleanDone && round.gcDone) this.#round = undefined;
  }

  #discoverHeaders(store: SQLiteMemoryStore, throughRowId: number): boolean {
    const afterRowId = store.sources.readCursor("headers");
    const page = store.sources.listPage({
      limit: headerPageSize, throughRowId,
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
    return page.nextCursor === null;
  }

  async #processTask(store: SQLiteMemoryStore, task: CleanTask, force: boolean): Promise<TaskOutcome> {
    const owner = randomUUID();
    if (!store.claimCleanTask(task.id, owner, Date.now(), 60_000, force)) return "contended";
    this.#claim = { id: task.id, owner };
    let reason: WaitReason = "unavailable";
    let building = false;
    const access = store.acquireSourceAccess();
    if (!access) {
      store.releaseCleanTask(task.id, owner);
      this.#claim = undefined;
      return "contended";
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
          return "complete";
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
          const report = await store.sourceCoverage.advance(task.sessionFile, scope.paths, version);
          const current = store.sourceRetentionState(task.sessionFile, task.sessionId);
          if (report.sessionId !== task.sessionId) reason = "unavailable";
          else if (report.status !== "processed" || current.pending) reason = "unprocessed";
          else reason = current.released ? "host_coordination" : "usage";
          if (report.status === "building") building = true;
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
                if (done) { this.#claim = undefined; return "complete"; }
              }
              reason = "unavailable";
            }
          }
        }
      }
    } catch (error) {
      if (isContention(error)) {
        store.releaseCleanTask(task.id, owner);
        this.#claim = undefined;
        return "contended";
      }
      if (error instanceof SourceWorkPending) { reason = "unprocessed"; building = true; }
      else reason = "unavailable";
    }
    finally { store.releaseSourceAccess(access); }
    if (this.#closed) return "waiting";
    const now = Date.now();
    store.recordCleanAssessment(task.id, owner, now, reason, now + (this.#options.retryDelayMs ?? (building ? continuationMs : pollMs)));
    this.#claim = undefined;
    return building ? "building" : "waiting";
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

function isContention(error: unknown): boolean {
  if (error instanceof DistillFailure) return error.reason === "conflict";
  // SQLite extended result codes retain the primary BUSY/LOCKED code in
  // their low byte. Other storage and validation failures use normal polling.
  return error instanceof Error && "code" in error && error.code === "ERR_SQLITE_ERROR" &&
    "errcode" in error && typeof error.errcode === "number" && Number.isSafeInteger(error.errcode) &&
    (error.errcode % 256 === 5 || error.errcode % 256 === 6);
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
