import { readIndexedEntry } from "./source-index.ts";
import { SourceWorkPending, type PagedSourceView } from "./source-view.ts";
import { withSourceAccess } from "./source-access.ts";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MemoryConflictError, ConsolidationConflictError, type SQLiteMemoryStore } from "./memory-store.ts";
import { DistillFailure } from "./distill-failure.ts";
import { distillRetryContext } from "./distill-retry-context.ts";
import type { DistillRetryContext } from "./distill-retry.js";
import type { ConsolidationJob, SessionObservation, SourceBatch, SourcePart } from "./consolidation-types.js";
import type { DistillTrigger, DistillTriggerInput } from "./task-types.js";
import {
  findCompactionRecord,
  isValidSourceSnapshot,
  loadSnapshot,
  planSources,
  type SourceSnapshot,
} from "./consolidation-source.ts";
import { consolidateWithModel, fitsConsolidationRequest } from "./consolidation-model.ts";

type Context = Pick<ExtensionContext, "model" | "modelRegistry" | "sessionManager"> &
  Partial<Pick<ExtensionContext, "isIdle" | "hasPendingMessages">>;
type ShutdownReason = "quit" | "reload" | "new" | "resume" | "fork";
type RunModel = typeof consolidateWithModel;

interface Options {
  readonly openStore: () => SQLiteMemoryStore;
  readonly hasStore?: () => boolean;
  readonly loadSnapshot?: typeof loadSnapshot;
  readonly loadCompactionRecord?: typeof findCompactionRecord;
  readonly runModel?: RunModel;
  readonly retryDelayMs?: number;
  readonly claimLeaseMs?: number;
  readonly onProgress?: () => void;
  readonly now?: () => number;
}

interface SourceReference {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly leafId: string | null;
  readonly cwd: string;
}

interface PendingCompact {
  readonly id: string;
  readonly source: SourceReference;
}

interface ActiveClaim {
  readonly token: string;
  readonly store: SQLiteMemoryStore;
}

class PendingSourceOverlapError extends Error {}

const MAX_RECOVERY_ITEMS_PER_PASS = 4;
const MAX_OBSERVATIONS_PER_PASS = 4;
const MAX_PENDING_SCAN_PAGES = 4;
const PENDING_SCAN_PAGE_SIZE = 32;
const MAX_MODEL_BATCHES_PER_PASS = 2;
const SOURCE_EVIDENCE_BYTES = 24 * 1024;
const CONTEXT_BYTES = 8 * 1024;
const DEFAULT_CLAIM_LEASE_MS = 60_000;
const INITIAL_IDLE_RETRY_MS = 100;
const MAX_IDLE_RETRY_MS = 5_000;

/** Coordinates durable source references and model work for one live Pi runtime. */
export class ConsolidationCoordinator {
  readonly #options: Options;
  readonly #workerId = randomUUID();
  readonly #idleWaiters = new Set<() => void>();
  #store: SQLiteMemoryStore | undefined;
  #context: Context | undefined;
  #before: PendingCompact | undefined;
  #working: Promise<void> | undefined;
  #controller: AbortController | undefined;
  #activeClaim: ActiveClaim | undefined;
  #heartbeat: ReturnType<typeof setInterval> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #deferredKick: Promise<void> | undefined;
  #resolveDeferredKick: (() => void) | undefined;
  #idleRetryMs = INITIAL_IDLE_RETRY_MS;
  #closed = false;
  #sourceWorkPending = false;
  #foregroundBusy = false;
  #compactionBusy = false;
  #recoveryNeeded = false;
  #lastObservation: string | undefined;
  #triggerCursor = 0;
  #triggerSweepEnd: number | undefined;
  #observationCursor = 0;
  #observationSweepEnd: number | undefined;
  #unobservedCursor = 0;
  #unobservedSweepEnd: number | undefined;
  #pendingTriggerCursor = 0;
  #jobCursor = 0;
  #jobSweepEnd: number | undefined;
  #preparationCursor = 0;
  #preparationSweepEnd: number | undefined;

  constructor(options: Options) { this.#options = options; }

  async start(ctx: Context): Promise<void> {
    if (this.#closed) return;
    this.#context = ctx;
    try {
      if (this.#options.hasStore && !this.#options.hasStore() && !this.#store) {
        return;
      }
      this.#recoveryNeeded = true;
      this.#kick();
    } catch {
      this.#recoveryNeeded = true;
      this.#retry();
    }
  }

  /** Save bounded identity/checkpoint metadata only; never inspect session entries. */
  observe(ctx: Context): void {
    if (this.#closed) return;
    this.#context = ctx;
    try {
      if (this.#options.hasStore && !this.#options.hasStore() && !this.#store) return;
      const source = this.#sourceReference(ctx);
      if (!source) return;
      this.#database().observeSession({ ...source, ownerPid: process.pid });
    } catch {
      // Observation is best effort. A later host event can record a newer boundary.
    }
    this.#kick();
  }

  /** Persist a candidate source boundary. Confirmation waits for the saved entry ID. */
  beforeCompact(ctx: Context): void {
    if (this.#closed) return;
    this.#context = ctx;
    this.#compactionBusy = true;
    this.#notifyIdleWaiters();
    try {
      const source = this.#sourceReference(ctx);
      if (!source) { this.#before = undefined; return; }
      const id = randomUUID();
      this.#before = { id, source };
      this.#database().enqueueDistillTrigger(this.#triggerInput(source, "compact", id));
    } catch {
      // afterCompact retries the bounded candidate write before confirming it.
    }
  }

  /**
   * Confirm only when Pi reports the persisted compaction record. The source leaf
   * remains the pre-compaction boundary, so the summary is never extraction input.
   */
  afterCompact(ctx: Context, compactionRecordId: string): void {
    if (this.#closed) return;
    this.#context = ctx;
    const candidate = this.#before;
    this.#before = undefined;
    this.#compactionBusy = false;
    if (candidate && typeof compactionRecordId === "string" && compactionRecordId.length > 0) {
      try {
        const store = this.#database();
        store.enqueueDistillTrigger(this.#triggerInput(candidate.source, "compact", candidate.id));
        if (store.confirmCompactionTrigger({ id: candidate.id, compactionRecordId })) this.#notifyProgress();
      } catch {
        this.#recoveryNeeded = true;
      }
    }
    this.#notifyIdleWaiters();
    this.#deferKick();
  }

  /** A failed compaction removes only its unconfirmed candidate. */
  compactFailed(): void {
    const candidate = this.#before;
    this.#before = undefined;
    this.#compactionBusy = false;
    if (candidate) {
      try { this.#store?.cancelCompactionTrigger(candidate.id); }
      catch { this.#recoveryNeeded = true; }
    }
    this.#notifyIdleWaiters();
    this.#kick();
  }

  /** Pi adapter signal: do not start or advance model requests while foreground work runs. */
  setForegroundBusy(busy: boolean): void {
    this.#foregroundBusy = busy;
    if (!busy) this.#idleRetryMs = INITIAL_IDLE_RETRY_MS;
    this.#notifyIdleWaiters();
    if (!busy && !this.#deferredKick) this.#kick();
  }

  async whenIdle(): Promise<void> {
    const deferred = this.#deferredKick;
    if (deferred) await deferred;
    await this.#working;
  }

  /**
   * Close captures one frozen source reference, then fences and aborts without
   * waiting on the provider. Reload deliberately creates no semantic trigger.
   */
  async shutdown(ctx: Context, reason: ShutdownReason): Promise<void> {
    if (this.#closed) return;
    this.#context = ctx;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#resolveDeferredKick?.();
    this.#resolveDeferredKick = undefined;
    this.#deferredKick = undefined;

    if (reason !== "reload") {
      try {
        const source = this.#sourceReference(ctx);
        if (source) {
          const store = this.#database();
          const id = closeTriggerId(source);
          store.enqueueDistillTrigger(this.#triggerInput(source, "close", id));
          store.acknowledgeObservation(source.sessionId, source.leafId);
        }
      } catch {
        // Persisted observations can recover a missed close after this process exits.
      }
    }

    this.#closed = true;
    this.#controller?.abort(new Error("Pi runtime is shutting down."));
    for (const waiter of [...this.#idleWaiters]) waiter();
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#heartbeat = undefined;
    if (this.#activeClaim) {
      try {
        this.#activeClaim.store.releaseDistillClaim({
          claimToken: this.#activeClaim.token,
          now: this.#now(),
        });
      } catch { /* The database lease remains the crash-recovery fence. */ }
      this.#activeClaim = undefined;
    }
    this.#store?.close();
    this.#store = undefined;
  }

  #now(): number { return this.#options.now?.() ?? Date.now(); }

  #retryContext(path: string, context = this.#context): Promise<DistillRetryContext> {
    return withSourceAccess(this.#options.openStore, async () => distillRetryContext(path, context?.model), this.#controller?.signal);
  }

  #database(): SQLiteMemoryStore { return this.#store ??= this.#options.openStore(); }

  #sourceReference(ctx: Context): SourceReference | undefined {
    const manager = ctx.sessionManager;
    const sessionFile = manager.getSessionFile();
    const sessionId = manager.getSessionId();
    const cwd = manager.getCwd();
    const leafId = manager.getLeafId();
    if (typeof sessionFile !== "string" || sessionFile.length === 0 || !existsSync(sessionFile)) return undefined;
    return { sessionId, sessionFile, cwd, leafId };
  }

  #triggerInput(source: SourceReference, reason: "close" | "compact", id: string): DistillTriggerInput {
    return { ...source, id, reason };
  }

  async #load(path: string, leaf?: string | null): Promise<SourceSnapshot> {
    if (this.#options.loadSnapshot) return withSourceAccess(this.#options.openStore,
      () => this.#options.loadSnapshot!(path, leaf), this.#controller?.signal);
    // A small source can finish its four phases in one pass; larger sources
    // persist their cursor and yield to the host between bounded passes.
    for (let page = 0; page < 4; page++) {
      if (this.#controller) await this.#waitUntilIdle(this.#controller.signal);
      const result = await withSourceAccess(this.#options.openStore,
        () => this.#database().sourceViews.advance(path, leaf, this.#controller?.signal), this.#controller?.signal);
      if (result.ready) {
        const view = result.view, header = view.source.header;
        if (!header) throw new DistillFailure("source_invalid", "Source header is unavailable.");
        return { sessionId: header.id, sessionFile: resolve(path), cwd: header.cwd,
          leafId: view.state.leafId, entries: [], indexed: view };
      }
    }
    throw new SourceWorkPending();
  }

  async #compaction(trigger: DistillTrigger, signal: AbortSignal): Promise<string | undefined> {
    if (this.#options.loadCompactionRecord) return withSourceAccess(this.#options.openStore,
      () => this.#options.loadCompactionRecord!(trigger.sessionFile, trigger.sessionId, trigger.cwd, trigger.leafId), signal);
    const snapshot = await this.#load(trigger.sessionFile, trigger.leafId);
    assertSnapshotMatches(snapshot, trigger);
    const view = snapshot.indexed;
    if (!view) return withSourceAccess(this.#options.openStore,
      () => findCompactionRecord(trigger.sessionFile, trigger.sessionId, trigger.cwd, trigger.leafId), signal);
    return withSourceAccess(this.#options.openStore, async () => {
      const source = view.source;
      const row = view.owner.database.prepare("SELECT metadata FROM source_index_entries WHERE generation=? AND json_extract(metadata,'$.type')='compaction' AND json_extract(metadata,'$.parentId') IS ? ORDER BY seq LIMIT 1")
        .get(source.generation, trigger.leafId);
      if (!row || typeof row["metadata"] !== "string") return undefined;
      const value: unknown = JSON.parse(row["metadata"]);
      if (typeof value !== "object" || value === null || !("id" in value) || typeof value.id !== "string") throw new DistillFailure("source_invalid", "Invalid compaction metadata.");
      const metadata = view.owner.index.entry(source.generation, value.id);
      if (!metadata) return undefined;
      const entry = await readIndexedEntry(source.path, source, metadata);
      await view.unchanged();
      return entry.type === "compaction" && entry.parentId === trigger.leafId ? entry.id : undefined;
    }, signal);
  }

  #kick(): void {
    const context = this.#context;
    if (this.#closed || this.#working || !context?.model) return;
    if (this.#options.hasStore && !this.#options.hasStore() && !this.#store) return;
    if (!this.#canRun(context)) {
      if (this.#recoveryNeeded || this.#hasQueuedWork()) this.#retryWhenIdle();
      return;
    }
    this.#idleRetryMs = INITIAL_IDLE_RETRY_MS;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    const resolveDeferred = this.#resolveDeferredKick;
    this.#resolveDeferredKick = undefined;
    this.#deferredKick = undefined;
    const controller = new AbortController();
    this.#controller = controller;
    const signal = controller.signal;
    this.#working = this.#runPass(context, signal)
      .catch(() => { this.#recoveryNeeded = true; })
      .finally(() => {
        this.#working = undefined;
        this.#controller = undefined;
        if (!this.#closed) this.#retryIfNeeded();
      });
    resolveDeferred?.();
  }

  #canRun(context: Context | undefined): boolean {
    if (this.#closed || this.#foregroundBusy || this.#compactionBusy || !context) return false;
    try {
      if (context.isIdle && !context.isIdle()) return false;
      if (context.hasPendingMessages && context.hasPendingMessages()) return false;
    } catch { return false; }
    return true;
  }

  #notifyIdleWaiters(): void {
    for (const waiter of [...this.#idleWaiters]) waiter();
  }

  #waitUntilIdle(signal: AbortSignal): Promise<void> {
    const context = this.#context;
    if (signal.aborted) return Promise.reject(signal.reason ?? new Error("Consolidation was cancelled."));
    if (this.#canRun(context)) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const cleanup = (): void => {
        this.#idleWaiters.delete(wake);
        signal.removeEventListener("abort", abort);
      };
      const abort = (): void => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(signal.reason ?? new Error("Consolidation was cancelled."));
      };
      const wake = (): void => {
        if (settled) return;
        if (signal.aborted) { abort(); return; }
        if (!this.#canRun(this.#context)) return;
        settled = true;
        cleanup();
        resolve();
      };
      this.#idleWaiters.add(wake);
      signal.addEventListener("abort", abort, { once: true });
      wake();
    });
  }

  #startClaim(store: SQLiteMemoryStore, token: string): void {
    this.#activeClaim = { store, token };
    const interval = Math.max(1_000, Math.floor((this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS) / 3));
    this.#heartbeat = setInterval(() => {
      try {
        const renewed = store.renewDistillClaim({
          claimToken: token,
          now: this.#now(),
          leaseMs: this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
        });
        if (!renewed) this.#controller?.abort(new Error("The durable consolidation claim was fenced."));
      } catch {
        this.#controller?.abort(new Error("The durable consolidation claim could not be renewed."));
      }
    }, interval);
    this.#heartbeat.unref();
  }

  #finishClaim(): void {
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#heartbeat = undefined;
    this.#activeClaim = undefined;
  }

  #retryIfNeeded(): void {
    if (this.#closed) return;
    const pending = this.#recoveryNeeded || this.#hasQueuedWork();
    if (!pending) return;
    if (!this.#canRun(this.#context)) {
      this.#retryWhenIdle();
      return;
    }
    this.#retry();
  }

  #hasQueuedWork(): boolean {
    try {
      if (this.#options.hasStore && !this.#options.hasStore() && !this.#store) return false;
      const store = this.#database();
      return store.listPendingConsolidationJobs({ limit: 1 }).items.length > 0 || this.#hasPendingTriggers(store);
    } catch { return true; }
  }

  #hasPendingTriggers(store: SQLiteMemoryStore): boolean {
    let cursor: number | undefined = this.#pendingTriggerCursor || undefined;
    for (let pageCount = 0; pageCount < MAX_PENDING_SCAN_PAGES; pageCount += 1) {
      const page = store.listDistillTriggers({ limit: PENDING_SCAN_PAGE_SIZE, ...(cursor === undefined ? {} : { cursor }) });
      if (page.items.some(item => item.status === "pending" || item.status === "candidate")) {
        this.#pendingTriggerCursor = 0;
        return true;
      }
      if (page.nextCursor === undefined) {
        this.#pendingTriggerCursor = 0;
        return false;
      }
      cursor = page.nextCursor;
    }
    // A bounded scan must not report an empty queue when later rows remain unseen.
    this.#pendingTriggerCursor = cursor ?? 0;
    return this.#pendingTriggerCursor !== 0;
  }

  #retry(): void {
    if (this.#closed || this.#timer || !this.#context?.model) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#kick();
    }, this.#options.retryDelayMs ?? (this.#sourceWorkPending ? 100 : 30_000));
    this.#timer.unref();
  }

  #retryWhenIdle(): void {
    if (this.#closed || this.#timer || !this.#context?.model) return;
    const delay = this.#idleRetryMs;
    this.#idleRetryMs = Math.min(MAX_IDLE_RETRY_MS, this.#idleRetryMs * 2);
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#kick();
    }, delay);
    this.#timer.unref();
  }

  #deferKick(): void {
    if (this.#closed || this.#deferredKick) return;
    if (this.#timer) clearTimeout(this.#timer);
    let resolveKick: (() => void) | undefined;
    const deferred = new Promise<void>(resolve => { resolveKick = resolve; });
    this.#deferredKick = deferred;
    this.#resolveDeferredKick = resolveKick;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      if (this.#deferredKick === deferred) this.#deferredKick = undefined;
      if (this.#resolveDeferredKick === resolveKick) this.#resolveDeferredKick = undefined;
      this.#kick();
      resolveKick?.();
    }, 0);
  }

  async #runPass(context: Context, signal: AbortSignal): Promise<void> {
    this.#sourceWorkPending = false;
    if (this.#recoveryNeeded) {
      await this.#recover(signal);
      if (!this.#recoveryNeeded && !signal.aborted) this.observe(context);
    }
    if (signal.aborted || !this.#canRun(context)) return;

    // Preparation is bounded separately from the two model batches below. The
    // global SQLite claim prevents another Pi process from preparing or distilling
    // at the same time.
    await this.#prepareOneTrigger(signal);
    if (signal.aborted || !this.#canRun(context)) return;

    const store = this.#database();
    const page = store.listPendingConsolidationJobs({ limit: PENDING_SCAN_PAGE_SIZE,
      ...(this.#jobSweepEnd === undefined ? {} : { throughCursor: this.#jobSweepEnd }),
      ...(this.#jobCursor === 0 ? {} : { cursor: this.#jobCursor }) });
    this.#jobCursor = page.nextCursor ?? 0;
    this.#jobSweepEnd = page.nextCursor === undefined ? undefined : page.throughCursor;
    let batches = 0;
    for (const job of page.items) {
      if (signal.aborted || batches >= MAX_MODEL_BATCHES_PER_PASS || !this.#canRun(context)) break;
      const retryContext = await this.#retryContext(job.sessionFile ?? "", context);
      const claim = store.claimDistillJob({
        retryContext,
        id: job.id,
        workerId: this.#workerId,
        now: this.#now(),
        leaseMs: this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
      });
      if (!claim) continue;
      batches++;
      this.#startClaim(store, claim.claimToken);
      await this.#processJob(claim.job, claim.claimToken, context, signal, store, retryContext);
      this.#finishClaim();
    }
  }

  async #recover(signal: AbortSignal): Promise<void> {
    const store = this.#database();
    let unresolved = false;
    const triggerPage = store.listDistillTriggers({
      limit: MAX_RECOVERY_ITEMS_PER_PASS,
      ...(this.#triggerSweepEnd === undefined ? {} : { throughCursor: this.#triggerSweepEnd }),
      ...(this.#triggerCursor === 0 ? {} : { cursor: this.#triggerCursor }),
    });
    for (const trigger of triggerPage.items) {
      if (signal.aborted) return;
      if (trigger.status !== "candidate") continue;
      const retryContext = await this.#retryContext(trigger.sessionFile);
      const claim = store.claimDistillTrigger({ id: trigger.id, allowCandidate: true, retryContext,
        workerId: this.#workerId, now: this.#now(), leaseMs: this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS });
      if (!claim) { unresolved = true; continue; }
      this.#startClaim(store, claim.claimToken);
      try {
        const compactionRecordId = await this.#compaction(trigger, signal);
        signal.throwIfAborted();
        if (!compactionRecordId) throw new DistillFailure("source_invalid", "A persisted compaction record is not available yet.");
        store.confirmCompactionTrigger({ id: trigger.id, compactionRecordId });
        store.releaseDistillClaim({ claimToken: claim.claimToken, now: this.#now() });
      } catch (error) {
        unresolved = true;
        this.#releaseClaim(store, claim.claimToken, error, retryContext, signal);
      } finally { this.#finishClaim(); }

    }
    this.#triggerCursor = triggerPage.nextCursor ?? 0;
    this.#triggerSweepEnd = triggerPage.nextCursor === undefined ? undefined : triggerPage.throughCursor;
    if (this.#triggerCursor !== 0) unresolved = true;

    const unobserved = store.listUnobservedSessionUsages({
      limit: MAX_OBSERVATIONS_PER_PASS,
      ...(this.#unobservedSweepEnd === undefined ? {} : { throughCursor: this.#unobservedSweepEnd }),
      ...(this.#unobservedCursor === 0 ? {} : { cursor: this.#unobservedCursor }),
    });
    for (const owner of unobserved.items) {
      if (signal.aborted) return;
      if (alive(owner.ownerPid) || !existsSync(owner.sessionFile)) continue;
      try {
        // Preserve the known boundary first. The observation recovery below owns
        // the fenced, retry-controlled read of any tail persisted before a crash.
        store.observeSession({ sessionId: owner.sessionId, sessionFile: owner.sessionFile,
          cwd: owner.cwd, leafId: owner.leafId, ownerPid: owner.ownerPid });
      } catch { unresolved = true; }

    }
    this.#unobservedCursor = unobserved.nextCursor ?? 0;
    this.#unobservedSweepEnd = unobserved.nextCursor === undefined ? undefined : unobserved.throughCursor;
    if (this.#unobservedCursor !== 0) unresolved = true;

    const observationPage = store.listObservedSessions({
      limit: MAX_OBSERVATIONS_PER_PASS,
      ...(this.#observationSweepEnd === undefined ? {} : { throughCursor: this.#observationSweepEnd }),
      ...(this.#observationCursor === 0 ? {} : { cursor: this.#observationCursor }),
    });
    const observations = rotateObservations(observationPage.items, this.#lastObservation);
    for (const observation of observations) {
      if (signal.aborted) return;
      this.#lastObservation = observation.sessionId;
      if (alive(observation.ownerPid) || !observation.sessionFile) continue;
      const source: SourceReference = {
        sessionId: observation.sessionId, sessionFile: observation.sessionFile,
        cwd: observation.cwd, leafId: observation.leafId,
      };
      let id = closeTriggerId(source);
      store.enqueueDistillTrigger(this.#triggerInput(source, "close", id));
      // A prior compact/close may already have prepared this exact boundary.
      // Recovery still must inspect the possible crash tail before acknowledging it.
      const original = store.readDistillTrigger(id);
      if (original?.status === "prepared" || original?.status === "complete") {
        id += "-recovery";
        store.enqueueDistillTrigger(this.#triggerInput(source, "close", id));
      }
      const retryContext = await this.#retryContext(source.sessionFile);
      const claim = store.claimDistillTrigger({ id, retryContext, workerId: this.#workerId,
        now: this.#now(), leaseMs: this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS });
      if (!claim) { unresolved = true; continue; }
      this.#startClaim(store, claim.claimToken);
      try {
        const latest = await this.#load(source.sessionFile);
        signal.throwIfAborted();
        if (!validSnapshot(latest) || latest.sessionId !== source.sessionId || latest.cwd !== source.cwd) {
          throw new DistillFailure("source_invalid", "Crash recovery source identity or coverage is unavailable.");
        }
        if (latest.leafId !== source.leafId) {
          const tail = { ...source, leafId: latest.leafId };
          store.enqueueDistillTrigger(this.#triggerInput(tail, "close", closeTriggerId(tail)));
        }
        store.acknowledgeObservation(observation.sessionId, observation.leafId);
        if (source.leafId === null) {
          // The recorded pre-message boundary contains no evidence. Its recovered
          // tail is durable above before this empty boundary can enable cleanup.
          store.prepareDistillTrigger({ id, claimToken: claim.claimToken, now: this.#now(), jobs: [] });
        } else {
          store.releaseDistillClaim({ claimToken: claim.claimToken, now: this.#now() });
        }
      } catch (error) {
        unresolved = true;
        this.#releaseClaim(store, claim.claimToken, error, retryContext, signal);
      } finally { this.#finishClaim(); }

    }

    this.#observationCursor = observationPage.nextCursor ?? 0;
    this.#observationSweepEnd = observationPage.nextCursor === undefined ? undefined : observationPage.throughCursor;
    if (this.#observationCursor !== 0) unresolved = true;
    this.#recoveryNeeded = unresolved || this.#triggerCursor !== 0 || this.#observationCursor !== 0;
  }

  async #prepareOneTrigger(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted || !this.#canRun(this.#context)) return false;
    const store = this.#database();
    const page = store.listDistillTriggers({ limit: PENDING_SCAN_PAGE_SIZE,
      ...(this.#preparationSweepEnd === undefined ? {} : { throughCursor: this.#preparationSweepEnd }),
      ...(this.#preparationCursor === 0 ? {} : { cursor: this.#preparationCursor }) });
    this.#preparationCursor = page.nextCursor ?? 0;
    this.#preparationSweepEnd = page.nextCursor === undefined ? undefined : page.throughCursor;
    for (const candidate of page.items) {
      if (signal.aborted || !this.#canRun(this.#context)) return false;
      if (candidate.status !== "pending") continue;
      const retryContext = await this.#retryContext(candidate.sessionFile);
      const claim = store.claimDistillTrigger({
        id: candidate.id, retryContext,
        workerId: this.#workerId,
        now: this.#now(),
        leaseMs: this.#options.claimLeaseMs ?? DEFAULT_CLAIM_LEASE_MS,
      });
      if (!claim) continue;
      this.#startClaim(store, claim.claimToken);
      try {
        const trigger = claim.trigger;
        const snapshot = await this.#load(trigger.sessionFile, trigger.leafId);
        assertSnapshotMatches(snapshot, trigger);
        if (!validSnapshot(snapshot)) throw new DistillFailure("source_invalid", "The frozen source boundary failed validation or exceeds its bounded preparation budget.");
        if (snapshot.indexed) {
          await this.#prepareIndexed(snapshot.indexed, trigger, claim.claimToken, store, signal);
          return true;
        }
        const jobs = this.#prepareJobs(snapshot, trigger, store);
        store.prepareDistillTrigger({
          id: trigger.id,
          claimToken: claim.claimToken,
          now: this.#now(),
          jobs,
        });
        if (jobs.length === 0) this.#notifyProgress();
        return true;
      } catch (error) {
        this.#releaseClaim(store, claim.claimToken, error, retryContext, signal);
        this.#recoveryNeeded = true;
        return true;
      } finally {
        this.#finishClaim();
      }
    }
    return false;
  }

  async #prepareIndexed(view: PagedSourceView, trigger: DistillTrigger, token: string, store: SQLiteMemoryStore, signal: AbortSignal): Promise<void> {
    if (store.pendingTriggerJob(trigger.id)) {
      store.releaseDistillClaim({ claimToken: token, now: this.#now() });
      this.#sourceWorkPending = true;
      return;
    }
    const after = store.readSourcePreparation(trigger.id, view.id);
    const page = await view.page(after);
    const processed = new Set(store.processedSources(page.items.map(item => item.part.key)));
    const evidence: SourcePart[] = [];
    let cursor = after;
    let reused: ConsolidationJob | undefined;
    for (const item of page.items) {
      signal.throwIfAborted();
      if (processed.has(item.part.key)) { cursor = item.seq; continue; }
      const pending = store.pendingJobForSource(item.part.key);
      if (pending) {
        if (pending.sessionId !== trigger.sessionId || pending.sessionFile !== trigger.sessionFile || pending.cwd !== trigger.cwd || pending.leafId !== trigger.leafId) {
          throw new PendingSourceOverlapError("A queued batch overlaps a different frozen source boundary.");
        }
        if (evidence.length === 0) reused = pending;
        break;
      }
      if (evidence.length > 0 && Buffer.byteLength(JSON.stringify([...evidence, item.part])) > SOURCE_EVIDENCE_BYTES) break;
      evidence.push(item.part); cursor = item.seq;
    }
    const keys = evidence.map(part => part.key);
    const jobs: ConsolidationJob[] = reused ? [reused] : evidence.length === 0 ? [] : [{
      id: createHash("sha256").update(JSON.stringify([trigger.sessionId, trigger.leafId, keys])).digest("hex"),
      sessionId: trigger.sessionId, sessionFile: trigger.sessionFile, cwd: trigger.cwd, leafId: trigger.leafId,
      sourceKeys: keys, lineageSessionIds: view.lineageSessionIds, ...view.boundary(evidence),
    }];
    await view.unchanged(); signal.throwIfAborted();
    const complete = !reused && page.nextCursor === undefined && cursor === (page.items.at(-1)?.seq ?? after);
    store.prepareDistillPage({ id: trigger.id, claimToken: token, now: this.#now(), viewId: view.id, after, cursor, complete, jobs });
    if (!complete) this.#sourceWorkPending = true;
    if (complete && jobs.length === 0) this.#notifyProgress();
  }

  #prepareJobs(snapshot: SourceSnapshot, trigger: DistillTrigger, store: SQLiteMemoryStore): ConsolidationJob[] {
    if (snapshot.pending || snapshot.ancestry?.unresolved) {
      throw new DistillFailure("source_invalid", snapshot.pendingReason ?? "The source branch is not fully available.");
    }
    const all = planSources(snapshot, new Set());
    const allKeys = all.flatMap(batch => [...batch.sourceKeys]);
    const processed = new Set(store.processedSources(allKeys));
    const sourceKeySet = new Set(allKeys);
    const queued: ConsolidationJob[] = [];
    for (const job of store.listConsolidationJobs()) {
      if (!job.sourceKeys.some(key => sourceKeySet.has(key))) continue;
      if (job.sessionId !== trigger.sessionId || job.sessionFile !== trigger.sessionFile || job.cwd !== trigger.cwd ||
        job.leafId !== trigger.leafId || !job.sourceKeys.every(key => sourceKeySet.has(key))) {
        throw new PendingSourceOverlapError("A queued batch overlaps this frozen range under a different source boundary.");
      }
      queued.push(job);
    }
    for (const job of queued) for (const key of job.sourceKeys) processed.add(key);

    const jobs: ConsolidationJob[] = [...queued];
    for (const planned of planSources(snapshot, processed)) {
      let evidence: SourcePart[] = [];
      const append = (): void => {
        if (evidence.length === 0) return;
        const sourceKeys = evidence.map(part => part.key);
        const id = createHash("sha256").update(JSON.stringify([planned.id, sourceKeys])).digest("hex");
        const batch = { ...planned, id, sourceKeys, evidence };
        jobs.push({
          id: batch.id,
          sessionId: trigger.sessionId,
          sessionFile: trigger.sessionFile,
          leafId: trigger.leafId,
          cwd: trigger.cwd,
          sourceKeys,
          lineageSessionIds: triggerLineageSessionIds(snapshot),
          ...sourceBoundary(snapshot, evidence),
        });
        evidence = [];
      };
      for (const part of planned.evidence) {
        if (evidence.length > 0 && Buffer.byteLength(JSON.stringify([...evidence, part])) > SOURCE_EVIDENCE_BYTES) append();
        evidence.push(part);
      }
      append();
    }
    return jobs;
  }

  async #processJob(
    job: ConsolidationJob,
    claimToken: string,
    context: Context,
    signal: AbortSignal,
    store: SQLiteMemoryStore,
    retryContext: DistillRetryContext,
  ): Promise<void> {
    try {
      const processed = new Set(store.processedSources(job.sourceKeys));
      const sourceKeys = job.sourceKeys.filter(key => !processed.has(key));
      if (sourceKeys.length === 0) {
        store.commitConsolidation({ jobId: job.id, owner: claimToken, now: this.#now(), sourceKeys, creates: [], updates: [], deletes: [] });
        this.#notifyProgress();
        return;
      }
      const sourceFile = job.sessionFile;
      if (typeof sourceFile !== "string" || sourceFile.length === 0) throw new DistillFailure("source_invalid", "A durable source file is unavailable.");
      const snapshot = await this.#load(sourceFile, job.leafId);
      assertSnapshotMatches(snapshot, job);
      if (!validSnapshot(snapshot)) throw new DistillFailure("source_invalid", "The frozen source boundary failed validation or exceeds its bounded preparation budget.");
      const view = snapshot.indexed;
      const currentParts = view ? [] : planSources(snapshot, new Set()).flatMap(batch => batch.evidence);
      const parts = new Map(currentParts.map(part => [part.key, part]));
      const evidence = view ? await view.readKeys(sourceKeys) : sourceKeys.map(key => {
        const part = parts.get(key);
        if (!part) throw new DistillFailure("source_invalid", "A frozen source reference changed after preparation.");
        return part;
      });
      const firstPosition = view?.position(sourceKeys[0] ?? "");
      const preceding = view && firstPosition !== undefined
        ? (await view.page(Math.max(0, firstPosition - 129), Math.min(128, Math.max(1, firstPosition - 1)))).items.filter(item => item.seq < firstPosition).map(item => item.part)
        : currentParts.slice(0, Math.max(0, currentParts.findIndex(part => part.key === sourceKeys[0])));
      const lineage = view ? { lineageSessionIds: view.lineageSessionIds } : {
        lineageKeys: [...parts.keys()],
        lineageAnchors: snapshot.entries.flatMap(entry =>
          typeof entry === "object" && entry !== null && "id" in entry && typeof entry.id === "string"
            ? [anchor(snapshot, entry.id)] : []),
        lineageSessionIds: triggerLineageSessionIds(snapshot),
      };
      const makeBatch = (selected: SourcePart[]): SourceBatch => ({
        id: job.id, sessionId: job.sessionId, sessionFile: job.sessionFile, leafId: job.leafId, cwd: job.cwd,
        sourceKeys: selected.map(part => part.key), evidence: selected, context: [],
        ...lineage, ...sourceBoundary(snapshot, selected),
      });
      // Durable work records the frozen range. Only the claiming model decides
      // the next request's prefix; committed keys remain the progress authority.
      let batch: SourceBatch | undefined;
      for (let count = 1; count <= evidence.length; count++) {
        const candidate = makeBatch(evidence.slice(0, count));
        if (!fitsConsolidationRequest(candidate, context.model, { reserveRetrieval: true })) break;
        batch = candidate;
      }
      if (!batch) {
        const first = makeBatch(evidence.slice(0, 1));
        if (fitsConsolidationRequest(first, context.model)) batch = first;
      }
      if (!batch) throw new DistillFailure("model_unavailable", "The selected model cannot fit one source part and the required consolidation request.");
      for (const part of preceding.reverse()) {
        const interpretation = [part, ...batch.context];
        if (Buffer.byteLength(JSON.stringify(interpretation)) > CONTEXT_BYTES) break;
        const candidate: SourceBatch = { ...batch, context: interpretation };
        if (!fitsConsolidationRequest(candidate, context.model, { reserveRetrieval: true })) break;
        batch = candidate;
      }
      await this.#waitUntilIdle(signal);
      signal.throwIfAborted();
      const run = this.#options.runModel ?? consolidateWithModel;
      const changes = await abortable(
        run(batch, context, store, signal, { waitUntilIdle: requestSignal => this.#waitUntilIdle(requestSignal),
          ...(view ? { isSiblingOrigin: origin => view.isSibling(origin) } : {}) }),
        signal,
      );
      signal.throwIfAborted();
      await view?.unchanged();
      signal.throwIfAborted();
      store.commitConsolidation({ ...changes, jobId: job.id, owner: claimToken, now: this.#now(),
        sourceKeys: batch.sourceKeys, evidence: sourceBoundary(snapshot, batch.evidence) });
      this.#notifyProgress();
    } catch (error) {
      if (!this.#closed) this.#releaseClaim(store, claimToken, error, retryContext, signal);
    }
  }

  #releaseClaim(store: SQLiteMemoryStore, claimToken: string, error: unknown,
    context: DistillRetryContext, signal: AbortSignal): void {
    if (error instanceof SourceWorkPending) this.#sourceWorkPending = true;
    try {
      store.releaseDistillClaim({
        claimToken,
        now: this.#now(),
        ...(signal.aborted || error instanceof SourceWorkPending ? {} : { failure: {
          reason: error instanceof DistillFailure ? error.reason
            : error instanceof MemoryConflictError || error instanceof ConsolidationConflictError || error instanceof PendingSourceOverlapError
              ? "conflict" as const : "transient" as const,
          context,
          ...(error instanceof DistillFailure && error.retryAfterMs !== undefined ? { retryAfterMs: error.retryAfterMs } : {}),
        } }),
      });
    } catch { /* Expiring the lease is the final recovery path. */ }
  }

  #notifyProgress(): void {
    try { this.#options.onProgress?.(); } catch { /* Cleanup wake failures must not block consolidation. */ }
  }
}

function rotateObservations(observations: readonly SessionObservation[], last: string | undefined): SessionObservation[] {
  if (!last) return [...observations];
  const index = observations.findIndex(observation => observation.sessionId === last);
  return index < 0 ? [...observations] : [...observations.slice(index + 1), ...observations.slice(0, index + 1)];
}

function closeTriggerId(source: SourceReference): string {
  return createHash("sha256").update(JSON.stringify([
    "pi-distill-close-trigger-v1", source.sessionId, source.sessionFile, source.leafId,
  ])).digest("hex");
}

function assertSnapshotMatches(snapshot: SourceSnapshot, reference: Pick<DistillTrigger, "sessionId" | "sessionFile" | "leafId" | "cwd"> | ConsolidationJob): void {
  if (snapshot.pending || snapshot.ancestry?.unresolved) {
    throw new DistillFailure("source_invalid", snapshot.pendingReason ?? "The source branch is not fully available.");
  }
  if (snapshot.sessionId !== reference.sessionId || snapshot.cwd !== reference.cwd || snapshot.leafId !== reference.leafId) {
    throw new DistillFailure("source_invalid", "The original source boundary is unavailable or changed.");
  }
  if (typeof reference.sessionFile !== "string" || snapshot.sessionFile === null ||
    resolve(snapshot.sessionFile) !== resolve(reference.sessionFile)) {
    throw new DistillFailure("source_invalid", "The persisted source file identity changed.");
  }
}

function triggerLineageSessionIds(snapshot: SourceSnapshot): string[] {
  if (snapshot.indexed) return snapshot.indexed.lineageSessionIds;
  return [...new Set([
    snapshot.sessionId,
    ...Array.from(snapshot.ancestry?.origins.values() ?? [], origin => origin.sessionId),
  ])];
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    return !(typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH");
  }
}

function sourceBoundary(snapshot: SourceSnapshot, evidence: readonly SourcePart[]): {
  sourceTime?: string;
  branchAnchor?: string;
} {
  if (snapshot.indexed) return snapshot.indexed.boundary(evidence);
  const ids = new Set(evidence.map(part => part.entryId));
  const times: string[] = [];
  let boundary: string | undefined;
  for (const entry of snapshot.entries) {
    if (typeof entry !== "object" || entry === null || !("id" in entry) || typeof entry.id !== "string") continue;
    const sourceId = snapshot.ancestry?.origins.get(entry.id)?.entryId ?? entry.id;
    const changedEvidence = "type" in entry && entry.type === "context_edit" && "targetId" in entry &&
      typeof entry.targetId === "string" && ids.has(snapshot.ancestry?.origins.get(entry.targetId)?.entryId ?? entry.targetId);
    if (!ids.has(sourceId) && !changedEvidence) continue;
    boundary = entry.id;
    if (!("timestamp" in entry) || typeof entry.timestamp !== "string") continue;
    const date = new Date(entry.timestamp);
    if (!Number.isNaN(date.getTime()) && date.toISOString() === entry.timestamp) times.push(entry.timestamp);
  }
  times.sort();
  const latest = times.at(-1);
  return {
    ...(latest === undefined ? {} : { sourceTime: latest }),
    ...(boundary === undefined ? {} : { branchAnchor: anchor(snapshot, boundary) }),
  };
}

function anchor(snapshot: SourceSnapshot, entryId: string): string {
  const origin = snapshot.ancestry?.origins.get(entryId);
  return createHash("sha256").update(JSON.stringify([
    "pi-distill-entry-anchor-v1", origin?.sessionId ?? snapshot.sessionId, origin?.entryId ?? entryId,
  ])).digest("hex");
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason ?? new Error("Consolidation cancelled."));
  return new Promise<T>((resolve, reject) => {
    const cleanup = (): void => signal.removeEventListener("abort", abort);
    const abort = (): void => { cleanup(); reject(signal.reason ?? new Error("Consolidation cancelled.")); };
    signal.addEventListener("abort", abort, { once: true });
    promise.then(
      value => { cleanup(); resolve(value); },
      error => { cleanup(); reject(error); },
    );
  });
}

function validSnapshot(snapshot: SourceSnapshot): boolean {
  return snapshot.indexed !== undefined ? !snapshot.pending && snapshot.indexed.state.phase === "ready" : isValidSourceSnapshot(snapshot);
}
