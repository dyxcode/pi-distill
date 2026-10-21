import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { SQLiteMemoryStore } from "./memory-store.js";
import type { ConsolidationJob, SourceBatch } from "./consolidation-types.js";
import { loadSnapshot, planSources, snapshotSession, type SourceSnapshot } from "./consolidation-source.ts";
import { consolidateWithModel } from "./consolidation-model.ts";

type Context = Pick<ExtensionContext, "model" | "modelRegistry" | "sessionManager">;
type ShutdownReason = "quit" | "reload" | "new" | "resume" | "fork";
interface Options {
  openStore: () => SQLiteMemoryStore;
  hasStore?: () => boolean;
  loadSnapshot?: typeof loadSnapshot;
  runModel?: typeof consolidateWithModel;
  retryDelayMs?: number;
  closeGraceMs?: number;
}

/** Owns background work; lifecycle events only register source boundaries. */
export class ConsolidationCoordinator {
  readonly #options: Options;
  readonly #snapshots = new Map<string, SourceSnapshot>();
  #store: SQLiteMemoryStore | undefined;
  #context: Context | undefined;
  #before: SourceSnapshot | undefined;
  #working: Promise<void> | undefined;
  #controller: AbortController | undefined;
  #claim: { id: string; owner: string } | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  #lastJob: string | undefined;
  #recoveryNeeded = false;
  readonly #recovered = new Set<string>();
  #lastObservation: string | undefined;

  constructor(options: Options) { this.#options = options; }

  async start(ctx: Context): Promise<void> {
    this.#context = ctx;
    if (this.#closed) return;
    try {
      if (this.#options.hasStore && !this.#options.hasStore()) { this.observe(ctx); return; }
      this.#recoveryNeeded = true;
      this.observe(ctx);
      this.#kick();
    } catch { this.#recoveryNeeded = true; this.#retry(); }
  }

  async #recover(ctx: Context): Promise<void> {
    this.#recoveryNeeded = false;
    const pending = this.#database().observedSessions();
    const last = pending.findIndex(observation => observation.sessionId === this.#lastObservation);
    const observations = [...pending.slice(last + 1), ...pending.slice(0, last + 1)];
    let attempted = 0;
    for (const observation of observations) {
      if (this.#closed) return;
      const key = JSON.stringify(observation);
      if (this.#recovered.has(key) || observation.sessionId === ctx.sessionManager.getSessionId() || alive(observation.ownerPid)) continue;
      if (attempted >= 4) { this.#recoveryNeeded = true; break; }
      attempted++;
      this.#lastObservation = observation.sessionId;
      try {
        const snapshot = await this.#load(observation.sessionFile, observation.leafId);
        if (this.#closed) return;
        if (snapshot.sessionId !== observation.sessionId || snapshot.pending) {
          this.#recoveryNeeded = true;
          continue;
        }
        this.#enqueue(snapshot);
        this.#recovered.add(key);
      } catch { this.#recoveryNeeded = true; }
    }
  }

  observe(ctx: Context): void {
    if (this.#closed) return;
    this.#context = ctx;
    try {
      const manager = ctx.sessionManager;
      const sessionFile = manager.getSessionFile();
      const leafId = manager.getLeafId();
      if (!sessionFile || leafId === null || !existsSync(sessionFile)) return;
      this.#database().observeSession({
        sessionId: manager.getSessionId(), sessionFile,
        cwd: manager.getCwd(), leafId, ownerPid: process.pid,
      });
      // Observation does not create a consolidation job. Previously queued work
      // can resume after a model is selected later in the session.
      this.#kick();
    } catch { /* A later event can retry observation without interrupting Pi. */ }
  }

  beforeCompact(ctx: Context): void {
    if (this.#closed) return;
    this.#context = ctx;
    try { this.#before = snapshotSession(ctx.sessionManager); this.observe(ctx); }
    catch { this.#before = undefined; }
  }

  afterCompact(ctx: Context): void {
    if (this.#closed) return;
    this.#context = ctx;
    const before = this.#before;
    this.#before = undefined;
    if (before) {
      try { this.#enqueue(before); } catch { this.#retry(); }
    }
    this.#kick();
  }

  compactFailed(): void { this.#before = undefined; }

  async whenIdle(): Promise<void> { await this.#working; }

  async shutdown(ctx: Context, reason: ShutdownReason): Promise<void> {
    if (this.#closed) return;
    this.#context = ctx;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    if (reason !== "reload") {
      try { this.#enqueue(snapshotSession(ctx.sessionManager)); } catch { /* Observe/recovery retains the source. */ }
      this.#kick();
    }
    // Bound shutdown even for providers which ignore cancellation. A fenced claim
    // and the signal check before commit also prevent a late provider response writing.
    const grace = reason === "reload" ? 0 : this.#options.closeGraceMs ?? 1_500;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      if (this.#working && grace > 0) await Promise.race([
        this.#working,
        new Promise<void>(resolve => { timer = setTimeout(resolve, grace); }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
      this.#closed = true;
      this.#controller?.abort();
      if (this.#timer) clearTimeout(this.#timer);
      if (this.#claim) {
        try { this.#store?.releaseConsolidation(this.#claim.id, this.#claim.owner); } catch { /* Lease expires on recovery. */ }
      }
      this.#store?.close();
      this.#store = undefined;
      this.#snapshots.clear();
    }
  }

  #database(): SQLiteMemoryStore { return this.#store ??= this.#options.openStore(); }
  #load(path: string, leaf?: string | null): Promise<SourceSnapshot> {
    return (this.#options.loadSnapshot ?? loadSnapshot)(path, leaf);
  }

  #enqueue(snapshot: SourceSnapshot): void {
    const all = planSources(snapshot, new Set());
    if (!all.length) {
      if (!snapshot.pending) this.#store?.acknowledgeObservation(snapshot.sessionId, snapshot.leafId);
      return;
    }
    const store = this.#database();
    const keys = all.flatMap(batch => [...batch.sourceKeys]);
    const processed = new Set(store.processedSources(keys));
    // Capturing a newer boundary must not create another job for ranges which
    // already have durable pending work.
    for (const queued of store.listConsolidationJobs()) {
      for (const key of queued.sourceKeys) processed.add(key);
    }
    const inputCap = Math.min(64 * 1024, (this.#context?.model?.contextWindow ?? 128_000) - 4_096);
    const evidenceCap = Math.max(2 * 1024, Math.min(24 * 1024, inputCap - 16 * 1024));
    for (const planned of planSources(snapshot, processed)) {
      const batches: SourceBatch[] = [];
      let evidence: SourceBatch["evidence"][number][] = [];
      const append = (): void => {
        if (!evidence.length) return;
        const sourceKeys = evidence.map(part => part.key);
        const id = createHash("sha256").update(JSON.stringify([planned.id, sourceKeys])).digest("hex");
        batches.push({ ...planned, id, sourceKeys, evidence });
        evidence = [];
      };
      for (const part of planned.evidence) {
        if (Buffer.byteLength(JSON.stringify([...evidence, part])) > evidenceCap) append();
        evidence.push(part);
      }
      append();
      for (const batch of batches) {
        const job: ConsolidationJob = {
          id: batch.id, sessionId: batch.sessionId, sessionFile: batch.sessionFile,
          leafId: batch.leafId, cwd: batch.cwd, sourceKeys: batch.sourceKeys,
          lineageSessionIds: [...new Set([
            snapshot.sessionId,
            ...Array.from(snapshot.ancestry?.origins.values() ?? [], origin => origin.sessionId),
          ])],
          ...sourceBoundary(snapshot, batch.evidence),
        };
        store.enqueueConsolidation(job);
        // Sessionless SDK sessions have no file to recover from. Their live snapshot
        // can still be processed during the current runtime.
        if (!snapshot.sessionFile) this.#snapshots.set(job.id, snapshot);
      }
    }
    store.acknowledgeObservation(snapshot.sessionId, snapshot.leafId);
  }

  #kick(): void {
    if (this.#closed || this.#working || !this.#context?.model) return;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    const context = this.#context;
    this.#controller = new AbortController();
    const signal = this.#controller.signal;
    this.#working = (async () => {
      if (this.#recoveryNeeded) await this.#recover(context);
      if (!signal.aborted) await this.#pass(context, signal);
    })().catch(() => { this.#recoveryNeeded = true; }).finally(() => {
      this.#working = undefined;
      if (!this.#closed) this.#retry();
    });
  }

  #retry(): void {
    if (this.#closed || this.#timer || !this.#context?.model) return;
    this.#timer = setTimeout(() => { this.#timer = undefined; this.#kick(); }, this.#options.retryDelayMs ?? 30_000);
    this.#timer.unref();
  }

  async #pass(context: Context, signal: AbortSignal): Promise<void> {
    const store = this.#database();
    const pending = store.listConsolidationJobs();
    const last = pending.findIndex(job => job.id === this.#lastJob);
    const jobs = [...pending.slice(last + 1), ...pending.slice(0, last + 1)];
    let attempted = 0;
    for (const job of jobs) {
      if (signal.aborted || attempted >= 2) break;
      const owner = randomUUID();
      if (!store.claimConsolidation(job.id, owner, Date.now(), 60_000)) continue;
      this.#lastJob = job.id;
      this.#claim = { id: job.id, owner };
      try {
        const processed = new Set(store.processedSources(job.sourceKeys));
        const sourceKeys = job.sourceKeys.filter(key => !processed.has(key));
        if (!sourceKeys.length) {
          store.commitConsolidation({jobId:job.id,owner,now:Date.now(),sourceKeys,creates:[],updates:[],deletes:[]});
          continue;
        }
        attempted++;
        const snapshot = this.#snapshots.get(job.id) ?? (job.sessionFile ? await this.#load(job.sessionFile, job.leafId) : undefined);
        if (!snapshot || snapshot.sessionId !== job.sessionId || snapshot.cwd !== job.cwd) {
          throw new Error("Source unavailable or its identity changed.");
        }
        const all = planSources(snapshot, new Set());
        const parts = new Map(all.flatMap(batch => batch.evidence).map(part => [part.key, part]));
        const evidence = sourceKeys.map(key => {
          const part = parts.get(key);
          if (!part) throw new Error("Source changed after capture.");
          return part;
        });
        const currentParts = [...parts.values()];
        const firstIndex = currentParts.findIndex(part => part.key === sourceKeys[0]);
        const inputCap = Math.min(64 * 1024, (context.model?.contextWindow ?? 128_000) - 4_096);
        const contextCap = Math.max(0, Math.min(8 * 1024, inputCap - Buffer.byteLength(JSON.stringify(evidence)) - 8 * 1024));
        const interpretation: SourceBatch["context"][number][] = [];
        for (const part of currentParts.slice(0, Math.max(0, firstIndex)).reverse()) {
          if (Buffer.byteLength(JSON.stringify([part, ...interpretation])) <= contextCap) interpretation.unshift(part);
        }
        const batch: SourceBatch = {
          ...job, sourceKeys, evidence, context: interpretation,
          lineageKeys: [...parts.keys()],
          lineageAnchors: snapshot.entries.flatMap(entry => typeof entry === "object" && entry !== null &&
            "id" in entry && typeof entry.id === "string" ? [anchor(snapshot, entry.id)] : []),
          lineageSessionIds: [...new Set([
            snapshot.sessionId,
            ...Array.from(snapshot.ancestry?.origins.values() ?? [], origin => origin.sessionId),
          ])],
        };
        signal.throwIfAborted();
        const timeout = AbortSignal.timeout(30_000);
        const combined = AbortSignal.any([signal, timeout]);
        const run = this.#options.runModel ?? consolidateWithModel;
        const changes = await abortable(run(batch, context, store, combined), combined);
        combined.throwIfAborted();
        store.commitConsolidation({ ...changes, jobId: job.id, owner, now: Date.now(), sourceKeys });
        this.#snapshots.delete(job.id);
      } catch { /* Invalid results and transient failures retain their original source range. */ }
      finally {
        if (!this.#closed) { try { store.releaseConsolidation(job.id, owner); } catch { /* Durable lease handles recovery. */ } }
        this.#claim = undefined;
      }
    }
  }
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    return !(typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH");
  }
}

function sourceBoundary(snapshot: SourceSnapshot, evidence: SourceBatch["evidence"]): {
  sourceTime?: string;
  branchAnchor?: string;
} {
  const ids = new Set(evidence.map(part => part.entryId));
  const times: string[] = [];
  let boundary: string | undefined;
  for (const entry of snapshot.entries) {
    if (typeof entry !== "object" || entry === null || !("id" in entry) || typeof entry.id !== "string") continue;
    const sourceId = snapshot.ancestry?.origins.get(entry.id)?.entryId ?? entry.id;
    const changedEvidence = "type" in entry && entry.type === "context_edit" && "targetId" in entry &&
      typeof entry.targetId === "string" && ids.has(snapshot.ancestry?.origins.get(entry.targetId)?.entryId ?? entry.targetId);
    if (!ids.has(sourceId) && !changedEvidence) continue;
    // Corrected text retains its original message ID. Its supporting branch
    // also needs the edit event, which an unedited sibling does not contain.
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
  return new Promise<T>((resolve, reject) => {
    const abort = (): void => reject(new Error("Consolidation cancelled."));
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, {once:true});
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
}
