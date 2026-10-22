import { randomUUID } from "node:crypto";
import type { SQLiteMemoryStore } from "./memory-store.js";
import { assessSessionRetention, type SessionRetentionAssessment } from "./session-retention.ts";
import type { WaitReason } from "./task-types.ts";

interface Options {
  readonly openStore: () => SQLiteMemoryStore;
  readonly hasStore?: () => boolean;
  readonly retryDelayMs?: number;
}

/** Independent, bounded disk work. An assessment never completes a deletion task. */
export class CleanupCoordinator {
  readonly #options: Options;
  #store: SQLiteMemoryStore | undefined;
  #working: Promise<void> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  #wakeAgain = false;
  #cursor: number | undefined;
  #legacyCursor: number | undefined;
  #discoveryCursor: string | undefined;
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
    // Rotate dependency discovery as well as tasks. This is a bounded view,
    // never evidence that every session on disk has been discovered.
    const known = store.listKnownSessionFiles(128, this.#discoveryCursor);
    this.#discoveryCursor = known.length === 128 ? known.at(-1) : undefined;
    for (const task of page.items) {
      if (this.#closed) return;
      const owner = randomUUID();
      if (!store.claimCleanTask(task.id, owner, Date.now(), 60_000, force)) continue;
      this.#claim = { id: task.id, owner };
      let reason: WaitReason = "unavailable";
      try {
        const report = await assessSessionRetention([...new Set([task.sessionFile, ...known])], store);
        const assessment = report.sessions.find(source => source.sessionFile === task.sessionFile);
        reason = assessment?.sessionId === task.sessionId ? assessmentReason(assessment, report.dependenciesComplete) : "unavailable";
      } catch { /* Keep unreadable evidence unresolved for the next wake. */ }
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
