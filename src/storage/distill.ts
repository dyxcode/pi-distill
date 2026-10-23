import { createHash, randomUUID } from "node:crypto";
import {
  canRetryDistillFailure,
  makeDistillFailureRecord,
  parseDistillFailureRecord,
  parseDistillRetryContext,
  parseDistillTaskKind,
  type DistillFailureRecord,
  type DistillRetryContext,
  type DistillTaskKind
} from "../distill/retry.ts";
import type {
  DistillClaimInput,
  DistillFailure,
  DistillFailureKind,
  DistillJobClaim,
  DistillTrigger,
  DistillTriggerClaim,
  DistillTriggerClaimInput,
  DistillTriggerInput,
  PrepareDistillTriggerInput,
  ReleaseDistillClaimInput,
  TaskSweepPage,
  TaskSweepPageOptions
} from "../distill/tasks.ts";
import type {
  ConsolidationJob
} from "../distill/types.ts";
import { ConsolidationConflictError } from '../distill/types.ts';
import { distillFailureColumns, jobColumns, textColumn, triggerColumns } from './columns.ts';
import type { StorageConnection } from './connection.ts';
import { parseConsolidationJob, parsePersistedJobRow } from './evidence-validation.ts';
import type { PersistedDistillLease, PersistedDistillTrigger } from './records.ts';
import type { RetentionTasks } from './retention.ts';
import { DistillSchedule, type ScheduledDistill } from "./schedule.ts";
import type { SessionRecords } from './sessions.ts';
import { parseDistillClaimInput, parseDistillTriggerClaimInput, parseDistillTriggerInput, parsePersistedDistillTrigger, parsePrepareDistillTriggerInput, parseReleaseDistillClaimInput, parseTaskSweepPageOptions } from './task-validation.ts';
import { fields, isRecord, parseNullableText, validateLeaseDuration, validateNow, validateText } from './validation.ts';
interface Dependencies {
  readonly sessions: Pick<SessionRecords, "assertSourceEnqueueAllowed">;
  readonly retention: Pick<RetentionTasks, "enqueueCleanTaskInTransaction">;
}

export class DistillQueue {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }

  enqueueDistillTrigger(trigger: DistillTriggerInput): void {
    this.connection.ensureOpen();
    const record = parseDistillTriggerInput(trigger);
    const triggerJson = JSON.stringify(record);
    const status = record.reason === "compact" ? "candidate" : "pending";
    this.connection.transaction(() => {
      if (record.sessionFile !== null) this.dependencies.sessions.assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
      const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(record.id);
      if (row !== undefined) {
        const existing = this.parseTriggerRow(row);
        if (JSON.stringify(existing.trigger) !== triggerJson) {
          throw new ConsolidationConflictError(record.id, "already exists with a different trigger boundary");
        }
        return;
      }
      if (status === "pending") this.schedule().enqueue(record.sessionId);
      this.connection.database.prepare(
        "INSERT INTO distill_triggers " +
        "(id, trigger_json, status, compaction_record_id, retry_category, retry_at) " +
        "VALUES (?, ?, ?, NULL, NULL, NULL)",
      ).run(record.id, triggerJson, status);
    });
  }

  confirmCompactionTrigger(input: { readonly id: string; readonly compactionRecordId: string; }): boolean {
    this.connection.ensureOpen();
    const { id, compactionRecordId } = fields(input, ["id", "compactionRecordId"]);
    validateText(id, "distill trigger ID");
    validateText(compactionRecordId, "compaction record ID");
    return this.connection.transaction(() => {
      const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = this.parseTriggerRow(row);
      if (current.trigger.reason !== "compact") {
        throw new ConsolidationConflictError(id, "is not a compaction candidate");
      }
      if (current.status !== "candidate") {
        if (current.compactionRecordId === compactionRecordId) return true;
        throw new ConsolidationConflictError(id, "has already been confirmed by a different compaction record");
      }
      this.schedule().enqueue(current.trigger.sessionId);
      this.connection.database.prepare(
        "UPDATE distill_triggers SET status = 'pending', compaction_record_id = ? " +
        "WHERE id = ? AND status = 'candidate'",
      ).run(compactionRecordId, id);
      return true;
    });
  }

  cancelCompactionTrigger(id: string): boolean {
    this.connection.ensureOpen();
    validateText(id, "distill trigger ID");
    return this.connection.transaction(() => {
      const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = this.parseTriggerRow(row);
      if (current.trigger.reason !== "compact" || current.status !== "candidate") return false;
      this.connection.database.prepare("DELETE FROM distill_triggers WHERE id = ? AND status = 'candidate'").run(id);
      this.clearDistillFailure("trigger", id);
      return true;
    });
  }

  listDistillTriggers(options: TaskSweepPageOptions): TaskSweepPage<DistillTrigger> {
    this.connection.ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.taskSweepThroughCursor("distill_triggers", page.throughCursor);
    const rows = this.connection.database.prepare(
      `SELECT rowid AS cursorRow, ${triggerColumns} FROM distill_triggers ` +
      "WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.connection.decodeRow(raw);
      const { cursorRow, ...triggerRow } = decoded;
      validateNow(cursorRow, "Stored distill trigger cursor");
      return this.distillTriggerItem(this.parseTriggerRow(triggerRow));
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.connection.decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next distill trigger cursor");
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  readDistillTrigger(id: string): DistillTrigger | undefined {
    this.connection.ensureOpen();
    validateText(id, "distill trigger ID");
    const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
    if (row === undefined) return undefined;
    return this.distillTriggerItem(this.parseTriggerRow(row));
  }

  readDistillFailure(kind: DistillFailureKind, id: string): DistillFailure | undefined {
    this.connection.ensureOpen();
    const taskKind = parseDistillTaskKind(kind);
    validateText(id, "distill task ID");
    return this.lookupDistillFailure(taskKind, id);
  }

  /** Advisory only; the fenced claim methods remain authoritative. */
  canAttemptDistill(
    kind: DistillFailureKind,
    id: string,
    context: DistillRetryContext | undefined,
    now: number,
  ): boolean {
    this.connection.ensureOpen();
    const taskKind = parseDistillTaskKind(kind);
    validateText(id, "distill task ID");
    validateNow(now, "Distill eligibility time");
    const retryContext = context === undefined ? undefined : parseDistillRetryContext(context);
    if (taskKind === "trigger") {
      const row = this.connection.database.prepare(
        `SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`,
      ).get(id);
      if (row === undefined) return false;
      const task = this.parseTriggerRow(this.connection.decodeRow(row));
      return (task.status === "pending" || task.status === "candidate") &&
        this.distillTaskCanAttempt(taskKind, id, retryContext, now, task.retryAt);
    }
    const row = this.connection.database.prepare(
      `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
    ).get(id);
    if (row === undefined) return false;
    const task = parsePersistedJobRow(this.connection.decodeRow(row));
    return task.status === "pending" && this.distillTaskCanAttempt(taskKind, id, retryContext, now, task.retryAt);
  }

  private schedule(): DistillSchedule { return new DistillSchedule(this.connection.database); }

  /** Bounded shared session rotation; actual claim/retry state remains authoritative. */
  listScheduledDistill(now: number, force = false, after = 0, through?: number) {
    this.connection.ensureOpen();
    return this.connection.transaction(() => {
      const backfilled = this.schedule().backfill();
      return { ...this.schedule().page(now, force, after, through), backfilled };
    });
  }

  readyDistillWork(now: number): boolean {
    this.connection.ensureOpen(); validateNow(now, "Scheduling time");
    return this.schedule().hasReady(now);
  }

  distillWakeDelay(now: number): number {
    this.connection.ensureOpen(); validateNow(now, "Scheduling time");
    return this.schedule().delay(now);
  }

  claimScheduledDistill(candidate: ScheduledDistill, input: DistillClaimInput):
    | { kind: "job"; claim: DistillJobClaim; position: number; }
    | { kind: "trigger"; claim: DistillTriggerClaim; position: number; }
    | { kind: "changed" | "waiting" | "ready" | "busy"; } {
    this.connection.ensureOpen();
    const request = parseDistillClaimInput(input);
    const raw = fields(candidate, ["sessionId", "position", "waiting", "kind", "id", "sessionFile"]);
    validateText(raw.sessionId, "scheduling session"); validateNow(raw.position, "Scheduling position");
    validateText(raw.id, "scheduling task");
    if ((raw.kind !== "job" && raw.kind !== "trigger") || typeof raw.waiting !== "boolean") throw new TypeError("Invalid scheduling candidate.");
    if (raw.sessionFile !== null) validateText(raw.sessionFile, "scheduling file");
    return this.connection.transaction(() => {
      const schedule = this.schedule();
      const front = schedule.front(candidate.sessionId);
      if (schedule.position(candidate.sessionId) !== candidate.position || front?.kind !== candidate.kind || front.id !== candidate.id) return { kind: "changed" };
      if (!this.canAttemptDistill(front.kind, front.id, request.retryContext, request.now)) {
        this.deferScheduled(candidate.sessionId, candidate.position, request.now);
        return { kind: "waiting" };
      }
      const lease = this.readDistillLease();
      if (lease.leaseUntil !== null && lease.leaseUntil > request.now) return { kind: "busy" };
      if (candidate.waiting) {
        schedule.turn(candidate.sessionId);
        return { kind: "ready" };
      }
      if (front.kind === "job") {
        const claim = this.claimConsolidationJob(front.id, request, randomUUID());
        if (!claim) return { kind: "busy" };
        return { kind: "job", claim, position: this.schedulePosition(candidate.sessionId) };
      }
      const claim = this.claimTriggerInTransaction({ ...request, id: front.id });
      if (!claim) return { kind: "busy" };
      return { kind: "trigger", claim, position: this.schedulePosition(candidate.sessionId) };
    });
  }

  private schedulePosition(sessionId: string): number {
    const position = this.schedule().position(sessionId);
    if (position === undefined) throw new Error("Missing claimed scheduling position.");
    return position;
  }

  private deferScheduled(sessionId: string, position: number, now: number): void {
    const schedule = this.schedule();
    if (schedule.position(sessionId) !== position) return;
    const front = schedule.front(sessionId);
    if (!front) return;
    const failure = this.lookupDistillFailure(front.kind, front.id);
    const table = front.kind === "job" ? "consolidation_jobs" : "distill_triggers";
    const retryAt = this.connection.database.prepare(`SELECT retry_at FROM ${table} WHERE id=?`).get(front.id)?.["retry_at"];
    const next = failure?.nextAttemptAt ?? retryAt;
    const probeAt = typeof next === "number" && next > now ? Math.min(next, now + 30_000) : now + 30_000;
    // Move examined waiters behind unexamined sessions too, so bounded probing
    // cannot repeatedly inspect the same first page after a host wake/restart.
    schedule.defer(sessionId, schedule.turn(sessionId), probeAt);
  }

  settleScheduledDistill(sessionId: string, position: number, context: DistillRetryContext | undefined, now: number): void {
    this.connection.ensureOpen(); validateText(sessionId, "scheduling session"); validateNow(position, "Scheduling position"); validateNow(now, "Scheduling time");
    const retryContext = context === undefined ? undefined : parseDistillRetryContext(context);
    this.connection.transaction(() => {
      const front = this.schedule().front(sessionId);
      if (front && !this.canAttemptDistill(front.kind, front.id, retryContext, now)) this.deferScheduled(sessionId, position, now);
    });
  }

  claimDistillTrigger(input: DistillTriggerClaimInput): DistillTriggerClaim | undefined {
    this.connection.ensureOpen();
    const request = parseDistillTriggerClaimInput(input);
    return this.connection.transaction(() => this.claimTriggerInTransaction(request));
  }

  private claimTriggerInTransaction(request: ReturnType<typeof parseDistillTriggerClaimInput>): DistillTriggerClaim | undefined {
    let rows: Record<string, unknown>[];
    if (request.id === undefined) {
      rows = this.connection.database.prepare(
        `SELECT ${triggerColumns} FROM distill_triggers ` +
        "WHERE status = 'pending' AND (retry_at IS NULL OR retry_at <= ?) " +
        "AND NOT EXISTS (SELECT 1 FROM distill_failures AS f WHERE f.task_kind = 'trigger' AND f.task_id = distill_triggers.id) " +
        "ORDER BY rowid LIMIT 32",
      ).all(request.now);
    } else {
      rows = [];
      const row = this.connection.database.prepare(
        `SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`,
      ).get(request.id);
      if (row !== undefined) rows.push(row);
    }
    for (const row of rows) {
      const current = this.parseTriggerRow(this.connection.decodeRow(row));
      if (current.status !== "pending" && !(request.allowCandidate && current.status === "candidate")) continue;
      if (request.id !== undefined && !this.distillTaskCanAttempt(
        "trigger", request.id, request.retryContext, request.now, current.retryAt,
      )) continue;
      if (!this.acquireDistillSlot({
        claimToken: randomUUID(),
        workerId: request.workerId,
        taskKind: "trigger",
        taskId: current.trigger.id,
        now: request.now,
        leaseMs: request.leaseMs,
      })) return undefined;
      const lease = this.readDistillLease();
      if (lease.claimToken === null || lease.leaseUntil === null) {
        throw new Error("Distill claim was not persisted.");
      }
      this.connection.database.prepare(
        "UPDATE distill_triggers SET retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
      ).run(current.trigger.id);
      return {
        trigger: this.distillTriggerItem({ ...current, retryCategory: null, retryAt: null }),
        claimToken: lease.claimToken,
        leaseUntil: lease.leaseUntil,
      };
    }
    return undefined;
  }

  readSourcePreparation(triggerId: string, viewId: string): number {
    validateText(triggerId, "trigger ID"); validateText(viewId, "source view ID");
    const row = this.connection.database.prepare("SELECT cursor FROM source_preparation WHERE trigger_id=? AND view_id=?").get(triggerId, viewId);
    if (!row) return 0;
    const cursor = row["cursor"];
    if (typeof cursor !== "number" || !Number.isSafeInteger(cursor) || cursor < 0) throw new TypeError("Invalid preparation cursor.");
    return cursor;
  }

  pendingTriggerJob(triggerId: string): ConsolidationJob | undefined {
    validateText(triggerId, "trigger ID");
    const row = this.connection.database.prepare(`SELECT ${textColumn("jobs.id", "id")} FROM consolidation_jobs jobs JOIN distill_trigger_jobs links ON links.job_id=jobs.id WHERE links.trigger_id=? AND jobs.status='pending' LIMIT 1`).get(triggerId);
    if (!row) return undefined;
    const id = this.connection.decodeRow(row)["id"]; validateText(id, "pending job ID");
    const job = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id=?`).get(id);
    return job ? parsePersistedJobRow(this.connection.decodeRow(job)).job : undefined;
  }

  pendingJobForSource(key: string): ConsolidationJob | undefined {
    validateText(key, "source key");
    const row = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE status='pending' AND EXISTS (SELECT 1 FROM json_each(job_json, '$.sourceKeys') WHERE value=?) LIMIT 1`).get(key);
    return row ? parsePersistedJobRow(this.connection.decodeRow(row)).job : undefined;
  }

  /** Backpressure keeps one outstanding prepared page while the frozen source range stays open. */
  prepareDistillPage(input: PrepareDistillTriggerInput & {
    readonly viewId: string; readonly after: number; readonly cursor: number; readonly complete: boolean;
  }): void {
    const raw = fields(input, ["id", "claimToken", "now", "jobs", "viewId", "after", "cursor", "complete"]);
    const request = parsePrepareDistillTriggerInput({ id: raw.id, claimToken: raw.claimToken, now: raw.now, jobs: raw.jobs });
    const { viewId, after, cursor, complete } = raw;
    validateText(viewId, "source view ID");
    if (!/^[a-f0-9]{64}$/.test(viewId) || typeof after !== "number" || !Number.isSafeInteger(after) || after < 0 ||
      typeof cursor !== "number" || !Number.isSafeInteger(cursor) || cursor < after || typeof complete !== "boolean" || request.jobs.length > 1) {
      throw new TypeError("Invalid bounded source preparation page.");
    }
    this.connection.transaction(() => {
      this.assertCurrentDistillLease("trigger", request.id, request.claimToken, request.now);
      const rawTrigger = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id=?`).get(request.id);
      if (!rawTrigger) throw new ConsolidationConflictError(request.id, "is missing");
      const current = this.parseTriggerRow(this.connection.decodeRow(rawTrigger));
      if (current.status !== "pending" || this.pendingTriggerJob(request.id) || this.readSourcePreparation(request.id, viewId) !== after) {
        throw new ConsolidationConflictError(request.id, "has changed preparation progress or outstanding work");
      }
      // Completed jobs retain their independent durable evidence; old page links are disposable.
      if (request.jobs.length > 0 || !complete) this.connection.database.prepare("DELETE FROM distill_trigger_jobs WHERE trigger_id=?").run(request.id);
      for (const job of request.jobs) {
        if (job.sessionId !== current.trigger.sessionId || job.sessionFile !== current.trigger.sessionFile || job.leafId !== current.trigger.leafId || job.cwd !== current.trigger.cwd) {
          throw new ConsolidationConflictError(job.id, "does not belong to the frozen trigger");
        }
        this.enqueueConsolidationInTransaction(job);
        const link = createHash("sha256").update(JSON.stringify([request.id, job.id])).digest("hex");
        this.connection.database.prepare("INSERT OR IGNORE INTO distill_trigger_jobs VALUES (?, ?, ?, 0)").run(link, request.id, job.id);
      }
      this.connection.database.prepare("INSERT INTO source_preparation VALUES (?, ?, ?) ON CONFLICT(trigger_id) DO UPDATE SET view_id=excluded.view_id, cursor=excluded.cursor")
        .run(request.id, viewId, cursor);
      const hasJobs = this.connection.database.prepare("SELECT 1 FROM distill_trigger_jobs WHERE trigger_id=? LIMIT 1").get(request.id) !== undefined;
      const status = complete ? (hasJobs ? "prepared" : "complete") : "pending";
      this.connection.database.prepare("UPDATE distill_triggers SET status=?, retry_category=NULL, retry_at=NULL WHERE id=?").run(status, request.id);
      this.clearDistillFailure("trigger", request.id);
      this.clearDistillLease();
      if (status === "prepared") this.completeReadyTriggers([request.id]);
      else if (status === "complete") this.dependencies.retention.enqueueCleanTaskInTransaction({ sessionId: current.trigger.sessionId, sessionFile: current.trigger.sessionFile, leafId: current.trigger.leafId, cwd: current.trigger.cwd, reason: current.trigger.reason, intentId: current.trigger.id });
    });
  }

  prepareDistillTrigger(input: PrepareDistillTriggerInput): void {
    this.connection.ensureOpen();
    const request = parsePrepareDistillTriggerInput(input);
    this.connection.transaction(() => {
      const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(request.id);
      if (row === undefined) throw new ConsolidationConflictError(request.id, "does not exist");
      const current = this.parseTriggerRow(this.connection.decodeRow(row));
      if (current.status === "prepared" || current.status === "complete") {
        const ids = this.triggerJobIds(request.id);
        if (ids.length !== request.jobs.length || ids.some((id, index) => id !== request.jobs[index]?.id)) {
          throw new ConsolidationConflictError(request.id, "was prepared with a different batch set");
        }
        for (const prepared of request.jobs) this.assertExistingJobPayload(prepared);
        return;
      }
      this.assertCurrentDistillLease("trigger", request.id, request.claimToken, request.now);
      if (current.status !== "pending") {
        throw new ConsolidationConflictError(request.id, "is not ready for preparation");
      }
      for (const prepared of request.jobs) {
        if (prepared.sessionId !== current.trigger.sessionId || prepared.sessionFile !== current.trigger.sessionFile) {
          throw new ConsolidationConflictError(prepared.id, "does not belong to the trigger session");
        }
        this.enqueueConsolidationInTransaction(prepared);
      }
      for (let index = 0; index < request.jobs.length; index += 1) {
        const prepared = request.jobs[index];
        if (prepared === undefined) throw new Error("Prepared batch index exceeded its validated input.");
        const linkId = createHash("sha256").update(JSON.stringify([request.id, prepared.id])).digest("hex");
        const existing = this.connection.database.prepare(
          `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
          `${textColumn("job_id", "jobId")}, ordinal FROM distill_trigger_jobs WHERE id = ?`,
        ).get(linkId);
        if (existing === undefined) {
          this.connection.database.prepare(
            "INSERT INTO distill_trigger_jobs (id, trigger_id, job_id, ordinal) VALUES (?, ?, ?, ?)",
          ).run(linkId, request.id, prepared.id, index);
        } else {
          const link = fields(this.connection.decodeRow(existing), ["id", "triggerId", "jobId", "ordinal"]);
          if (link.triggerId !== request.id || link.jobId !== prepared.id || link.ordinal !== index) {
            throw new ConsolidationConflictError(request.id, "has inconsistent batch associations");
          }
        }
      }
      this.connection.database.prepare(
        "UPDATE distill_triggers SET status = ?, retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
      ).run(request.jobs.length === 0 ? "complete" : "prepared", request.id);
      this.clearDistillFailure("trigger", request.id);
      this.clearDistillLease();
      if (request.jobs.length === 0) {
        this.dependencies.retention.enqueueCleanTaskInTransaction({
          sessionId: current.trigger.sessionId,
          sessionFile: current.trigger.sessionFile,
          leafId: current.trigger.leafId,
          cwd: current.trigger.cwd,
          reason: current.trigger.reason,
          intentId: current.trigger.id,
        });
      } else {
        this.completeReadyTriggers([request.id]);
      }
    });
  }

  claimDistillJob(input: DistillClaimInput & { readonly id: string; }): DistillJobClaim | undefined {
    this.connection.ensureOpen();
    if (!isRecord(input)) throw new TypeError("Distill job claim input must be an object.");
    const allowed = ["id", "workerId", "now", "leaseMs", "retryContext"];
    if (Reflect.ownKeys(input).some(key => typeof key !== "string" || !allowed.includes(key)) ||
      !["id", "workerId", "now", "leaseMs"].every(key => Object.hasOwn(input, key))) {
      throw new TypeError("Distill job claim requires id, workerId, now, and leaseMs.");
    }
    const { id, workerId, now, leaseMs } = input;
    validateText(id, "consolidation job ID");
    const request = parseDistillClaimInput({
      workerId, now, leaseMs,
      ...(Object.hasOwn(input, "retryContext") ? { retryContext: input["retryContext"] } : {}),
    });
    return this.connection.transaction(() => this.claimConsolidationJob(id, request, randomUUID()));
  }

  renewDistillClaim(input: { readonly claimToken: string; readonly now: number; readonly leaseMs: number; }): boolean {
    this.connection.ensureOpen();
    const { claimToken, now, leaseMs } = fields(input, ["claimToken", "now", "leaseMs"]);
    validateText(claimToken, "distill claim token");
    const request = parseDistillClaimInput({ workerId: "lease-renewal", now, leaseMs });
    return this.connection.transaction(() => {
      const lease = this.readDistillLease();
      if (lease.claimToken !== claimToken || lease.leaseUntil === null || lease.leaseUntil <= request.now) return false;
      const until = request.now + request.leaseMs;
      this.connection.database.prepare("UPDATE distill_lease SET lease_until = ? WHERE singleton = 1").run(until);
      if (lease.taskKind === "job" && lease.taskId !== null) {
        const changed = this.connection.database.prepare(
          "UPDATE consolidation_jobs SET lease_until = ? WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(until, lease.taskId, claimToken);
        if (changed.changes !== 1) throw new ConsolidationConflictError(lease.taskId, "lost its durable claim");
      }
      return true;
    });
  }

  releaseDistillClaim(input: ReleaseDistillClaimInput): boolean {
    this.connection.ensureOpen();
    const request = parseReleaseDistillClaimInput(input);
    return this.connection.transaction(() => {
      const lease = this.readDistillLease();
      if (lease.claimToken !== request.claimToken || lease.taskKind === null || lease.taskId === null ||
        lease.leaseUntil === null || lease.leaseUntil <= request.now) return false;
      if (request.failure !== undefined) {
        const kind = lease.taskKind;
        const id = lease.taskId;
        const row = kind === "job"
          ? this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(id)
          : this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
        if (row === undefined) return false;
        if (kind === "job") {
          const current = parsePersistedJobRow(this.connection.decodeRow(row));
          if (current.status !== "pending" || current.leaseOwner !== request.claimToken ||
            current.leaseUntil === null || current.leaseUntil <= request.now) return false;
        } else {
          const current = this.parseTriggerRow(this.connection.decodeRow(row));
          if (current.status !== "pending" && current.status !== "candidate") return false;
        }
        const previous = this.lookupDistillFailure(kind, id);
        const failure = makeDistillFailureRecord({
          kind,
          id,
          failure: request.failure,
          ...(previous === undefined ? {} : { previous }),
          now: request.now,
        });
        this.persistDistillFailure(failure);
        const retryCategory = failure.nextAttemptAt === null
          ? null
          : failure.reason === "transient" || failure.reason === "conflict" ? "transient"
            : failure.reason === "model_invalid" || failure.reason === "model_budget" ? "invalid" : "unavailable";
        if (kind === "job") {
          this.connection.database.prepare(
            "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, retry_category = ?, retry_at = ? " +
            "WHERE id = ? AND status = 'pending' AND lease_owner = ?",
          ).run(retryCategory, failure.nextAttemptAt, id, request.claimToken);
        } else {
          this.connection.database.prepare(
            "UPDATE distill_triggers SET retry_category = ?, retry_at = ? WHERE id = ? AND status = 'pending'",
          ).run(retryCategory, failure.nextAttemptAt, id);
        }
        this.clearDistillLease();
        return true;
      }
      if (lease.taskKind === "job") {
        this.connection.database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, " +
          "retry_category = ?, retry_at = ? WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(request.retry?.category ?? null, request.retry?.retryAt ?? null, lease.taskId, request.claimToken);
      } else if (request.retry) {
        this.connection.database.prepare(
          "UPDATE distill_triggers SET retry_category = ?, retry_at = ? " +
          "WHERE id = ? AND status = 'pending'",
        ).run(request.retry.category, request.retry.retryAt, lease.taskId);
      }
      this.clearDistillLease();
      return true;
    });
  }

  enqueueConsolidation(job: ConsolidationJob): void {
    this.connection.ensureOpen();
    const record = parseConsolidationJob(job);
    this.connection.transaction(() => this.enqueueConsolidationInTransaction(record));
  }

  listConsolidationJobs(): readonly ConsolidationJob[] {
    this.connection.ensureOpen();
    const rows = this.connection.database.prepare(
      `SELECT ${jobColumns} FROM consolidation_jobs ` +
      "WHERE status = 'pending' ORDER BY rowid",
    ).all();
    return rows.map((row) => parsePersistedJobRow(this.connection.decodeRow(row)).job);
  }

  listPendingConsolidationJobs(options: TaskSweepPageOptions): TaskSweepPage<ConsolidationJob> {
    this.connection.ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.taskSweepThroughCursor("consolidation_jobs", page.throughCursor);
    const rows = this.connection.database.prepare(
      `SELECT rowid AS cursorRow, ${jobColumns} FROM consolidation_jobs ` +
      "WHERE status = 'pending' AND rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.connection.decodeRow(raw);
      const { cursorRow, ...jobRow } = decoded;
      validateNow(cursorRow, "Stored pending consolidation job cursor");
      const record = parsePersistedJobRow(jobRow);
      if (record.status !== "pending") throw new TypeError("Pending consolidation job query returned a terminal job.");
      return record.job;
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.connection.decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next pending consolidation job cursor");
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  claimConsolidation(id: string, owner: string, now: number, leaseMs: number): boolean {
    this.connection.ensureOpen();
    validateText(id, "consolidation job ID");
    validateText(owner, "consolidation lease owner");
    validateNow(now, "Consolidation claim time");
    validateLeaseDuration(leaseMs, now, "Consolidation lease duration");
    return this.connection.transaction(() => this.claimConsolidationJob(
      id,
      { workerId: owner, now, leaseMs },
      owner,
    ) !== undefined);
  }

  releaseConsolidation(id: string, owner: string): void {
    this.connection.ensureOpen();
    validateText(id, "consolidation job ID");
    validateText(owner, "consolidation lease owner");
    this.connection.transaction(() => {
      const row = this.connection.database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(id);
      if (row === undefined) return;
      const current = parsePersistedJobRow(this.connection.decodeRow(row));
      if (current.status === "pending" && current.leaseOwner === owner) {
        this.connection.database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, " +
          "retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
        ).run(id);
        const lease = this.readDistillLease();
        if (lease.claimToken === owner && lease.taskKind === "job" && lease.taskId === id) {
          this.clearDistillLease();
        }
      }
    });
  }

  parseTriggerRow(value: unknown): PersistedDistillTrigger {
    const row = this.connection.decodeRow(value as Record<string, unknown>);
    const id = row["id"];
    validateText(id, "stored distill trigger ID");
    return parsePersistedDistillTrigger(row, this.triggerJobIds(id));
  }

  private lookupDistillFailure(kind: DistillTaskKind, id: string): DistillFailureRecord | undefined {
    const raw = this.connection.database.prepare(
      `SELECT ${distillFailureColumns} FROM distill_failures WHERE task_kind = ? AND task_id = ?`,
    ).get(kind, id);
    if (raw === undefined) return undefined;
    return this.parseDistillFailureRow(raw);
  }

  parseDistillFailureRow(value: unknown): DistillFailureRecord {
    const row = this.connection.decodeRow(fields(value, [
      "kind", "id", "reason", "sourceVersion", "modelKey", "policyVersion", "attempts", "nextAttemptAt",
    ]));
    return parseDistillFailureRecord({
      kind: row["kind"],
      id: row["id"],
      reason: row["reason"],
      context: {
        sourceVersion: row["sourceVersion"],
        modelKey: row["modelKey"],
        policyVersion: row["policyVersion"],
      },
      attempts: row["attempts"],
      nextAttemptAt: row["nextAttemptAt"],
    });
  }

  taskSweepThroughCursor(
    table: "distill_triggers" | "consolidation_jobs" | "session_observations" | "session_usages",
    requested: number | undefined,
  ): number {
    if (requested !== undefined) return requested;
    const row = this.connection.database.prepare(`SELECT COALESCE(MAX(rowid), 0) AS throughCursor FROM ${table}`).get();
    if (row === undefined) throw new Error("Task sweep upper cursor could not be read.");
    const throughCursor = row["throughCursor"];
    validateNow(throughCursor, "Stored task sweep upper cursor");
    return throughCursor;
  }

  private distillTaskCanAttempt(
    kind: DistillTaskKind,
    id: string,
    context: DistillRetryContext | undefined,
    now: number,
    retryAt: number | null,
  ): boolean {
    const failure = this.lookupDistillFailure(kind, id);
    if (failure !== undefined) return canRetryDistillFailure(failure, context, now);
    return retryAt === null || retryAt <= now;
  }

  private persistDistillFailure(value: DistillFailureRecord): void {
    const failure = parseDistillFailureRecord(value);
    this.connection.database.prepare(
      "INSERT INTO distill_failures " +
      "(task_kind, task_id, reason, source_version, model_key, policy_version, attempts, next_attempt_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
      "ON CONFLICT(task_kind, task_id) DO UPDATE SET reason = excluded.reason, " +
      "source_version = excluded.source_version, model_key = excluded.model_key, " +
      "policy_version = excluded.policy_version, attempts = excluded.attempts, " +
      "next_attempt_at = excluded.next_attempt_at",
    ).run(
      failure.kind,
      failure.id,
      failure.reason,
      failure.context.sourceVersion,
      failure.context.modelKey,
      failure.context.policyVersion,
      failure.attempts,
      failure.nextAttemptAt,
    );
  }

  clearDistillFailure(kind: DistillTaskKind, id: string): void {
    this.connection.database.prepare("DELETE FROM distill_failures WHERE task_kind = ? AND task_id = ?").run(kind, id);
  }

  distillTriggerItem(record: PersistedDistillTrigger): DistillTrigger {
    return {
      ...record.trigger,
      status: record.status,
      compactionRecordId: record.compactionRecordId,
      jobIds: record.jobIds,
      retryCategory: record.retryCategory,
      retryAt: record.retryAt,
    };
  }

  private triggerJobIds(triggerId: string): readonly string[] {
    const rows = this.connection.database.prepare(
      `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
      `${textColumn("job_id", "jobId")}, ordinal ` +
      "FROM distill_trigger_jobs WHERE trigger_id = ? ORDER BY ordinal, rowid",
    ).all(triggerId);
    const result: string[] = [];
    const seen = new Set<string>();
    for (let index = 0; index < rows.length; index += 1) {
      const link = fields(this.connection.decodeRow(rows[index] ?? {}), ["id", "triggerId", "jobId", "ordinal"]);
      validateText(link["id"], "distill trigger batch link ID");
      validateText(link["triggerId"], "distill trigger batch trigger ID");
      validateText(link["jobId"], "distill trigger batch job ID");
      if (link["triggerId"] !== triggerId || link["ordinal"] !== index || typeof link["ordinal"] !== "number") {
        throw new TypeError("Stored distill trigger batch order is invalid.");
      }
      const expectedId = createHash("sha256").update(JSON.stringify([triggerId, link["jobId"]])).digest("hex");
      if (link["id"] !== expectedId || seen.has(link["jobId"])) {
        throw new TypeError("Stored distill trigger batch identity is invalid.");
      }
      seen.add(link["jobId"]);
      result.push(link["jobId"]);
    }
    return result;
  }

  jobTriggerIds(jobId: string): readonly string[] {
    const rows = this.connection.database.prepare(
      `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
      `${textColumn("job_id", "jobId")} ` +
      "FROM distill_trigger_jobs WHERE job_id = ? ORDER BY rowid",
    ).all(jobId);
    return rows.map(raw => {
      const link = fields(this.connection.decodeRow(raw), ["id", "triggerId", "jobId"]);
      validateText(link["id"], "distill trigger batch link ID");
      validateText(link["triggerId"], "distill trigger batch trigger ID");
      validateText(link["jobId"], "distill trigger batch job ID");
      const expectedId = createHash("sha256").update(JSON.stringify([link["triggerId"], jobId])).digest("hex");
      if (link["jobId"] !== jobId || link["id"] !== expectedId) {
        throw new TypeError("Stored distill trigger batch identity is invalid.");
      }
      return link["triggerId"];
    });
  }

  readDistillLease(): PersistedDistillLease {
    const row = this.connection.database.prepare(
      "SELECT singleton, " + [
        textColumn("claim_token", "claimToken"), textColumn("worker_id", "workerId"),
        textColumn("task_kind", "taskKind"), textColumn("task_id", "taskId"), "lease_until AS leaseUntil",
      ].join(", ") + " FROM distill_lease WHERE singleton = 1",
    ).get();
    if (row === undefined) throw new TypeError("Global distill lease record is missing.");
    const { singleton, claimToken, workerId, taskKind, taskId, leaseUntil } = fields(
      this.connection.decodeRow(row), ["singleton", "claimToken", "workerId", "taskKind", "taskId", "leaseUntil"],
    );
    if (singleton !== 1) throw new TypeError("Global distill lease identity is invalid.");
    const parsedToken = parseNullableText(claimToken, "stored distill claim token");
    const parsedWorker = parseNullableText(workerId, "stored distill worker ID");
    const parsedTaskId = parseNullableText(taskId, "stored distill task ID");
    let parsedKind: "trigger" | "job" | null = null;
    if (taskKind !== null) {
      if (taskKind !== "trigger" && taskKind !== "job") throw new TypeError("Stored distill task kind is invalid.");
      parsedKind = taskKind;
    }
    let parsedUntil: number | null = null;
    if (leaseUntil !== null) {
      validateNow(leaseUntil, "Stored global distill lease expiration");
      parsedUntil = leaseUntil;
    }
    const allNull = parsedToken === null && parsedWorker === null && parsedKind === null && parsedTaskId === null && parsedUntil === null;
    const allPresent = parsedToken !== null && parsedWorker !== null && parsedKind !== null && parsedTaskId !== null && parsedUntil !== null;
    if (!allNull && !allPresent) throw new TypeError("Stored global distill lease state is inconsistent.");
    return { claimToken: parsedToken, workerId: parsedWorker, taskKind: parsedKind, taskId: parsedTaskId, leaseUntil: parsedUntil };
  }

  private acquireDistillSlot(input: {
    readonly claimToken: string;
    readonly workerId: string;
    readonly taskKind: "trigger" | "job";
    readonly taskId: string;
    readonly now: number;
    readonly leaseMs: number;
  }): boolean {
    const current = this.readDistillLease();
    if (current.claimToken !== null && current.leaseUntil !== null) {
      if (current.leaseUntil > input.now || current.claimToken === input.claimToken) return false;
      if (current.taskKind === "job" && current.taskId !== null) {
        this.connection.database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL " +
          "WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(current.taskId, current.claimToken);
      }
    }
    // Schema v3 allowed independent per-job claims. Keep a migrated live claim
    // from racing the new database-wide lease, even when it belongs to another job.
    const legacyClaim = this.connection.database.prepare(
      "SELECT 1 FROM consolidation_jobs WHERE status = 'pending' " +
      "AND lease_owner IS NOT NULL AND lease_until > ? LIMIT 1",
    ).get(input.now);
    if (legacyClaim !== undefined) return false;
    this.connection.database.prepare(
      "UPDATE distill_lease SET claim_token = ?, worker_id = ?, task_kind = ?, task_id = ?, lease_until = ? " +
      "WHERE singleton = 1",
    ).run(input.claimToken, input.workerId, input.taskKind, input.taskId, input.now + input.leaseMs);
    const table = input.taskKind === "job" ? "consolidation_jobs" : "distill_triggers";
    const column = input.taskKind === "job" ? "job_json" : "trigger_json";
    const row = this.connection.database.prepare(`SELECT json_quote(json_extract(${column}, '$.sessionId')) AS session FROM ${table} WHERE id=?`).get(input.taskId);
    const value = row?.["session"];
    if (typeof value !== "string") throw new TypeError("Missing scheduling session.");
    const sessionId: unknown = JSON.parse(value); validateText(sessionId, "scheduling session");
    this.schedule().turn(sessionId);
    return true;
  }

  assertCurrentDistillLease(
    taskKind: "trigger" | "job",
    taskId: string,
    claimToken: string,
    now: number,
  ): void {
    const lease = this.readDistillLease();
    if (
      lease.claimToken !== claimToken || lease.taskKind !== taskKind || lease.taskId !== taskId ||
      lease.leaseUntil === null || lease.leaseUntil <= now
    ) throw new ConsolidationConflictError(taskId, "has no current global distill lease for this owner");
  }

  clearDistillLease(): void {
    const lease = this.readDistillLease();
    if (lease.taskKind !== null && lease.taskId !== null) {
      const table = lease.taskKind === "job" ? "consolidation_jobs" : "distill_triggers";
      const column = lease.taskKind === "job" ? "job_json" : "trigger_json";
      const row = this.connection.database.prepare(`SELECT json_quote(json_extract(${column}, '$.sessionId')) AS session FROM ${table} WHERE id=?`).get(lease.taskId);
      const key = row?.["session"];
      if (typeof key === "string") {
        const sessionId: unknown = JSON.parse(key); validateText(sessionId, "scheduling session");
        if (!this.schedule().front(sessionId)) this.connection.database.prepare("DELETE FROM distill_schedule WHERE session_key=?").run(key);
      }
    }
    this.connection.database.prepare(
      "UPDATE distill_lease SET claim_token = NULL, worker_id = NULL, task_kind = NULL, task_id = NULL, lease_until = NULL " +
      "WHERE singleton = 1",
    ).run();
  }

  private claimConsolidationJob(
    id: string,
    request: DistillClaimInput,
    claimToken: string,
  ): DistillJobClaim | undefined {
    const row = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(id);
    if (row === undefined) return undefined;
    const current = parsePersistedJobRow(this.connection.decodeRow(row));
    if (current.status !== "pending" ||
      !this.distillTaskCanAttempt("job", id, request.retryContext, request.now, current.retryAt)) return undefined;
    if (current.leaseUntil !== null && current.leaseUntil > request.now) return undefined;
    const earlierPending = this.connection.database.prepare(
      "SELECT 1 FROM consolidation_jobs AS earlier " +
      "JOIN consolidation_jobs AS current ON current.id = ? " +
      "WHERE earlier.rowid < current.rowid AND earlier.status = 'pending' " +
      "AND json_extract(earlier.job_json, '$.sessionId') = ? LIMIT 1",
    ).get(id, current.job.sessionId);
    if (earlierPending !== undefined) return undefined;
    if (!this.acquireDistillSlot({
      claimToken,
      workerId: request.workerId,
      taskKind: "job",
      taskId: id,
      now: request.now,
      leaseMs: request.leaseMs,
    })) return undefined;
    const leaseUntil = request.now + request.leaseMs;
    this.connection.database.prepare(
      "UPDATE consolidation_jobs SET lease_owner = ?, lease_until = ?, retry_category = NULL, retry_at = NULL " +
      "WHERE id = ? AND status = 'pending'",
    ).run(claimToken, leaseUntil, id);
    return { job: current.job, claimToken, leaseUntil };
  }

  private enqueueConsolidationInTransaction(record: ConsolidationJob): void {
    if (record.sessionFile !== null) this.dependencies.sessions.assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
    const serialized = JSON.stringify(record);
    const row = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(record.id);
    if (row !== undefined) {
      const existing = parsePersistedJobRow(this.connection.decodeRow(row));
      if (JSON.stringify(existing.job) !== serialized) {
        throw new ConsolidationConflictError(record.id, "already exists with a different payload");
      }
      return;
    }
    this.schedule().enqueue(record.sessionId);
    this.connection.database.prepare(
      "INSERT INTO consolidation_jobs " +
      "(id, job_json, status, lease_owner, lease_until, retry_category, retry_at) " +
      "VALUES (?, ?, 'pending', NULL, NULL, NULL, NULL)",
    ).run(record.id, serialized);
  }

  private assertExistingJobPayload(record: ConsolidationJob): void {
    const row = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(record.id);
    if (row === undefined) throw new ConsolidationConflictError(record.id, "is missing from its prepared trigger");
    const existing = parsePersistedJobRow(this.connection.decodeRow(row));
    if (JSON.stringify(existing.job) !== JSON.stringify(record)) {
      throw new ConsolidationConflictError(record.id, "already exists with a different payload");
    }
  }

  private completeReadyTriggers(triggerIds: readonly string[]): void {
    for (const triggerId of new Set(triggerIds)) {
      const row = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(triggerId);
      if (row === undefined) throw new ConsolidationConflictError(triggerId, "has a missing trigger record");
      const current = this.parseTriggerRow(this.connection.decodeRow(row));
      if (current.status !== "prepared") continue;
      const remaining = this.connection.database.prepare(
        "SELECT 1 FROM distill_trigger_jobs AS links " +
        "JOIN consolidation_jobs AS jobs ON jobs.id = links.job_id " +
        "WHERE links.trigger_id = ? AND jobs.status = 'pending' LIMIT 1",
      ).get(triggerId);
      if (remaining !== undefined) continue;
      this.connection.database.prepare(
        "UPDATE distill_triggers SET status = 'complete', retry_category = NULL, retry_at = NULL " +
        "WHERE id = ? AND status = 'prepared'",
      ).run(triggerId);
      this.dependencies.retention.enqueueCleanTaskInTransaction({
        sessionId: current.trigger.sessionId,
        sessionFile: current.trigger.sessionFile,
        leafId: current.trigger.leafId,
        cwd: current.trigger.cwd,
        reason: current.trigger.reason,
        intentId: current.trigger.id,
      });
    }
  }

  finishConsolidationJob(job: ConsolidationJob, claimToken: string): void {
    const triggerIds = this.jobTriggerIds(job.id);
    if (triggerIds.length > 0) this.completeReadyTriggers(triggerIds);
    else if (job.sessionFile !== null) {
      this.dependencies.retention.enqueueCleanTaskInTransaction({
        sessionId: job.sessionId,
        sessionFile: job.sessionFile,
        leafId: job.leafId,
        cwd: job.cwd,
        reason: "legacy",
        intentId: "legacy-job:" + job.id,
      });
    }
    const lease = this.readDistillLease();
    if (lease.claimToken !== claimToken || lease.taskKind !== "job" || lease.taskId !== job.id) {
      throw new ConsolidationConflictError(job.id, "lost its global distill lease before completion");
    }
    this.clearDistillLease();
  }
}
