import { isAbsolute } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type {
  ConsolidationCommit,
  ConsolidationJob,
  SessionObservation,
} from "./consolidation-types.js";
import type { SessionUsage } from "./session-usage-types.js";
import type {
  CleanTask,
  CleanTaskInput,
  CleanTaskPageOptions,
  DistillClaimInput,
  DistillJobClaim,
  DistillTrigger,
  DistillTriggerInput,
  DistillTriggerClaim,
  PrepareDistillTriggerInput,
  ReleaseDistillClaimInput,
  RetryCategory,
  RetryRequest,
  TaskPage,
  TaskPageOptions,
  WaitReason,
} from "./task-types.js";

const initialColumns = `
  id TEXT PRIMARY KEY NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL`;
const legacySchema = `CREATE TABLE memories (${initialColumns}) STRICT`;
const currentSchema = `CREATE TABLE memories (${initialColumns},
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL
) STRICT`;
const maxSearchBatchSize = 128;
const maxKnownSessionFiles = 1_024;
// Assessment reads validate at most this many persisted metadata records per snapshot.
const maxSessionMetadataRows = 16_384;

const consolidationJobsV3Schema =
  "CREATE TABLE consolidation_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "job_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";
const consolidationJobsSchema =
  "CREATE TABLE consolidation_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "job_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL)), " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL))" +
  ") STRICT";
const consolidationProgressSchema =
  "CREATE TABLE consolidation_progress (" +
  "source_key TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "processed_at INTEGER NOT NULL" +
  ") STRICT";
const memoryOriginsSchema =
  "CREATE TABLE memory_origins (" +
  "memory_id TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "revision INTEGER NOT NULL" +
  ") STRICT";
const sessionObservationsSchema =
  "CREATE TABLE session_observations (" +
  "session_id TEXT PRIMARY KEY NOT NULL, " +
  "session_file TEXT NOT NULL, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "owner_pid INTEGER NOT NULL, " +
  "captured INTEGER NOT NULL DEFAULT 0 CHECK (captured IN (0, 1))" +
  ") STRICT";
const sessionUsagesSchema =
  "CREATE TABLE session_usages (" +
  "owner_id TEXT PRIMARY KEY NOT NULL, " +
  "process_token TEXT NOT NULL, " +
  "session_id TEXT NOT NULL, " +
  "session_file TEXT NOT NULL, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "owner_pid INTEGER NOT NULL CHECK (owner_pid > 0), " +
  "state TEXT NOT NULL CHECK (state IN ('active', 'closing', 'released'))" +
  ") STRICT";
const distillTriggersSchema =
  "CREATE TABLE distill_triggers (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "trigger_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('candidate', 'pending', 'prepared', 'complete')), " +
  "compaction_record_id TEXT, " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL))" +
  ") STRICT";
const triggerJobsSchema =
  "CREATE TABLE distill_trigger_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "trigger_id TEXT NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "ordinal INTEGER NOT NULL CHECK (ordinal >= 0)" +
  ") STRICT";
const distillLeaseSchema =
  "CREATE TABLE distill_lease (" +
  "singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), " +
  "claim_token TEXT, " +
  "worker_id TEXT, " +
  "task_kind TEXT CHECK (task_kind IS NULL OR task_kind IN ('trigger', 'job')), " +
  "task_id TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((claim_token IS NULL AND worker_id IS NULL AND task_kind IS NULL AND " +
  "task_id IS NULL AND lease_until IS NULL) OR " +
  "(claim_token IS NOT NULL AND worker_id IS NOT NULL AND task_kind IS NOT NULL AND " +
  "task_id IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";
const cleanTasksSchema =
  "CREATE TABLE clean_tasks (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "task_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'waiting')), " +
  "wait_reason TEXT CHECK (wait_reason IS NULL OR wait_reason IN " +
  "('host_coordination', 'unprocessed', 'usage', 'dependencies', 'unavailable')), " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((status = 'pending' AND wait_reason IS NULL) OR status = 'waiting'), " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL)), " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";
const sourceAccessSchema = "CREATE TABLE source_access (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), token TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0)) STRICT";
const deletionsSchema = "CREATE TABLE session_deletions (session_file TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL, file_version TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))) STRICT";
const latestSchemaVersion = 5;

export interface SessionDeletion {
  readonly sessionFile: string;
  readonly sessionId: string;
  readonly intentId: string;
  readonly fileVersion: string;
  readonly state: "deleting" | "complete";
}

/** ESRCH establishes exit; permission failures and PID reuse remain protected. */
export function processHasExited(pid: number): boolean {
  try { process.kill(pid, 0); return false; }
  catch (error) { return error instanceof Error && "code" in error && error.code === "ESRCH"; }
}
const maxTaskPageSize = 128;

// Node 22 truncates TEXT results at NUL. Return bytes only at the result boundary;
// predicates and ordering must continue to use the original TEXT columns.
function textColumn(column: string, alias = column): string {
  // Keep NULL distinct and make other storage classes fail text validation.
  return `CASE typeof(${column}) WHEN 'text' THEN CAST(${column} AS BLOB) WHEN 'null' THEN NULL ELSE 0 END AS ${alias}`;
}

const memoryColumns = [
  textColumn("id"), textColumn("content"), textColumn("created_at", "createdAt"),
  "revision", textColumn("updated_at", "updatedAt"),
].join(", ");
const jobColumns = [
  textColumn("id"), textColumn("job_json"), textColumn("status"),
  textColumn("lease_owner"), "lease_until", textColumn("retry_category", "retryCategory"), "retry_at AS retryAt",
].join(", ");
const legacyJobColumns = [
  textColumn("id"), textColumn("job_json"), textColumn("status"),
  textColumn("lease_owner"), "lease_until", "NULL AS retryCategory", "NULL AS retryAt",
].join(", ");
const triggerColumns = [
  textColumn("id"), textColumn("trigger_json", "triggerJson"), textColumn("status"),
  textColumn("compaction_record_id", "compactionRecordId"),
  textColumn("retry_category", "retryCategory"), "retry_at AS retryAt",
].join(", ");
const cleanTaskColumns = [
  textColumn("id"), textColumn("task_json", "taskJson"), textColumn("status"),
  textColumn("wait_reason", "waitReason"), textColumn("retry_category", "retryCategory"),
  "retry_at AS retryAt", textColumn("lease_owner", "leaseOwner"), "lease_until AS leaseUntil",
].join(", ");
const progressColumns = [textColumn("source_key"), textColumn("job_id"), "processed_at"].join(", ");
const originColumns = [textColumn("memory_id", "memoryId"), textColumn("job_id", "jobId"), "revision"].join(", ");
const observationColumns = [
  textColumn("session_id"), textColumn("session_file"), textColumn("cwd"),
  textColumn("leaf_id"), "owner_pid", "captured",
].join(", ");
const sessionUsageColumns = [
  textColumn("owner_id", "ownerId"), textColumn("process_token", "processToken"),
  textColumn("session_id", "sessionId"), textColumn("session_file", "sessionFile"),
  textColumn("cwd"), textColumn("leaf_id", "leafId"), "owner_pid AS ownerPid",
  textColumn("state"),
].join(", ");
const schemaColumns = [textColumn("name"), textColumn("type"), textColumn("sql")].join(", ");

interface PersistedDistillTrigger {
  readonly trigger: DistillTriggerInput;
  readonly status: DistillTrigger["status"];
  readonly compactionRecordId: string | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
  readonly jobIds: readonly string[];
}

interface PersistedCleanTask {
  readonly task: CleanTaskInput;
  readonly status: CleanTask["status"];
  readonly waitReason: WaitReason | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
  readonly leaseOwner: string | null;
  readonly leaseUntil: number | null;
}

interface PersistedDistillLease {
  readonly claimToken: string | null;
  readonly workerId: string | null;
  readonly taskKind: "trigger" | "job" | null;
  readonly taskId: string | null;
  readonly leaseUntil: number | null;
}

interface PersistedConsolidationJob {
  readonly job: ConsolidationJob;
  readonly status: "pending" | "complete";
  readonly leaseOwner: string | null;
  readonly leaseUntil: number | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
}

interface SchemaColumn {
  readonly name: string;
  readonly type: "TEXT" | "INTEGER";
  readonly notNull: 0 | 1;
  readonly primaryKey: 0 | 1;
}

export interface MemoryInput {
  readonly id: string;
  readonly content: string;
  readonly createdAt: string;
}

export interface Memory extends MemoryInput {
  readonly revision: number;
  readonly updatedAt: string;
}

export interface MemoryOrigin {
  readonly sessionId: string;
  readonly sourceKeys: readonly string[];
  readonly lineageSessionIds?: readonly string[];
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

export interface MemoryUpdate {
  readonly id: string;
  readonly expectedRevision: number;
  readonly content: string;
  readonly updatedAt: string;
}

export interface MemoryForget {
  readonly id: string;
  readonly expectedRevision: number;
}

export interface MemorySearchRequest {
  readonly terms: readonly string[];
  readonly match: "any" | "all";
  readonly after?: string;
  readonly limit?: number;
}

export interface MemorySearchRow {
  readonly memory: Memory;
  readonly matchedTerms: readonly string[];
}

export class MemoryConflictError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`A different memory already exists for ID "${memoryId}".`);
    this.name = "MemoryConflictError";
    this.memoryId = memoryId;
  }
}

export class MemoryNotFoundError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`No memory exists for ID "${memoryId}".`);
    this.name = "MemoryNotFoundError";
    this.memoryId = memoryId;
  }
}

export class ConsolidationConflictError extends Error {
  readonly resourceId: string;

  constructor(resourceId: string, reason = "conflicts with current consolidation state") {
    super("Consolidation record \"" + resourceId + "\" " + reason + ".");
    this.name = "ConsolidationConflictError";
    this.resourceId = resourceId;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fields(value: unknown, names: readonly string[]): Record<string, unknown> {
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).length !== names.length ||
    names.some((name) => !Object.hasOwn(value, name))
  ) {
    throw new TypeError(`Memory data requires exactly these fields: ${names.join(", ")}.`);
  }
  return value;
}

function validateText(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`Memory ${field} must be a nonempty string.`);
  }
  // Unicode mode matches lone surrogates, while valid pairs remain single code points.
  if (/[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`Memory ${field} must contain well-formed Unicode.`);
  }
}

function validateTimestamp(value: unknown, field: string): asserts value is string {
  if (
    typeof value !== "string" ||
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString() !== value
  ) {
    throw new TypeError(`Memory ${field} must be a canonical UTC ISO timestamp.`);
  }
}

function validateRevision(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new TypeError("Memory revision must be a positive safe integer.");
  }
}

function parseInput(value: unknown): MemoryInput {
  const { id, content, createdAt } = fields(value, ["id", "content", "createdAt"]);
  validateText(id, "ID");
  validateText(content, "content");
  validateTimestamp(createdAt, "createdAt");
  return { id, content, createdAt };
}

function parseMemory(value: unknown): Memory {
  const { id, content, createdAt, revision, updatedAt } = fields(
    value, ["id", "content", "createdAt", "revision", "updatedAt"],
  );
  const input = parseInput({ id, content, createdAt });
  validateRevision(revision);
  validateTimestamp(updatedAt, "updatedAt");
  return { ...input, revision, updatedAt };
}

function parseUpdate(value: unknown): MemoryUpdate {
  const { id, expectedRevision, content, updatedAt } = fields(
    value, ["id", "expectedRevision", "content", "updatedAt"],
  );
  validateText(id, "ID");
  validateRevision(expectedRevision);
  validateText(content, "content");
  validateTimestamp(updatedAt, "updatedAt");
  return { id, expectedRevision, content, updatedAt };
}

function parseForget(value: unknown): MemoryForget {
  const { id, expectedRevision } = fields(value, ["id", "expectedRevision"]);
  validateText(id, "ID");
  validateRevision(expectedRevision);
  return { id, expectedRevision };
}

function validateAbsolutePath(value: unknown, field: string): asserts value is string {
  validateText(value, field);
  if (!isAbsolute(value) || value.includes("\u0000")) {
    throw new TypeError(field + " must be an absolute filesystem path.");
  }
}

function parseNullableText(value: unknown, field: string): string | null {
  if (value === null) return null;
  validateText(value, field);
  return value;
}

function parseSourceKeys(value: unknown, field: string, allowEmpty: boolean): readonly string[] {
  if (!Array.isArray(value) || (!allowEmpty && value.length === 0)) {
    throw new TypeError(field + " must be " + (allowEmpty ? "an array." : "a nonempty array."));
  }
  const result: string[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < value.length; index += 1) {
    const key: unknown = value[index];
    validateText(key, field + " item " + (index + 1));
    if (seen.has(key)) throw new TypeError(field + " must not contain duplicate keys.");
    seen.add(key);
    result.push(key);
  }
  return result;
}

function parseConsolidationJob(value: unknown): ConsolidationJob {
  const required = ["id", "sessionId", "sessionFile", "leafId", "cwd", "sourceKeys"];
  const allowed = [...required, "lineageSessionIds", "sourceTime", "branchAnchor"];
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).some((key) => typeof key !== "string" || !allowed.includes(key)) ||
    required.some((name) => !Object.hasOwn(value, name))
  ) throw new TypeError("Consolidation job requires its identity, source, and path fields.");
  const { id, sessionId, sessionFile, leafId, cwd, sourceKeys } = value;
  validateText(id, "consolidation job ID");
  validateText(sessionId, "session ID");
  if (sessionFile !== null) validateAbsolutePath(sessionFile, "session file");
  const parsedLeafId = parseNullableText(leafId, "session leaf ID");
  validateAbsolutePath(cwd, "session working directory");
  const parsed: {
    id: string;
    sessionId: string;
    sessionFile: string | null;
    leafId: string | null;
    cwd: string;
    sourceKeys: readonly string[];
    lineageSessionIds?: readonly string[];
    sourceTime?: string;
    branchAnchor?: string;
  } = {
    id,
    sessionId,
    sessionFile,
    leafId: parsedLeafId,
    cwd,
    sourceKeys: parseSourceKeys(sourceKeys, "consolidation source keys", false),
  };
  if (Object.hasOwn(value, "lineageSessionIds")) {
    parsed.lineageSessionIds = parseSourceKeys(
      value["lineageSessionIds"],
      "consolidation lineage session IDs",
      false,
    );
  }
  if (Object.hasOwn(value, "sourceTime")) {
    const sourceTime = value["sourceTime"];
    validateTimestamp(sourceTime, "consolidation source time");
    parsed.sourceTime = sourceTime;
  }
  if (Object.hasOwn(value, "branchAnchor")) {
    const branchAnchor = value["branchAnchor"];
    validateText(branchAnchor, "consolidation branch anchor");
    parsed.branchAnchor = branchAnchor;
  }
  return parsed;
}

function parseSessionObservation(value: unknown): SessionObservation {
  const { sessionId, sessionFile, cwd, leafId, ownerPid } = fields(
    value, ["sessionId", "sessionFile", "cwd", "leafId", "ownerPid"],
  );
  validateText(sessionId, "session ID");
  validateAbsolutePath(sessionFile, "session file");
  validateAbsolutePath(cwd, "session working directory");
  const parsedLeafId = parseNullableText(leafId, "session leaf ID");
  if (typeof ownerPid !== "number" || !Number.isSafeInteger(ownerPid) || ownerPid < 1) {
    throw new TypeError("Session owner PID must be a positive safe integer.");
  }
  return { sessionId, sessionFile, cwd, leafId: parsedLeafId, ownerPid };
}

function parseSessionUsage(value: unknown): SessionUsage {
  const { ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state } = fields(
    value,
    ["sessionId", "sessionFile", "cwd", "leafId", "ownerPid", "ownerId", "processToken", "state"],
  );
  validateText(ownerId, "session usage owner ID");
  validateText(processToken, "session usage process token");
  if (state !== "active" && state !== "closing" && state !== "released") {
    throw new TypeError("Session usage state must be active, closing, or released.");
  }
  return {
    ...parseSessionObservation({ sessionId, sessionFile, cwd, leafId, ownerPid }),
    ownerId,
    processToken,
    state,
  };
}

function parseStoredSessionObservation(value: unknown): {
  readonly observation: SessionObservation;
  readonly captured: boolean;
} {
  const { session_id, session_file, cwd, leaf_id, owner_pid, captured } = fields(
    value, ["session_id", "session_file", "cwd", "leaf_id", "owner_pid", "captured"],
  );
  if (captured !== 0 && captured !== 1) {
    throw new TypeError("Stored session observation capture state is invalid.");
  }
  return {
    observation: parseSessionObservation({
      sessionId: session_id,
      sessionFile: session_file,
      cwd,
      leafId: leaf_id,
      ownerPid: owner_pid,
    }),
    captured: captured === 1,
  };
}

function parseStoredSessionUsage(value: unknown): SessionUsage {
  const { ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state } = fields(
    value,
    ["ownerId", "processToken", "sessionId", "sessionFile", "cwd", "leafId", "ownerPid", "state"],
  );
  return parseSessionUsage({ ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state });
}

function sameSessionUsageIdentity(first: SessionUsage, second: SessionUsage): boolean {
  return first.ownerId === second.ownerId &&
    first.processToken === second.processToken &&
    first.sessionId === second.sessionId &&
    first.sessionFile === second.sessionFile &&
    first.cwd === second.cwd &&
    first.ownerPid === second.ownerPid;
}

function validateNow(value: unknown, field: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(field + " must be a nonnegative safe integer.");
  }
}

function parseConsolidationCommit(value: unknown): ConsolidationCommit {
  const required = ["jobId", "owner", "now", "creates", "updates", "deletes"];
  const allowed = [...required, "sourceKeys"];
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).some((key) => typeof key !== "string" || !allowed.includes(key)) ||
    required.some((name) => !Object.hasOwn(value, name))
  ) throw new TypeError("Consolidation commit requires jobId, owner, now, creates, updates, and deletes.");
  const { jobId, owner, now, creates, updates, deletes } = value;
  validateText(jobId, "consolidation job ID");
  validateText(owner, "consolidation lease owner");
  validateNow(now, "Consolidation commit time");
  if (!Array.isArray(creates) || !Array.isArray(updates) || !Array.isArray(deletes)) {
    throw new TypeError("Consolidation changes must be arrays.");
  }

  const parsedCreates: MemoryInput[] = [];
  const parsedUpdates: MemoryUpdate[] = [];
  const parsedDeletes: MemoryForget[] = [];
  const changedIds = new Set<string>();
  for (let index = 0; index < creates.length; index += 1) {
    const parsed = parseInput(creates[index]);
    if (changedIds.has(parsed.id)) throw new TypeError("A memory may appear only once in a consolidation commit.");
    changedIds.add(parsed.id);
    parsedCreates.push(parsed);
  }
  for (let index = 0; index < updates.length; index += 1) {
    const parsed = parseUpdate(updates[index]);
    if (changedIds.has(parsed.id)) throw new TypeError("A memory may appear only once in a consolidation commit.");
    changedIds.add(parsed.id);
    parsedUpdates.push(parsed);
  }
  for (let index = 0; index < deletes.length; index += 1) {
    const parsed = parseForget(deletes[index]);
    if (changedIds.has(parsed.id)) throw new TypeError("A memory may appear only once in a consolidation commit.");
    changedIds.add(parsed.id);
    parsedDeletes.push(parsed);
  }

  const parsed: {
    jobId: string;
    owner: string;
    now: number;
    creates: readonly MemoryInput[];
    updates: readonly MemoryUpdate[];
    deletes: readonly MemoryForget[];
    sourceKeys?: readonly string[];
  } = { jobId, owner, now, creates: parsedCreates, updates: parsedUpdates, deletes: parsedDeletes };
  if (Object.hasOwn(value, "sourceKeys")) {
    parsed.sourceKeys = parseSourceKeys(value["sourceKeys"], "consolidation commit source keys", true);
  }
  return parsed;
}

function parsePersistedJobRow(value: unknown): PersistedConsolidationJob {
  const { id, job_json, status, lease_owner, lease_until, retryCategory, retryAt } = fields(
    value, ["id", "job_json", "status", "lease_owner", "lease_until", "retryCategory", "retryAt"],
  );
  if (typeof job_json !== "string") throw new TypeError("Stored consolidation job JSON must be text.");
  let decoded: unknown;
  try {
    decoded = JSON.parse(job_json);
  } catch {
    throw new TypeError("Stored consolidation job JSON is invalid.");
  }
  const job = parseConsolidationJob(decoded);
  if (JSON.stringify(job) !== job_json || id !== job.id) {
    throw new TypeError("Stored consolidation job record is not canonical or has a mismatched ID.");
  }
  if (status !== "pending" && status !== "complete") {
    throw new TypeError("Stored consolidation job status is invalid.");
  }
  const leaseOwner = parseNullableText(lease_owner, "consolidation lease owner");
  let leaseUntil: number | null = null;
  if (lease_until !== null) {
    validateNow(lease_until, "Stored consolidation lease expiration");
    leaseUntil = lease_until;
  }
  if ((leaseOwner === null) !== (leaseUntil === null) || (status === "complete" && leaseOwner !== null)) {
    throw new TypeError("Stored consolidation lease state is inconsistent.");
  }
  const parsedRetry = parseRetryFields(retryCategory, retryAt, "Stored consolidation retry");
  if (status === "complete" && parsedRetry.retryCategory !== null) {
    throw new TypeError("Stored completed consolidation job has retry metadata.");
  }
  return { job, status, leaseOwner, leaseUntil, ...parsedRetry };
}

function parseRetryCategory(value: unknown, field: string): RetryCategory | null {
  if (value === null) return null;
  if (value !== "transient" && value !== "unavailable" && value !== "invalid") {
    throw new TypeError(field + " category is invalid.");
  }
  return value;
}

function parseRetryFields(
  categoryValue: unknown,
  atValue: unknown,
  field: string,
): { readonly retryCategory: RetryCategory | null; readonly retryAt: number | null } {
  const retryCategory = parseRetryCategory(categoryValue, field);
  let retryAt: number | null = null;
  if (atValue !== null) {
    validateNow(atValue, field + " time");
    retryAt = atValue;
  }
  if ((retryCategory === null) !== (retryAt === null)) {
    throw new TypeError(field + " metadata is inconsistent.");
  }
  return { retryCategory, retryAt };
}

function parseDistillTriggerInput(value: unknown): DistillTriggerInput {
  const { id, sessionId, sessionFile, leafId, cwd, reason } = fields(
    value, ["id", "sessionId", "sessionFile", "leafId", "cwd", "reason"],
  );
  validateText(id, "distill trigger ID");
  validateText(sessionId, "distill trigger session ID");
  validateAbsolutePath(sessionFile, "distill trigger session file");
  validateAbsolutePath(cwd, "distill trigger working directory");
  const parsedLeafId = parseNullableText(leafId, "distill trigger leaf ID");
  if (reason !== "close" && reason !== "compact") {
    throw new TypeError("Distill trigger reason must be close or compact.");
  }
  return { id, sessionId, sessionFile, leafId: parsedLeafId, cwd, reason };
}

function parsePersistedDistillTrigger(value: unknown, jobIds: readonly string[]): PersistedDistillTrigger {
  const { id, triggerJson, status, compactionRecordId, retryCategory, retryAt } = fields(
    value, ["id", "triggerJson", "status", "compactionRecordId", "retryCategory", "retryAt"],
  );
  if (typeof triggerJson !== "string") throw new TypeError("Stored distill trigger JSON must be text.");
  let decoded: unknown;
  try { decoded = JSON.parse(triggerJson); }
  catch { throw new TypeError("Stored distill trigger JSON is invalid."); }
  const trigger = parseDistillTriggerInput(decoded);
  if (JSON.stringify(trigger) !== triggerJson || id !== trigger.id) {
    throw new TypeError("Stored distill trigger is not canonical or has a mismatched ID.");
  }
  if (status !== "candidate" && status !== "pending" && status !== "prepared" && status !== "complete") {
    throw new TypeError("Stored distill trigger status is invalid.");
  }
  const parsedCompactionId = parseNullableText(compactionRecordId, "stored compaction record ID");
  if (
    (trigger.reason === "close" && (status === "candidate" || parsedCompactionId !== null)) ||
    (trigger.reason === "compact" && ((status === "candidate") !== (parsedCompactionId === null)))
  ) throw new TypeError("Stored compaction trigger confirmation state is inconsistent.");
  if ((status === "candidate" || status === "pending") && jobIds.length !== 0) {
    throw new TypeError("Unprepared distill trigger has associated batches.");
  }
  if (status === "prepared" && jobIds.length === 0) {
    throw new TypeError("Prepared distill trigger has no associated batches.");
  }
  const parsedRetry = parseRetryFields(retryCategory, retryAt, "Stored distill trigger retry");
  if (status === "complete" && parsedRetry.retryCategory !== null) {
    throw new TypeError("Stored completed distill trigger has retry metadata.");
  }
  return { trigger, status, compactionRecordId: parsedCompactionId, ...parsedRetry, jobIds: [...jobIds] };
}

function parseCleanTaskInput(value: unknown): CleanTaskInput {
  const { sessionId, sessionFile, leafId, cwd, reason, intentId } = fields(
    value, ["sessionId", "sessionFile", "leafId", "cwd", "reason", "intentId"],
  );
  validateText(sessionId, "clean task session ID");
  validateAbsolutePath(sessionFile, "clean task session file");
  validateAbsolutePath(cwd, "clean task working directory");
  const parsedLeafId = parseNullableText(leafId, "clean task leaf ID");
  if (reason !== "close" && reason !== "compact" && reason !== "legacy") {
    throw new TypeError("Clean task reason is invalid.");
  }
  validateText(intentId, "clean task intent ID");
  return { sessionId, sessionFile, leafId: parsedLeafId, cwd, reason, intentId };
}

function cleanTaskId(sessionFile: string): string {
  return "clean-" + createHash("sha256").update(sessionFile).digest("hex");
}

function parsePersistedCleanTask(value: unknown): PersistedCleanTask {
  const { id, taskJson, status, waitReason, retryCategory, retryAt, leaseOwner, leaseUntil } = fields(
    value,
    ["id", "taskJson", "status", "waitReason", "retryCategory", "retryAt", "leaseOwner", "leaseUntil"],
  );
  if (typeof taskJson !== "string") throw new TypeError("Stored clean task JSON must be text.");
  let decoded: unknown;
  try { decoded = JSON.parse(taskJson); }
  catch { throw new TypeError("Stored clean task JSON is invalid."); }
  const task = parseCleanTaskInput(decoded);
  if (JSON.stringify(task) !== taskJson || id !== cleanTaskId(task.sessionFile)) {
    throw new TypeError("Stored clean task is not canonical or has a mismatched ID.");
  }
  if (status !== "pending" && status !== "waiting") throw new TypeError("Stored clean task status is invalid.");
  let parsedWaitReason: WaitReason | null = null;
  if (waitReason !== null) {
    if (
      waitReason !== "host_coordination" && waitReason !== "unprocessed" && waitReason !== "usage" &&
      waitReason !== "dependencies" && waitReason !== "unavailable"
    ) throw new TypeError("Stored clean task wait reason is invalid.");
    parsedWaitReason = waitReason;
  }
  if ((status === "pending") !== (parsedWaitReason === null)) {
    throw new TypeError("Stored clean task waiting state is inconsistent.");
  }
  const parsedRetry = parseRetryFields(retryCategory, retryAt, "Stored clean task retry");
  const parsedLeaseOwner = parseNullableText(leaseOwner, "stored clean task lease owner");
  let parsedLeaseUntil: number | null = null;
  if (leaseUntil !== null) {
    validateNow(leaseUntil, "Stored clean task lease expiration");
    parsedLeaseUntil = leaseUntil;
  }
  if ((parsedLeaseOwner === null) !== (parsedLeaseUntil === null)) {
    throw new TypeError("Stored clean task lease state is inconsistent.");
  }
  return {
    task,
    status,
    waitReason: parsedWaitReason,
    ...parsedRetry,
    leaseOwner: parsedLeaseOwner,
    leaseUntil: parsedLeaseUntil,
  };
}

function parseTaskPageOptions(value: unknown, allowNow: boolean): {
  readonly limit: number;
  readonly cursor?: number;
  readonly now?: number;
} {
  const allowed = allowNow ? ["limit", "cursor", "now"] : ["limit", "cursor"];
  if (
    !isRecord(value) || Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(value, "limit")
  ) throw new TypeError("Task page requires a limit and only supported cursor/time fields.");
  const limit = value["limit"];
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > maxTaskPageSize) {
    throw new TypeError("Task page limit must be an integer from 1 through " + maxTaskPageSize + ".");
  }
  const parsed: { limit: number; cursor?: number; now?: number } = { limit };
  if (Object.hasOwn(value, "cursor")) {
    const cursor = value["cursor"];
    if (typeof cursor !== "number" || !Number.isSafeInteger(cursor) || cursor < 0) {
      throw new TypeError("Task page cursor must be a nonnegative safe integer.");
    }
    parsed.cursor = cursor;
  }
  if (Object.hasOwn(value, "now")) {
    validateNow(value["now"], "Task page time");
    parsed.now = value["now"];
  }
  return parsed;
}

function parseDistillClaimInput(value: unknown): DistillClaimInput {
  const { workerId, now, leaseMs } = fields(value, ["workerId", "now", "leaseMs"]);
  validateText(workerId, "distill worker ID");
  validateNow(now, "Distill claim time");
  if (typeof leaseMs !== "number" || !Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new TypeError("Distill lease duration must be a positive safe integer.");
  }
  if (!Number.isSafeInteger(now + leaseMs)) throw new TypeError("Distill lease expiration is outside the safe integer range.");
  return { workerId, now, leaseMs };
}

function parseRetryRequest(value: unknown): RetryRequest {
  const { category, retryAt } = fields(value, ["category", "retryAt"]);
  const retryCategory = parseRetryCategory(category, "Retry");
  if (retryCategory === null) throw new TypeError("Retry category is required.");
  validateNow(retryAt, "Retry time");
  return { category: retryCategory, retryAt };
}

function parseReleaseDistillClaimInput(value: unknown): ReleaseDistillClaimInput {
  if (!isRecord(value)) throw new TypeError("Distill claim release must be an object.");
  const allowed = ["claimToken", "now", "retry"];
  if (Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key))) {
    throw new TypeError("Distill claim release has unsupported fields.");
  }
  const { claimToken, now } = value;
  validateText(claimToken, "distill claim token");
  validateNow(now, "Distill release time");
  return {
    claimToken,
    now,
    ...(Object.hasOwn(value, "retry") ? { retry: parseRetryRequest(value["retry"]) } : {}),
  };
}

function parsePrepareDistillTriggerInput(value: unknown): PrepareDistillTriggerInput {
  const { id, claimToken, now, jobs } = fields(value, ["id", "claimToken", "now", "jobs"]);
  validateText(id, "distill trigger ID");
  validateText(claimToken, "distill claim token");
  validateNow(now, "Distill preparation time");
  if (!Array.isArray(jobs)) throw new TypeError("Prepared consolidation batches must be an array.");
  const parsedJobs: ConsolidationJob[] = [];
  const seen = new Set<string>();
  for (const rawJob of jobs) {
    const parsed = parseConsolidationJob(rawJob);
    if (seen.has(parsed.id)) throw new TypeError("Prepared batches must have unique job IDs.");
    seen.add(parsed.id);
    parsedJobs.push(parsed);
  }
  return { id, claimToken, now, jobs: parsedJobs };
}

function validateTaskCursor(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(field + " must be a nonnegative safe integer.");
  }
  return value;
}

function validateLeaseDuration(value: unknown, now: number, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || !Number.isSafeInteger(now + value)) {
    throw new TypeError(field + " must be a positive safe integer that yields a safe expiration.");
  }
  return value;
}

function parseMemoryOriginRow(value: unknown): {
  readonly memoryId: string;
  readonly jobId: string;
  readonly revision: number;
} {
  const { memoryId, jobId, revision } = fields(value, ["memoryId", "jobId", "revision"]);
  validateText(memoryId, "memory origin ID");
  validateText(jobId, "memory origin job ID");
  validateRevision(revision);
  return { memoryId, jobId, revision };
}

function parseProgressRow(value: unknown): {
  readonly sourceKey: string;
  readonly jobId: string;
  readonly processedAt: number;
} {
  const { source_key, job_id, processed_at } = fields(value, ["source_key", "job_id", "processed_at"]);
  validateText(source_key, "processed source key");
  validateText(job_id, "processed source job ID");
  validateNow(processed_at, "Stored source processing time");
  return { sourceKey: source_key, jobId: job_id, processedAt: processed_at };
}

function validateSearchTerm(value: unknown, index: number): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`Memory search term ${index + 1} must be a nonempty string.`);
  }
  if (/[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`Memory search term ${index + 1} must contain well-formed Unicode.`);
  }
  if ([...value].length > 128) {
    throw new TypeError(`Memory search term ${index + 1} must not exceed 128 Unicode code points.`);
  }
}

function parseSearchRequest(value: unknown): MemorySearchRequest {
  const allowedFields: readonly string[] = ["terms", "match", "after", "limit"];
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).some((key) => typeof key !== "string" || !allowedFields.includes(key)) ||
    !Object.hasOwn(value, "terms") || !Object.hasOwn(value, "match")
  ) {
    throw new TypeError("Memory search requires terms and match, with optional after and limit fields.");
  }

  const rawTerms = value["terms"];
  if (!Array.isArray(rawTerms) || rawTerms.length < 1 || rawTerms.length > 8) {
    throw new TypeError("Memory search requires between 1 and 8 terms.");
  }
  const terms: string[] = [];
  for (let index = 0; index < rawTerms.length; index += 1) {
    const term: unknown = rawTerms[index];
    validateSearchTerm(term, index);
    terms.push(term);
  }

  const match = value["match"];
  if (match !== "any" && match !== "all") {
    throw new TypeError('Memory search match must be "any" or "all".');
  }

  const parsed: { terms: readonly string[]; match: "any" | "all"; after?: string; limit?: number } = {
    terms,
    match,
  };
  if (Object.hasOwn(value, "after")) {
    const after = value["after"];
    validateText(after, "search cursor ID");
    parsed.after = after;
  }
  if (Object.hasOwn(value, "limit")) {
    const limit = value["limit"];
    if (
      typeof limit !== "number" || !Number.isSafeInteger(limit) ||
      limit < 1 || limit > maxSearchBatchSize
    ) {
      throw new TypeError("Memory search limit must be an integer from 1 to 128.");
    }
    parsed.limit = limit;
  }
  return parsed;
}

export class SQLiteMemoryStore {
  readonly #database: DatabaseSync;
  readonly #textDecoder: TextDecoder;
  readonly #select: StatementSync;
  readonly #insert: StatementSync;
  readonly #update: StatementSync;
  readonly #delete: StatementSync;
  readonly #upsertOrigin: StatementSync;
  readonly #deleteOrigin: StatementSync;

  constructor(path: string) {
    if (typeof path !== "string" || !isAbsolute(path)) {
      throw new TypeError("Memory database requires an absolute file path.");
    }

    // Keep lock contention bounded; callers can retry a failed operation.
    this.#database = new DatabaseSync(path, { timeout: 1_000 });
    try {
      const encoding = this.#database.prepare("PRAGMA encoding").get()?.["encoding"];
      if (encoding !== "UTF-8" && encoding !== "UTF-16le" && encoding !== "UTF-16be") {
        throw new Error("Unsupported memory database text encoding.");
      }
      // CAST(TEXT AS BLOB) uses the database encoding. Preserve a literal BOM
      // and reject malformed bytes instead of silently replacing characters.
      this.#textDecoder = new TextDecoder(encoding, { fatal: true, ignoreBOM: true });
      this.#database.exec("PRAGMA synchronous = FULL");
      this.#transaction(() => this.#initializeSchema());
      this.#select = this.#database.prepare(
        `SELECT ${memoryColumns} FROM memories WHERE id = ?`,
      );
      this.#insert = this.#database.prepare(
        `INSERT INTO memories (id, content, created_at, revision, updated_at)
         VALUES (?, ?, ?, 1, ?)`,
      );
      this.#update = this.#database.prepare(
        "UPDATE memories SET content = ?, revision = ?, updated_at = ? WHERE id = ?",
      );
      this.#delete = this.#database.prepare("DELETE FROM memories WHERE id = ?");
      this.#upsertOrigin = this.#database.prepare(
        "INSERT INTO memory_origins (memory_id, job_id, revision) VALUES (?, ?, ?) " +
        "ON CONFLICT(memory_id) DO UPDATE SET job_id = excluded.job_id, revision = excluded.revision",
      );
      this.#deleteOrigin = this.#database.prepare("DELETE FROM memory_origins WHERE memory_id = ?");
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  save(memory: MemoryInput): void {
    this.#ensureOpen();
    const record = parseInput(memory);
    this.#transaction(() => {
      const existing = this.get(record.id);
      if (existing === undefined) {
        this.#insert.run(record.id, record.content, record.createdAt, record.createdAt);
      } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
        throw new MemoryConflictError(record.id);
      }
    });
  }

  update(update: MemoryUpdate): Memory {
    this.#ensureOpen();
    const request = parseUpdate(update);
    return this.#transaction(() => {
      const current = this.get(request.id);
      if (current === undefined) throw new MemoryNotFoundError(request.id);

      if (current.revision !== request.expectedRevision) {
        // Confirm an applied effect only while its resulting state is still current.
        if (
          current.revision === request.expectedRevision + 1 &&
          current.content === request.content &&
          current.updatedAt === request.updatedAt
        ) return current;
        throw new MemoryConflictError(request.id);
      }
      if (current.content === request.content) return current;
      if (current.revision === Number.MAX_SAFE_INTEGER) {
        throw new RangeError("Memory revision limit reached.");
      }

      const result = {
        ...current,
        content: request.content,
        revision: current.revision + 1,
        updatedAt: request.updatedAt,
      };
      this.#update.run(result.content, result.revision, result.updatedAt, result.id);
      this.#deleteOrigin.run(result.id);
      return result;
    });
  }

  forget(forget: MemoryForget): void {
    this.#ensureOpen();
    const request = parseForget(forget);
    this.#transaction(() => {
      const current = this.get(request.id);
      if (current === undefined) {
        this.#deleteOrigin.run(request.id);
        return;
      }
      if (current.revision !== request.expectedRevision) {
        throw new MemoryConflictError(request.id);
      }
      this.#delete.run(request.id);
      this.#deleteOrigin.run(request.id);
    });
  }

  get(id: string): Memory | undefined {
    this.#ensureOpen();
    validateText(id, "ID");
    const row = this.#select.get(id);
    return row === undefined ? undefined : parseMemory(this.#decodeRow(row));
  }

  memoryOrigin(id: string): MemoryOrigin | undefined {
    this.#ensureOpen();
    validateText(id, "ID");
    const row = this.#database.prepare(
      "SELECT " + [
        textColumn("m.id", "currentId"), textColumn("m.content", "content"),
        textColumn("m.created_at", "createdAt"), "m.revision AS currentRevision",
        textColumn("m.updated_at", "updatedAt"), textColumn("o.memory_id", "memoryId"),
        textColumn("o.job_id", "originJobId"), "o.revision AS originRevision",
        textColumn("j.id", "jobRecordId"), textColumn("j.job_json", "jobJson"),
        textColumn("j.status", "jobStatus"), textColumn("j.lease_owner", "leaseOwner"),
        "j.lease_until AS leaseUntil", textColumn("j.retry_category", "retryCategory"),
        "j.retry_at AS retryAt",
      ].join(", ") + " " +
      "FROM memories AS m " +
      "LEFT JOIN memory_origins AS o ON o.memory_id = m.id " +
      "LEFT JOIN consolidation_jobs AS j ON j.id = o.job_id " +
      "WHERE m.id = ?",
    ).get(id);
    if (row === undefined) return undefined;
    const {
      currentId,
      content,
      createdAt,
      currentRevision,
      updatedAt,
      memoryId,
      originJobId,
      originRevision,
      jobRecordId,
      jobJson,
      jobStatus,
      leaseOwner,
      leaseUntil,
      retryCategory,
      retryAt,
    } = fields(this.#decodeRow(row), [
      "currentId",
      "content",
      "createdAt",
      "currentRevision",
      "updatedAt",
      "memoryId",
      "originJobId",
      "originRevision",
      "jobRecordId",
      "jobJson",
      "jobStatus",
      "leaseOwner",
      "leaseUntil",
      "retryCategory",
      "retryAt",
    ]);
    const current = parseMemory({
      id: currentId,
      content,
      createdAt,
      revision: currentRevision,
      updatedAt,
    });
    if (memoryId === null) {
      if (
        originJobId !== null || originRevision !== null || jobRecordId !== null || jobJson !== null ||
        jobStatus !== null || leaseOwner !== null || leaseUntil !== null ||
        retryCategory !== null || retryAt !== null
      ) throw new TypeError("Stored memory origin row is inconsistent.");
      return undefined;
    }
    const origin = parseMemoryOriginRow({ memoryId, jobId: originJobId, revision: originRevision });
    if (origin.memoryId !== current.id || origin.revision !== current.revision) return undefined;
    if (jobRecordId === null) throw new TypeError("Stored memory origin references a missing consolidation job.");
    const job = parsePersistedJobRow({
      id: jobRecordId,
      job_json: jobJson,
      status: jobStatus,
      lease_owner: leaseOwner,
      lease_until: leaseUntil,
      retryCategory,
      retryAt,
    });
    if (job.status !== "complete") throw new TypeError("Stored memory origin references an incomplete job.");
    return {
      sessionId: job.job.sessionId,
      sourceKeys: [...job.job.sourceKeys],
      ...(job.job.lineageSessionIds === undefined
        ? {}
        : { lineageSessionIds: [...job.job.lineageSessionIds] }),
      ...(job.job.sourceTime === undefined ? {} : { sourceTime: job.job.sourceTime }),
      ...(job.job.branchAnchor === undefined ? {} : { branchAnchor: job.job.branchAnchor }),
    };
  }

  searchBatch(request: MemorySearchRequest): readonly MemorySearchRow[] {
    this.#ensureOpen();
    const search = parseSearchRequest(request);
    const limit = search.limit ?? maxSearchBatchSize;
    const afterClause = search.after === undefined ? "" : "WHERE id COLLATE BINARY > ?";
    const matchedTermColumns = search.terms.map((_, index) =>
      `CASE WHEN instr(lower(content), lower(?)) > 0 THEN 1 ELSE 0 END AS matched_${index}`,
    ).join(",\n       ");
    const statement = this.#database.prepare(`
      WITH examined AS (
        SELECT id, content, created_at AS createdAt, revision, updated_at AS updatedAt
        FROM memories
        ${afterClause}
        ORDER BY id COLLATE BINARY
        LIMIT ?
      )
      SELECT ${[
        textColumn("id"), textColumn("content"), textColumn("createdAt"),
        "revision", textColumn("updatedAt"),
      ].join(", ")},
        ${matchedTermColumns}
      FROM examined
      ORDER BY examined.id COLLATE BINARY
    `);
    const parameters: (string | number)[] = [];
    if (search.after !== undefined) parameters.push(search.after);
    parameters.push(limit, ...search.terms);

    return statement.all(...parameters).map((rawRow) => {
      const row = this.#decodeRow(rawRow);
      const memory = parseMemory({
        id: row["id"],
        content: row["content"],
        createdAt: row["createdAt"],
        revision: row["revision"],
        updatedAt: row["updatedAt"],
      });
      const matchedTerms: string[] = [];
      for (let index = 0; index < search.terms.length; index += 1) {
        const flag = row[`matched_${index}`];
        if (flag === 1) {
          const term = search.terms[index];
          if (term === undefined) throw new Error("Memory search term index exceeded the validated input.");
          matchedTerms.push(term);
        } else if (flag !== 0) {
          throw new Error("SQLite returned an invalid memory search match flag.");
        }
      }
      return { memory, matchedTerms };
    });
  }

  enqueueDistillTrigger(trigger: DistillTriggerInput): void {
    this.#ensureOpen();
    const record = parseDistillTriggerInput(trigger);
    const triggerJson = JSON.stringify(record);
    const status = record.reason === "compact" ? "candidate" : "pending";
    this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(record.id);
      if (row !== undefined) {
        const existing = this.#readDistillTrigger(row);
        if (JSON.stringify(existing.trigger) !== triggerJson) {
          throw new ConsolidationConflictError(record.id, "already exists with a different trigger boundary");
        }
        return;
      }
      this.#database.prepare(
        "INSERT INTO distill_triggers " +
        "(id, trigger_json, status, compaction_record_id, retry_category, retry_at) " +
        "VALUES (?, ?, ?, NULL, NULL, NULL)",
      ).run(record.id, triggerJson, status);
    });
  }

  confirmCompactionTrigger(input: { readonly id: string; readonly compactionRecordId: string }): boolean {
    this.#ensureOpen();
    const { id, compactionRecordId } = fields(input, ["id", "compactionRecordId"]);
    validateText(id, "distill trigger ID");
    validateText(compactionRecordId, "compaction record ID");
    return this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = this.#readDistillTrigger(row);
      if (current.trigger.reason !== "compact") {
        throw new ConsolidationConflictError(id, "is not a compaction candidate");
      }
      if (current.status !== "candidate") {
        if (current.compactionRecordId === compactionRecordId) return true;
        throw new ConsolidationConflictError(id, "has already been confirmed by a different compaction record");
      }
      this.#database.prepare(
        "UPDATE distill_triggers SET status = 'pending', compaction_record_id = ? " +
        "WHERE id = ? AND status = 'candidate'",
      ).run(compactionRecordId, id);
      return true;
    });
  }

  cancelCompactionTrigger(id: string): boolean {
    this.#ensureOpen();
    validateText(id, "distill trigger ID");
    return this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = this.#readDistillTrigger(row);
      if (current.trigger.reason !== "compact" || current.status !== "candidate") return false;
      this.#database.prepare("DELETE FROM distill_triggers WHERE id = ? AND status = 'candidate'").run(id);
      return true;
    });
  }

  listDistillTriggers(options: TaskPageOptions): TaskPage<DistillTrigger> {
    this.#ensureOpen();
    const page = parseTaskPageOptions(options, false);
    const cursor = page.cursor ?? 0;
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${triggerColumns} FROM distill_triggers ` +
      "WHERE rowid > ? ORDER BY rowid LIMIT ?",
    ).all(cursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.#decodeRow(raw);
      const { cursorRow, ...triggerRow } = decoded;
      validateNow(cursorRow, "Stored distill trigger cursor");
      return this.#distillTriggerItem(this.#readDistillTrigger(triggerRow));
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.#decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next distill trigger cursor");
    return { items, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  claimDistillTrigger(input: DistillClaimInput): DistillTriggerClaim | undefined {
    this.#ensureOpen();
    const request = parseDistillClaimInput(input);
    return this.#transaction(() => {
      const rows = this.#database.prepare(
        `SELECT ${triggerColumns} FROM distill_triggers ` +
        "WHERE status = 'pending' AND (retry_at IS NULL OR retry_at <= ?) ORDER BY rowid LIMIT 32",
      ).all(request.now);
      for (const row of rows) {
        const current = this.#readDistillTrigger(this.#decodeRow(row));
        if (!this.#acquireDistillSlot({
          claimToken: randomUUID(),
          workerId: request.workerId,
          taskKind: "trigger",
          taskId: current.trigger.id,
          now: request.now,
          leaseMs: request.leaseMs,
        })) return undefined;
        const lease = this.#readDistillLease();
        if (lease.claimToken === null || lease.leaseUntil === null) {
          throw new Error("Distill claim was not persisted.");
        }
        this.#database.prepare(
          "UPDATE distill_triggers SET retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
        ).run(current.trigger.id);
        return {
          trigger: this.#distillTriggerItem({ ...current, retryCategory: null, retryAt: null }),
          claimToken: lease.claimToken,
          leaseUntil: lease.leaseUntil,
        };
      }
      return undefined;
    });
  }

  prepareDistillTrigger(input: PrepareDistillTriggerInput): void {
    this.#ensureOpen();
    const request = parsePrepareDistillTriggerInput(input);
    this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(request.id);
      if (row === undefined) throw new ConsolidationConflictError(request.id, "does not exist");
      const current = this.#readDistillTrigger(this.#decodeRow(row));
      if (current.status === "prepared" || current.status === "complete") {
        const ids = this.#triggerJobIds(request.id);
        if (ids.length !== request.jobs.length || ids.some((id, index) => id !== request.jobs[index]?.id)) {
          throw new ConsolidationConflictError(request.id, "was prepared with a different batch set");
        }
        for (const prepared of request.jobs) this.#assertExistingJobPayload(prepared);
        return;
      }
      this.#assertCurrentDistillLease("trigger", request.id, request.claimToken, request.now);
      if (current.status !== "pending") {
        throw new ConsolidationConflictError(request.id, "is not ready for preparation");
      }
      for (const prepared of request.jobs) {
        if (prepared.sessionId !== current.trigger.sessionId || prepared.sessionFile !== current.trigger.sessionFile) {
          throw new ConsolidationConflictError(prepared.id, "does not belong to the trigger session");
        }
        this.#enqueueConsolidationInTransaction(prepared);
      }
      for (let index = 0; index < request.jobs.length; index += 1) {
        const prepared = request.jobs[index];
        if (prepared === undefined) throw new Error("Prepared batch index exceeded its validated input.");
        const linkId = createHash("sha256").update(JSON.stringify([request.id, prepared.id])).digest("hex");
        const existing = this.#database.prepare(
          "SELECT id, trigger_id AS triggerId, job_id AS jobId, ordinal FROM distill_trigger_jobs WHERE id = ?",
        ).get(linkId);
        if (existing === undefined) {
          this.#database.prepare(
            "INSERT INTO distill_trigger_jobs (id, trigger_id, job_id, ordinal) VALUES (?, ?, ?, ?)",
          ).run(linkId, request.id, prepared.id, index);
        } else {
          const link = fields(this.#decodeRow(existing), ["id", "triggerId", "jobId", "ordinal"]);
          if (link.triggerId !== request.id || link.jobId !== prepared.id || link.ordinal !== index) {
            throw new ConsolidationConflictError(request.id, "has inconsistent batch associations");
          }
        }
      }
      this.#database.prepare(
        "UPDATE distill_triggers SET status = ?, retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
      ).run(request.jobs.length === 0 ? "complete" : "prepared", request.id);
      this.#clearDistillLease();
      if (request.jobs.length === 0) {
        this.#enqueueCleanTaskInTransaction({
          sessionId: current.trigger.sessionId,
          sessionFile: current.trigger.sessionFile,
          leafId: current.trigger.leafId,
          cwd: current.trigger.cwd,
          reason: current.trigger.reason,
          intentId: current.trigger.id,
        });
      } else {
        this.#completeReadyTriggers([request.id]);
      }
    });
  }

  claimDistillJob(input: DistillClaimInput & { readonly id: string }): DistillJobClaim | undefined {
    this.#ensureOpen();
    const { id, workerId, now, leaseMs } = fields(input, ["id", "workerId", "now", "leaseMs"]);
    validateText(id, "consolidation job ID");
    const request = parseDistillClaimInput({ workerId, now, leaseMs });
    return this.#transaction(() => this.#claimConsolidationJob(id, request, randomUUID()));
  }

  renewDistillClaim(input: { readonly claimToken: string; readonly now: number; readonly leaseMs: number }): boolean {
    this.#ensureOpen();
    const { claimToken, now, leaseMs } = fields(input, ["claimToken", "now", "leaseMs"]);
    validateText(claimToken, "distill claim token");
    const request = parseDistillClaimInput({ workerId: "lease-renewal", now, leaseMs });
    return this.#transaction(() => {
      const lease = this.#readDistillLease();
      if (lease.claimToken !== claimToken || lease.leaseUntil === null || lease.leaseUntil <= request.now) return false;
      const until = request.now + request.leaseMs;
      this.#database.prepare("UPDATE distill_lease SET lease_until = ? WHERE singleton = 1").run(until);
      if (lease.taskKind === "job" && lease.taskId !== null) {
        const changed = this.#database.prepare(
          "UPDATE consolidation_jobs SET lease_until = ? WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(until, lease.taskId, claimToken);
        if (changed.changes !== 1) throw new ConsolidationConflictError(lease.taskId, "lost its durable claim");
      }
      return true;
    });
  }

  releaseDistillClaim(input: ReleaseDistillClaimInput): boolean {
    this.#ensureOpen();
    const request = parseReleaseDistillClaimInput(input);
    return this.#transaction(() => {
      const lease = this.#readDistillLease();
      if (lease.claimToken !== request.claimToken || lease.taskKind === null || lease.taskId === null) return false;
      if (lease.taskKind === "job") {
        this.#database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, " +
          "retry_category = ?, retry_at = ? WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(request.retry?.category ?? null, request.retry?.retryAt ?? null, lease.taskId, request.claimToken);
      } else if (request.retry) {
        this.#database.prepare(
          "UPDATE distill_triggers SET retry_category = ?, retry_at = ? " +
          "WHERE id = ? AND status = 'pending'",
        ).run(request.retry.category, request.retry.retryAt, lease.taskId);
      }
      this.#clearDistillLease();
      return true;
    });
  }

  enqueueCleanTask(input: CleanTaskInput): CleanTask {
    this.#ensureOpen();
    const task = parseCleanTaskInput(input);
    return this.#transaction(() => this.#enqueueCleanTaskInTransaction(task));
  }

  listCleanTasks(options: CleanTaskPageOptions): TaskPage<CleanTask> {
    this.#ensureOpen();
    const page = parseTaskPageOptions(options, true);
    const cursor = page.cursor ?? 0;
    const clauses = ["rowid > ?", "NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = json_extract(clean_tasks.task_json, '$.sessionFile') AND d.state = 'complete')"];
    const params: (number | string)[] = [cursor];
    if (page.now !== undefined) {
      clauses.push("(retry_at IS NULL OR retry_at <= ?)");
      params.push(page.now);
    }
    params.push(page.limit + 1);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${cleanTaskColumns} FROM clean_tasks ` +
      `WHERE ${clauses.join(" AND ")} ORDER BY rowid LIMIT ?`,
    ).all(...params);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.#decodeRow(raw);
      const { cursorRow, ...taskRow } = decoded;
      validateNow(cursorRow, "Stored clean task cursor");
      return this.#cleanTaskItem(parsePersistedCleanTask(taskRow));
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.#decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next clean task cursor");
    return { items, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  /**
   * `owner` is a fresh, single-attempt fencing token, not a reusable worker ID.
   * The caller must use a new token after release, expiry, or intent replacement.
   */
  claimCleanTask(id: string, owner: string, now: number, leaseMs: number, force = false): boolean {
    this.#ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task claim owner");
    validateNow(now, "Clean task claim time");
    validateLeaseDuration(leaseMs, now, "Clean task lease duration");
    if (typeof force !== "boolean") throw new TypeError("Clean task force flag must be boolean.");
    return this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = parsePersistedCleanTask(this.#decodeRow(row));
      if (current.status !== "pending" && current.status !== "waiting") return false;
      if (this.getDeletion(current.task.sessionFile)?.state === "complete") return false;
      if (!force && current.retryAt !== null && current.retryAt > now) return false;
      if (current.leaseOwner !== null && current.leaseUntil !== null && current.leaseUntil > now) return false;
      this.#database.prepare(
        "UPDATE clean_tasks SET lease_owner = ?, lease_until = ? WHERE id = ?",
      ).run(owner, now + leaseMs, id);
      return true;
    });
  }

  releaseCleanTask(id: string, owner: string): boolean {
    this.#ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task lease owner");
    return this.#transaction(() => {
      const changed = this.#database.prepare(
        "UPDATE clean_tasks SET lease_owner = NULL, lease_until = NULL " +
        "WHERE id = ? AND lease_owner = ?",
      ).run(id, owner);
      return changed.changes === 1;
    });
  }

  recordCleanAssessment(id: string, owner: string, now: number, reason: WaitReason, retryAt: number): boolean {
    this.#ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task lease owner");
    validateNow(now, "Clean assessment time");
    validateNow(retryAt, "Clean assessment retry time");
    if (
      reason !== "host_coordination" && reason !== "unprocessed" && reason !== "usage" &&
      reason !== "dependencies" && reason !== "unavailable"
    ) throw new TypeError("Clean assessment reason is invalid.");
    return this.#transaction(() => {
      const row = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = parsePersistedCleanTask(this.#decodeRow(row));
      if (
        current.leaseOwner !== owner || current.leaseUntil === null || current.leaseUntil <= now
      ) return false;
      this.#database.prepare(
        "UPDATE clean_tasks SET status = 'waiting', wait_reason = ?, retry_category = ?, retry_at = ?, " +
        "lease_owner = NULL, lease_until = NULL WHERE id = ? AND lease_owner = ?",
      ).run(reason, reason === "unavailable" ? "unavailable" : "transient", retryAt, id, owner);
      return true;
    });
  }

  reconcileLegacyCleanTasks(limit = 32, cursor?: number): { readonly created: number; readonly nextCursor?: number } {
    this.#ensureOpen();
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > maxTaskPageSize) {
      throw new TypeError("Legacy cleanup reconciliation limit must be from 1 through " + maxTaskPageSize + ".");
    }
    const after = validateTaskCursor(cursor, "Legacy cleanup reconciliation cursor") ?? 0;
    return this.#transaction(() => {
      const rows = this.#database.prepare(
        `SELECT rowid AS cursorRow, ${jobColumns} FROM consolidation_jobs ` +
        "WHERE rowid > ? AND status = 'complete' ORDER BY rowid LIMIT ?",
      ).all(after, limit + 1);
      const hasMore = rows.length > limit;
      const visible = rows.slice(0, limit);
      let created = 0;
      for (const raw of visible) {
        const decoded = this.#decodeRow(raw);
        const { cursorRow, ...jobRow } = decoded;
        validateNow(cursorRow, "Legacy job cursor");
        const record = parsePersistedJobRow(jobRow);
        if (this.#jobTriggerIds(record.job.id).length > 0 || record.job.sessionFile === null) continue;
        const cleanId = cleanTaskId(record.job.sessionFile);
        const cleanRow = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(cleanId);
        if (cleanRow !== undefined) {
          parsePersistedCleanTask(this.#decodeRow(cleanRow));
          continue;
        }
        this.#enqueueCleanTaskInTransaction({
          sessionId: record.job.sessionId,
          sessionFile: record.job.sessionFile,
          leafId: record.job.leafId,
          cwd: record.job.cwd,
          reason: "legacy",
          intentId: "legacy-job:" + record.job.id,
        });
        created += 1;
      }
      const last = visible.at(-1);
      const nextCursor = hasMore && last !== undefined ? this.#decodeRow(last)["cursorRow"] : undefined;
      if (nextCursor !== undefined) validateNow(nextCursor, "Next legacy job cursor");
      return { created, ...(nextCursor === undefined ? {} : { nextCursor }) };
    });
  }

  enqueueConsolidation(job: ConsolidationJob): void {
    this.#ensureOpen();
    const record = parseConsolidationJob(job);
    this.#transaction(() => this.#enqueueConsolidationInTransaction(record));
  }

  listConsolidationJobs(): readonly ConsolidationJob[] {
    this.#ensureOpen();
    const rows = this.#database.prepare(
      `SELECT ${jobColumns} FROM consolidation_jobs ` +
      "WHERE status = 'pending' ORDER BY rowid",
    ).all();
    return rows.map((row) => parsePersistedJobRow(this.#decodeRow(row)).job);
  }

  claimConsolidation(id: string, owner: string, now: number, leaseMs: number): boolean {
    this.#ensureOpen();
    validateText(id, "consolidation job ID");
    validateText(owner, "consolidation lease owner");
    validateNow(now, "Consolidation claim time");
    validateLeaseDuration(leaseMs, now, "Consolidation lease duration");
    return this.#transaction(() => this.#claimConsolidationJob(
      id,
      { workerId: owner, now, leaseMs },
      owner,
    ) !== undefined);
  }

  releaseConsolidation(id: string, owner: string): void {
    this.#ensureOpen();
    validateText(id, "consolidation job ID");
    validateText(owner, "consolidation lease owner");
    this.#transaction(() => {
      const row = this.#database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(id);
      if (row === undefined) return;
      const current = parsePersistedJobRow(this.#decodeRow(row));
      if (current.status === "pending" && current.leaseOwner === owner) {
        this.#database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, " +
          "retry_category = NULL, retry_at = NULL WHERE id = ? AND status = 'pending'",
        ).run(id);
        const lease = this.#readDistillLease();
        if (lease.claimToken === owner && lease.taskKind === "job" && lease.taskId === id) {
          this.#clearDistillLease();
        }
      }
    });
  }

  processedSources(keys: readonly string[]): readonly string[] {
    this.#ensureOpen();
    const requested = parseSourceKeys(keys, "processed source keys", true);
    const select = this.#database.prepare(
      `SELECT ${progressColumns} FROM consolidation_progress WHERE source_key = ?`,
    );
    const result: string[] = [];
    for (const key of requested) {
      const row = select.get(key);
      if (row !== undefined) {
        const processed = parseProgressRow(this.#decodeRow(row));
        result.push(processed.sourceKey);
      }
    }
    return result;
  }

  /** One snapshot; queued source triggers count as unresolved retention evidence. */
  readConsolidationStatus(keys: readonly string[]): {
    readonly processedSources: readonly string[];
    readonly pendingJobs: readonly ConsolidationJob[];
    readonly pendingTriggers: readonly DistillTrigger[];
    readonly observations: readonly SessionObservation[];
    readonly usages: readonly SessionUsage[];
  } {
    this.#ensureOpen();
    return this.#transaction(() => {
      const rowCount = this.#database.prepare(
        "SELECT COUNT(*) AS totalRows FROM (" +
        "SELECT 1 FROM consolidation_jobs WHERE status = 'pending' " +
        "UNION ALL SELECT 1 FROM distill_triggers WHERE status <> 'complete' " +
        "UNION ALL SELECT 1 FROM session_observations " +
        "UNION ALL SELECT 1 FROM session_usages LIMIT ?)",
      ).get(maxSessionMetadataRows + 1)?.["totalRows"];
      if (typeof rowCount !== "number" || !Number.isSafeInteger(rowCount) || rowCount < 0) {
        throw new TypeError("SQLite returned an invalid consolidation status row count.");
      }
      if (rowCount > maxSessionMetadataRows) {
        throw new RangeError(
          "Consolidation status exceeds the " + maxSessionMetadataRows + "-row metadata limit.",
        );
      }
      return {
        processedSources: this.processedSources(keys),
        pendingJobs: this.listConsolidationJobs(),
        pendingTriggers: this.#listPendingDistillTriggers(),
        observations: this.#database.prepare(
          `SELECT ${observationColumns} FROM session_observations ORDER BY session_id COLLATE BINARY`,
        ).all().map(row => parseStoredSessionObservation(this.#decodeRow(row)).observation),
        usages: this.#database.prepare(
          `SELECT ${sessionUsageColumns} FROM session_usages ORDER BY owner_id COLLATE BINARY`,
        ).all().map(row => parseStoredSessionUsage(this.#decodeRow(row))),
      };
    }, "DEFERRED");
  }

  commitConsolidation(commit: ConsolidationCommit): boolean {
    this.#ensureOpen();
    const request = parseConsolidationCommit(commit);
    return this.#transaction(() => {
      const row = this.#database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(request.jobId);
      if (row === undefined) {
        throw new ConsolidationConflictError(request.jobId, "does not exist");
      }
      const current = parsePersistedJobRow(this.#decodeRow(row));
      if (current.status === "complete") return false;
      if (
        current.leaseOwner !== request.owner ||
        current.leaseUntil === null ||
        current.leaseUntil <= request.now
      ) {
        throw new ConsolidationConflictError(request.jobId, "has no current lease for this owner");
      }
      this.#assertCurrentDistillLease("job", request.jobId, request.owner, request.now);

      const selectProgress = this.#database.prepare(
        `SELECT ${progressColumns} FROM consolidation_progress WHERE source_key = ?`,
      );
      const processed = new Set<string>();
      for (const key of current.job.sourceKeys) {
        const processedRow = selectProgress.get(key);
        if (processedRow !== undefined) processed.add(parseProgressRow(this.#decodeRow(processedRow)).sourceKey);
      }
      const remaining = current.job.sourceKeys.filter((key) => !processed.has(key));
      const requestedSources = request.sourceKeys ?? remaining;
      if (
        requestedSources.length !== remaining.length ||
        requestedSources.some((key, index) => key !== remaining[index])
      ) {
        throw new ConsolidationConflictError(request.jobId, "source set changed after the batch was prepared");
      }
      if (remaining.length === 0) {
        const completion = this.#database.prepare(
          "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL, " +
          "retry_category = NULL, retry_at = NULL " +
          "WHERE id = ? AND status = 'pending'",
        ).run(request.jobId);
        if (completion.changes !== 1) {
          throw new ConsolidationConflictError(request.jobId, "changed before completion");
        }
        this.#finishConsolidationJob(current.job, request.owner);
        return false;
      }

      for (const record of request.creates) {
        const existing = this.get(record.id);
        let revision = 1;
        if (existing === undefined) {
          this.#insert.run(record.id, record.content, record.createdAt, record.createdAt);
        } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
          throw new MemoryConflictError(record.id);
        } else {
          revision = existing.revision;
        }
        this.#upsertOrigin.run(record.id, request.jobId, revision);
      }
      for (const update of request.updates) {
        const existing = this.get(update.id);
        if (existing === undefined) throw new MemoryNotFoundError(update.id);
        if (existing.revision !== update.expectedRevision) {
          if (
            existing.revision === update.expectedRevision + 1 &&
            existing.content === update.content &&
            existing.updatedAt === update.updatedAt
          ) {
            this.#upsertOrigin.run(update.id, request.jobId, existing.revision);
            continue;
          }
          throw new MemoryConflictError(update.id);
        }
        if (existing.content === update.content) {
          this.#upsertOrigin.run(update.id, request.jobId, existing.revision);
          continue;
        }
        if (existing.revision === Number.MAX_SAFE_INTEGER) {
          throw new RangeError("Memory revision limit reached.");
        }
        const revision = existing.revision + 1;
        this.#update.run(
          update.content,
          revision,
          update.updatedAt,
          existing.id,
        );
        this.#upsertOrigin.run(update.id, request.jobId, revision);
      }
      for (const removal of request.deletes) {
        const existing = this.get(removal.id);
        if (existing === undefined) {
          this.#deleteOrigin.run(removal.id);
          continue;
        }
        if (existing.revision !== removal.expectedRevision) {
          throw new MemoryConflictError(removal.id);
        }
        this.#delete.run(removal.id);
        this.#deleteOrigin.run(removal.id);
      }

      const insertProgress = this.#database.prepare(
        "INSERT INTO consolidation_progress (source_key, job_id, processed_at) VALUES (?, ?, ?)",
      );
      for (const key of remaining) insertProgress.run(key, request.jobId, request.now);
      const completion = this.#database.prepare(
        "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL, " +
        "retry_category = NULL, retry_at = NULL " +
        "WHERE id = ? AND status = 'pending'",
      ).run(request.jobId);
      if (completion.changes !== 1) {
        throw new ConsolidationConflictError(request.jobId, "changed before completion");
      }
      this.#finishConsolidationJob(current.job, request.owner);
      return true;
    });
  }

  observeSession(observation: SessionObservation): void {
    this.#ensureOpen();
    const record = parseSessionObservation(observation);
    this.#transaction(() => {
      this.#database.prepare(
        "INSERT INTO session_observations " +
        "(session_id, session_file, cwd, leaf_id, owner_pid, captured) " +
        "VALUES (?, ?, ?, ?, ?, 0) ON CONFLICT(session_id) DO UPDATE SET " +
        "session_file = excluded.session_file, cwd = excluded.cwd, " +
        "leaf_id = excluded.leaf_id, owner_pid = excluded.owner_pid, " +
        "captured = CASE WHEN session_observations.session_file = excluded.session_file " +
        "AND session_observations.cwd = excluded.cwd " +
        "AND session_observations.leaf_id IS excluded.leaf_id " +
        "THEN session_observations.captured ELSE 0 END",
      ).run(record.sessionId, record.sessionFile, record.cwd, record.leafId, record.ownerPid);
    });
  }

  saveSessionUsage(usage: SessionUsage): void {
    this.#ensureOpen();
    const record = parseSessionUsage(usage);
    this.#transaction(() => {
      const row = this.#database.prepare(
        `SELECT ${sessionUsageColumns} FROM session_usages WHERE owner_id = ?`,
      ).get(record.ownerId);
      if (row === undefined) {
        if (record.state !== "active") {
          throw new ConsolidationConflictError(record.ownerId, "cannot be created outside the active state");
        }
        this.#database.prepare(
          "INSERT INTO session_usages " +
          "(owner_id, process_token, session_id, session_file, cwd, leaf_id, owner_pid, state) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ).run(
          record.ownerId,
          record.processToken,
          record.sessionId,
          record.sessionFile,
          record.cwd,
          record.leafId,
          record.ownerPid,
          record.state,
        );
        return;
      }

      const current = parseStoredSessionUsage(this.#decodeRow(row));
      if (!sameSessionUsageIdentity(current, record)) {
        throw new ConsolidationConflictError(record.ownerId, "already exists with different owner identity");
      }
      const checkpointChanged = current.leafId !== record.leafId;
      const transitionAllowed = current.state === "active"
        ? record.state === "active" || record.state === "closing"
        : current.state === "closing"
          ? (record.state === "closing" || record.state === "released") && !checkpointChanged
          : record.state === "released" && !checkpointChanged;
      if (!transitionAllowed) {
        throw new ConsolidationConflictError(record.ownerId, "cannot change its session usage state or checkpoint");
      }
      if (current.state === record.state && !checkpointChanged) return;
      this.#database.prepare(
        "UPDATE session_usages SET leaf_id = ?, state = ? WHERE owner_id = ?",
      ).run(record.leafId, record.state, record.ownerId);
    });
  }

  acknowledgeObservation(sessionId: string, leafId: string | null): void {
    this.#ensureOpen();
    validateText(sessionId, "session ID");
    const currentLeafId = parseNullableText(leafId, "session leaf ID");
    this.#transaction(() => {
      this.#database.prepare(
        "UPDATE session_observations SET captured = 1 WHERE session_id = ? AND leaf_id IS ?",
      ).run(sessionId, currentLeafId);
    });
  }

  observedSessions(): readonly SessionObservation[] {
    this.#ensureOpen();
    return this.#database.prepare(
      `SELECT ${observationColumns} FROM session_observations ` +
      "WHERE captured = 0 ORDER BY session_observations.session_id COLLATE BINARY",
    ).all().map((row) => parseStoredSessionObservation(this.#decodeRow(row)).observation);
  }

  /** Returns a bounded stable rowid page of uncaptured session observations. */
  listObservedSessions(options: TaskPageOptions): TaskPage<SessionObservation> {
    this.#ensureOpen();
    const page = parseTaskPageOptions(options, false);
    const cursor = page.cursor ?? 0;
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${observationColumns} FROM session_observations ` +
      "WHERE captured = 0 AND rowid > ? ORDER BY rowid LIMIT ?",
    ).all(cursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.#decodeRow(raw);
      const { cursorRow, ...observationRow } = decoded;
      validateNow(cursorRow, "Stored session observation cursor");
      return parseStoredSessionObservation(observationRow).observation;
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.#decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next session observation cursor");
    return { items, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  /** Startup recovery also covers a crash between the first file write and message_end. */
  listUnobservedSessionUsages(options: TaskPageOptions): TaskPage<SessionUsage> {
    const page = parseTaskPageOptions(options, false);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${sessionUsageColumns} FROM session_usages ` +
      "WHERE rowid > ? AND NOT EXISTS (SELECT 1 FROM session_observations o WHERE o.session_id = session_usages.session_id) " +
      "AND NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = session_usages.session_file AND d.state = 'complete') " +
      "ORDER BY rowid LIMIT ?",
    ).all(page.cursor ?? 0, page.limit + 1);
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const {cursorRow, ...owner} = this.#decodeRow(raw);
      validateNow(cursorRow, "Session usage cursor");
      return parseStoredSessionUsage(owner);
    });
    const nextCursor = rows.length > page.limit ? visible.at(-1)?.["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next session usage cursor");
    return {items, ...(nextCursor === undefined ? {} : {nextCursor})};
  }

  listSessionUsages(): readonly SessionUsage[] {
    this.#ensureOpen();
    return this.#database.prepare(
      `SELECT ${sessionUsageColumns} FROM session_usages ORDER BY owner_id COLLATE BINARY`,
    ).all().map((row) => parseStoredSessionUsage(this.#decodeRow(row)));
  }

  /** Returns a fair lexical page of known paths after validating bounded task metadata. */
  listKnownSessionFiles(limit = maxKnownSessionFiles, afterPath?: string): readonly string[] {
    this.#ensureOpen();
    if (
      typeof limit !== "number" || !Number.isSafeInteger(limit) ||
      limit < 1 || limit > maxKnownSessionFiles
    ) {
      throw new TypeError(
        "Known session file limit must be an integer from 1 through " + maxKnownSessionFiles + ".",
      );
    }
    if (afterPath !== undefined) validateAbsolutePath(afterPath, "known session file cursor");
    return this.#transaction(() => {
      const rowCount = this.#database.prepare(
        "SELECT COUNT(*) AS totalRows FROM (" +
        "SELECT 1 FROM consolidation_jobs " +
        "UNION ALL SELECT 1 FROM distill_triggers " +
        "UNION ALL SELECT 1 FROM clean_tasks " +
        "UNION ALL SELECT 1 FROM session_observations " +
        "UNION ALL SELECT 1 FROM session_usages LIMIT ?)",
      ).get(maxSessionMetadataRows + 1)?.["totalRows"];
      if (typeof rowCount !== "number" || !Number.isSafeInteger(rowCount) || rowCount < 0) {
        throw new TypeError("SQLite returned an invalid session metadata row count.");
      }
      if (rowCount > maxSessionMetadataRows) {
        throw new RangeError(
          "Session discovery exceeds the " + maxSessionMetadataRows + "-row metadata limit.",
        );
      }
      for (const row of this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs`).iterate()) {
        parsePersistedJobRow(this.#decodeRow(row));
      }
      for (const row of this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers`).iterate()) {
        this.#readDistillTrigger(this.#decodeRow(row));
      }
      for (const row of this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks`).iterate()) {
        parsePersistedCleanTask(this.#decodeRow(row));
      }
      for (const row of this.#database.prepare(
        `SELECT ${observationColumns} FROM session_observations`,
      ).iterate()) parseStoredSessionObservation(this.#decodeRow(row));
      for (const row of this.#database.prepare(
        `SELECT ${sessionUsageColumns} FROM session_usages`,
      ).iterate()) {
        parseStoredSessionUsage(this.#decodeRow(row));
      }
      const paths = this.#database.prepare(
        "SELECT " + textColumn("path") + " FROM (" +
        "SELECT json_extract(job_json, '$.sessionFile') AS path FROM consolidation_jobs " +
        "UNION SELECT json_extract(trigger_json, '$.sessionFile') AS path FROM distill_triggers " +
        "UNION SELECT json_extract(task_json, '$.sessionFile') AS path FROM clean_tasks " +
        "UNION SELECT session_file AS path FROM session_observations " +
        "UNION SELECT session_file AS path FROM session_usages" +
        ") WHERE path IS NOT NULL AND NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = path AND d.state = 'complete') AND (? IS NULL OR path COLLATE BINARY > ?) " +
        "ORDER BY path COLLATE BINARY LIMIT ?",
      ).all(afterPath ?? null, afterPath ?? null, limit);
      return paths.map((row) => {
        const { path } = fields(this.#decodeRow(row), ["path"]);
        validateAbsolutePath(path, "known session file");
        return path;
      });
    }, "DEFERRED");
  }

  /** Distinguishes a path reserved by a fresh empty session from a captured source. */
  hasSessionSourceWork(path: string): boolean {
    validateAbsolutePath(path, "Source work path");
    return this.#database.prepare(
      "SELECT 1 FROM session_observations WHERE session_file = ? " +
      "UNION ALL SELECT 1 FROM consolidation_jobs WHERE json_extract(job_json, '$.sessionFile') = ? " +
      "UNION ALL SELECT 1 FROM distill_triggers WHERE json_extract(trigger_json, '$.sessionFile') = ? " +
      "UNION ALL SELECT 1 FROM clean_tasks WHERE json_extract(task_json, '$.sessionFile') = ? LIMIT 1",
    ).get(path, path, path, path) !== undefined;
  }

  /** Serializes bounded internal source reads and cleanup. Never held during LLM work. */
  acquireSourceAccess(): string | undefined {
    return this.#transaction(() => {
      const access = this.#sourceAccess();
      if (access && !processHasExited(access.pid)) return undefined;
      const token = randomUUID();
      this.#database.prepare("DELETE FROM source_access").run();
      this.#database.prepare("INSERT INTO source_access VALUES (1, ?, ?)").run(token, process.pid);
      return token;
    });
  }

  releaseSourceAccess(token: string): boolean {
    validateText(token, "Source access token");
    return this.#transaction(() => this.#database.prepare("DELETE FROM source_access WHERE token = ?").run(token).changes === 1);
  }

  #sourceAccess(): { token: string; pid: number } | undefined {
    const raw = this.#database.prepare(`SELECT ${textColumn("token")}, owner_pid FROM source_access`).get();
    if (!raw) return undefined;
    const row = this.#decodeRow(raw);
    validateText(row["token"], "Stored source token");
    const pid = row["owner_pid"];
    if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) throw new TypeError("Invalid source owner PID.");
    return { token: row["token"], pid };
  }

  /** Compare on this connection only; includes other connections and local mutations. */
  retentionVersion(): string {
    this.#ensureOpen();
    return `${this.#database.prepare("PRAGMA data_version").get()?.["data_version"]}:${this.#database.prepare("SELECT total_changes() AS n").get()?.["n"]}`;
  }

  reconcileExitedSessionOwners(): void {
    this.#transaction(() => {
      const rows = this.#database.prepare(`SELECT ${sessionUsageColumns} FROM session_usages WHERE state != 'released'`).all();
      for (const raw of rows) {
        const owner = parseStoredSessionUsage(this.#decodeRow(raw));
        if (processHasExited(owner.ownerPid)) this.#database.prepare("UPDATE session_usages SET state = 'released' WHERE owner_id = ?").run(owner.ownerId);
      }
    });
  }

  getDeletion(path: string): SessionDeletion | undefined {
    validateAbsolutePath(path, "Deletion path");
    const raw = this.#database.prepare(`SELECT ${["session_file", "session_id", "intent_id", "file_version", "state"].map(c => textColumn(c)).join(", ")} FROM session_deletions WHERE session_file = ?`).get(path);
    if (!raw) return undefined;
    const row = this.#decodeRow(raw);
    validateAbsolutePath(row["session_file"], "Stored deletion path");
    validateText(row["session_id"], "Stored deletion session ID");
    validateText(row["intent_id"], "Stored deletion intent ID");
    validateText(row["file_version"], "Stored deletion file version");
    if (!/^\d+:\d+:\d+:\d+:\d+$/.test(row["file_version"])) throw new TypeError("Invalid deletion file version.");
    if (row["state"] !== "deleting" && row["state"] !== "complete") throw new TypeError("Invalid deletion state.");
    return {sessionFile:row["session_file"], sessionId:row["session_id"], intentId:row["intent_id"], fileVersion:row["file_version"], state:row["state"]};
  }

  #deletionTask(id: string, owner: string, access: string, version: string): CleanTaskInput | undefined {
    if (this.retentionVersion() !== version || this.#sourceAccess()?.token !== access) return undefined;
    const raw = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (!raw) return undefined;
    const record = parsePersistedCleanTask(this.#decodeRow(raw));
    if (record.leaseOwner !== owner || record.leaseUntil === null || record.leaseUntil <= Date.now()) return undefined;
    return record.task;
  }

  prepareDeletion(id: string, owner: string, access: string, version: string, fileVersion: string): boolean {
    if (typeof fileVersion !== "string" || !/^\d+:\d+:\d+:\d+:\d+$/.test(fileVersion)) throw new TypeError("Invalid deletion file version.");
    return this.#transaction(() => {
      const task = this.#deletionTask(id, owner, access, version);
      if (!task) return false;
      const previous = this.getDeletion(task.sessionFile);
      if (previous && (previous.state === "complete" || previous.sessionId !== task.sessionId || previous.fileVersion.split(":").slice(0, 2).join(":") !== fileVersion.split(":").slice(0, 2).join(":"))) return false;
      this.#database.prepare("INSERT INTO session_deletions VALUES (?, ?, ?, ?, 'deleting') ON CONFLICT(session_file) DO UPDATE SET intent_id = excluded.intent_id, file_version = excluded.file_version")
        .run(task.sessionFile, task.sessionId, task.intentId, fileVersion);
      return true;
    });
  }

  /** Callback performs only bounded synchronous identity checks/unlink, under the writer reservation. */
  finishDeletion(id: string, owner: string, access: string, version: string, mutate: () => boolean): boolean {
    return this.#transaction(() => {
      const task = this.#deletionTask(id, owner, access, version);
      if (!task) return false;
      const receipt = this.getDeletion(task.sessionFile);
      if (!receipt || receipt.state !== "deleting" || receipt.sessionId !== task.sessionId || receipt.intentId !== task.intentId) return false;
      if (mutate() !== true) return false;
      this.#database.prepare("UPDATE session_deletions SET state = 'complete' WHERE session_file = ?").run(task.sessionFile);
      this.#database.prepare("UPDATE clean_tasks SET lease_owner = NULL, lease_until = NULL WHERE id = ?").run(id);
      return true;
    });
  }

  close(): void {
    if (this.#database.isOpen) this.#database.close();
  }

  #decodeRow(row: Record<string, unknown>): Record<string, unknown> {
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Uint8Array) row[key] = this.#textDecoder.decode(value);
    }
    return row;
  }

  #readDistillTrigger(value: unknown): PersistedDistillTrigger {
    const row = this.#decodeRow(value as Record<string, unknown>);
    const id = row["id"];
    validateText(id, "stored distill trigger ID");
    return parsePersistedDistillTrigger(row, this.#triggerJobIds(id));
  }

  #distillTriggerItem(record: PersistedDistillTrigger): DistillTrigger {
    return {
      ...record.trigger,
      status: record.status,
      compactionRecordId: record.compactionRecordId,
      jobIds: record.jobIds,
      retryCategory: record.retryCategory,
      retryAt: record.retryAt,
    };
  }

  #listPendingDistillTriggers(): readonly DistillTrigger[] {
    return this.#database.prepare(
      `SELECT ${triggerColumns} FROM distill_triggers WHERE status <> 'complete' ORDER BY rowid`,
    ).all().map(row => this.#distillTriggerItem(this.#readDistillTrigger(row)));
  }

  #triggerJobIds(triggerId: string): readonly string[] {
    const rows = this.#database.prepare(
      "SELECT id, trigger_id AS triggerId, job_id AS jobId, ordinal " +
      "FROM distill_trigger_jobs WHERE trigger_id = ? ORDER BY ordinal, rowid",
    ).all(triggerId);
    const result: string[] = [];
    const seen = new Set<string>();
    for (let index = 0; index < rows.length; index += 1) {
      const link = fields(this.#decodeRow(rows[index] ?? {}), ["id", "triggerId", "jobId", "ordinal"]);
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

  #jobTriggerIds(jobId: string): readonly string[] {
    const rows = this.#database.prepare(
      "SELECT id, trigger_id AS triggerId, job_id AS jobId " +
      "FROM distill_trigger_jobs WHERE job_id = ? ORDER BY rowid",
    ).all(jobId);
    return rows.map(raw => {
      const link = fields(this.#decodeRow(raw), ["id", "triggerId", "jobId"]);
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

  #readDistillLease(): PersistedDistillLease {
    const row = this.#database.prepare(
      "SELECT singleton, " + [
        textColumn("claim_token", "claimToken"), textColumn("worker_id", "workerId"),
        textColumn("task_kind", "taskKind"), textColumn("task_id", "taskId"), "lease_until AS leaseUntil",
      ].join(", ") + " FROM distill_lease WHERE singleton = 1",
    ).get();
    if (row === undefined) throw new TypeError("Global distill lease record is missing.");
    const { singleton, claimToken, workerId, taskKind, taskId, leaseUntil } = fields(
      this.#decodeRow(row), ["singleton", "claimToken", "workerId", "taskKind", "taskId", "leaseUntil"],
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

  #acquireDistillSlot(input: {
    readonly claimToken: string;
    readonly workerId: string;
    readonly taskKind: "trigger" | "job";
    readonly taskId: string;
    readonly now: number;
    readonly leaseMs: number;
  }): boolean {
    const current = this.#readDistillLease();
    if (current.claimToken !== null && current.leaseUntil !== null) {
      if (current.leaseUntil > input.now || current.claimToken === input.claimToken) return false;
      if (current.taskKind === "job" && current.taskId !== null) {
        this.#database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL " +
          "WHERE id = ? AND status = 'pending' AND lease_owner = ?",
        ).run(current.taskId, current.claimToken);
      }
    }
    // Schema v3 allowed independent per-job claims. Keep a migrated live claim
    // from racing the new database-wide lease, even when it belongs to another job.
    const legacyClaim = this.#database.prepare(
      "SELECT 1 FROM consolidation_jobs WHERE status = 'pending' " +
      "AND lease_owner IS NOT NULL AND lease_until > ? LIMIT 1",
    ).get(input.now);
    if (legacyClaim !== undefined) return false;
    this.#database.prepare(
      "UPDATE distill_lease SET claim_token = ?, worker_id = ?, task_kind = ?, task_id = ?, lease_until = ? " +
      "WHERE singleton = 1",
    ).run(input.claimToken, input.workerId, input.taskKind, input.taskId, input.now + input.leaseMs);
    return true;
  }

  #assertCurrentDistillLease(
    taskKind: "trigger" | "job",
    taskId: string,
    claimToken: string,
    now: number,
  ): void {
    const lease = this.#readDistillLease();
    if (
      lease.claimToken !== claimToken || lease.taskKind !== taskKind || lease.taskId !== taskId ||
      lease.leaseUntil === null || lease.leaseUntil <= now
    ) throw new ConsolidationConflictError(taskId, "has no current global distill lease for this owner");
  }

  #clearDistillLease(): void {
    this.#database.prepare(
      "UPDATE distill_lease SET claim_token = NULL, worker_id = NULL, task_kind = NULL, task_id = NULL, lease_until = NULL " +
      "WHERE singleton = 1",
    ).run();
  }

  #claimConsolidationJob(
    id: string,
    request: DistillClaimInput,
    claimToken: string,
  ): DistillJobClaim | undefined {
    const row = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(id);
    if (row === undefined) return undefined;
    const current = parsePersistedJobRow(this.#decodeRow(row));
    if (current.status !== "pending" || (current.retryAt !== null && current.retryAt > request.now)) return undefined;
    if (current.leaseUntil !== null && current.leaseUntil > request.now) return undefined;
    const earlierPending = this.#database.prepare(
      "SELECT 1 FROM consolidation_jobs AS earlier " +
      "JOIN consolidation_jobs AS current ON current.id = ? " +
      "WHERE earlier.rowid < current.rowid AND earlier.status = 'pending' " +
      "AND json_extract(earlier.job_json, '$.sessionId') = ? LIMIT 1",
    ).get(id, current.job.sessionId);
    if (earlierPending !== undefined) return undefined;
    if (!this.#acquireDistillSlot({
      claimToken,
      workerId: request.workerId,
      taskKind: "job",
      taskId: id,
      now: request.now,
      leaseMs: request.leaseMs,
    })) return undefined;
    const leaseUntil = request.now + request.leaseMs;
    this.#database.prepare(
      "UPDATE consolidation_jobs SET lease_owner = ?, lease_until = ?, retry_category = NULL, retry_at = NULL " +
      "WHERE id = ? AND status = 'pending'",
    ).run(claimToken, leaseUntil, id);
    return { job: current.job, claimToken, leaseUntil };
  }

  #enqueueConsolidationInTransaction(record: ConsolidationJob): void {
    const serialized = JSON.stringify(record);
    const row = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(record.id);
    if (row !== undefined) {
      const existing = parsePersistedJobRow(this.#decodeRow(row));
      if (JSON.stringify(existing.job) !== serialized) {
        throw new ConsolidationConflictError(record.id, "already exists with a different payload");
      }
      return;
    }
    this.#database.prepare(
      "INSERT INTO consolidation_jobs " +
      "(id, job_json, status, lease_owner, lease_until, retry_category, retry_at) " +
      "VALUES (?, ?, 'pending', NULL, NULL, NULL, NULL)",
    ).run(record.id, serialized);
  }

  #assertExistingJobPayload(record: ConsolidationJob): void {
    const row = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(record.id);
    if (row === undefined) throw new ConsolidationConflictError(record.id, "is missing from its prepared trigger");
    const existing = parsePersistedJobRow(this.#decodeRow(row));
    if (JSON.stringify(existing.job) !== JSON.stringify(record)) {
      throw new ConsolidationConflictError(record.id, "already exists with a different payload");
    }
  }

  #enqueueCleanTaskInTransaction(task: CleanTaskInput): CleanTask {
    const record = parseCleanTaskInput(task);
    const id = cleanTaskId(record.sessionFile);
    const serialized = JSON.stringify(record);
    const row = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (row === undefined) {
      this.#database.prepare(
        "INSERT INTO clean_tasks " +
        "(id, task_json, status, wait_reason, retry_category, retry_at, lease_owner, lease_until) " +
        "VALUES (?, ?, 'pending', NULL, NULL, NULL, NULL, NULL)",
      ).run(id, serialized);
      const inserted = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (inserted === undefined) throw new Error("Clean task insert was not persisted.");
      return this.#cleanTaskItem(parsePersistedCleanTask(this.#decodeRow(inserted)));
    }
    const existing = parsePersistedCleanTask(this.#decodeRow(row));
    if (JSON.stringify(existing.task) === serialized) return this.#cleanTaskItem(existing);
    this.#database.prepare(
      "UPDATE clean_tasks SET task_json = ?, status = 'pending', wait_reason = NULL, retry_category = NULL, " +
      "retry_at = NULL, lease_owner = NULL, lease_until = NULL WHERE id = ?",
    ).run(serialized, id);
    const refreshed = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (refreshed === undefined) throw new Error("Clean task refresh was not persisted.");
    return this.#cleanTaskItem(parsePersistedCleanTask(this.#decodeRow(refreshed)));
  }

  #cleanTaskItem(record: PersistedCleanTask): CleanTask {
    return {
      ...record.task,
      id: cleanTaskId(record.task.sessionFile),
      status: record.status,
      ...(record.waitReason === null ? {} : { waitReason: record.waitReason }),
      ...(record.retryCategory === null ? {} : { retryCategory: record.retryCategory }),
      ...(record.retryAt === null ? {} : { retryAt: record.retryAt }),
    };
  }

  #completeReadyTriggers(triggerIds: readonly string[]): void {
    for (const triggerId of new Set(triggerIds)) {
      const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(triggerId);
      if (row === undefined) throw new ConsolidationConflictError(triggerId, "has a missing trigger record");
      const current = this.#readDistillTrigger(this.#decodeRow(row));
      if (current.status !== "prepared") continue;
      const remaining = this.#database.prepare(
        "SELECT 1 FROM distill_trigger_jobs AS links " +
        "JOIN consolidation_jobs AS jobs ON jobs.id = links.job_id " +
        "WHERE links.trigger_id = ? AND jobs.status = 'pending' LIMIT 1",
      ).get(triggerId);
      if (remaining !== undefined) continue;
      this.#database.prepare(
        "UPDATE distill_triggers SET status = 'complete', retry_category = NULL, retry_at = NULL " +
        "WHERE id = ? AND status = 'prepared'",
      ).run(triggerId);
      this.#enqueueCleanTaskInTransaction({
        sessionId: current.trigger.sessionId,
        sessionFile: current.trigger.sessionFile,
        leafId: current.trigger.leafId,
        cwd: current.trigger.cwd,
        reason: current.trigger.reason,
        intentId: current.trigger.id,
      });
    }
  }

  #finishConsolidationJob(job: ConsolidationJob, claimToken: string): void {
    const triggerIds = this.#jobTriggerIds(job.id);
    if (triggerIds.length > 0) this.#completeReadyTriggers(triggerIds);
    else if (job.sessionFile !== null) {
      this.#enqueueCleanTaskInTransaction({
        sessionId: job.sessionId,
        sessionFile: job.sessionFile,
        leafId: job.leafId,
        cwd: job.cwd,
        reason: "legacy",
        intentId: "legacy-job:" + job.id,
      });
    }
    const lease = this.#readDistillLease();
    if (lease.claimToken !== claimToken || lease.taskKind !== "job" || lease.taskId !== job.id) {
      throw new ConsolidationConflictError(job.id, "lost its global distill lease before completion");
    }
    this.#clearDistillLease();
  }

  #initializeSchema(): void {
    const version = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (version !== 0 && version !== 1 && version !== 2 && version !== 3 && version !== 4 && version !== latestSchemaVersion) {
      throw new Error("Unsupported memory database schema version: " + String(version) + ".");
    }
    const objects = this.#database.prepare(
      `SELECT ${schemaColumns} FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`,
    ).all().map((row) => this.#decodeRow(row));
    if (version === 0 && objects.length === 0) {
      this.#database.exec(currentSchema);
      this.#createAuxiliaryTables();
      this.#database.exec("PRAGMA user_version = 3");
    } else if (version === 0) {
      if (
        objects.length !== 1 ||
        objects[0]?.["name"] !== "memories" ||
        objects[0]?.["type"] !== "table"
      ) throw new Error("Unsupported memory database schema.");
      this.#validateTable(
        "memories",
        objects[0]?.["sql"],
        legacySchema,
        [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      );
      for (const row of this.#database.prepare(
        `SELECT ${textColumn("id")}, ${textColumn("content")}, ${textColumn("created_at", "createdAt")} FROM memories`,
      ).iterate()) parseInput(this.#decodeRow(row));
      this.#database.exec("ALTER TABLE memories RENAME TO memories_legacy");
      this.#database.exec(currentSchema);
      this.#database.exec(
        "INSERT INTO memories (id, content, created_at, revision, updated_at) " +
        "SELECT id, content, created_at, 1, created_at FROM memories_legacy; " +
        "DROP TABLE memories_legacy",
      );
      this.#createAuxiliaryTables();
      this.#database.exec("PRAGMA user_version = 3");
    } else if (version === 1) {
      if (
        objects.length !== 1 ||
        objects[0]?.["name"] !== "memories" ||
        objects[0]?.["type"] !== "table"
      ) throw new Error("Unsupported memory database schema.");
      this.#validateTable(
        "memories",
        objects[0]?.["sql"],
        currentSchema,
        [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "updated_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      );
      this.#validateMemoryRows();
      this.#createAuxiliaryTables();
      this.#database.exec("PRAGMA user_version = 3");
    } else if (version === 2) {
      this.#validateSchema(false, true);
      this.#validateAllRows(false, true);
      this.#database.exec(sessionUsagesSchema);
      this.#database.exec("PRAGMA user_version = 3");
    } else if (version === 3) {
      this.#validateSchema(true, true);
      this.#validateAllRows(true, true);
    }
    if (version !== 4 && version !== latestSchemaVersion) this.#migrateV3ToV4();
    if (version !== latestSchemaVersion) {
      this.#validateSchema(true);
      this.#validateAllRows();
      this.#database.exec(sourceAccessSchema);
      this.#database.exec(deletionsSchema);
      this.#database.exec("PRAGMA user_version = 5");
    }
    this.#validateLatestSchema();
    this.#validateAllRows();
    this.#sourceAccess();
    for (const row of this.#database.prepare(`SELECT ${textColumn("session_file")} FROM session_deletions`).iterate()) {
      const path = this.#decodeRow(row)["session_file"];
      validateAbsolutePath(path, "Stored deletion path");
      this.getDeletion(path);
    }
  }

  #migrateV3ToV4(): void {
    const currentVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 3) throw new Error("Memory schema migration expected version 3.");
    this.#database.exec(
      "ALTER TABLE consolidation_jobs RENAME TO consolidation_jobs_v3; " +
      consolidationJobsSchema + "; " +
      "INSERT INTO consolidation_jobs (rowid, id, job_json, status, lease_owner, lease_until, retry_category, retry_at) " +
      "SELECT rowid, id, job_json, status, lease_owner, lease_until, NULL, NULL " +
      "FROM consolidation_jobs_v3 ORDER BY rowid; " +
      "DROP TABLE consolidation_jobs_v3",
    );
    this.#createTaskTables();
    this.#database.exec("PRAGMA user_version = 4");
  }

  #createAuxiliaryTables(): void {
    this.#database.exec(consolidationJobsV3Schema);
    this.#database.exec(consolidationProgressSchema);
    this.#database.exec(memoryOriginsSchema);
    this.#database.exec(sessionObservationsSchema);
    this.#database.exec(sessionUsagesSchema);
  }

  #createTaskTables(): void {
    this.#database.exec(distillTriggersSchema);
    this.#database.exec(triggerJobsSchema);
    this.#database.exec(distillLeaseSchema);
    this.#database.exec(cleanTasksSchema);
    this.#database.prepare(
      "INSERT INTO distill_lease (singleton, claim_token, worker_id, task_kind, task_id, lease_until) " +
      "VALUES (1, NULL, NULL, NULL, NULL, NULL)",
    ).run();
  }

  #validateLatestSchema(): void {
    this.#validateSchema(true);
  }

  #validateSchema(includeSessionUsages: boolean, useLegacyJobSchema = false): void {
    const objects = this.#database.prepare(
      `SELECT ${schemaColumns} FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`,
    ).all().map((row) => this.#decodeRow(row));
    const schemas = new Map<string, { readonly sql: string; readonly columns: readonly SchemaColumn[] }>([
      ["memories", {
        sql: currentSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "updated_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["consolidation_jobs", {
        sql: useLegacyJobSchema ? consolidationJobsV3Schema : consolidationJobsSchema,
        columns: useLegacyJobSchema ? [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ] : [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      }],
      ["consolidation_progress", {
        sql: consolidationProgressSchema,
        columns: [
          { name: "source_key", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "processed_at", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["memory_origins", {
        sql: memoryOriginsSchema,
        columns: [
          { name: "memory_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["session_observations", {
        sql: sessionObservationsSchema,
        columns: [
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "captured", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
    ]);
    if (includeSessionUsages) {
      schemas.set("session_usages", {
        sql: sessionUsagesSchema,
        columns: [
          { name: "owner_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "process_token", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "state", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      });
    }
    if (!useLegacyJobSchema && includeSessionUsages) {
      schemas.set("distill_triggers", {
        sql: distillTriggersSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "trigger_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "compaction_record_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
      schemas.set("distill_trigger_jobs", {
        sql: triggerJobsSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "trigger_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "ordinal", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      });
      schemas.set("distill_lease", {
        sql: distillLeaseSchema,
        columns: [
          { name: "singleton", type: "INTEGER", notNull: 1, primaryKey: 1 },
          { name: "claim_token", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "worker_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "task_kind", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "task_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
      schemas.set("clean_tasks", {
        sql: cleanTasksSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "task_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "wait_reason", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
    }
    if (this.#database.prepare("PRAGMA user_version").get()?.["user_version"] === 5) {
      schemas.set("source_access", { sql: sourceAccessSchema, columns: [
        { name: "singleton", type: "INTEGER", notNull: 1, primaryKey: 1 },
        { name: "token", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
      ] });
      schemas.set("session_deletions", { sql: deletionsSchema, columns: [
        { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 1 },
        { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "intent_id", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "file_version", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "state", type: "TEXT", notNull: 1, primaryKey: 0 },
      ] });
    }
    if (objects.length !== schemas.size) throw new Error("Unsupported memory database schema.");
    for (const object of objects) {
      const name = object["name"];
      if (typeof name !== "string" || object["type"] !== "table") {
        throw new Error("Unsupported memory database schema.");
      }
      const schema = schemas.get(name);
      if (schema === undefined) throw new Error("Unsupported memory database schema.");
      this.#validateTable(name, object["sql"], schema.sql, schema.columns);
    }
  }

  #validateTable(
    name: string,
    sql: unknown,
    expectedSql: string,
    expectedColumns: readonly SchemaColumn[],
  ): void {
    if (
      typeof sql !== "string" ||
      sql.replace(/\s+/g, "").toUpperCase() !== expectedSql.replace(/\s+/g, "").toUpperCase()
    ) throw new Error("Unsupported memory database schema.");
    const table = this.#database.prepare(
      "SELECT strict, wr FROM pragma_table_list WHERE schema = 'main' AND name = ?",
    ).get(name);
    const columns = this.#database.prepare(
      `SELECT ${textColumn("name")}, ${textColumn("type")}, "notnull", pk, hidden FROM pragma_table_xinfo(?)`,
    ).all(name).map((row) => this.#decodeRow(row));
    if (
      table?.["strict"] !== 1 || table["wr"] !== 0 || columns.length !== expectedColumns.length ||
      columns.some((column, index) => {
        const expected = expectedColumns[index];
        return expected === undefined ||
          column["name"] !== expected.name ||
          column["type"] !== expected.type ||
          column["notnull"] !== expected.notNull ||
          column["pk"] !== expected.primaryKey ||
          column["hidden"] !== 0;
      })
    ) throw new Error("Unsupported memory database schema.");
  }

  #validateMemoryRows(): void {
    for (const row of this.#database.prepare(
      `SELECT ${memoryColumns} FROM memories`,
    ).iterate()) parseMemory(this.#decodeRow(row));
  }

  #validateAllRows(includeSessionUsages = true, useLegacyJobColumns = false): void {
    this.#validateMemoryRows();
    const jobs = this.#database.prepare(
      `SELECT ${useLegacyJobColumns ? legacyJobColumns : jobColumns} FROM consolidation_jobs`,
    ).all().map((row) => parsePersistedJobRow(this.#decodeRow(row)));
    const jobsById = new Map(jobs.map((record) => [record.job.id, record]));
    for (const row of this.#database.prepare(
      `SELECT ${progressColumns} FROM consolidation_progress`,
    ).iterate()) {
      const progress = parseProgressRow(this.#decodeRow(row));
      const sourceJob = jobsById.get(progress.jobId);
      if (sourceJob === undefined || !sourceJob.job.sourceKeys.includes(progress.sourceKey)) {
        throw new TypeError("Stored source progress does not match its consolidation job.");
      }
    }
    for (const row of this.#database.prepare(
      `SELECT ${originColumns} FROM memory_origins`,
    ).iterate()) {
      const origin = parseMemoryOriginRow(this.#decodeRow(row));
      const sourceJob = jobsById.get(origin.jobId);
      if (sourceJob === undefined || sourceJob.status !== "complete") {
        throw new TypeError("Stored memory origin does not match a completed consolidation job.");
      }
    }
    for (const row of this.#database.prepare(
      `SELECT ${observationColumns} FROM session_observations`,
    ).iterate()) parseStoredSessionObservation(this.#decodeRow(row));
    if (includeSessionUsages) {
      for (const row of this.#database.prepare(
        `SELECT ${sessionUsageColumns} FROM session_usages`,
      ).iterate()) parseStoredSessionUsage(this.#decodeRow(row));
      if (!useLegacyJobColumns) {
        const triggers = new Map<string, PersistedDistillTrigger>();
        for (const row of this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers`).iterate()) {
          const trigger = this.#readDistillTrigger(row);
          triggers.set(trigger.trigger.id, trigger);
          for (const jobId of trigger.jobIds) {
            const linkedJob = jobsById.get(jobId);
            if (
              linkedJob === undefined ||
              linkedJob.job.sessionId !== trigger.trigger.sessionId ||
              linkedJob.job.sessionFile !== trigger.trigger.sessionFile
            ) throw new TypeError("Stored distill trigger batch does not match its session source.");
            if (trigger.status === "complete" && linkedJob.status !== "complete") {
              throw new TypeError("Stored completed distill trigger has pending batches.");
            }
          }
        }
        for (const raw of this.#database.prepare(
          "SELECT id, trigger_id AS triggerId, job_id AS jobId " +
          "FROM distill_trigger_jobs ORDER BY rowid",
        ).iterate()) {
          const link = fields(this.#decodeRow(raw), ["id", "triggerId", "jobId"]);
          validateText(link["id"], "stored trigger batch link ID");
          validateText(link["triggerId"], "stored trigger batch trigger ID");
          validateText(link["jobId"], "stored trigger batch job ID");
          if (!triggers.has(link["triggerId"]) || !jobsById.has(link["jobId"])) {
            throw new TypeError("Stored distill trigger batch link is orphaned.");
          }
        }
        for (const row of this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks`).iterate()) {
          parsePersistedCleanTask(this.#decodeRow(row));
        }
        const globalLease = this.#readDistillLease();
        if (globalLease.claimToken !== null && globalLease.taskKind !== null && globalLease.taskId !== null) {
          if (globalLease.taskKind === "job") {
            const leasedJob = jobsById.get(globalLease.taskId);
            if (
              leasedJob === undefined || leasedJob.status !== "pending" ||
              leasedJob.leaseOwner !== globalLease.claimToken ||
              leasedJob.leaseUntil !== globalLease.leaseUntil
            ) throw new TypeError("Stored global distill lease does not match its consolidation job.");
          } else {
            const leasedTrigger = triggers.get(globalLease.taskId);
            if (leasedTrigger === undefined || leasedTrigger.status !== "pending") {
              throw new TypeError("Stored global distill lease does not match its trigger.");
            }
          }
        }
      }
    }
  }

  #transaction<T>(operation: () => T, mode: "IMMEDIATE" | "DEFERRED" = "IMMEDIATE"): T {
    // Mutations reserve the writer; read-only assessments use a SQLite read snapshot.
    this.#database.exec(mode === "IMMEDIATE" ? "BEGIN IMMEDIATE" : "BEGIN DEFERRED");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) {
        try {
          this.#database.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], "Memory transaction and rollback failed.");
        }
      }
      throw error;
    }
  }

  #ensureOpen(): void {
    if (!this.#database.isOpen) throw new Error("Memory store is closed.");
  }
}
