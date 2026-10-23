import { dirname, isAbsolute } from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import { SourceRegistry, initializeSourceRegistry, SOURCE_REGISTRY_SCHEMAS } from "./source-registry.ts";
import type {
  ConsolidationCommit,
  ConsolidationJob,
  SessionObservation,
} from "./consolidation-types.js";
import type { SessionUsage } from "./session-usage-types.js";
import {
  canRetryDistillFailure,
  makeDistillFailureRecord,
  parseDistillFailureInput,
  parseDistillFailureRecord,
  parseDistillRetryContext,
  parseDistillTaskKind,
  type DistillFailureRecord,
  type DistillRetryContext,
  type DistillTaskKind,
} from "./distill-retry.ts";
import type {
  CleanTask,
  CleanTaskInput,
  CleanTaskPageOptions,
  DistillClaimInput,
  DistillFailure,
  DistillFailureKind,
  DistillJobClaim,
  DistillTrigger,
  DistillTriggerInput,
  DistillTriggerClaim,
  DistillTriggerClaimInput,
  PrepareDistillTriggerInput,
  ReleaseDistillClaimInput,
  RetryCategory,
  RetryRequest,
  TaskPage,
  TaskSweepPage,
  TaskSweepPageOptions,
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
const maxMetadataGcPageSize = 128;

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
const consolidationProgressSchemaV6 =
  "CREATE TABLE consolidation_progress (" +
  "source_key TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "processed_at INTEGER NOT NULL" +
  ") STRICT";
const consolidationProgressSchema =
  "CREATE TABLE consolidation_progress (" +
  "source_key TEXT PRIMARY KEY NOT NULL, " +
  "processed_at INTEGER NOT NULL" +
  ") STRICT";
const memoryOriginsSchemaV6 =
  "CREATE TABLE memory_origins (" +
  "memory_id TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "revision INTEGER NOT NULL" +
  ") STRICT";
const memoryOriginsSchema =
  "CREATE TABLE memory_origins (" +
  "memory_id TEXT PRIMARY KEY NOT NULL, " +
  "evidence_id TEXT NOT NULL, " +
  "revision INTEGER NOT NULL" +
  ") STRICT";
const memoryEvidenceSchema =
  "CREATE TABLE memory_evidence (" +
  "evidence_id TEXT PRIMARY KEY NOT NULL, " +
  "session_id TEXT NOT NULL, " +
  "session_file TEXT, " +
  "source_generation TEXT, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "source_keys TEXT NOT NULL, " +
  "lineage_session_ids TEXT, " +
  "source_time TEXT, " +
  "branch_anchor TEXT, " +
  "CHECK ((session_file IS NULL AND source_generation IS NULL) OR " +
  "(session_file IS NOT NULL AND source_generation IS NOT NULL))" +
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
const distillFailuresSchema =
  "CREATE TABLE distill_failures (" +
  "task_kind TEXT NOT NULL CHECK (task_kind IN ('trigger', 'job')), " +
  "task_id TEXT NOT NULL, " +
  "reason TEXT NOT NULL CHECK (reason IN ('source_invalid', 'model_unavailable', 'model_invalid', 'model_budget', 'transient', 'conflict')), " +
  "source_version TEXT NOT NULL, " +
  "model_key TEXT NOT NULL, " +
  "policy_version TEXT NOT NULL, " +
  "attempts INTEGER NOT NULL CHECK (attempts > 0), " +
  "next_attempt_at INTEGER, " +
  "PRIMARY KEY (task_kind, task_id)" +
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
const deletionsSchemaV5 = "CREATE TABLE session_deletions (session_file TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL, file_version TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))) STRICT";
const deletionsSchema = "CREATE TABLE session_deletions (session_file TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL, file_version TEXT NOT NULL, source_generation TEXT, state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))) STRICT";
const sourceBackfillSchema = "CREATE TABLE source_backfill (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), phase INTEGER NOT NULL, cursor_row INTEGER NOT NULL) STRICT";
const metadataGcSchema = "CREATE TABLE metadata_gc_state (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), phase INTEGER NOT NULL CHECK (phase BETWEEN 0 AND 7), cursor_row INTEGER NOT NULL CHECK (cursor_row >= 0)) STRICT";
const storeIndexes = [
  "CREATE INDEX jobs_source_status ON consolidation_jobs (json_extract(job_json, '$.sessionFile'), status)",
  "CREATE INDEX triggers_source_status ON distill_triggers (json_extract(trigger_json, '$.sessionFile'), status)",
  "CREATE INDEX clean_source ON clean_tasks (json_extract(task_json, '$.sessionFile'))",
  "CREATE INDEX usages_source ON session_usages (session_file)",
  "CREATE INDEX observations_source ON session_observations (session_file)",
  "CREATE INDEX trigger_links_job ON distill_trigger_jobs (job_id)",
];
const storeIndexesV7 = [
  "CREATE INDEX memory_origins_evidence ON memory_origins (evidence_id)",
  "CREATE INDEX session_deletions_generation ON session_deletions (source_generation, state)",
];
const storeSchemas = [sourceBackfillSchema, ...storeIndexes].map(sql => {
  const match = /^CREATE (TABLE|INDEX) (\w+)/.exec(sql);
  if (!match?.[2]) throw new Error("Invalid store schema declaration.");
  return {name:match[2],sql};
});
const storeSchemasV7 = [...storeSchemas, ...[metadataGcSchema, ...storeIndexesV7].map(sql => {
  const match = /^CREATE (TABLE|INDEX) (\w+)/.exec(sql);
  if (!match?.[2]) throw new Error("Invalid store schema declaration.");
  return {name:match[2],sql};
})];
const storeSchemasV8 = [...storeSchemasV7, { name: "distill_failures", sql: distillFailuresSchema }];
const latestSchemaVersion = 8;

export interface SessionDeletion {
  readonly sessionFile: string;
  readonly sessionId: string;
  readonly intentId: string;
  readonly fileVersion: string;
  readonly sourceGeneration: string | null;
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
const progressColumnsV6 = [textColumn("source_key"), textColumn("job_id"), "processed_at"].join(", ");
const progressColumns = [textColumn("source_key"), "processed_at"].join(", ");
const originColumnsV6 = [textColumn("memory_id", "memoryId"), textColumn("job_id", "jobId"), "revision"].join(", ");
const originColumns = [textColumn("memory_id", "memoryId"), textColumn("evidence_id", "evidenceId"), "revision"].join(", ");
const evidenceColumns = [
  textColumn("evidence_id", "evidenceId"), textColumn("session_id", "sessionId"),
  textColumn("session_file", "sessionFile"), textColumn("source_generation", "sourceGeneration"),
  textColumn("cwd"), textColumn("leaf_id", "leafId"), textColumn("source_keys", "sourceKeys"),
  textColumn("lineage_session_ids", "lineageSessionIds"), textColumn("source_time", "sourceTime"),
  textColumn("branch_anchor", "branchAnchor"),
].join(", ");
const deletionColumns = [
  textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
  textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"),
  textColumn("source_generation", "sourceGeneration"), textColumn("state"),
].join(", ");
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
const distillFailureColumns = [
  textColumn("task_kind", "kind"), textColumn("task_id", "id"), textColumn("reason"),
  textColumn("source_version", "sourceVersion"), textColumn("model_key", "modelKey"),
  textColumn("policy_version", "policyVersion"), "attempts", "next_attempt_at AS nextAttemptAt",
].join(", ");

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

interface MemoryEvidence {
  readonly evidenceId: string;
  readonly sessionId: string;
  readonly sessionFile: string | null;
  readonly sourceGeneration: string | null;
  readonly cwd: string;
  readonly leafId: string | null;
  readonly sourceKeys: readonly string[];
  readonly lineageSessionIds?: readonly string[];
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

interface SchemaColumn {
  readonly name: string;
  readonly type: "TEXT" | "INTEGER";
  readonly notNull: 0 | 1;
  readonly primaryKey: 0 | 1 | 2;
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

function parseTaskSweepPageOptions(value: unknown): {
  readonly limit: number;
  readonly cursor?: number;
  readonly throughCursor?: number;
} {
  const allowed = ["limit", "cursor", "throughCursor"];
  if (!isRecord(value) || Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(value, "limit")) {
    throw new TypeError("Task sweep page requires a limit and only supported cursor fields.");
  }
  const base = parseTaskPageOptions({
    limit: value["limit"],
    ...(Object.hasOwn(value, "cursor") ? { cursor: value["cursor"] } : {}),
  }, false);
  let throughCursor: number | undefined;
  if (Object.hasOwn(value, "throughCursor")) {
    validateNow(value["throughCursor"], "Task sweep upper cursor");
    throughCursor = value["throughCursor"];
    if ((base.cursor ?? 0) > throughCursor) throw new TypeError("Task sweep cursor cannot exceed its upper cursor.");
  }
  return {
    limit: base.limit,
    ...(base.cursor === undefined ? {} : { cursor: base.cursor }),
    ...(throughCursor === undefined ? {} : { throughCursor }),
  };
}

function parseDistillClaimInput(value: unknown): DistillClaimInput {
  if (!isRecord(value)) throw new TypeError("Distill claim input must be an object.");
  const allowed = ["workerId", "now", "leaseMs", "retryContext"];
  if (Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(value, "workerId") || !Object.hasOwn(value, "now") || !Object.hasOwn(value, "leaseMs")) {
    throw new TypeError("Distill claim requires workerId, now, and leaseMs.");
  }
  const { workerId, now, leaseMs } = value;
  validateText(workerId, "distill worker ID");
  validateNow(now, "Distill claim time");
  if (typeof leaseMs !== "number" || !Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new TypeError("Distill lease duration must be a positive safe integer.");
  }
  if (!Number.isSafeInteger(now + leaseMs)) throw new TypeError("Distill lease expiration is outside the safe integer range.");
  return {
    workerId,
    now,
    leaseMs,
    ...(Object.hasOwn(value, "retryContext") ? { retryContext: parseDistillRetryContext(value["retryContext"]) } : {}),
  };
}

function parseDistillTriggerClaimInput(value: unknown): DistillTriggerClaimInput {
  if (!isRecord(value)) throw new TypeError("Distill trigger claim input must be an object.");
  const allowed = ["id", "workerId", "now", "leaseMs", "retryContext", "allowCandidate"];
  if (Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(value, "workerId") || !Object.hasOwn(value, "now") || !Object.hasOwn(value, "leaseMs")) {
    throw new TypeError("Distill trigger claim requires workerId, now, and leaseMs.");
  }
  const base = parseDistillClaimInput({
    workerId: value["workerId"],
    now: value["now"],
    leaseMs: value["leaseMs"],
    ...(Object.hasOwn(value, "retryContext") ? { retryContext: value["retryContext"] } : {}),
  });
  let id: string | undefined;
  if (Object.hasOwn(value, "id")) {
    const idValue = value["id"];
    validateText(idValue, "distill trigger ID");
    id = idValue;
  }
  const allowCandidate = Object.hasOwn(value, "allowCandidate") ? value["allowCandidate"] : false;
  if (typeof allowCandidate !== "boolean") throw new TypeError("Candidate claim flag must be boolean.");
  if (allowCandidate && id === undefined) throw new TypeError("Candidate claims require a trigger ID.");
  return {
    ...base,
    ...(id === undefined ? {} : { id }),
    ...(allowCandidate ? { allowCandidate: true } : {}),
  };
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
  const allowed = ["claimToken", "now", "retry", "failure"];
  if (Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key))) {
    throw new TypeError("Distill claim release has unsupported fields.");
  }
  const { claimToken, now } = value;
  validateText(claimToken, "distill claim token");
  validateNow(now, "Distill release time");
  if (Object.hasOwn(value, "retry") && Object.hasOwn(value, "failure")) {
    throw new TypeError("Distill claim release cannot contain both retry and failure metadata.");
  }
  return {
    claimToken,
    now,
    ...(Object.hasOwn(value, "retry") ? { retry: parseRetryRequest(value["retry"]) } : {}),
    ...(Object.hasOwn(value, "failure") ? { failure: parseDistillFailureInput(value["failure"]) } : {}),
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
  readonly evidenceId: string;
  readonly revision: number;
} {
  const { memoryId, evidenceId, revision } = fields(value, ["memoryId", "evidenceId", "revision"]);
  validateText(memoryId, "memory origin ID");
  validateText(evidenceId, "memory origin evidence ID");
  validateRevision(revision);
  return { memoryId, evidenceId, revision };
}

function parseMemoryOriginRowV6(value: unknown): {
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
  readonly processedAt: number;
} {
  const { source_key, processed_at } = fields(value, ["source_key", "processed_at"]);
  validateText(source_key, "processed source key");
  validateNow(processed_at, "Stored source processing time");
  return { sourceKey: source_key, processedAt: processed_at };
}

function parseProgressRowV6(value: unknown): {
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

function parseEvidenceRow(value: unknown): MemoryEvidence {
  const row = fields(value, [
    "evidenceId", "sessionId", "sessionFile", "sourceGeneration", "cwd", "leafId",
    "sourceKeys", "lineageSessionIds", "sourceTime", "branchAnchor",
  ]);
  validateText(row["evidenceId"], "stored evidence ID");
  validateText(row["sessionId"], "stored evidence session ID");
  const sessionFile = row["sessionFile"] === null
    ? null
    : (validateAbsolutePath(row["sessionFile"], "stored evidence session file"), row["sessionFile"] as string);
  const sourceGeneration = row["sourceGeneration"] === null
    ? null
    : (validateText(row["sourceGeneration"], "stored source generation"), row["sourceGeneration"] as string);
  if ((sessionFile === null) !== (sourceGeneration === null)) {
    throw new TypeError("Stored evidence source identity is incomplete.");
  }
  validateAbsolutePath(row["cwd"], "stored evidence working directory");
  const leafId = parseNullableText(row["leafId"], "stored evidence leaf ID");
  const parseJson = (value: unknown, field: string): unknown => {
    validateText(value, field);
    try { return JSON.parse(value); }
    catch (error) { throw new TypeError(field + " is not valid JSON.", { cause: error }); }
  };
  const sourceKeys = parseSourceKeys(parseJson(row["sourceKeys"], "stored evidence source keys"), "stored evidence source keys", false);
  const lineage = row["lineageSessionIds"] === null
    ? undefined
    : parseSourceKeys(parseJson(row["lineageSessionIds"], "stored evidence lineage IDs"), "stored evidence lineage IDs", false);
  const sourceTime = row["sourceTime"] === null
    ? undefined
    : (validateTimestamp(row["sourceTime"], "stored evidence source time"), row["sourceTime"] as string);
  const branchAnchor = row["branchAnchor"] === null
    ? undefined
    : (validateText(row["branchAnchor"], "stored evidence branch anchor"), row["branchAnchor"] as string);
  return {
    evidenceId: row["evidenceId"],
    sessionId: row["sessionId"],
    sessionFile,
    sourceGeneration,
    cwd: row["cwd"],
    leafId,
    sourceKeys,
    ...(lineage === undefined ? {} : { lineageSessionIds: lineage }),
    ...(sourceTime === undefined ? {} : { sourceTime }),
    ...(branchAnchor === undefined ? {} : { branchAnchor }),
  };
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
  #sources: SourceRegistry | undefined;
  get sources(): SourceRegistry { return this.#sources ??= new SourceRegistry(this.#database); }
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
        "INSERT INTO memory_origins (memory_id, evidence_id, revision) VALUES (?, ?, ?) " +
        "ON CONFLICT(memory_id) DO UPDATE SET evidence_id = excluded.evidence_id, revision = excluded.revision",
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
        textColumn("o.evidence_id", "originEvidenceId"), "o.revision AS originRevision",
        textColumn("e.evidence_id", "evidenceId"), textColumn("e.session_id", "sessionId"),
        textColumn("e.session_file", "sessionFile"), textColumn("e.source_generation", "sourceGeneration"),
        textColumn("e.cwd", "cwd"), textColumn("e.leaf_id", "leafId"),
        textColumn("e.source_keys", "sourceKeys"), textColumn("e.lineage_session_ids", "lineageSessionIds"),
        textColumn("e.source_time", "sourceTime"), textColumn("e.branch_anchor", "branchAnchor"),
      ].join(", ") + " " +
      "FROM memories AS m " +
      "LEFT JOIN memory_origins AS o ON o.memory_id = m.id " +
      "LEFT JOIN memory_evidence AS e ON e.evidence_id = o.evidence_id " +
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
      originEvidenceId,
      originRevision,
      evidenceId,
      sessionId,
      sessionFile,
      sourceGeneration,
      cwd,
      leafId,
      sourceKeys,
      lineageSessionIds,
      sourceTime,
      branchAnchor,
    } = fields(this.#decodeRow(row), [
      "currentId",
      "content",
      "createdAt",
      "currentRevision",
      "updatedAt",
      "memoryId",
      "originEvidenceId",
      "originRevision",
      "evidenceId",
      "sessionId",
      "sessionFile",
      "sourceGeneration",
      "cwd",
      "leafId",
      "sourceKeys",
      "lineageSessionIds",
      "sourceTime",
      "branchAnchor",
    ]);
    const current = parseMemory({
      id: currentId,
      content,
      createdAt,
      revision: currentRevision,
      updatedAt,
    });
    if (memoryId === null) {
      if (originEvidenceId !== null || originRevision !== null || evidenceId !== null || sessionId !== null ||
        sessionFile !== null || sourceGeneration !== null || cwd !== null || leafId !== null || sourceKeys !== null ||
        lineageSessionIds !== null || sourceTime !== null || branchAnchor !== null) {
        throw new TypeError("Stored memory origin row is inconsistent.");
      }
      return undefined;
    }
    const origin = parseMemoryOriginRow({ memoryId, evidenceId: originEvidenceId, revision: originRevision });
    if (origin.memoryId !== current.id || origin.revision !== current.revision) return undefined;
    if (evidenceId === null) throw new TypeError("Stored memory origin references missing evidence.");
    const evidence = parseEvidenceRow({
      evidenceId, sessionId, sessionFile, sourceGeneration, cwd, leafId, sourceKeys,
      lineageSessionIds, sourceTime, branchAnchor,
    });
    return {
      sessionId: evidence.sessionId,
      sourceKeys: [...evidence.sourceKeys],
      ...(evidence.lineageSessionIds === undefined
        ? {}
        : { lineageSessionIds: [...evidence.lineageSessionIds] }),
      ...(evidence.sourceTime === undefined ? {} : { sourceTime: evidence.sourceTime }),
      ...(evidence.branchAnchor === undefined ? {} : { branchAnchor: evidence.branchAnchor }),
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
      if (record.sessionFile !== null) this.#assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
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
      this.#clearDistillFailure("trigger", id);
      return true;
    });
  }

  listDistillTriggers(options: TaskSweepPageOptions): TaskSweepPage<DistillTrigger> {
    this.#ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.#taskSweepThroughCursor("distill_triggers", page.throughCursor);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${triggerColumns} FROM distill_triggers ` +
      "WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
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
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  readDistillTrigger(id: string): DistillTrigger | undefined {
    this.#ensureOpen();
    validateText(id, "distill trigger ID");
    const row = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
    if (row === undefined) return undefined;
    return this.#distillTriggerItem(this.#readDistillTrigger(row));
  }

  readDistillFailure(kind: DistillFailureKind, id: string): DistillFailure | undefined {
    this.#ensureOpen();
    const taskKind = parseDistillTaskKind(kind);
    validateText(id, "distill task ID");
    return this.#readDistillFailure(taskKind, id);
  }

  /** Advisory only; the fenced claim methods remain authoritative. */
  canAttemptDistill(
    kind: DistillFailureKind,
    id: string,
    context: DistillRetryContext | undefined,
    now: number,
  ): boolean {
    this.#ensureOpen();
    const taskKind = parseDistillTaskKind(kind);
    validateText(id, "distill task ID");
    validateNow(now, "Distill eligibility time");
    const retryContext = context === undefined ? undefined : parseDistillRetryContext(context);
    if (taskKind === "trigger") {
      const row = this.#database.prepare(
        `SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`,
      ).get(id);
      if (row === undefined) return false;
      const task = this.#readDistillTrigger(this.#decodeRow(row));
      return (task.status === "pending" || task.status === "candidate") &&
        this.#distillTaskCanAttempt(taskKind, id, retryContext, now, task.retryAt);
    }
    const row = this.#database.prepare(
      `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
    ).get(id);
    if (row === undefined) return false;
    const task = parsePersistedJobRow(this.#decodeRow(row));
    return task.status === "pending" && this.#distillTaskCanAttempt(taskKind, id, retryContext, now, task.retryAt);
  }

  claimDistillTrigger(input: DistillTriggerClaimInput): DistillTriggerClaim | undefined {
    this.#ensureOpen();
    const request = parseDistillTriggerClaimInput(input);
    return this.#transaction(() => {
      let rows: Record<string, unknown>[];
      if (request.id === undefined) {
        rows = this.#database.prepare(
          `SELECT ${triggerColumns} FROM distill_triggers ` +
          "WHERE status = 'pending' AND (retry_at IS NULL OR retry_at <= ?) " +
          "AND NOT EXISTS (SELECT 1 FROM distill_failures AS f WHERE f.task_kind = 'trigger' AND f.task_id = distill_triggers.id) " +
          "ORDER BY rowid LIMIT 32",
        ).all(request.now);
      } else {
        rows = [];
        const row = this.#database.prepare(
          `SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`,
        ).get(request.id);
        if (row !== undefined) rows.push(row);
      }
      for (const row of rows) {
        const current = this.#readDistillTrigger(this.#decodeRow(row));
        if (current.status !== "pending" && !(request.allowCandidate && current.status === "candidate")) continue;
        if (request.id !== undefined && !this.#distillTaskCanAttempt(
          "trigger", request.id, request.retryContext, request.now, current.retryAt,
        )) continue;
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
          `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
          `${textColumn("job_id", "jobId")}, ordinal FROM distill_trigger_jobs WHERE id = ?`,
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
      this.#clearDistillFailure("trigger", request.id);
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
      if (lease.claimToken !== request.claimToken || lease.taskKind === null || lease.taskId === null ||
        lease.leaseUntil === null || lease.leaseUntil <= request.now) return false;
      if (request.failure !== undefined) {
        const kind = lease.taskKind;
        const id = lease.taskId;
        const row = kind === "job"
          ? this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(id)
          : this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(id);
        if (row === undefined) return false;
        if (kind === "job") {
          const current = parsePersistedJobRow(this.#decodeRow(row));
          if (current.status !== "pending" || current.leaseOwner !== request.claimToken ||
            current.leaseUntil === null || current.leaseUntil <= request.now) return false;
        } else {
          const current = this.#readDistillTrigger(this.#decodeRow(row));
          if (current.status !== "pending" && current.status !== "candidate") return false;
        }
        const previous = this.#readDistillFailure(kind, id);
        const failure = makeDistillFailureRecord({
          kind,
          id,
          failure: request.failure,
          ...(previous === undefined ? {} : { previous }),
          now: request.now,
        });
        this.#persistDistillFailure(failure);
        const retryCategory = failure.nextAttemptAt === null
          ? null
          : failure.reason === "transient" || failure.reason === "conflict" ? "transient"
            : failure.reason === "model_invalid" || failure.reason === "model_budget" ? "invalid" : "unavailable";
        if (kind === "job") {
          this.#database.prepare(
            "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, retry_category = ?, retry_at = ? " +
            "WHERE id = ? AND status = 'pending' AND lease_owner = ?",
          ).run(retryCategory, failure.nextAttemptAt, id, request.claimToken);
        } else {
          this.#database.prepare(
            "UPDATE distill_triggers SET retry_category = ?, retry_at = ? WHERE id = ? AND status = 'pending'",
          ).run(retryCategory, failure.nextAttemptAt, id);
        }
        this.#clearDistillLease();
        return true;
      }
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

  listPendingConsolidationJobs(options: TaskSweepPageOptions): TaskSweepPage<ConsolidationJob> {
    this.#ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.#taskSweepThroughCursor("consolidation_jobs", page.throughCursor);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${jobColumns} FROM consolidation_jobs ` +
      "WHERE status = 'pending' AND rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.#decodeRow(raw);
      const { cursorRow, ...jobRow } = decoded;
      validateNow(cursorRow, "Stored pending consolidation job cursor");
      const record = parsePersistedJobRow(jobRow);
      if (record.status !== "pending") throw new TypeError("Pending consolidation job query returned a terminal job.");
      return record.job;
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.#decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next pending consolidation job cursor");
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
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
  readConsolidationStatus(keys: readonly string[], scope?: readonly string[]): {
    readonly processedSources: readonly string[];
    readonly pendingJobs: readonly ConsolidationJob[];
    readonly pendingTriggers: readonly DistillTrigger[];
    readonly observations: readonly SessionObservation[];
    readonly usages: readonly SessionUsage[];
  } {
    this.#ensureOpen();
    return this.#transaction(() => {
      const filter = this.#scopeFilter(scope);
      const rows = (table: string, columns: string, path: string, condition = "1") => {
        const items = this.#database.prepare(`SELECT ${columns} FROM ${table} WHERE ${condition} AND ${filter.sql(path)} ORDER BY rowid LIMIT ?`)
          .all(...filter.values, maxSessionMetadataRows + 1);
        if (items.length > maxSessionMetadataRows) throw new RangeError("Source metadata exceeds its assessment budget.");
        return items;
      };
      return {
        processedSources: this.processedSources(keys),
        pendingJobs: rows("consolidation_jobs", jobColumns, "json_extract(job_json, '$.sessionFile')", "status = 'pending'")
          .map(row => parsePersistedJobRow(this.#decodeRow(row)).job),
        pendingTriggers: rows("distill_triggers", triggerColumns, "json_extract(trigger_json, '$.sessionFile')", "status <> 'complete'")
          .map(row => this.#distillTriggerItem(this.#readDistillTrigger(row))),
        observations: rows("session_observations", observationColumns, "session_file")
          .map(row => parseStoredSessionObservation(this.#decodeRow(row)).observation),
        usages: rows("session_usages", sessionUsageColumns, "session_file")
          .map(row => parseStoredSessionUsage(this.#decodeRow(row))),
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
        this.#clearDistillFailure("job", request.jobId);
        this.#finishConsolidationJob(current.job, request.owner);
        return false;
      }

      if (request.creates.length > 0 || request.updates.length > 0) {
        this.#persistMemoryEvidence(current.job);
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
        "INSERT INTO consolidation_progress (source_key, processed_at) VALUES (?, ?)",
      );
      for (const key of remaining) insertProgress.run(key, request.now);
      const completion = this.#database.prepare(
        "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL, " +
        "retry_category = NULL, retry_at = NULL " +
        "WHERE id = ? AND status = 'pending'",
      ).run(request.jobId);
      if (completion.changes !== 1) {
        throw new ConsolidationConflictError(request.jobId, "changed before completion");
      }
      this.#clearDistillFailure("job", request.jobId);
      this.#finishConsolidationJob(current.job, request.owner);
      return true;
    });
  }

  observeSession(observation: SessionObservation): void {
    this.#ensureOpen();
    const record = parseSessionObservation(observation);
    this.#transaction(() => {
      if (record.sessionFile !== null) this.#registerSource({sessionId: record.sessionId, sessionFile: record.sessionFile, cwd: record.cwd});
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
      if (record.sessionFile !== null) this.sources.register({sessionId: record.sessionId, sessionFile: record.sessionFile, cwd: record.cwd});
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
  listObservedSessions(options: TaskSweepPageOptions): TaskSweepPage<SessionObservation> {
    this.#ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.#taskSweepThroughCursor("session_observations", page.throughCursor);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${observationColumns} FROM session_observations ` +
      "WHERE captured = 0 AND rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
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
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  /** Startup recovery also covers a crash between the first file write and message_end. */
  listUnobservedSessionUsages(options: TaskSweepPageOptions): TaskSweepPage<SessionUsage> {
    const page = parseTaskSweepPageOptions(options);
    const throughCursor = this.#taskSweepThroughCursor("session_usages", page.throughCursor);
    const rows = this.#database.prepare(
      `SELECT rowid AS cursorRow, ${sessionUsageColumns} FROM session_usages ` +
      "WHERE rowid > ? AND rowid <= ? AND NOT EXISTS (SELECT 1 FROM session_observations o WHERE o.session_id = session_usages.session_id) " +
      "AND NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = session_usages.session_file AND d.state = 'complete') " +
      "ORDER BY rowid LIMIT ?",
    ).all(page.cursor ?? 0, throughCursor, page.limit + 1);
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const {cursorRow, ...owner} = this.#decodeRow(raw);
      validateNow(cursorRow, "Session usage cursor");
      return parseStoredSessionUsage(owner);
    });
    const nextCursor = rows.length > page.limit ? visible.at(-1)?.["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next session usage cursor");
    return {items, throughCursor, ...(nextCursor === undefined ? {} : {nextCursor})};
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
    return this.#database.prepare(
      "SELECT " + textColumn("session_file", "path") + " FROM managed_sources " +
      "WHERE deleted = 0 AND (? IS NULL OR session_file COLLATE BINARY > ?) " +
      "GROUP BY session_file ORDER BY session_file COLLATE BINARY LIMIT ?",
    ).all(afterPath ?? null, afterPath ?? null, limit).map(raw => {
      const row = this.#decodeRow(raw);
      validateAbsolutePath(row["path"], "Stored source path");
      return row["path"];
    });
  }

  #registerSource(source: {sessionId: string; sessionFile: string; cwd: string}): void {
    this.sources.register(source);
  }

  #assertSourceEnqueueAllowed(sessionId: string, sessionFile: string, cwd: string): void {
    this.#registerSource({sessionId, sessionFile, cwd});
    const source = this.sources.get(sessionFile, sessionId);
    const receipt = this.getDeletion(sessionFile);
    if (source?.deleted || (receipt?.state === "complete" && receipt.sessionId === sessionId)) {
      throw new ConsolidationConflictError(sessionFile, "has a completed deletion receipt for this source generation");
    }
  }

  #persistMemoryEvidence(job: ConsolidationJob, allowDeletedSource = false): void {
    if (job.sessionFile !== null && !allowDeletedSource) {
      this.#assertSourceEnqueueAllowed(job.sessionId, job.sessionFile, job.cwd);
    }
    let source = job.sessionFile === null ? undefined : this.sources.get(job.sessionFile, job.sessionId);
    if (job.sessionFile !== null && !source) {
      source = this.sources.register({sessionId: job.sessionId, sessionFile: job.sessionFile, cwd: job.cwd});
    }
    if (allowDeletedSource && job.sessionFile !== null) {
      const receipt = this.getDeletion(job.sessionFile);
      if (receipt?.state === "complete" && receipt.sessionId === job.sessionId) {
        this.sources.markDeleted(job.sessionFile, job.sessionId);
        source = this.sources.get(job.sessionFile, job.sessionId);
      }
    } else if (source?.deleted) {
      throw new ConsolidationConflictError(job.id, "belongs to a deleted source generation");
    }
    const evidence: MemoryEvidence = {
      evidenceId: job.id,
      sessionId: job.sessionId,
      sessionFile: job.sessionFile,
      sourceGeneration: source?.generation ?? null,
      cwd: job.cwd,
      leafId: job.leafId,
      sourceKeys: job.sourceKeys,
      ...(job.lineageSessionIds === undefined ? {} : { lineageSessionIds: job.lineageSessionIds }),
      ...(job.sourceTime === undefined ? {} : { sourceTime: job.sourceTime }),
      ...(job.branchAnchor === undefined ? {} : { branchAnchor: job.branchAnchor }),
    };
    if ((evidence.sessionFile === null) !== (evidence.sourceGeneration === null)) {
      throw new ConsolidationConflictError(job.id, "has no stable source generation for memory provenance");
    }
    const raw = this.#database.prepare(`SELECT ${evidenceColumns} FROM memory_evidence WHERE evidence_id = ?`).get(job.id);
    if (raw !== undefined) {
      const stored = parseEvidenceRow(this.#decodeRow(raw));
      if (JSON.stringify(stored) !== JSON.stringify(evidence)) {
        throw new ConsolidationConflictError(job.id, "already has different immutable memory provenance");
      }
      return;
    }
    this.#database.prepare(
      "INSERT INTO memory_evidence " +
      "(evidence_id, session_id, session_file, source_generation, cwd, leaf_id, source_keys, " +
      "lineage_session_ids, source_time, branch_anchor) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ).run(
      evidence.evidenceId,
      evidence.sessionId,
      evidence.sessionFile,
      evidence.sourceGeneration,
      evidence.cwd,
      evidence.leafId,
      JSON.stringify(evidence.sourceKeys),
      evidence.lineageSessionIds === undefined ? null : JSON.stringify(evidence.lineageSessionIds),
      evidence.sourceTime ?? null,
      evidence.branchAnchor ?? null,
    );
  }

  /** The live Pi manager supplies the header before a new file has been written. */
  reserveSessionSource(source: {sessionId: string; sessionFile: string; cwd: string; parentSession?: string}): void {
    this.#transaction(() => {
      this.sources.register({sessionId:source.sessionId,sessionFile:source.sessionFile,cwd:source.cwd});
      this.sources.setReserved(source.sessionFile, source.sessionId, source.parentSession);
    });
  }

  /** Bounded conversion of the old task-derived inventory; safe to repeat after interruption. */
  backfillSources(limit = 128): boolean {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw new TypeError("Invalid backfill budget.");
    return this.#transaction(() => this.#backfillSources(limit));
  }

  /** Reclaim terminal metadata in a restartable sweep; receipts, origins, and progress survive. */
  collectMetadata(limit = maxMetadataGcPageSize): { readonly removed: number; readonly hasMore: boolean } {
    this.#ensureOpen();
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > maxMetadataGcPageSize) {
      throw new TypeError("Metadata collection limit must be an integer from 1 through 128.");
    }
    return this.#transaction(() => {
      const access = this.#sourceAccess();
      if (access && !processHasExited(access.pid)) return { removed: 0, hasMore: false };
      if (access) this.#database.prepare("DELETE FROM source_access WHERE token = ?").run(access.token);
      if (!this.#backfillSources(maxTaskPageSize)) return { removed: 0, hasMore: true };

      const state = this.#database.prepare(
        "SELECT phase, cursor_row AS cursorRow FROM metadata_gc_state WHERE singleton = 1",
      ).get();
      if (state === undefined) throw new Error("Metadata collection state is missing.");
      let phase = state["phase"];
      let cursor = state["cursorRow"];
      if (typeof phase !== "number" || !Number.isSafeInteger(phase) || phase < 0 || phase > 7 ||
        typeof cursor !== "number" || !Number.isSafeInteger(cursor) || cursor < 0) {
        throw new TypeError("Stored metadata collection progress is invalid.");
      }

      let examined = 0;
      let removed = 0;
      while (phase < 8 && examined < limit) {
        const remaining = limit - examined;
        const rows = this.#metadataGcRows(phase, cursor, remaining);
        if (rows.length === 0) {
          phase += 1;
          cursor = 0;
          continue;
        }
        for (const raw of rows) {
          const row = this.#decodeRow(raw);
          const rowId = row["gcRow"];
          if (typeof rowId !== "number" || !Number.isSafeInteger(rowId) || rowId <= cursor) {
            throw new TypeError("Stored metadata collection row cursor is invalid.");
          }
          cursor = rowId;
          examined += 1;
          if (this.#collectMetadataRow(phase, row)) removed += 1;
        }
        if (rows.length < remaining) {
          phase += 1;
          cursor = 0;
        }
      }

      if (phase >= 8) {
        this.#database.prepare("UPDATE metadata_gc_state SET phase = 0, cursor_row = 0 WHERE singleton = 1").run();
        return { removed, hasMore: removed > 0 };
      }
      this.#database.prepare("UPDATE metadata_gc_state SET phase = ?, cursor_row = ? WHERE singleton = 1")
        .run(phase, cursor);
      return { removed, hasMore: true };
    });
  }

  #metadataGcRows(phase: number, cursor: number, limit: number): Record<string, unknown>[] {
    const queries: readonly string[] = [
      `SELECT rowid AS gcRow, ${cleanTaskColumns} FROM clean_tasks WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ${textColumn("job_id", "jobId")} FROM distill_trigger_jobs WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${triggerColumns} FROM distill_triggers WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${jobColumns} FROM consolidation_jobs WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${observationColumns} FROM session_observations WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${sessionUsageColumns} FROM session_usages WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      `SELECT row_id AS gcRow, ${textColumn("generation")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("session_id", "sessionId")}, deleted FROM managed_sources WHERE row_id > ? ORDER BY row_id LIMIT ?`,
      `SELECT rowid AS gcRow, ${evidenceColumns} FROM memory_evidence WHERE rowid > ? ORDER BY rowid LIMIT ?`,
    ];
    const query = queries[phase];
    if (query === undefined) throw new Error("Metadata collection phase is invalid.");
    return this.#database.prepare(query).all(cursor, limit);
  }

  #collectMetadataRow(phase: number, row: Record<string, unknown>): boolean {
    const rowId = row["gcRow"];
    if (typeof rowId !== "number" || !Number.isSafeInteger(rowId) || rowId < 1) {
      throw new TypeError("Stored metadata row ID is invalid.");
    }
    if (phase === 0) {
      const record = parsePersistedCleanTask(this.#metadataPayload(row));
      if (record.task.sessionFile === null || !this.#metadataSourceEligible(record.task.sessionFile, record.task.sessionId)) return false;
      return this.#database.prepare("DELETE FROM clean_tasks WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 1) {
      const link = fields(this.#metadataPayload(row), ["id", "triggerId", "jobId"]);
      validateText(link["id"], "stored trigger batch link ID");
      validateText(link["triggerId"], "stored trigger batch ID");
      validateText(link["jobId"], "stored trigger batch job ID");
      const triggerRow = this.#database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(link["triggerId"]);
      const jobRow = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(link["jobId"]);
      if (triggerRow === undefined || jobRow === undefined) throw new TypeError("Stored trigger batch link is orphaned.");
      const trigger = this.#readDistillTrigger(triggerRow);
      const job = parsePersistedJobRow(this.#decodeRow(jobRow));
      if (trigger.status !== "complete" || job.status !== "complete" || job.job.sessionFile === null ||
        job.job.sessionFile !== trigger.trigger.sessionFile || job.job.sessionId !== trigger.trigger.sessionId ||
        !this.#metadataSourceEligible(trigger.trigger.sessionFile, trigger.trigger.sessionId)) return false;
      return this.#database.prepare("DELETE FROM distill_trigger_jobs WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 2) {
      const trigger = this.#readDistillTrigger(this.#metadataPayload(row));
      if (trigger.status !== "complete" || trigger.jobIds.length > 0 || trigger.trigger.sessionFile === null ||
        !this.#metadataSourceEligible(trigger.trigger.sessionFile, trigger.trigger.sessionId)) return false;
      const removed = this.#database.prepare("DELETE FROM distill_triggers WHERE rowid = ?").run(rowId).changes === 1;
      if (removed) this.#clearDistillFailure("trigger", trigger.trigger.id);
      return removed;
    }
    if (phase === 3) {
      const job = parsePersistedJobRow(this.#metadataPayload(row));
      if (job.status !== "complete" || job.job.sessionFile === null ||
        this.#database.prepare("SELECT 1 AS present FROM distill_trigger_jobs WHERE job_id = ? LIMIT 1").get(job.job.id) !== undefined ||
        !this.#metadataSourceEligible(job.job.sessionFile, job.job.sessionId)) return false;
      const removed = this.#database.prepare("DELETE FROM consolidation_jobs WHERE rowid = ?").run(rowId).changes === 1;
      if (removed) this.#clearDistillFailure("job", job.job.id);
      return removed;
    }
    if (phase === 4) {
      const stored = parseStoredSessionObservation(this.#metadataPayload(row));
      const observation = stored.observation;
      if (!stored.captured || !this.#metadataSourceEligible(observation.sessionFile, observation.sessionId)) return false;
      return this.#database.prepare("DELETE FROM session_observations WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 5) {
      const usage = parseStoredSessionUsage(this.#metadataPayload(row));
      if (usage.state !== "released" || !this.#metadataSourceEligible(usage.sessionFile, usage.sessionId)) return false;
      return this.#database.prepare("DELETE FROM session_usages WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 6) {
      validateAbsolutePath(row["sessionFile"], "stored managed source path");
      validateText(row["sessionId"], "stored managed source session ID");
      validateText(row["generation"], "stored managed source generation");
      if (row["deleted"] !== 1) return false;
      const eligible = this.#metadataSourceEligible(row["sessionFile"], row["sessionId"]);
      if (!eligible || eligible.generation !== row["generation"] ||
        this.#hasOperationalSourceMetadata(row["sessionFile"])) return false;
      return this.#database.prepare(
        "DELETE FROM managed_sources WHERE row_id = ? AND generation = ? AND deleted = 1",
      ).run(rowId, eligible.generation).changes === 1;
    }
    if (phase === 7) {
      const evidence = parseEvidenceRow(this.#metadataPayload(row));
      if (this.#database.prepare("SELECT 1 AS present FROM memory_origins WHERE evidence_id = ? LIMIT 1").get(evidence.evidenceId) !== undefined) return false;
      return this.#database.prepare("DELETE FROM memory_evidence WHERE rowid = ?").run(rowId).changes === 1;
    }
    throw new Error("Metadata collection phase is invalid.");
  }

  #metadataPayload(row: Record<string, unknown>): Record<string, unknown> {
    const { gcRow: _cursor, ...payload } = row;
    return payload;
  }

  #metadataSourceEligible(sessionFile: string, sessionId: string): { readonly generation: string } | undefined {
    const source = this.sources.get(sessionFile, sessionId);
    if (!source || !source.deleted) return undefined;
    const receipt = this.getDeletion(sessionFile);
    if (receipt?.state !== "complete" || receipt.sessionId !== sessionId || receipt.sourceGeneration !== source.generation) return undefined;
    if (this.sources.hasDependents(sessionFile) || this.#hasLiveSourceReferences(sessionFile)) return undefined;
    return { generation: source.generation };
  }

  #hasLiveSourceReferences(sessionFile: string): boolean {
    const filter = this.#scopeFilter([sessionFile]);
    for (const [table, column, condition] of [
      ["consolidation_jobs", "json_extract(job_json, '$.sessionFile')", "status = 'pending'"],
      ["distill_triggers", "json_extract(trigger_json, '$.sessionFile')", "status <> 'complete'"],
      ["session_observations", "session_file", "captured = 0"],
      ["session_usages", "session_file", "state <> 'released'"],
    ] as const) {
      if (this.#database.prepare(`SELECT 1 AS present FROM ${table} WHERE ${condition} AND ${filter.sql(column)} LIMIT 1`)
        .get(...filter.values) !== undefined) return true;
    }
    return false;
  }

  #hasOperationalSourceMetadata(sessionFile: string): boolean {
    const filter = this.#scopeFilter([sessionFile]);
    const queries = [
      ["clean_tasks", "json_extract(task_json, '$.sessionFile')"],
      ["consolidation_jobs", "json_extract(job_json, '$.sessionFile')"],
      ["distill_triggers", "json_extract(trigger_json, '$.sessionFile')"],
      ["session_observations", "session_file"],
      ["session_usages", "session_file"],
    ] as const;
    for (const [table, column] of queries) {
      if (this.#database.prepare(`SELECT 1 AS present FROM ${table} WHERE ${filter.sql(column)} LIMIT 1`)
        .get(...filter.values) !== undefined) return true;
    }
    return this.#database.prepare(
      "SELECT 1 AS present FROM distill_trigger_jobs AS links " +
      "JOIN distill_triggers AS triggers ON triggers.id = links.trigger_id WHERE " +
      filter.sql("json_extract(triggers.trigger_json, '$.sessionFile')") + " LIMIT 1",
    ).get(...filter.values) !== undefined;
  }

  #backfillSources(limit: number): boolean {
    const state = this.#database.prepare("SELECT phase, cursor_row FROM source_backfill WHERE singleton = 1").get();
    if (!state || typeof state["phase"] !== "number" || typeof state["cursor_row"] !== "number") throw new Error("Invalid source backfill progress.");
    let phase = state["phase"], cursor = state["cursor_row"], remaining = limit;
    const tables = ["consolidation_jobs", "distill_triggers", "clean_tasks", "session_observations", "session_usages"];
    if (!Number.isSafeInteger(phase) || phase < 0 || phase > tables.length || !Number.isSafeInteger(cursor) || cursor < 0) throw new Error("Invalid source backfill progress.");
    while (phase < tables.length && remaining > 0) {
      const table = tables[phase]!;
      const columns = [jobColumns, triggerColumns, cleanTaskColumns, observationColumns, sessionUsageColumns][phase]!;
      const rows = this.#database.prepare(`SELECT rowid AS cursorRow, ${columns} FROM ${table} WHERE rowid > ? ORDER BY rowid LIMIT ?`).all(cursor, remaining);
      for (const raw of rows) {
        const {cursorRow, ...fields} = this.#decodeRow(raw);
        validateNow(cursorRow, "Source backfill cursor");
        let source: {sessionId:string; sessionFile:string|null; cwd:string};
        if (phase === 0) source = parsePersistedJobRow(fields).job;
        else if (phase === 1) source = this.#readDistillTrigger(fields).trigger;
        else if (phase === 2) source = parsePersistedCleanTask(fields).task;
        else if (phase === 3) source = parseStoredSessionObservation(fields).observation;
        else source = parseStoredSessionUsage(fields);
        if (source.sessionFile !== null) {
          this.sources.register({sessionId:source.sessionId, sessionFile:source.sessionFile, cwd:source.cwd});
          const receipt = this.getDeletion(source.sessionFile);
          if (receipt?.state === "complete" && receipt.sessionId === source.sessionId) this.sources.markDeleted(source.sessionFile,source.sessionId);
        }
        cursor = cursorRow;
      }
      remaining -= rows.length;
      if (rows.length === 0) { phase++; cursor = 0; }
    }
    this.#database.prepare("UPDATE source_backfill SET phase = ?, cursor_row = ? WHERE singleton = 1").run(phase,cursor);
    if (phase === tables.length) this.sources.setBackfillComplete(true);
    return phase === tables.length;
  }

  hasUncapturedObservation(scope: readonly string[]): boolean {
    const filter = this.#scopeFilter(scope);
    return this.#database.prepare(`SELECT 1 FROM session_observations WHERE captured = 0 AND ${filter.sql("session_file")} LIMIT 1`).get(...filter.values) !== undefined;
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

  #scopeFilter(scope?: readonly string[]): { values: string[]; sql: (column: string) => string } {
    if (scope === undefined) return { values: [], sql: () => "1" };
    if (!Array.isArray(scope) || scope.length > 1024) throw new TypeError("Invalid source scope.");
    for (const path of scope) validateAbsolutePath(path, "Source scope path");
    const requested = [...new Set(scope)].sort();
    const placeholders = requested.map(() => "?").join(",");
    const related = requested.length === 0 ? [] : this.#database.prepare(
      "SELECT DISTINCT session_file AS path FROM managed_sources WHERE deleted = 0 AND (" +
      `session_id IN (SELECT session_id FROM managed_sources WHERE session_file IN (${placeholders})) OR ` +
      `physical_identity IN (SELECT physical_identity FROM managed_sources WHERE session_file IN (${placeholders}))) LIMIT 1025`,
    ).all(...requested, ...requested);
    if (related.length > 1024) throw new RangeError("Source aliases exceed the assessment budget.");
    const values = [...new Set([...requested, ...related.map(raw => {
      const path = this.#decodeRow(raw)["path"]; validateAbsolutePath(path, "Source alias path"); return path;
    })])].sort();
    return { values, sql: column => values.length ? `${column} IN (${values.map(() => "?").join(",")})` : "0" };
  }

  /** Compare on this connection only; includes other connections and local mutations. */
  retentionVersion(scope?: readonly string[]): string {
    this.#ensureOpen();
    if (scope !== undefined) {
      const filter = this.#scopeFilter(scope);
      const digest = createHash("sha256").update(String(this.sources.topologyVersion()));
      for (const [table, columns, path, condition] of [
        ["session_observations", observationColumns, "session_file", "1"],
        ["session_usages", sessionUsageColumns, "session_file", "1"],
        ["consolidation_jobs", "job_json, status", "json_extract(job_json, '$.sessionFile')", "status = 'pending'"],
        ["distill_triggers", "trigger_json, status, compaction_record_id", "json_extract(trigger_json, '$.sessionFile')", "status <> 'complete'"],
        ["clean_tasks", "task_json", "json_extract(task_json, '$.sessionFile')", "1"],
      ]) {
        let count = 0;
        for (const raw of this.#database.prepare(`SELECT ${columns} FROM ${table} WHERE ${condition} AND ${filter.sql(path!)} ORDER BY rowid LIMIT ?`)
          .iterate(...filter.values, maxSessionMetadataRows + 1)) {
          if (++count > maxSessionMetadataRows) throw new RangeError("Source metadata exceeds its fence budget.");
          digest.update(JSON.stringify(this.#decodeRow(raw)));
        }
      }
      return digest.digest("hex");
    }
    return `${this.#database.prepare("PRAGMA data_version").get()?.["data_version"]}:${this.#database.prepare("SELECT total_changes() AS n").get()?.["n"]}`;
  }

  reconcileExitedSessionOwners(scope?: readonly string[]): void {
    this.#transaction(() => {
      const filter = this.#scopeFilter(scope);
      const rows = this.#database.prepare(`SELECT ${sessionUsageColumns} FROM session_usages WHERE state != 'released' AND ${filter.sql("session_file")} ORDER BY rowid LIMIT 128`).all(...filter.values);
      for (const raw of rows) {
        const owner = parseStoredSessionUsage(this.#decodeRow(raw));
        if (processHasExited(owner.ownerPid)) this.#database.prepare("UPDATE session_usages SET state = 'released' WHERE owner_id = ?").run(owner.ownerId);
      }
    });
  }

  getDeletion(path: string): SessionDeletion | undefined {
    validateAbsolutePath(path, "Deletion path");
    const schemaVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    const columns = typeof schemaVersion === "number" && schemaVersion >= 7
      ? deletionColumns
      : [textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
        textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"),
        "NULL AS sourceGeneration", textColumn("state")].join(", ");
    const raw = this.#database.prepare(`SELECT ${columns} FROM session_deletions WHERE session_file = ?`).get(path);
    if (!raw) return undefined;
    return this.#parseDeletionRow(this.#decodeRow(raw));
  }

  #parseDeletionRow(value: unknown): SessionDeletion {
    const row = fields(value, ["sessionFile", "sessionId", "intentId", "fileVersion", "sourceGeneration", "state"]);
    validateAbsolutePath(row["sessionFile"], "Stored deletion path");
    validateText(row["sessionId"], "Stored deletion session ID");
    validateText(row["intentId"], "Stored deletion intent ID");
    validateText(row["fileVersion"], "Stored deletion file version");
    if (!/^\d+:\d+:\d+:\d+:\d+$/.test(row["fileVersion"])) throw new TypeError("Invalid deletion file version.");
    const sourceGeneration = parseNullableText(row["sourceGeneration"], "Stored deletion source generation");
    if (row["state"] !== "deleting" && row["state"] !== "complete") throw new TypeError("Invalid deletion state.");
    return {
      sessionFile: row["sessionFile"],
      sessionId: row["sessionId"],
      intentId: row["intentId"],
      fileVersion: row["fileVersion"],
      sourceGeneration,
      state: row["state"],
    };
  }

  #deletionTask(id: string, owner: string, access: string, version: string, scope?: readonly string[]): CleanTaskInput | undefined {
    if (this.retentionVersion(scope) !== version || this.#sourceAccess()?.token !== access) return undefined;
    const raw = this.#database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (!raw) return undefined;
    const record = parsePersistedCleanTask(this.#decodeRow(raw));
    if (record.leaseOwner !== owner || record.leaseUntil === null || record.leaseUntil <= Date.now()) return undefined;
    return record.task;
  }

  prepareDeletion(id: string, owner: string, access: string, version: string, fileVersion: string, scope?: readonly string[]): boolean {
    if (typeof fileVersion !== "string" || !/^\d+:\d+:\d+:\d+:\d+$/.test(fileVersion)) throw new TypeError("Invalid deletion file version.");
    return this.#transaction(() => {
      const task = this.#deletionTask(id, owner, access, version, scope);
      if (!task) return false;
      const source = this.sources.get(task.sessionFile, task.sessionId);
      if (!source || source.deleted) return false;
      const previous = this.getDeletion(task.sessionFile);
      if (previous && (previous.state === "complete" || previous.sessionId !== task.sessionId || previous.fileVersion.split(":").slice(0, 2).join(":") !== fileVersion.split(":").slice(0, 2).join(":"))) return false;
      this.#database.prepare(
        "INSERT INTO session_deletions (session_file, session_id, intent_id, file_version, source_generation, state) " +
        "VALUES (?, ?, ?, ?, ?, 'deleting') ON CONFLICT(session_file) DO UPDATE SET " +
        "intent_id = excluded.intent_id, file_version = excluded.file_version, source_generation = excluded.source_generation",
      ).run(task.sessionFile, task.sessionId, task.intentId, fileVersion, source.generation);
      return true;
    });
  }

  /** Callback performs only bounded synchronous identity checks/unlink, under the writer reservation. */
  finishDeletion(id: string, owner: string, access: string, version: string, mutate: () => boolean, scope?: readonly string[]): boolean {
    return this.#transaction(() => {
      const task = this.#deletionTask(id, owner, access, version, scope);
      if (!task) return false;
      const receipt = this.getDeletion(task.sessionFile);
      const source = this.sources.get(task.sessionFile, task.sessionId);
      if (!receipt || receipt.state !== "deleting" || receipt.sessionId !== task.sessionId ||
        receipt.intentId !== task.intentId || !source || source.deleted || receipt.sourceGeneration !== source.generation) return false;
      if (mutate() !== true) return false;
      this.#database.prepare("UPDATE session_deletions SET state = 'complete' WHERE session_file = ?").run(task.sessionFile);
      this.sources.markDeleted(task.sessionFile, task.sessionId);
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

  #readDistillFailure(kind: DistillTaskKind, id: string): DistillFailureRecord | undefined {
    const raw = this.#database.prepare(
      `SELECT ${distillFailureColumns} FROM distill_failures WHERE task_kind = ? AND task_id = ?`,
    ).get(kind, id);
    if (raw === undefined) return undefined;
    return this.#parseDistillFailureRow(raw);
  }

  #parseDistillFailureRow(value: unknown): DistillFailureRecord {
    const row = this.#decodeRow(fields(value, [
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

  #taskSweepThroughCursor(
    table: "distill_triggers" | "consolidation_jobs" | "session_observations" | "session_usages",
    requested: number | undefined,
  ): number {
    if (requested !== undefined) return requested;
    const row = this.#database.prepare(`SELECT COALESCE(MAX(rowid), 0) AS throughCursor FROM ${table}`).get();
    if (row === undefined) throw new Error("Task sweep upper cursor could not be read.");
    const throughCursor = row["throughCursor"];
    validateNow(throughCursor, "Stored task sweep upper cursor");
    return throughCursor;
  }

  #distillTaskCanAttempt(
    kind: DistillTaskKind,
    id: string,
    context: DistillRetryContext | undefined,
    now: number,
    retryAt: number | null,
  ): boolean {
    const failure = this.#readDistillFailure(kind, id);
    if (failure !== undefined) return canRetryDistillFailure(failure, context, now);
    return retryAt === null || retryAt <= now;
  }

  #persistDistillFailure(value: DistillFailureRecord): void {
    const failure = parseDistillFailureRecord(value);
    this.#database.prepare(
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

  #clearDistillFailure(kind: DistillTaskKind, id: string): void {
    this.#database.prepare("DELETE FROM distill_failures WHERE task_kind = ? AND task_id = ?").run(kind, id);
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

  #triggerJobIds(triggerId: string): readonly string[] {
    const rows = this.#database.prepare(
      `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
      `${textColumn("job_id", "jobId")}, ordinal ` +
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
      `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
      `${textColumn("job_id", "jobId")} ` +
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
    if (current.status !== "pending" ||
      !this.#distillTaskCanAttempt("job", id, request.retryContext, request.now, current.retryAt)) return undefined;
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
    if (record.sessionFile !== null) this.#assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
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
    if (record.sessionFile !== null) this.#assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
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
    if (version !== 0 && version !== 1 && version !== 2 && version !== 3 && version !== 4 && version !== 5 && version !== 6 && version !== 7 && version !== latestSchemaVersion) {
      throw new Error("Unsupported memory database schema version: " + String(version) + ".");
    }
    if (version === latestSchemaVersion) {
      this.#validateLatestSchema();
      this.#validateMemoryRows();
      this.#sourceAccess();
      return;
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
    if (version <= 3) this.#migrateV3ToV4();
    if (version < 5) {
      this.#validateSchema(true);
      this.#validateAllRows();
      this.#database.exec(sourceAccessSchema);
      this.#database.exec(deletionsSchemaV5);
      this.#database.exec("PRAGMA user_version = 5");
    }
    if (version < 6) {
      this.#validateLatestSchema();
      this.#validateAllRows();
      initializeSourceRegistry(this.#database);
      for (const sql of [sourceBackfillSchema, ...storeIndexes]) this.#database.exec(sql);
      this.#database.prepare("INSERT INTO source_backfill VALUES (1,0,0)").run();
      this.#backfillSources(128);
      this.#database.exec("PRAGMA user_version = 6");
    } else {
      this.#validateSchema(true);
      this.#validateAllRows();
      initializeSourceRegistry(this.#database);
    }
    if (version <= 6) this.#migrateV6ToV7();
    this.#migrateV7ToV8();
    this.#validateLatestSchema();
    this.#validateAllRows();
    this.#sourceAccess();
  }

  #migrateV6ToV7(): void {
    const currentVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 6) throw new Error("Memory schema migration expected version 6.");
    this.#validateSchema(true);
    this.#validateAllRows();

    this.#database.exec(
      "ALTER TABLE memory_origins RENAME TO memory_origins_v6; " +
      "ALTER TABLE consolidation_progress RENAME TO consolidation_progress_v6; " +
      "ALTER TABLE session_deletions RENAME TO session_deletions_v6; " +
      memoryEvidenceSchema + "; " + memoryOriginsSchema + "; " + consolidationProgressSchema + "; " + deletionsSchema + "; " +
      metadataGcSchema + "; " + storeIndexesV7.join("; "),
    );
    this.#database.prepare("INSERT INTO metadata_gc_state VALUES (1, 0, 0)").run();

    const originRows = this.#database.prepare(
      `SELECT ${originColumnsV6} FROM memory_origins_v6 ORDER BY rowid`,
    ).all();
    const insertOrigin = this.#database.prepare(
      "INSERT INTO memory_origins (memory_id, evidence_id, revision) VALUES (?, ?, ?)",
    );
    const originsToMigrate: { memoryId: string; job: ConsolidationJob; revision: number }[] = [];
    for (const raw of originRows) {
      const origin = parseMemoryOriginRowV6(this.#decodeRow(raw));
      const jobRow = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(origin.jobId);
      if (jobRow === undefined) throw new TypeError("Stored memory origin references a missing consolidation job.");
      const job = parsePersistedJobRow(this.#decodeRow(jobRow));
      if (job.status !== "complete") throw new TypeError("Stored memory origin references an incomplete consolidation job.");
      originsToMigrate.push({memoryId:origin.memoryId, job:job.job, revision:origin.revision});
    }
    for (const origin of originsToMigrate) {
      this.#persistMemoryEvidence(origin.job, true);
      insertOrigin.run(origin.memoryId, origin.job.id, origin.revision);
    }

    const progressRows = this.#database.prepare(
      `SELECT ${progressColumnsV6} FROM consolidation_progress_v6 ORDER BY rowid`,
    ).all();
    const insertProgress = this.#database.prepare(
      "INSERT INTO consolidation_progress (source_key, processed_at) VALUES (?, ?)",
    );
    for (const raw of progressRows) {
      const progress = parseProgressRowV6(this.#decodeRow(raw));
      const jobRow = this.#database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(progress.jobId);
      if (jobRow === undefined || !parsePersistedJobRow(this.#decodeRow(jobRow)).job.sourceKeys.includes(progress.sourceKey)) {
        throw new TypeError("Stored source progress does not match its consolidation job.");
      }
      insertProgress.run(progress.sourceKey, progress.processedAt);
    }

    const deletionRows = this.#database.prepare(
      "SELECT " + [textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
        textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"), textColumn("state")].join(", ") +
      " FROM session_deletions_v6 ORDER BY rowid",
    ).all();
    const insertDeletion = this.#database.prepare(
      "INSERT INTO session_deletions (session_file, session_id, intent_id, file_version, source_generation, state) " +
      "VALUES (?, ?, ?, ?, ?, ?)",
    );
    for (const raw of deletionRows) {
      const row = this.#decodeRow(raw);
      validateAbsolutePath(row["sessionFile"], "Stored deletion path");
      validateText(row["sessionId"], "Stored deletion session ID");
      validateText(row["intentId"], "Stored deletion intent ID");
      validateText(row["fileVersion"], "Stored deletion file version");
      if (!/^\d+:\d+:\d+:\d+:\d+$/.test(row["fileVersion"])) throw new TypeError("Invalid deletion file version.");
      if (row["state"] !== "deleting" && row["state"] !== "complete") throw new TypeError("Invalid deletion state.");
      let source = this.sources.get(row["sessionFile"], row["sessionId"]);
      if (!source) {
        source = this.sources.register({
          sessionId: row["sessionId"],
          sessionFile: row["sessionFile"],
          cwd: dirname(row["sessionFile"]),
        });
      }
      if (row["state"] === "complete") this.sources.markDeleted(row["sessionFile"], row["sessionId"]);
      insertDeletion.run(row["sessionFile"], row["sessionId"], row["intentId"], row["fileVersion"], source.generation, row["state"]);
    }

    this.#database.exec(
      "DROP TABLE memory_origins_v6; DROP TABLE consolidation_progress_v6; DROP TABLE session_deletions_v6; " +
      "PRAGMA user_version = 7",
    );
  }

  #migrateV7ToV8(): void {
    const currentVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 7) throw new Error("Memory schema migration expected version 7.");
    this.#validateSchema(true);
    this.#validateAllRows();
    this.#database.exec(`${distillFailuresSchema}; PRAGMA user_version = 8`);
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
    this.#database.exec(consolidationProgressSchemaV6);
    this.#database.exec(memoryOriginsSchemaV6);
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
    const schemaVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    const usesV7Metadata = typeof schemaVersion === "number" && schemaVersion >= 7;
    const usesV8Metadata = schemaVersion === 8;
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
        sql: usesV7Metadata ? consolidationProgressSchema : consolidationProgressSchemaV6,
        columns: usesV7Metadata ? [
          { name: "source_key", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "processed_at", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ] : [
          { name: "source_key", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "processed_at", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["memory_origins", {
        sql: usesV7Metadata ? memoryOriginsSchema : memoryOriginsSchemaV6,
        columns: usesV7Metadata ? [
          { name: "memory_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "evidence_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ] : [
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
    if (usesV7Metadata) {
      schemas.set("memory_evidence", {
        sql: memoryEvidenceSchema,
        columns: [
          { name: "evidence_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_file", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_generation", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_keys", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lineage_session_ids", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_time", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "branch_anchor", type: "TEXT", notNull: 0, primaryKey: 0 },
        ],
      });
    }
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
    if (typeof schemaVersion === "number" && schemaVersion >= 5) {
      schemas.set("source_access", { sql: sourceAccessSchema, columns: [
        { name: "singleton", type: "INTEGER", notNull: 1, primaryKey: 1 },
        { name: "token", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
      ] });
      schemas.set("session_deletions", { sql: usesV7Metadata ? deletionsSchema : deletionsSchemaV5, columns: [
        { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 1 },
        { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "intent_id", type: "TEXT", notNull: 1, primaryKey: 0 },
        { name: "file_version", type: "TEXT", notNull: 1, primaryKey: 0 },
        ...(usesV7Metadata ? [{ name: "source_generation", type: "TEXT" as const, notNull: 0 as const, primaryKey: 0 as const }] : []),
        { name: "state", type: "TEXT", notNull: 1, primaryKey: 0 },
      ] });
    }
    const additional = typeof schemaVersion === "number" && schemaVersion >= 6
      ? [...SOURCE_REGISTRY_SCHEMAS, ...(usesV8Metadata ? storeSchemasV8 : usesV7Metadata ? storeSchemasV7 : storeSchemas)]
      : [];
    const extraSchemas = new Map(additional.map(schema => [schema.name, schema.sql]));
    if (objects.length !== schemas.size + extraSchemas.size) throw new Error("Unsupported memory database schema.");
    for (const object of objects) {
      const name = object["name"];
      if (typeof name === "string" && extraSchemas.has(name)) {
        const expected = extraSchemas.get(name)!;
        if (typeof object["sql"] !== "string" || object["sql"].replace(/\s+/g, "").toUpperCase() !== expected.replace(/\s+/g, "").toUpperCase()) throw new Error("Unsupported registry schema.");
        continue;
      }
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
    const schemaVersion = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    const usesV7Metadata = typeof schemaVersion === "number" && schemaVersion >= 7;
    const jobs = this.#database.prepare(
      `SELECT ${useLegacyJobColumns ? legacyJobColumns : jobColumns} FROM consolidation_jobs`,
    ).all().map((row) => parsePersistedJobRow(this.#decodeRow(row)));
    const jobsById = new Map(jobs.map((record) => [record.job.id, record]));
    for (const row of this.#database.prepare(
      `SELECT ${usesV7Metadata ? progressColumns : progressColumnsV6} FROM consolidation_progress`,
    ).iterate()) {
      if (usesV7Metadata) {
        parseProgressRow(this.#decodeRow(row));
      } else {
        const progress = parseProgressRowV6(this.#decodeRow(row));
        const sourceJob = jobsById.get(progress.jobId);
        if (sourceJob === undefined || !sourceJob.job.sourceKeys.includes(progress.sourceKey)) {
          throw new TypeError("Stored source progress does not match its consolidation job.");
        }
      }
    }
    const evidence = new Map<string, MemoryEvidence>();
    if (usesV7Metadata) {
      for (const row of this.#database.prepare(`SELECT ${evidenceColumns} FROM memory_evidence`).iterate()) {
        const record = parseEvidenceRow(this.#decodeRow(row));
        if (evidence.has(record.evidenceId)) throw new TypeError("Stored memory evidence ID is duplicated.");
        evidence.set(record.evidenceId, record);
      }
    }
    for (const row of this.#database.prepare(
      `SELECT ${usesV7Metadata ? originColumns : originColumnsV6} FROM memory_origins`,
    ).iterate()) {
      if (usesV7Metadata) {
        const origin = parseMemoryOriginRow(this.#decodeRow(row));
        if (!evidence.has(origin.evidenceId)) throw new TypeError("Stored memory origin references missing evidence.");
      } else {
        const origin = parseMemoryOriginRowV6(this.#decodeRow(row));
        const sourceJob = jobsById.get(origin.jobId);
        if (sourceJob === undefined || sourceJob.status !== "complete") {
          throw new TypeError("Stored memory origin does not match a completed consolidation job.");
        }
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
        if (schemaVersion === 8) {
          for (const raw of this.#database.prepare(`SELECT ${distillFailureColumns} FROM distill_failures`).iterate()) {
            const failure = this.#parseDistillFailureRow(raw);
            const status = failure.kind === "job"
              ? jobsById.get(failure.id)?.status
              : triggers.get(failure.id)?.status;
            if (status !== "pending" && !(failure.kind === "trigger" && status === "candidate")) {
              throw new TypeError("Stored distill failure does not match pending work.");
            }
          }
        }
        for (const raw of this.#database.prepare(
          `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
          `${textColumn("job_id", "jobId")} ` +
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
            if (leasedTrigger === undefined || (leasedTrigger.status !== "pending" && leasedTrigger.status !== "candidate")) {
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
