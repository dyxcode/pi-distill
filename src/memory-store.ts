import { isAbsolute } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";
import type {
  ConsolidationCommit,
  ConsolidationJob,
  SessionObservation,
} from "./consolidation-types.js";

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

const consolidationJobsSchema =
  "CREATE TABLE consolidation_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "job_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL))" +
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
const latestSchemaVersion = 2;

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
  textColumn("lease_owner"), "lease_until",
].join(", ");
const progressColumns = [textColumn("source_key"), textColumn("job_id"), "processed_at"].join(", ");
const originColumns = [textColumn("memory_id", "memoryId"), textColumn("job_id", "jobId"), "revision"].join(", ");
const observationColumns = [
  textColumn("session_id"), textColumn("session_file"), textColumn("cwd"),
  textColumn("leaf_id"), "owner_pid", "captured",
].join(", ");
const schemaColumns = [textColumn("name"), textColumn("type"), textColumn("sql")].join(", ");

interface PersistedConsolidationJob {
  readonly job: ConsolidationJob;
  readonly status: "pending" | "complete";
  readonly leaseOwner: string | null;
  readonly leaseUntil: number | null;
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
  const { id, job_json, status, lease_owner, lease_until } = fields(
    value, ["id", "job_json", "status", "lease_owner", "lease_until"],
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
  return { job, status, leaseOwner, leaseUntil };
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
        "j.lease_until AS leaseUntil",
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
        jobStatus !== null || leaseOwner !== null || leaseUntil !== null
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

  enqueueConsolidation(job: ConsolidationJob): void {
    this.#ensureOpen();
    const record = parseConsolidationJob(job);
    const serialized = JSON.stringify(record);
    this.#transaction(() => {
      const row = this.#database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(record.id);
      if (row !== undefined) {
        const existing = parsePersistedJobRow(this.#decodeRow(row));
        if (JSON.stringify(existing.job) !== serialized) {
          throw new ConsolidationConflictError(record.id, "already exists with a different payload");
        }
        return;
      }
      this.#database.prepare(
        "INSERT INTO consolidation_jobs (id, job_json, status, lease_owner, lease_until) VALUES (?, ?, 'pending', NULL, NULL)",
      ).run(record.id, serialized);
    });
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
    if (typeof leaseMs !== "number" || !Number.isSafeInteger(leaseMs) || leaseMs < 1) {
      throw new TypeError("Consolidation lease duration must be a positive safe integer.");
    }
    const leaseUntil = now + leaseMs;
    if (!Number.isSafeInteger(leaseUntil)) {
      throw new TypeError("Consolidation lease expiration must be a safe integer.");
    }

    return this.#transaction(() => {
      const row = this.#database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(id);
      if (row === undefined) return false;
      const current = parsePersistedJobRow(this.#decodeRow(row));
      if (current.status !== "pending") return false;
      if (current.leaseUntil !== null && current.leaseUntil > now) return false;
      const earlierPending = this.#database.prepare(
        "SELECT 1 FROM consolidation_jobs AS earlier " +
        "JOIN consolidation_jobs AS current ON current.id = ? " +
        "WHERE earlier.rowid < current.rowid AND earlier.status = 'pending' " +
        "AND json_extract(earlier.job_json, '$.sessionId') = ? LIMIT 1",
      ).get(id, current.job.sessionId);
      if (earlierPending !== undefined) return false;
      this.#database.prepare(
        "UPDATE consolidation_jobs SET lease_owner = ?, lease_until = ? WHERE id = ? AND status = 'pending'",
      ).run(owner, leaseUntil, id);
      return true;
    });
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
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL WHERE id = ? AND status = 'pending'",
        ).run(id);
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
          "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL " +
          "WHERE id = ? AND status = 'pending'",
        ).run(request.jobId);
        if (completion.changes !== 1) {
          throw new ConsolidationConflictError(request.jobId, "changed before completion");
        }
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
        "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL " +
        "WHERE id = ? AND status = 'pending'",
      ).run(request.jobId);
      if (completion.changes !== 1) {
        throw new ConsolidationConflictError(request.jobId, "changed before completion");
      }
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

  close(): void {
    if (this.#database.isOpen) this.#database.close();
  }

  #decodeRow(row: Record<string, unknown>): Record<string, unknown> {
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Uint8Array) row[key] = this.#textDecoder.decode(value);
    }
    return row;
  }

  #initializeSchema(): void {
    const version = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (version !== 0 && version !== 1 && version !== latestSchemaVersion) {
      throw new Error("Unsupported memory database schema version: " + String(version) + ".");
    }
    const objects = this.#database.prepare(
      `SELECT ${schemaColumns} FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`,
    ).all().map((row) => this.#decodeRow(row));
    if (version === 0 && objects.length === 0) {
      this.#database.exec(currentSchema);
      this.#createAuxiliaryTables();
      this.#database.exec("PRAGMA user_version = 2");
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
      this.#database.exec("PRAGMA user_version = 2");
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
      this.#database.exec("PRAGMA user_version = 2");
    }
    this.#validateLatestSchema();
    this.#validateAllRows();
  }

  #createAuxiliaryTables(): void {
    this.#database.exec(consolidationJobsSchema);
    this.#database.exec(consolidationProgressSchema);
    this.#database.exec(memoryOriginsSchema);
    this.#database.exec(sessionObservationsSchema);
  }

  #validateLatestSchema(): void {
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
        sql: consolidationJobsSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
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

  #validateAllRows(): void {
    this.#validateMemoryRows();
    const jobs = this.#database.prepare(
      `SELECT ${jobColumns} FROM consolidation_jobs`,
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
  }

  #transaction<T>(operation: () => T): T {
    // Reserve the writer before reading so competing changes cannot race our checks.
    this.#database.exec("BEGIN IMMEDIATE");
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
