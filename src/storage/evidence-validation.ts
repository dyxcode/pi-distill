import type {
  ConsolidationCommit,
  ConsolidationEvidenceBoundary,
  ConsolidationJob
} from "../distill/types.ts";
import type { MemoryForget, MemoryInput, MemoryUpdate } from '../memory/types.ts';
import { parseForget, parseInput, parseUpdate } from './memory-validation.ts';
import type { MemoryEvidence, PersistedConsolidationJob } from './records.ts';
import { fields, isRecord, parseNullableText, parseRetryFields, parseSourceKeys, validateAbsolutePath, validateNow, validateRevision, validateText, validateTimestamp } from './validation.ts';
export function parseConsolidationJob(value: unknown): ConsolidationJob {
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

export function parseConsolidationCommit(value: unknown): ConsolidationCommit {
  const required = ["jobId", "owner", "now", "creates", "updates", "deletes"];
  const allowed = [...required, "sourceKeys", "evidence"];
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
    evidence?: ConsolidationEvidenceBoundary;
  } = { jobId, owner, now, creates: parsedCreates, updates: parsedUpdates, deletes: parsedDeletes };
  if (Object.hasOwn(value, "sourceKeys")) {
    parsed.sourceKeys = parseSourceKeys(value["sourceKeys"], "consolidation commit source keys", true);
  }
  if (Object.hasOwn(value, "evidence")) {
    const evidence = value["evidence"];
    if (!isRecord(evidence) || Reflect.ownKeys(evidence).some(
      key => typeof key !== "string" || !["sourceTime", "branchAnchor"].includes(key),
    )) throw new TypeError("Consolidation evidence must contain only sourceTime and branchAnchor.");
    const parsedEvidence: { sourceTime?: string; branchAnchor?: string; } = {};
    if (Object.hasOwn(evidence, "sourceTime")) {
      validateTimestamp(evidence["sourceTime"], "consolidation evidence source time");
      parsedEvidence.sourceTime = evidence["sourceTime"];
    }
    if (Object.hasOwn(evidence, "branchAnchor")) {
      validateText(evidence["branchAnchor"], "consolidation evidence branch anchor");
      parsedEvidence.branchAnchor = evidence["branchAnchor"];
    }
    parsed.evidence = parsedEvidence;
  }
  return parsed;
}

export function parsePersistedJobRow(value: unknown): PersistedConsolidationJob {
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

export function parseMemoryOriginRow(value: unknown): {
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

export function parseMemoryOriginRowV6(value: unknown): {
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

export function parseProgressRow(value: unknown): {
  readonly sourceKey: string;
  readonly processedAt: number;
} {
  const { source_key, processed_at } = fields(value, ["source_key", "processed_at"]);
  validateText(source_key, "processed source key");
  validateNow(processed_at, "Stored source processing time");
  return { sourceKey: source_key, processedAt: processed_at };
}

export function parseProgressRowV6(value: unknown): {
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

export function parseEvidenceRow(value: unknown): MemoryEvidence {
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
