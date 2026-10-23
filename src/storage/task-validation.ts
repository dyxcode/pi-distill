import { createHash } from "node:crypto";
import {
  parseDistillFailureInput,
  parseDistillRetryContext
} from "../distill/retry.ts";
import type {
  CleanTaskInput,
  DistillClaimInput,
  DistillTriggerClaimInput,
  DistillTriggerInput,
  PrepareDistillTriggerInput,
  ReleaseDistillClaimInput,
  RetryRequest,
  WaitReason
} from "../distill/tasks.ts";
import type {
  ConsolidationJob
} from "../distill/types.ts";
import { parseConsolidationJob } from './evidence-validation.ts';
import { maxTaskPageSize } from './limits.ts';
import type { PersistedCleanTask, PersistedDistillTrigger } from './records.ts';
import { fields, isRecord, parseNullableText, parseRetryCategory, parseRetryFields, validateAbsolutePath, validateNow, validateText } from './validation.ts';
export function parseDistillTriggerInput(value: unknown): DistillTriggerInput {
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

export function parsePersistedDistillTrigger(value: unknown, jobIds: readonly string[]): PersistedDistillTrigger {
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
  if (status === "candidate" && jobIds.length !== 0) {
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

export function parseCleanTaskInput(value: unknown): CleanTaskInput {
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

export function cleanTaskId(sessionFile: string): string {
  return "clean-" + createHash("sha256").update(sessionFile).digest("hex");
}

export function parsePersistedCleanTask(value: unknown): PersistedCleanTask {
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

export function parseTaskPageOptions(value: unknown, allowNow: boolean): {
  readonly limit: number;
  readonly cursor?: number;
  readonly now?: number;
  readonly throughCursor?: number;
} {
  const allowed = allowNow ? ["limit", "cursor", "now", "throughCursor"] : ["limit", "cursor"];
  if (
    !isRecord(value) || Reflect.ownKeys(value).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(value, "limit")
  ) throw new TypeError("Task page requires a limit and only supported cursor/time fields.");
  const limit = value["limit"];
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > maxTaskPageSize) {
    throw new TypeError("Task page limit must be an integer from 1 through " + maxTaskPageSize + ".");
  }
  const parsed: { limit: number; cursor?: number; now?: number; throughCursor?: number; } = { limit };
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
  if (Object.hasOwn(value, "throughCursor")) {
    validateNow(value["throughCursor"], "Task sweep boundary");
    parsed.throughCursor = value["throughCursor"];
  }
  return parsed;
}

export function parseTaskSweepPageOptions(value: unknown): {
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

export function parseDistillClaimInput(value: unknown): DistillClaimInput {
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

export function parseDistillTriggerClaimInput(value: unknown): DistillTriggerClaimInput {
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

export function parseRetryRequest(value: unknown): RetryRequest {
  const { category, retryAt } = fields(value, ["category", "retryAt"]);
  const retryCategory = parseRetryCategory(category, "Retry");
  if (retryCategory === null) throw new TypeError("Retry category is required.");
  validateNow(retryAt, "Retry time");
  return { category: retryCategory, retryAt };
}

export function parseReleaseDistillClaimInput(value: unknown): ReleaseDistillClaimInput {
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

export function parsePrepareDistillTriggerInput(value: unknown): PrepareDistillTriggerInput {
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
