export type DistillTaskKind = "trigger" | "job";

export type DistillFailureReason =
  | "source_invalid"
  | "model_unavailable"
  | "model_invalid"
  | "model_budget"
  | "transient"
  | "conflict";

export interface DistillRetryContext {
  readonly sourceVersion: string;
  readonly modelKey: string;
  readonly policyVersion: string;
}

/** Input supplied when a current lease owner records a failed attempt. */
export interface DistillFailureInput {
  readonly reason: DistillFailureReason;
  readonly context: DistillRetryContext;
  readonly retryAfterMs?: number;
}

/** Bounded scheduling metadata. It deliberately contains no source or model text. */
export interface DistillFailureRecord {
  readonly kind: DistillTaskKind;
  readonly id: string;
  readonly reason: DistillFailureReason;
  readonly context: DistillRetryContext;
  readonly attempts: number;
  readonly nextAttemptAt: number | null;
}

const identityMaxLength = 512;
const transientDelaysMs = [30_000, 2 * 60_000, 10 * 60_000, 30 * 60_000] as const;
const modelProbeDelayMs = 30_000;
const modelCooldownMs = 6 * 60 * 60_000;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function record(value: unknown, field: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new TypeError(`${field} must be an object.`);
  }
  return value;
}

function exactFields(value: unknown, names: readonly string[], field: string): Record<string, unknown> {
  const parsed = record(value, field);
  if (Reflect.ownKeys(parsed).length !== names.length || names.some(name => !Object.hasOwn(parsed, name))) {
    throw new TypeError(`${field} requires exactly these fields: ${names.join(", ")}.`);
  }
  return parsed;
}

function identity(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0 || value.length > identityMaxLength ||
    value.includes("\u0000") || /[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`${field} must be a nonempty, bounded Unicode string.`);
  }
  return value;
}

export function parseDistillRetryContext(value: unknown): DistillRetryContext {
  const parsed = exactFields(value, ["sourceVersion", "modelKey", "policyVersion"], "Distill retry context");
  return {
    sourceVersion: identity(parsed["sourceVersion"], "Distill source version"),
    modelKey: identity(parsed["modelKey"], "Distill model key"),
    policyVersion: identity(parsed["policyVersion"], "Distill policy version"),
  };
}

export function parseDistillFailureInput(value: unknown): DistillFailureInput {
  const parsed = record(value, "Distill failure");
  const allowed = ["reason", "context", "retryAfterMs"];
  if (Reflect.ownKeys(parsed).some(key => typeof key !== "string" || !allowed.includes(key)) ||
    !Object.hasOwn(parsed, "reason") || !Object.hasOwn(parsed, "context")) {
    throw new TypeError("Distill failure requires a reason and retry context.");
  }
  const reason = parsed["reason"];
  if (reason !== "source_invalid" && reason !== "model_unavailable" && reason !== "model_invalid" &&
    reason !== "model_budget" && reason !== "transient" && reason !== "conflict") {
    throw new TypeError("Distill failure reason is invalid.");
  }
  let retryAfterMs: number | undefined;
  if (Object.hasOwn(parsed, "retryAfterMs")) {
    const retryAfterValue = parsed["retryAfterMs"];
    if (typeof retryAfterValue !== "number" || !Number.isSafeInteger(retryAfterValue) || retryAfterValue < 0) {
      throw new TypeError("Distill retry-after duration must be a nonnegative safe integer.");
    }
    retryAfterMs = retryAfterValue;
  }
  return {
    reason,
    context: parseDistillRetryContext(parsed["context"]),
    ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
  };
}

export function parseDistillTaskKind(value: unknown): DistillTaskKind {
  if (value !== "trigger" && value !== "job") throw new TypeError("Distill task kind is invalid.");
  return value;
}

export function parseDistillFailureRecord(value: unknown): DistillFailureRecord {
  const parsed = exactFields(value, ["kind", "id", "reason", "context", "attempts", "nextAttemptAt"], "Stored distill failure");
  const kind = parseDistillTaskKind(parsed["kind"]);
  const id = identity(parsed["id"], "Stored distill failure ID");
  const { reason, context } = parseDistillFailureInput({ reason: parsed["reason"], context: parsed["context"] });
  const attempts = parsed["attempts"];
  if (typeof attempts !== "number" || !Number.isSafeInteger(attempts) || attempts < 1) {
    throw new TypeError("Stored distill failure attempts must be a positive safe integer.");
  }
  const nextAttemptAt = parsed["nextAttemptAt"];
  if (nextAttemptAt !== null && (typeof nextAttemptAt !== "number" || !Number.isSafeInteger(nextAttemptAt) || nextAttemptAt < 0)) {
    throw new TypeError("Stored distill failure next attempt time is invalid.");
  }
  return { kind, id, reason, context, attempts, nextAttemptAt };
}

/** The failure class determines which identity changes make an old hold obsolete. */
export function sameDistillFailureContext(
  reason: DistillFailureReason,
  previous: DistillRetryContext,
  current: DistillRetryContext,
): boolean {
  if (reason === "source_invalid") {
    return previous.sourceVersion === current.sourceVersion && previous.policyVersion === current.policyVersion;
  }
  if (reason === "model_unavailable") {
    return previous.modelKey === current.modelKey && previous.policyVersion === current.policyVersion;
  }
  if (reason === "transient" || reason === "conflict") return true;
  // A job is a frozen source batch. New tail entries can change the file version
  // without changing the model failure, so they must not reset its cooldown.
  return previous.modelKey === current.modelKey && previous.policyVersion === current.policyVersion;
}

export function canRetryDistillFailure(
  failure: DistillFailureRecord,
  context: DistillRetryContext | undefined,
  now: number,
): boolean {
  if (failure.reason === "transient" || failure.reason === "conflict") {
    return failure.nextAttemptAt !== null && failure.nextAttemptAt <= now;
  }
  if (context === undefined || !sameDistillFailureContext(failure.reason, failure.context, context)) return context !== undefined;
  return failure.nextAttemptAt !== null && failure.nextAttemptAt <= now;
}

function sameFailureFamily(previous: DistillFailureReason, current: DistillFailureReason): boolean {
  if (previous === "transient" || previous === "conflict") {
    return current === "transient" || current === "conflict";
  }
  if (previous === "model_invalid" || previous === "model_budget") {
    return current === "model_invalid" || current === "model_budget";
  }
  return previous === current;
}

export function makeDistillFailureRecord(input: {
  readonly kind: DistillTaskKind;
  readonly id: string;
  readonly failure: DistillFailureInput;
  readonly previous?: DistillFailureRecord;
  readonly now: number;
}): DistillFailureRecord {
  const failure = parseDistillFailureInput(input.failure);
  const id = identity(input.id, "Distill task ID");
  if (!Number.isSafeInteger(input.now) || input.now < 0) throw new TypeError("Distill failure time is invalid.");
  const attempts = input.previous !== undefined && sameFailureFamily(input.previous.reason, failure.reason) &&
    sameDistillFailureContext(failure.reason, input.previous.context, failure.context)
    ? input.previous.attempts + 1
    : 1;
  if (!Number.isSafeInteger(attempts)) throw new RangeError("Distill retry attempt limit reached.");

  let delay: number | null;
  if (failure.reason === "source_invalid" || failure.reason === "model_unavailable") {
    delay = null;
  } else if (failure.reason === "transient" || failure.reason === "conflict") {
    delay = transientDelaysMs[Math.min(attempts - 1, transientDelaysMs.length - 1)] ?? transientDelaysMs.at(-1)!;
  } else {
    delay = attempts === 1 ? modelProbeDelayMs : modelCooldownMs;
  }
  if (delay !== null && failure.retryAfterMs !== undefined) delay = Math.max(delay, failure.retryAfterMs);
  const nextAttemptAt = delay === null
    ? null
    : delay > Number.MAX_SAFE_INTEGER - input.now
      ? Number.MAX_SAFE_INTEGER
      : input.now + delay;
  if (nextAttemptAt !== null && !Number.isSafeInteger(nextAttemptAt)) {
    throw new RangeError("Distill retry time is outside the safe integer range.");
  }
  return {
    kind: input.kind,
    id,
    reason: failure.reason,
    context: failure.context,
    attempts,
    nextAttemptAt,
  };
}
