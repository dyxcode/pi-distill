import type { DistillFailureReason } from "./retry.ts";

const failureReasons = [
  "source_invalid",
  "model_unavailable",
  "model_invalid",
  "model_budget",
  "transient",
  "conflict",
] as const satisfies readonly DistillFailureReason[];

export interface DistillFailureOptions {
  readonly cause?: unknown;
  readonly retryAfterMs?: number;
}

/** A bounded internal classification for a distillation attempt failure. */
export class DistillFailure extends Error {
  readonly reason: DistillFailureReason;
  readonly retryAfterMs?: number;

  constructor(reason: DistillFailureReason, message: string, options: DistillFailureOptions = {}) {
    if (!failureReasons.includes(reason)) {
      throw new TypeError("Distillation failure reason is invalid.");
    }
    if (options.retryAfterMs !== undefined &&
      (!Number.isSafeInteger(options.retryAfterMs) || options.retryAfterMs < 0)) {
      throw new RangeError("Distillation retry-after duration must be a nonnegative safe integer.");
    }

    super(message, options.cause === undefined ? {} : { cause: options.cause });
    this.name = "DistillFailure";
    this.reason = reason;
    if (options.retryAfterMs !== undefined) this.retryAfterMs = options.retryAfterMs;
  }
}
