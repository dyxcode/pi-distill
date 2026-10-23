import { isAbsolute } from "node:path";
import type {
  RetryCategory
} from "../distill/tasks.ts";
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function fields(value: unknown, names: readonly string[]): Record<string, unknown> {
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).length !== names.length ||
    names.some((name) => !Object.hasOwn(value, name))
  ) {
    throw new TypeError(`Memory data requires exactly these fields: ${names.join(", ")}.`);
  }
  return value;
}

export function validateText(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`Memory ${field} must be a nonempty string.`);
  }
  // Unicode mode matches lone surrogates, while valid pairs remain single code points.
  if (/[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`Memory ${field} must contain well-formed Unicode.`);
  }
}

export function validateTimestamp(value: unknown, field: string): asserts value is string {
  if (
    typeof value !== "string" ||
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString() !== value
  ) {
    throw new TypeError(`Memory ${field} must be a canonical UTC ISO timestamp.`);
  }
}

export function validateRevision(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new TypeError("Memory revision must be a positive safe integer.");
  }
}

export function validateAbsolutePath(value: unknown, field: string): asserts value is string {
  validateText(value, field);
  if (!isAbsolute(value) || value.includes("\u0000")) {
    throw new TypeError(field + " must be an absolute filesystem path.");
  }
}

export function parseNullableText(value: unknown, field: string): string | null {
  if (value === null) return null;
  validateText(value, field);
  return value;
}

export function parseSourceKeys(value: unknown, field: string, allowEmpty: boolean): readonly string[] {
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

export function validateNow(value: unknown, field: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(field + " must be a nonnegative safe integer.");
  }
}

export function parseRetryCategory(value: unknown, field: string): RetryCategory | null {
  if (value === null) return null;
  if (value !== "transient" && value !== "unavailable" && value !== "invalid") {
    throw new TypeError(field + " category is invalid.");
  }
  return value;
}

export function parseRetryFields(
  categoryValue: unknown,
  atValue: unknown,
  field: string,
): { readonly retryCategory: RetryCategory | null; readonly retryAt: number | null; } {
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

export function validateTaskCursor(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(field + " must be a nonnegative safe integer.");
  }
  return value;
}

export function validateLeaseDuration(value: unknown, now: number, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || !Number.isSafeInteger(now + value)) {
    throw new TypeError(field + " must be a positive safe integer that yields a safe expiration.");
  }
  return value;
}
