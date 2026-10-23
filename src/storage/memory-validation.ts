import type { Memory, MemoryForget, MemoryInput, MemorySearchRequest, MemoryUpdate } from '../memory/types.ts';
import { fields, isRecord, validateRevision, validateText, validateTimestamp } from './validation.ts';
export const maxSearchBatchSize = 128;

export function parseInput(value: unknown): MemoryInput {
  const { id, content, createdAt } = fields(value, ["id", "content", "createdAt"]);
  validateText(id, "ID");
  validateText(content, "content");
  validateTimestamp(createdAt, "createdAt");
  return { id, content, createdAt };
}

export function parseMemory(value: unknown): Memory {
  const { id, content, createdAt, revision, updatedAt } = fields(
    value, ["id", "content", "createdAt", "revision", "updatedAt"],
  );
  const input = parseInput({ id, content, createdAt });
  validateRevision(revision);
  validateTimestamp(updatedAt, "updatedAt");
  return { ...input, revision, updatedAt };
}

export function parseUpdate(value: unknown): MemoryUpdate {
  const { id, expectedRevision, content, updatedAt } = fields(
    value, ["id", "expectedRevision", "content", "updatedAt"],
  );
  validateText(id, "ID");
  validateRevision(expectedRevision);
  validateText(content, "content");
  validateTimestamp(updatedAt, "updatedAt");
  return { id, expectedRevision, content, updatedAt };
}

export function parseForget(value: unknown): MemoryForget {
  const { id, expectedRevision } = fields(value, ["id", "expectedRevision"]);
  validateText(id, "ID");
  validateRevision(expectedRevision);
  return { id, expectedRevision };
}

export function validateSearchTerm(value: unknown, index: number): asserts value is string {
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

export function parseSearchRequest(value: unknown): MemorySearchRequest {
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

  const parsed: { terms: readonly string[]; match: "any" | "all"; after?: string; limit?: number; } = {
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
