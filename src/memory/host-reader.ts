import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { StorageConnection } from "../storage/connection.ts";
import { Memories } from "../storage/memories.ts";
import { currentSchema, latestSchemaVersion } from "../storage/schema-definitions.ts";

export interface HostMemorySearchRequestV1 {
  readonly query: string;
  readonly cursor?: string;
  readonly limit?: number;
}
export interface HostMemorySearchItemV1 {
  readonly id: string;
  readonly revision: number;
  readonly preview: string;
}
export interface HostMemorySearchResponseV1 {
  readonly items: readonly HostMemorySearchItemV1[];
  readonly nextCursor?: string;
}
export interface HostMemoryReadRequestV1 {
  readonly id: string;
  readonly expectedRevision: number;
  readonly cursor?: string;
}
export interface HostMemoryReadResponseV1 {
  readonly id: string;
  readonly revision: number;
  readonly text: string;
  readonly nextCursor?: string;
}
export interface HostMemoryReaderV1 {
  readonly version: 1;
  search(request: HostMemorySearchRequestV1): HostMemorySearchResponseV1;
  read(request: HostMemoryReadRequestV1): HostMemoryReadResponseV1;
  close(): void;
}
export type HostMemoryErrorCode =
  | "invalid_input"
  | "invalid_cursor"
  | "memory_not_found"
  | "memory_changed"
  | "memory_unavailable";
export class HostMemoryError extends Error {
  readonly code: HostMemoryErrorCode;
  constructor(code: HostMemoryErrorCode) {
    super(code);
    this.name = "HostMemoryError";
    this.code = code;
  }
}
function fail(code: HostMemoryErrorCode): never {
  throw new HostMemoryError(code);
}
function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Reflect.ownKeys(value).some((key) => typeof key !== "string" || !keys.includes(key))
  )
    fail("invalid_input");
  return Object.fromEntries(Object.entries(value));
}
function text(value: unknown, maximum: number): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    value.includes("\0") ||
    /[\uD800-\uDFFF]/u.test(value) ||
    [...value].length > maximum
  )
    fail("invalid_input");
  return value;
}
function revision(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) fail("invalid_input");
  return value;
}
function optionalCursor(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value || value.length > 8192) fail("invalid_cursor");
  return value;
}

class HostMemoryReader implements HostMemoryReaderV1 {
  readonly version = 1 as const;
  readonly #connection: StorageConnection;
  readonly #memories: Memories;
  readonly #secret = randomBytes(32);
  constructor(path: string) {
    this.#connection = new StorageConnection(path, { readOnly: true });
    try {
      const database = this.#connection.database;
      if (database.prepare("PRAGMA user_version").get()?.["user_version"] !== latestSchemaVersion)
        fail("memory_unavailable");
      // Only the memory table belongs to this reader; lifecycle tables remain with the writer.
      const schema = database
        .prepare("SELECT sql FROM sqlite_schema WHERE type='table' AND name='memories'")
        .get()?.["sql"];
      if (schema !== currentSchema) fail("memory_unavailable");
      this.#memories = new Memories(this.#connection);
    } catch (error) {
      this.#connection.close();
      throw error;
    }
  }
  #execute<T>(operation: () => T): T {
    try {
      this.#connection.ensureOpen();
      return operation();
    } catch (error) {
      if (error instanceof HostMemoryError) throw error;
      return fail("memory_unavailable");
    }
  }
  #encode(value: Record<string, unknown>): string {
    const body = Buffer.from(JSON.stringify(value)).toString("base64url");
    return body + "." + createHmac("sha256", this.#secret).update(body).digest("base64url");
  }
  #decode(cursor: string): Record<string, unknown> {
    try {
      const [body, signature, extra] = cursor.split(".");
      if (!body || !signature || extra !== undefined) fail("invalid_cursor");
      const expected = createHmac("sha256", this.#secret).update(body).digest("base64url");
      if (
        signature.length !== expected.length ||
        !timingSafeEqual(Buffer.from(signature), Buffer.from(expected))
      )
        fail("invalid_cursor");
      const value: unknown = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
      if (!value || typeof value !== "object" || Array.isArray(value)) fail("invalid_cursor");
      return Object.fromEntries(Object.entries(value));
    } catch {
      return fail("invalid_cursor");
    }
  }
  search(input: HostMemorySearchRequestV1): HostMemorySearchResponseV1 {
    const request = object(input, ["query", "cursor", "limit"]);
    const query = text(request["query"], 128).trim();
    const limit = request["limit"] ?? 20;
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > 20)
      fail("invalid_input");
    const cursor = optionalCursor(request["cursor"]);
    let after: string | undefined;
    if (cursor) {
      const decoded = this.#decode(cursor);
      if (
        decoded["kind"] !== "search" ||
        decoded["query"] !== query ||
        typeof decoded["after"] !== "string"
      )
        fail("invalid_cursor");
      after = decoded["after"];
    }
    return this.#execute(() => {
      const rows = this.#memories.searchBatch({
        terms: [query],
        match: "any",
        limit: 128,
        ...(after === undefined ? {} : { after }),
      });
      const items: HostMemorySearchItemV1[] = [];
      let consumed = 0;
      let lastId: string | undefined;
      for (const { memory, matchedTerms } of rows) {
        consumed++;
        lastId = memory.id;
        if (matchedTerms.length === 0) continue;
        if ([...memory.id].length > 512) fail("memory_unavailable");
        const matchIndex = memory.content.toLowerCase().indexOf(query.toLowerCase());
        const start = Math.max(
          0,
          [...memory.content.slice(0, Math.max(0, matchIndex))].length - 80,
        );
        items.push({
          id: memory.id,
          revision: memory.revision,
          preview: [...memory.content].slice(start, start + 500).join(""),
        });
        if (items.length === limit) break;
      }
      return {
        items,
        ...((consumed < rows.length || rows.length === 128) && lastId !== undefined
          ? { nextCursor: this.#encode({ kind: "search", query, after: lastId }) }
          : {}),
      };
    });
  }
  read(input: HostMemoryReadRequestV1): HostMemoryReadResponseV1 {
    const request = object(input, ["id", "expectedRevision", "cursor"]);
    const id = text(request["id"], 512);
    const expectedRevision = revision(request["expectedRevision"]);
    const cursor = optionalCursor(request["cursor"]);
    let offset = 0;
    if (cursor) {
      const decoded = this.#decode(cursor);
      if (
        decoded["kind"] !== "read" ||
        decoded["id"] !== id ||
        decoded["revision"] !== expectedRevision ||
        typeof decoded["offset"] !== "number" ||
        !Number.isSafeInteger(decoded["offset"]) ||
        decoded["offset"] < 1
      )
        fail("invalid_cursor");
      offset = decoded["offset"];
    }
    return this.#execute(() => {
      const memory = this.#memories.get(id);
      if (!memory) fail("memory_not_found");
      if (memory.revision !== expectedRevision) fail("memory_changed");
      const characters = [...memory.content];
      if (offset > characters.length) fail("invalid_cursor");
      const end = offset + 8000;
      return {
        id,
        revision: expectedRevision,
        text: characters.slice(offset, end).join(""),
        ...(end < characters.length
          ? {
              nextCursor: this.#encode({
                kind: "read",
                id,
                revision: expectedRevision,
                offset: end,
              }),
            }
          : {}),
      };
    });
  }
  close(): void {
    this.#connection.close();
  }
}

/** Open an existing Distill store for bounded local reading without migrations or Pi. */
export function openMemoryReaderV1(databasePath: string): HostMemoryReaderV1 {
  try {
    return new HostMemoryReader(databasePath);
  } catch {
    return fail("memory_unavailable");
  }
}
