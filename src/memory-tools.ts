import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import type { Memory, SQLiteMemoryStore } from "./memory-store.js";

export interface MemoryToolResult {
  content: { type: "text"; text: string }[];
  details: Record<string, unknown>;
  isError?: boolean;
}

type Body = Record<string, unknown>;
type Match = "any" | "all";
interface SearchRequest { terms: string[]; match: Match; limit: number; cursor?: string }
interface ReadRequest { id: string; expectedRevision: number; cursor?: string }
interface Candidate {
  id: string;
  revision: number;
  matchedTerms: readonly string[];
  preview: { text: string; omittedBefore: boolean; omittedAfter: boolean };
}

const RESULT_BYTES = 16 * 1024;
const RUN_BYTES = 64 * 1024;
const CONTROL_RESERVE = 512;
const RUN_CALLS = 12;
const CHUNK_BYTES = 8 * 1024;
class InvalidInput extends Error {}
class ResultTooLarge extends Error {}
class BudgetExhausted extends Error {}

function record(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidInput("Expected an object.");
  }
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowed.includes(key)) throw new InvalidInput("Unknown field.");
  }
  // The guard establishes an object; copying entries also avoids prototype lookups.
  return Object.fromEntries(Object.entries(value));
}

function text(value: unknown): string {
  if (typeof value !== "string" || !value.trim() || /[\uD800-\uDFFF]/u.test(value)) {
    throw new InvalidInput("Expected nonempty, well-formed Unicode text.");
  }
  return value;
}
function integer(value: unknown, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new InvalidInput("Integer outside the allowed range.");
  }
  return value;
}
function fold(value: string): string {
  return value.replace(/[A-Z]/g, character => character.toLowerCase());
}
function searchRequest(value: unknown): SearchRequest {
  const input = record(value, ["terms", "match", "limit", "cursor"]);
  if (!Array.isArray(input["terms"]) || input["terms"].length < 1 || input["terms"].length > 8) {
    throw new InvalidInput("Supply between 1 and 8 literal terms.");
  }
  const terms = [...new Set(Array.from(input["terms"], (term: unknown) => {
    const result = fold(text(term));
    if ([...result].length > 128) throw new InvalidInput("Terms allow at most 128 code points.");
    return result;
  }))];
  const match = Object.hasOwn(input, "match") ? input["match"] : "any";
  if (match !== "any" && match !== "all") throw new InvalidInput("match must be any or all.");
  const result: SearchRequest = { terms, match, limit: integer(Object.hasOwn(input, "limit") ? input["limit"] : 20, 1, 50) };
  if (Object.hasOwn(input, "cursor")) result.cursor = text(input["cursor"]);
  return result;
}
function readRequest(value: unknown): ReadRequest {
  const input = record(value, ["id", "expectedRevision", "cursor"]);
  const result: ReadRequest = {
    id: text(input["id"]), expectedRevision: integer(input["expectedRevision"], 1, Number.MAX_SAFE_INTEGER),
  };
  if (Object.hasOwn(input, "cursor")) result.cursor = text(input["cursor"]);
  return result;
}
function envelope(body: Body, error = false): MemoryToolResult {
  const result: MemoryToolResult = {
    content: [{ type: "text", text: JSON.stringify(body) }], details: body,
  };
  if (error) result.isError = true;
  return result;
}
function size(result: MemoryToolResult): number {
  return Buffer.byteLength(JSON.stringify(result));
}
function boundary(content: string, position: number): number {
  const code = content.charCodeAt(position);
  return code >= 0xdc00 && code <= 0xdfff ? position - 1 : position;
}
function matchPosition(content: string, terms: readonly string[]): number {
  const stride = 8192;
  const lookahead = Math.max(...terms.map(term => term.length));
  for (let offset = 0; offset < content.length; offset += stride) {
    // Look ahead so a term crossing the chunk boundary can precede a shorter match.
    const normalized = fold(content.slice(offset, offset + stride + lookahead));
    const positions = terms.map(term => normalized.indexOf(term)).filter(position => position >= 0);
    const position = Math.min(...positions);
    if (position < stride) return offset + position;
  }
  throw new Error("SQLite match could not be located in the original content.");
}
function preview(memory: Memory, match: number, width: number): Candidate["preview"] {
  const start = boundary(memory.content, Math.max(0, match - Math.floor(width / 4)));
  const end = boundary(memory.content, Math.min(memory.content.length, start + width));
  return { text: memory.content.slice(start, end), omittedBefore: start > 0, omittedAfter: end < memory.content.length };
}

/** Read-only tools with run-scoped budgets, independent of the Pi renderer. */
export class MemoryTools {
  readonly #openStore: () => SQLiteMemoryStore;
  readonly #cursorKey = randomBytes(32);
  #store: SQLiteMemoryStore | undefined;
  #queue: Promise<void> = Promise.resolve();
  #epoch = 0;
  #calls = 0;
  #bytes = 0;
  #refused = false;
  #seen = new Map<string, number>();

  constructor(openStore: () => SQLiteMemoryStore) {
    this.#openStore = openStore;
  }

  reset(): void {
    this.#epoch++;
    this.#calls = 0;
    this.#bytes = 0;
    this.#refused = false;
    this.#seen.clear();
  }

  close(): void {
    this.#epoch++;
    this.#store?.close();
    this.#store = undefined;
  }

  search(input: unknown, signal?: AbortSignal): Promise<MemoryToolResult> {
    return this.#enqueue(async (cap, epoch) => this.#search(searchRequest(input), cap, epoch, signal), signal);
  }

  read(input: unknown, signal?: AbortSignal): Promise<MemoryToolResult> {
    return this.#enqueue(async cap => this.#read(readRequest(input), cap), signal);
  }

  #enqueue(operation: (cap: number, epoch: number) => Promise<Body>, signal?: AbortSignal): Promise<MemoryToolResult> {
    const epoch = this.#epoch;
    const pending = this.#queue.then(async () => {
      if (epoch !== this.#epoch) return envelope({ status: "cancelled", incomplete: true });
      if (this.#refused || this.#calls >= RUN_CALLS || this.#bytes >= RUN_BYTES - CONTROL_RESERVE) {
        return this.#exhausted();
      }
      this.#calls++;
      const cap = Math.min(RESULT_BYTES, RUN_BYTES - CONTROL_RESERVE - this.#bytes);
      let body: Body;
      let error = false;
      try {
        body = signal?.aborted ? { status: "cancelled", incomplete: true } : await operation(cap, epoch);
      } catch (caught) {
        error = true;
        if (caught instanceof BudgetExhausted) return this.#exhausted();
        if (caught instanceof InvalidInput) body = { status: "invalid_input", message: caught.message };
        else if (caught instanceof ResultTooLarge) body = { status: "result_too_large", incomplete: true };
        else body = { status: "unavailable", incomplete: true, message: "Memory storage is unavailable. Continue without it." };
      }
      if (epoch !== this.#epoch) return envelope({ status: "cancelled", incomplete: true });
      const result = this.#response(body, error);
      if (size(result) > cap) return this.#exhausted();
      this.#bytes += size(result);
      return result;
    });
    this.#queue = pending.then(() => undefined, () => undefined);
    return pending;
  }

  #database(): SQLiteMemoryStore {
    this.#store ??= this.#openStore();
    return this.#store;
  }

  #response(body: Body, error = false): MemoryToolResult {
    const remaining = { calls: RUN_CALLS - this.#calls, bytes: Math.max(0, RUN_BYTES - this.#bytes) };
    let result = envelope({ ...body, remaining }, error);
    // Byte counts include their own decimal representation; use the conservative fixed point.
    for (let i = 0; i < 4; i++) {
      remaining.bytes = Math.min(remaining.bytes, Math.max(0, RUN_BYTES - this.#bytes - size(result)));
      result = envelope({ ...body, remaining }, error);
    }
    return result;
  }

  #exhausted(): MemoryToolResult {
    const result = envelope({ status: "budget_exhausted", incomplete: true });
    if (!this.#refused) {
      this.#bytes += size(result);
      this.#refused = true;
    }
    return result;
  }

  #fits(body: Body, cap: number): boolean {
    return size(this.#response(body)) <= cap;
  }

  #tooLarge(cap: number): never {
    if (cap < RESULT_BYTES) throw new BudgetExhausted();
    throw new ResultTooLarge();
  }

  #encode(body: Body): string {
    const payload = Buffer.from(JSON.stringify(body)).toString("base64url");
    const signature = createHmac("sha256", this.#cursorKey).update(payload).digest("base64url");
    return `${payload}.${signature}`;
  }

  #decode(cursor: string, allowed: readonly string[]): Record<string, unknown> {
    if (cursor.length > RESULT_BYTES) throw new InvalidInput("Invalid continuation.");
    const [payload, signature, extra] = cursor.split(".");
    if (!payload || !signature || extra !== undefined) throw new InvalidInput("Invalid continuation.");
    const expected = createHmac("sha256", this.#cursorKey).update(payload).digest();
    const received = Buffer.from(signature, "base64url");
    if (received.toString("base64url") !== signature || received.length !== expected.length || !timingSafeEqual(received, expected)) {
      throw new InvalidInput("Invalid or expired continuation.");
    }
    try {
      return record(JSON.parse(Buffer.from(payload, "base64url").toString("utf8")), allowed);
    } catch {
      throw new InvalidInput("Invalid continuation.");
    }
  }

  #query(request: SearchRequest): string {
    return JSON.stringify({ terms: [...request.terms].sort(), match: request.match });
  }

  async #search(request: SearchRequest, cap: number, epoch: number, signal?: AbortSignal): Promise<Body> {
    const query = this.#query(request);
    let after: string | undefined;
    if (request.cursor !== undefined) {
      const cursor = this.#decode(request.cursor, ["kind", "query", "after"]);
      if (cursor["kind"] !== "search" || cursor["query"] !== query) throw new InvalidInput("Continuation belongs to another query.");
      after = text(cursor["after"]);
    }
    const candidates: Candidate[] = [];
    const deadline = performance.now() + 250;
    const result = (hasMore: boolean, stopReason: string): Body => {
      const uniqueCandidates = new Set([...this.#seen.keys(), ...candidates.map(candidate => candidate.id)]).size;
      const body: Body = { status: "ok", candidates, hasMore, stopReason, uniqueCandidates };
      if (hasMore && after !== undefined) body["nextCursor"] = this.#encode({ kind: "search", query, after });
      return body;
    };
    const finish = (hasMore: boolean, stopReason: string): Body => {
      const body = result(hasMore, stopReason);
      if (!this.#fits(body, cap)) this.#tooLarge(cap);
      for (const item of candidates) this.#seen.set(item.id, item.revision);
      return body;
    };
    while (true) {
      if (signal?.aborted || epoch !== this.#epoch) return { status: "cancelled", incomplete: true };
      const batch = this.#database().searchBatch({ terms: request.terms, match: request.match, ...(after !== undefined ? { after } : {}) });
      for (const [index, row] of batch.entries()) {
        const previous = after;
        const matches = request.match === "any" ? row.matchedTerms.length > 0 : row.matchedTerms.length === request.terms.length;
        if (!matches) { after = row.memory.id; continue; }
        const position = matchPosition(row.memory.content, row.matchedTerms);
        const candidate: Candidate = { id: row.memory.id, revision: row.memory.revision, matchedTerms: row.matchedTerms, preview: preview(row.memory, position, 640) };
        candidates.push(candidate);
        after = row.memory.id;
        // Include a continuation even on the tentative final row, so returning a page never loses it.
        if (!this.#fits(result(true, "output_limit"), cap)) {
          if (candidates.length > 1) {
            candidates.pop(); after = previous;
            return finish(true, "output_limit");
          }
          for (const width of [256, 64, 16, 0]) {
            candidate.preview = preview(row.memory, position, width);
            if (this.#fits(result(true, "output_limit"), cap)) break;
          }
          if (!this.#fits(result(true, "output_limit"), cap)) this.#tooLarge(cap);
        }
        if (candidates.length >= request.limit) {
          const hasMore = index + 1 < batch.length || batch.length === 128;
          return finish(hasMore, hasMore ? "candidate_limit" : "exhausted");
        }
      }
      if (batch.length < 128) return finish(false, "exhausted");
      await yieldToEventLoop();
      if (signal?.aborted || epoch !== this.#epoch) return { status: "cancelled", incomplete: true };
      if (performance.now() >= deadline) return finish(true, "scan_deadline");
    }
  }

  #read(request: ReadRequest, cap: number): Body {
    let offset = 0;
    if (request.cursor !== undefined) {
      const cursor = this.#decode(request.cursor, ["kind", "id", "revision", "offset"]);
      if (cursor["kind"] !== "read" || cursor["id"] !== request.id || cursor["revision"] !== request.expectedRevision) {
        throw new InvalidInput("Continuation belongs to another record or revision.");
      }
      offset = integer(cursor["offset"], 0, Number.MAX_SAFE_INTEGER);
    }
    const memory = this.#database().get(request.id);
    if (memory === undefined) return { status: "not_found" };
    if (memory.revision !== request.expectedRevision) return { status: "stale", currentRevision: memory.revision };
    if (offset > memory.content.length || boundary(memory.content, offset) !== offset) throw new InvalidInput("Invalid read position.");
    let end = offset;
    let bytes = 0;
    for (const character of memory.content.slice(offset)) {
      const next = bytes + Buffer.byteLength(character);
      if (next > CHUNK_BYTES) break;
      end += character.length; bytes = next;
    }
    const body = (): Body => {
      const result: Body = {
        status: "ok", id: memory.id, revision: memory.revision,
        createdAt: memory.createdAt, updatedAt: memory.updatedAt,
        content: memory.content.slice(offset, end), hasMore: end < memory.content.length,
      };
      if (end < memory.content.length) result["nextCursor"] = this.#encode({ kind: "read", id: memory.id, revision: memory.revision, offset: end });
      return result;
    };
    while (!this.#fits(body(), cap)) {
      let next = boundary(memory.content, offset + Math.floor((end - offset) / 2));
      if (next <= offset) {
        // A midpoint can split the first supplementary character; try that whole character.
        const first = memory.content.codePointAt(offset);
        next = offset + (first !== undefined && first > 0xffff ? 2 : 1);
        if (next >= end) this.#tooLarge(cap);
      }
      end = next;
    }
    return body();
  }
}
