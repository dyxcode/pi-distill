import { createHash, randomUUID } from "node:crypto";
import { lstat, open } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { validateEntryShape, validateSessionHeader, stableJson, boundedJsonBytes } from "./consolidation-source.ts";
import { DistillFailure } from "./distill-failure.ts";

export const SOURCE_INDEX_SCHEMAS = [
  { name: "source_index_files", sql: "CREATE TABLE source_index_files (path TEXT PRIMARY KEY NOT NULL, generation TEXT NOT NULL UNIQUE, progress TEXT NOT NULL) STRICT" },
  { name: "source_index_entries", sql: "CREATE TABLE source_index_entries (generation TEXT NOT NULL, seq INTEGER NOT NULL, entry_key TEXT NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY (generation, seq), UNIQUE (generation, entry_key)) STRICT" },
];
const READ_BYTES = 64 * 1024;
const PAGE_BYTES = 4 * 1024 * 1024;
const PAGE_ENTRIES = 256;
const RECORD_BYTES = 16 * 1024 * 1024;
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export interface IndexedHeader { readonly id: string; readonly cwd: string; readonly parentSession?: string }
export interface SourceIndexState {
  readonly path: string;
  readonly generation: string;
  readonly version: string;
  readonly offset: number;
  readonly entries: number;
  readonly header: IndexedHeader | null;
  readonly complete: boolean;
  readonly waitingForTail: boolean;
}
export interface IndexedEntry {
  readonly seq: number;
  readonly id: string;
  readonly parentId: string | null;
  readonly type: string;
  readonly timestamp: string;
  readonly offset: number;
  readonly length: number;
  readonly signature: string;
  readonly targetId?: string;
  readonly firstKeptEntryId?: string | null;
}
function invalid(message: string): never { throw new DistillFailure("source_invalid", message); }
function conflict(): never { throw new DistillFailure("conflict", "Source index version or page changed; stale writer cannot commit."); }
function object(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function integer(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }
function string(value: unknown): value is string { return typeof value === "string" && value.length > 0; }
function parsed(value: unknown): unknown {
  if (typeof value !== "string") return invalid("Invalid stored source index JSON.");
  try { return JSON.parse(value); } catch { return invalid("Invalid stored source index JSON."); }
}
function state(value: unknown): SourceIndexState {
  if (!object(value) || !string(value["path"]) || !isAbsolute(value["path"]) || value["path"].includes("\0") ||
    !string(value["generation"]) || !string(value["version"]) || !/^\d+:\d+:\d+:\d+:\d+$/.test(value["version"]) ||
    !integer(value["offset"]) || !integer(value["entries"]) || typeof value["complete"] !== "boolean" || typeof value["waitingForTail"] !== "boolean") return invalid("Invalid stored source index state.");
  const header = value["header"];
  if (header !== null && (!object(header) || !string(header["id"]) || typeof header["cwd"] !== "string" ||
    (header["parentSession"] !== undefined && !string(header["parentSession"])))) return invalid("Invalid indexed header.");
  const fileBytes = Number(value["version"].split(":")[2]);
  if (!Number.isSafeInteger(fileBytes) || value["offset"] > fileBytes ||
    (value["complete"] && (value["offset"] !== fileBytes || header === null || value["waitingForTail"])) ||
    (value["entries"] > 0 && header === null)) return invalid("Inconsistent indexed source completion.");
  return { path: value["path"], generation: value["generation"], version: value["version"], offset: value["offset"], entries: value["entries"],
    complete: value["complete"], waitingForTail: value["waitingForTail"],
    header: header === null ? null : { id: String(header["id"]), cwd: String(header["cwd"]),
      ...(typeof header["parentSession"] === "string" ? { parentSession: header["parentSession"] } : {}) } };
}
function entryMetadata(value: unknown): IndexedEntry {
  if (!object(value) || !integer(value["seq"]) || value["seq"] < 1 || !string(value["id"]) ||
    (value["parentId"] !== null && !string(value["parentId"])) || !string(value["type"]) || !string(value["timestamp"]) ||
    !integer(value["offset"]) || !integer(value["length"]) || value["length"] < 1 || value["length"] > RECORD_BYTES + 1 ||
    typeof value["signature"] !== "string" || !/^[a-f0-9]{64}$/.test(value["signature"]) ||
    (value["targetId"] !== undefined && !string(value["targetId"])) ||
    (value["firstKeptEntryId"] !== undefined && value["firstKeptEntryId"] !== null && !string(value["firstKeptEntryId"]))) return invalid("Invalid indexed entry metadata.");
  return { seq: value["seq"], id: value["id"], parentId: value["parentId"], type: value["type"], timestamp: value["timestamp"],
    offset: value["offset"], length: value["length"], signature: value["signature"],
    ...(typeof value["targetId"] === "string" ? { targetId: value["targetId"] } : {}),
    ...(typeof value["firstKeptEntryId"] === "string" || value["firstKeptEntryId"] === null ? { firstKeptEntryId: value["firstKeptEntryId"] } : {}) };
}
function digest(value: unknown): string { return createHash("sha256").update(stableJson(value)).digest("hex"); }

/** Rebuildable structural metadata. Page compare-and-swap fences concurrent scanners. */
export class SourceIndex {
  readonly database: DatabaseSync;
  constructor(database: DatabaseSync) { this.database = database; }
  current(path: string): SourceIndexState | undefined {
    const row = this.database.prepare("SELECT progress FROM source_index_files WHERE path = ?").get(path);
    if (!row) return undefined;
    const result = state(parsed(row["progress"]));
    if (result.path !== path) return invalid("Indexed source path differs from its key.");
    return result;
  }
  begin(path: string, version: string): SourceIndexState {
    const current = this.current(path);
    if (current?.version === version) return current;
    const next = state({ path, version, generation: randomUUID(), offset: 0, entries: 0, header: null, complete: false, waitingForTail: false });
    this.database.prepare("INSERT INTO source_index_files VALUES (?, ?, ?) ON CONFLICT(path) DO UPDATE SET generation=excluded.generation, progress=excluded.progress")
      .run(path, next.generation, JSON.stringify(next));
    return next;
  }
  entry(generation: string, id: string): IndexedEntry | undefined {
    const row = this.database.prepare("SELECT metadata FROM source_index_entries WHERE generation=? AND entry_key=?").get(generation, JSON.stringify(id));
    if (!row) return undefined;
    const result = entryMetadata(parsed(row["metadata"]));
    if (result.id !== id) return invalid("Indexed entry identity differs from its key.");
    return result;
  }
  entries(generation: string, after: number, limit = 128): IndexedEntry[] {
    if (!integer(after) || !integer(limit) || limit < 1 || limit > 128) throw new TypeError("Invalid source index page.");
    return this.database.prepare("SELECT metadata FROM source_index_entries WHERE generation=? AND seq>? ORDER BY seq LIMIT ?")
      .all(generation, after, limit).map(row => entryMetadata(parsed(row["metadata"])));
  }
  commitPage(previous: SourceIndexState, rows: readonly IndexedEntry[], next: SourceIndexState): void {
    state(previous); state(next);
    if (rows.length > PAGE_ENTRIES || next.generation !== previous.generation || next.path !== previous.path || next.version !== previous.version ||
      next.entries !== previous.entries + rows.length || next.offset < previous.offset) return invalid("Invalid source page progress.");
    this.database.exec("SAVEPOINT source_index_page");
    try {
      if (JSON.stringify(this.current(previous.path)) !== JSON.stringify(previous)) conflict();
      const insert = this.database.prepare("INSERT INTO source_index_entries VALUES (?, ?, ?, ?)");
      let seq = previous.entries;
      for (const raw of rows) {
        const row = entryMetadata(raw);
        if (row.seq !== ++seq || row.offset < previous.offset || row.offset + row.length > next.offset) return invalid("Invalid indexed entry position.");
        if (this.entry(previous.generation, row.id)) return invalid("Session contains a duplicate entry id.");
        if (row.parentId !== null && !this.entry(previous.generation, row.parentId)) return invalid("Session entry has a missing or later parent, or a parent cycle.");
        insert.run(previous.generation, row.seq, JSON.stringify(row.id), JSON.stringify(row));
      }
      this.database.prepare("UPDATE source_index_files SET progress=? WHERE path=? AND generation=?")
        .run(JSON.stringify(next), next.path, next.generation);
      this.database.exec("RELEASE source_index_page");
    } catch (error) {
      this.database.exec("ROLLBACK TO source_index_page; RELEASE source_index_page");
      throw error;
    }
  }
}

export async function indexedFileVersion(path: string): Promise<string> {
  const info = await lstat(path, { bigint: true });
  if (!info.isFile()) return invalid("Indexed session must be a regular file.");
  if (info.size > BigInt(Number.MAX_SAFE_INTEGER)) return invalid("Session file exceeds addressable offsets.");
  return [info.dev, info.ino, info.size, info.mtimeNs, info.ctimeNs].join(":");
}

/** One bounded scan slice. Only complete validated records advance the durable cursor. */
export async function scanSourcePage(path: string, index: SourceIndex, signal?: AbortSignal): Promise<SourceIndexState> {
  signal?.throwIfAborted();
  path = resolve(path);
  const version = await indexedFileVersion(path);
  const previous = index.begin(path, version);
  if (previous.complete) return previous;
  const file = await open(path, "r");
  const rows: IndexedEntry[] = [];
  let offset = previous.offset;
  let readAt = offset;
  let header = previous.header;
  let complete = false;
  let waitingForTail = false;
  let fragments: Buffer[] = [];
  let fragmentBytes = 0;
  const consume = (bytes: Buffer, terminated: boolean): boolean => {
    let value: unknown;
    try { value = JSON.parse(decoder.decode(bytes)); }
    catch { if (!terminated) { waitingForTail = true; return false; } return invalid("Session JSONL record is malformed or not valid UTF-8."); }
    if (boundedJsonBytes(value, RECORD_BYTES) === undefined) return invalid("Session record exceeds its structured size or depth limit.");
    if (header === null) {
      validateSessionHeader(value);
      header = { id: value.id, cwd: value.cwd, ...(value.parentSession ? { parentSession: value.parentSession } : {}) };
    } else {
      try { validateEntryShape(value, previous.entries + rows.length + 2); }
      catch (error) { if (!terminated) { waitingForTail = true; return false; } throw error; }
      rows.push({ seq: previous.entries + rows.length + 1, id: value.id, parentId: value.parentId, type: value.type,
        timestamp: value.timestamp, offset, length: bytes.length + (terminated ? 1 : 0), signature: digest(value),
        ...(value.type === "context_edit" ? { targetId: value.targetId } : {}),
        ...(value.type === "compaction" ? { firstKeptEntryId: value.firstKeptEntryId } : {}) });
    }
    offset += bytes.length + (terminated ? 1 : 0);
    return true;
  };
  try {
    outer: while (rows.length < PAGE_ENTRIES && offset - previous.offset < PAGE_BYTES) {
      signal?.throwIfAborted();
      const buffer = Buffer.allocUnsafe(READ_BYTES);
      const { bytesRead } = await file.read(buffer, 0, buffer.length, readAt);
      readAt += bytesRead;
      if (bytesRead === 0) {
        if (fragmentBytes > 0) consume(Buffer.concat(fragments, fragmentBytes), false);
        if (header === null) return invalid("Session file is empty.");
        complete = !waitingForTail;
        break;
      }
      let start = 0;
      for (let pos = 0; pos < bytesRead; pos++) {
        if (buffer[pos] !== 10) continue;
        const part = buffer.subarray(start, pos);
        fragmentBytes += part.length;
        if (fragmentBytes > RECORD_BYTES) return invalid("Session record exceeds its byte limit.");
        fragments.push(part);
        consume(Buffer.concat(fragments, fragmentBytes), true);
        fragments = []; fragmentBytes = 0; start = pos + 1;
        if (rows.length >= PAGE_ENTRIES || offset - previous.offset >= PAGE_BYTES) break outer;
      }
      if (start < bytesRead) { const part = buffer.subarray(start, bytesRead); fragments.push(part); fragmentBytes += part.length; }
      if (fragmentBytes > RECORD_BYTES) return invalid("Session record exceeds its byte limit.");
    }
    signal?.throwIfAborted();
    if (await indexedFileVersion(path) !== version) conflict();
    signal?.throwIfAborted();
    const next: SourceIndexState = { ...previous, offset, entries: previous.entries + rows.length, header, complete, waitingForTail };
    index.commitPage(previous, rows, next);
    return next;
  } finally { await file.close(); }
}

/** Read only a verified record; the caller retains source-access protection. */
export async function readIndexedEntry(path: string, source: SourceIndexState, row: IndexedEntry): Promise<SessionEntry> {
  entryMetadata(row);
  if (resolve(path) !== source.path || await indexedFileVersion(path) !== source.version) conflict();
  const file = await open(path, "r");
  try {
    const buffer = Buffer.allocUnsafe(row.length);
    let read = 0;
    while (read < buffer.length) {
      const { bytesRead } = await file.read(buffer, read, buffer.length - read, row.offset + read);
      if (bytesRead === 0) conflict();
      read += bytesRead;
    }
    let value: unknown;
    try { value = JSON.parse(decoder.decode(buffer)); } catch { return invalid("Indexed entry is malformed."); }
    if (boundedJsonBytes(value, RECORD_BYTES) === undefined) return invalid("Indexed record exceeds its structured limit.");
    validateEntryShape(value, row.seq + 1);
    if (value.id !== row.id || value.type !== row.type || value.parentId !== row.parentId || value.timestamp !== row.timestamp ||
      (value.type === "context_edit" ? value.targetId : undefined) !== row.targetId ||
      (value.type === "compaction" ? value.firstKeptEntryId : undefined) !== row.firstKeptEntryId ||
      digest(value) !== row.signature || await indexedFileVersion(path) !== source.version) conflict();
    return value;
  } finally { await file.close(); }
}
