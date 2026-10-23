import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { DistillFailure } from "./distill-failure.ts";
import { SourceIndex, indexedFileVersion, scanSourcePage, readIndexedEntry, parseIndexState, type SourceIndexState } from "./source-index.ts";
import { sourceEntryIdentity, sourcePartsForEntry, isContextEditableContent } from "./consolidation-source.ts";
import type { SourcePart, ConsolidationEvidenceBoundary } from "./consolidation-types.js";
import type { MemoryOrigin } from "./memory-store.ts";

export const SOURCE_VIEW_SCHEMAS = [
  { name: "source_views", sql: "CREATE TABLE source_views (id TEXT PRIMARY KEY NOT NULL, progress TEXT NOT NULL) STRICT" },
  { name: "source_view_entries", sql: "CREATE TABLE source_view_entries (view_id TEXT NOT NULL, seq INTEGER NOT NULL, entry_key TEXT NOT NULL, origin_key TEXT NOT NULL, anchor TEXT NOT NULL, identity TEXT, PRIMARY KEY (view_id, seq), UNIQUE (view_id, entry_key)) STRICT" },
  { name: "source_view_anchors", sql: "CREATE INDEX source_view_anchors ON source_view_entries (view_id, anchor)" },
  { name: "source_view_edits", sql: "CREATE TABLE source_view_edits (view_id TEXT NOT NULL, target_key TEXT NOT NULL, scope INTEGER NOT NULL, last_seq INTEGER NOT NULL, source_time TEXT, PRIMARY KEY (view_id, target_key, scope)) STRICT" },
  { name: "source_view_parts", sql: "CREATE TABLE source_view_parts (view_id TEXT NOT NULL, seq INTEGER NOT NULL, source_key TEXT NOT NULL, entry_seq INTEGER NOT NULL, ordinal INTEGER NOT NULL, source_time TEXT, anchor TEXT NOT NULL, PRIMARY KEY (view_id, seq), UNIQUE (view_id, source_key)) STRICT" },
  { name: "source_preparation", sql: "CREATE TABLE source_preparation (trigger_id TEXT PRIMARY KEY NOT NULL, view_id TEXT NOT NULL, cursor INTEGER NOT NULL) STRICT" },
  { name: "source_index_compactions", sql: "CREATE INDEX source_index_compactions ON source_index_entries (generation, json_extract(metadata, '$.type'), json_extract(metadata, '$.parentId'))" },
];
const PAGE = 128;
export class SourceWorkPending extends Error { constructor() { super("Source processing has more bounded pages."); } }
type Phase = "walk" | "edits" | "parts" | "ready";
interface ViewState {
  id: string; sources: SourceIndexState[]; leafId: string | null; phase: Phase;
  nextId: string | null; count: number; cursor: number; partCursor: number; partCount: number;
  conversation: string; compaction: number; keptId: string | null; kept: number;
}
interface ViewEntry { seq: number; id: string; origin: string; anchor: string; identity?: string }
function fail(message: string): never { throw new DistillFailure("source_invalid", message); }
function hash(value: string): string { return createHash("sha256").update(value).digest("hex"); }
function record(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function number(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }
function text(value: unknown): value is string { return typeof value === "string" && value.length > 0; }
function json(value: unknown): unknown {
  if (typeof value !== "string") return fail("Invalid source projection metadata.");
  try { return JSON.parse(value); } catch { return fail("Malformed source projection metadata."); }
}
function stringJson(value: unknown): string { const parsed = json(value); if (typeof parsed !== "string") return fail("Invalid source reference."); return parsed; }
function assertState(value: unknown): asserts value is ViewState {
  if (!record(value) || !text(value["id"]) || !Array.isArray(value["sources"]) || value["sources"].length < 1 || value["sources"].length > 129 ||
    (value["leafId"] !== null && !text(value["leafId"])) || (value["nextId"] !== null && !text(value["nextId"])) ||
    !["walk", "edits", "parts", "ready"].includes(String(value["phase"])) || !text(value["conversation"]) ||
    !["count", "cursor", "partCursor", "partCount", "compaction", "kept"].every(key => number(value[key])) ||
    (value["keptId"] !== null && !text(value["keptId"]))) return fail("Invalid source projection progress.");
  for (const source of value["sources"]) { const parsed = parseIndexState(source); if (!parsed.complete || !parsed.header) return fail("Projection references an incomplete index."); }
}
function viewEntry(row: Record<string, unknown> | undefined): ViewEntry | undefined {
  if (!row) return undefined;
  if (!number(row["seq"]) || row["seq"] < 1 || !text(row["anchor"]) || (row["identity"] !== null && !text(row["identity"]))) return fail("Invalid indexed branch entry.");
  return { seq: row["seq"], id: stringJson(row["entry_key"]), origin: stringJson(row["origin_key"]), anchor: row["anchor"],
    ...(typeof row["identity"] === "string" ? { identity: row["identity"] } : {}) };
}
function iso(value: string): string | undefined { const time = Date.parse(value); return Number.isFinite(time) && new Date(time).toISOString() === value ? value : undefined; }

/** Disk-backed effective branch projection. No conversation content is persisted. */
export class SourceViews {
  readonly database: DatabaseSync;
  readonly index: SourceIndex;
  constructor(database: DatabaseSync, index: SourceIndex) { this.database = database; this.index = index; }
  state(id: string): ViewState {
    const row = this.database.prepare("SELECT progress FROM source_views WHERE id=?").get(id);
    const value = json(row?.["progress"]); assertState(value); return value;
  }
  entry(id: string, seq: number): ViewEntry | undefined {
    return viewEntry(this.database.prepare("SELECT * FROM source_view_entries WHERE view_id=? AND seq=?").get(id, seq));
  }
  byId(id: string, entryId: string): ViewEntry | undefined {
    return viewEntry(this.database.prepare("SELECT * FROM source_view_entries WHERE view_id=? AND entry_key=?").get(id, JSON.stringify(entryId)));
  }
  async unchanged(state: ViewState): Promise<void> {
    for (const source of state.sources) {
      if (await indexedFileVersion(source.path) !== source.version) throw new SourceWorkPending();
    }
  }
  async advance(path: string, leaf?: string | null, signal?: AbortSignal): Promise<{ ready: false } | { ready: true; view: PagedSourceView }> {
    signal?.throwIfAborted();
    const sources: SourceIndexState[] = [];
    const paths = new Set<string>(), identities = new Set<string>();
    let current: string | undefined = resolve(path);
    while (current !== undefined) {
      if (paths.has(current) || sources.length > 128) return fail("Source ancestry contains a cycle or exceeds its depth limit.");
      paths.add(current);
      const source = await scanSourcePage(current, this.index, signal);
      if (source.waitingForTail) return fail("Source has an incomplete trailing record.");
      if (!source.complete) return { ready: false };
      if (!source.header || identities.has(source.header.id)) return fail("Source ancestry has duplicate session identities.");
      identities.add(source.header.id); sources.push(source);
      current = source.header.parentSession ? resolve(dirname(current), source.header.parentSession) : undefined;
    }
    const root = sources[0]; if (!root?.header) return fail("Source header is unavailable.");
    const selected = leaf === undefined ? this.index.entries(root.generation, Math.max(0, root.entries - 1), 1)[0]?.id ?? null : leaf;
    const id = hash(JSON.stringify([sources.map(source => source.generation), selected]));
    const initial: ViewState = { id, sources, leafId: selected, nextId: selected, phase: "walk", count: 0,
      cursor: 0, partCursor: 0, partCount: 0, conversation: hash("pi-distill-source-root-v1"), compaction: 0, keptId: null, kept: 0 };
    this.database.prepare("INSERT OR IGNORE INTO source_views VALUES (?, ?)").run(id, JSON.stringify(initial));
    const before = this.state(id);
    if (before.phase === "ready") { await this.unchanged(before); return { ready: true, view: new PagedSourceView(this, id) }; }
    const next: ViewState = { ...before };
    const operations: (() => void)[] = [];
    if (next.phase === "walk") {
      for (let work = 0; next.nextId !== null && work < PAGE; work++) {
        const entry = this.index.entry(root.generation, next.nextId);
        if (!entry) return fail("Frozen source branch has a missing entry.");
        let origin = root.header.id;
        for (const parent of sources.slice(1)) {
          if (this.index.entry(parent.generation, entry.id)?.signature !== entry.signature) break;
          if (!parent.header) return fail("Missing ancestor header.");
          origin = parent.header.id;
        }
        const seq = ++next.count;
        const anchor = hash(JSON.stringify(["pi-distill-entry-anchor-v1", origin, entry.id]));
        operations.push(() => this.database.prepare("INSERT INTO source_view_entries VALUES (?, ?, ?, ?, ?, NULL)")
          .run(id, seq, JSON.stringify(entry.id), JSON.stringify(origin), anchor));
        if (entry.type === "compaction" && next.compaction === 0) { next.compaction = seq; next.keptId = entry.firstKeptEntryId ?? null; }
        next.nextId = entry.parentId;
      }
      if (next.nextId === null) { next.phase = "edits"; next.cursor = 0; }
    } else if (next.phase === "edits") {
      next.kept = next.keptId === null ? 0 : this.byId(id, next.keptId)?.seq ?? 0;
      const rows = this.database.prepare("SELECT * FROM source_view_entries WHERE view_id=? AND seq>? ORDER BY seq LIMIT ?").all(id, next.cursor, PAGE);
      for (const row of rows) {
        signal?.throwIfAborted();
        const item = viewEntry(row); if (!item) return fail("Missing branch metadata.");
        const meta = this.index.entry(root.generation, item.id); if (!meta) return fail("Missing indexed entry.");
        if (meta.type === "context_edit") {
          const edit = await readIndexedEntry(root.path, root, meta);
          if (edit.type !== "context_edit") return fail("Invalid edit record.");
          const target = this.byId(id, edit.targetId);
          const targetMeta = target ? this.index.entry(root.generation, target.id) : undefined;
          if (!target || target.seq <= item.seq || !targetMeta) return fail("Edit target is not an earlier branch message.");
          const targetEntry = await readIndexedEntry(root.path, root, targetMeta);
          if (targetEntry.type !== "message" || !["user", "assistant", "toolResult"].includes(targetEntry.message.role) ||
            (edit.replacement !== null && !isContextEditableContent(targetEntry.message.role, edit.replacement.content))) return fail("Unsupported context edit target or replacement.");
          for (const scope of this.inContext(next, item.seq) ? [0, 1] : [0]) {
            operations.push(() => this.database.prepare("INSERT INTO source_view_edits VALUES (?, ?, ?, ?, ?) ON CONFLICT(view_id,target_key,scope) DO UPDATE SET source_time=CASE WHEN excluded.source_time > source_time OR source_time IS NULL THEN excluded.source_time ELSE source_time END")
              .run(id, JSON.stringify(edit.targetId), scope, item.seq, iso(edit.timestamp) ?? null));
          }
        }
        next.cursor = item.seq;
      }
      if (rows.length < PAGE || next.cursor === next.count) { next.phase = "parts"; next.cursor = next.count; next.partCursor = 0; }
    } else if (next.phase === "parts") {
      let work = 0;
      while (next.cursor > 0 && work < PAGE) {
        signal?.throwIfAborted();
        const item = this.entry(id, next.cursor); if (!item) return fail("Missing projected branch entry.");
        const metadata = this.index.entry(root.generation, item.id);
        if (!metadata) return fail("Missing source entry metadata.");
        if (metadata.type !== "message" && metadata.type !== "context_edit") { next.cursor--; work++; continue; }
        const material = await this.materialize(next, item);
        const step = sourceEntryIdentity(material.entry, { sessionId: item.origin, entryId: item.id }, next.conversation, material.effective, material.projected);
        const parts = sourcePartsForEntry(material.entry, material.effective, material.projected, { sessionId: item.origin, entryId: item.id }, step.identity);
        operations.push(() => this.database.prepare("UPDATE source_view_entries SET identity=? WHERE view_id=? AND seq=?").run(step.identity, id, item.seq));
        const edit = this.edit(id, item.id, 0);
        const boundary = edit ? this.entry(id, edit.seq) : item;
        const boundaryTime = [iso(material.entry.timestamp), edit?.time].filter((value): value is string => value !== undefined).sort().at(-1);
        while (next.partCursor < parts.length && work < PAGE) {
          const ordinal = next.partCursor++;
          const part = parts[ordinal]; if (!part) return fail("Missing projected evidence part.");
          const seq = ++next.partCount;
          operations.push(() => this.database.prepare("INSERT INTO source_view_parts VALUES (?, ?, ?, ?, ?, ?, ?)")
            .run(id, seq, part.key, item.seq, ordinal, boundaryTime ?? null, boundary?.anchor ?? item.anchor));
          work++;
        }
        if (next.partCursor === parts.length) { next.cursor--; next.partCursor = 0; next.conversation = step.nextFingerprint; work++; }
      }
      if (next.cursor === 0) next.phase = "ready";
    }
    await this.unchanged(before); signal?.throwIfAborted();
    this.database.exec("SAVEPOINT source_view_page");
    try {
      if (JSON.stringify(this.state(id)) !== JSON.stringify(before)) throw new DistillFailure("conflict", "Source projection page changed.");
      for (const operation of operations) operation();
      this.database.prepare("UPDATE source_views SET progress=? WHERE id=?").run(JSON.stringify(next), id);
      this.database.exec("RELEASE source_view_page");
    } catch (error) { this.database.exec("ROLLBACK TO source_view_page; RELEASE source_view_page"); throw error; }
    return next.phase === "ready" ? { ready: true, view: new PagedSourceView(this, id) } : { ready: false };
  }
  inContext(state: ViewState, seq: number): boolean {
    return state.compaction === 0 || seq < state.compaction || (state.kept > state.compaction && seq > state.compaction && seq <= state.kept);
  }
  edit(view: string, target: string, scope: number): { seq: number; time?: string } | undefined {
    const row = this.database.prepare("SELECT last_seq, source_time FROM source_view_edits WHERE view_id=? AND target_key=? AND scope=?").get(view, JSON.stringify(target), scope);
    if (!row) return undefined;
    if (!number(row["last_seq"]) || (row["source_time"] !== null && typeof row["source_time"] !== "string")) return fail("Invalid projected edit metadata.");
    return { seq: row["last_seq"], ...(typeof row["source_time"] === "string" ? { time: row["source_time"] } : {}) };
  }
  async materialize(state: ViewState, item: ViewEntry) {
    const root = state.sources[0]; if (!root) return fail("Missing source version.");
    const meta = this.index.entry(root.generation, item.id); if (!meta) return fail("Missing entry range.");
    const entry = await readIndexedEntry(root.path, root, meta);
    const replacement = async (scope: number) => {
      const edit = this.edit(state.id, item.id, scope);
      if (!edit) return undefined;
      const ref = this.entry(state.id, edit.seq);
      const position = ref ? this.index.entry(root.generation, ref.id) : undefined;
      if (!position) return fail("Missing edit range.");
      const record = await readIndexedEntry(root.path, root, position);
      if (record.type !== "context_edit") return fail("Invalid indexed edit.");
      return record.replacement;
    };
    const global = await replacement(0);
    const effective = global === undefined ? undefined : global === null ? { content: undefined, omitted: true } : { content: global.content, omitted: false };
    const contextual = this.inContext(state, item.seq) ? await replacement(1) : undefined;
    const projected = entry.type === "message" && "content" in entry.message && this.inContext(state, item.seq)
      ? { messages: contextual === null ? [] : [{ ...entry.message, content: contextual === undefined ? entry.message.content : contextual.content }] } : undefined;
    return { entry, effective, projected };
  }
}

export class PagedSourceView {
  readonly owner: SourceViews; readonly id: string;
  constructor(owner: SourceViews, id: string) { this.owner = owner; this.id = id; }
  get state(): ViewState { const state = this.owner.state(this.id); if (state.phase !== "ready") return fail("Source view is not ready."); return state; }
  get source(): SourceIndexState { const source = this.state.sources[0]; if (!source) return fail("Missing source."); return source; }
  get lineageSessionIds(): string[] { return this.state.sources.flatMap(source => source.header ? [source.header.id] : []); }
  async unchanged(): Promise<void> { await this.owner.unchanged(this.state); }
  position(key: string): number | undefined {
    const row = this.owner.database.prepare("SELECT seq FROM source_view_parts WHERE view_id=? AND source_key=?").get(this.id, key);
    if (!row) return undefined;
    if (!number(row["seq"])) return fail("Invalid evidence position."); return row["seq"];
  }
  async page(after = 0, limit = PAGE): Promise<{ items: { seq: number; part: SourcePart }[]; nextCursor?: number }> {
    if (!number(after) || !number(limit) || limit < 1 || limit > PAGE) throw new TypeError("Invalid evidence page.");
    const state = this.state; await this.unchanged();
    const rows = this.owner.database.prepare("SELECT * FROM source_view_parts WHERE view_id=? AND seq>? ORDER BY seq LIMIT ?").all(this.id, after, limit);
    const items: { seq: number; part: SourcePart }[] = [];
    let cached: { seq: number; parts: SourcePart[] } | undefined;
    for (const row of rows) {
      if (!number(row["seq"]) || !number(row["entry_seq"]) || !number(row["ordinal"]) || !text(row["source_key"])) return fail("Invalid indexed evidence metadata.");
      if (cached?.seq !== row["entry_seq"]) {
        const entry = this.owner.entry(this.id, row["entry_seq"]); if (!entry?.identity) return fail("Missing source identity.");
        const material = await this.owner.materialize(state, entry);
        cached = { seq: entry.seq, parts: sourcePartsForEntry(material.entry, material.effective, material.projected, { sessionId: entry.origin, entryId: entry.id }, entry.identity) };
      }
      const part = cached.parts[row["ordinal"]];
      if (!part || part.key !== row["source_key"]) return fail("Frozen source reference changed.");
      items.push({ seq: row["seq"], part });
    }
    await this.unchanged();
    const last = items.at(-1)?.seq ?? after;
    return { items, ...(last < state.partCount ? { nextCursor: last } : {}) };
  }
  async readKeys(keys: readonly string[]): Promise<SourcePart[]> {
    const parts: SourcePart[] = [];
    for (const key of keys) {
      const seq = this.position(key); if (seq === undefined) return fail("A frozen source reference changed after preparation.");
      const item = (await this.page(seq - 1, 1)).items[0]; if (!item || item.part.key !== key) return fail("Missing frozen evidence.");
      parts.push(item.part);
    }
    return parts;
  }
  boundary(parts: readonly SourcePart[]): ConsolidationEvidenceBoundary {
    let last = Number.MAX_SAFE_INTEGER; let anchor: string | undefined; let time: string | undefined;
    for (const part of parts) {
      const row = this.owner.database.prepare("SELECT source_time, anchor FROM source_view_parts WHERE view_id=? AND source_key=?").get(this.id, part.key);
      if (!row || !text(row["anchor"])) return fail("Missing source boundary.");
      const boundary = this.owner.database.prepare("SELECT seq FROM source_view_entries WHERE view_id=? AND anchor=?").get(this.id, row["anchor"]);
      if (!boundary || !number(boundary["seq"])) return fail("Invalid source anchor.");
      if (boundary["seq"] < last) { last = boundary["seq"]; anchor = row["anchor"]; }
      if (typeof row["source_time"] === "string" && (time === undefined || row["source_time"] > time)) time = row["source_time"];
    }
    return { ...(anchor === undefined ? {} : { branchAnchor: anchor }), ...(time === undefined ? {} : { sourceTime: time }) };
  }
  isSibling(origin: MemoryOrigin | undefined): boolean {
    if (!origin || !(origin.lineageSessionIds ?? [origin.sessionId]).some(id => this.lineageSessionIds.includes(id))) return false;
    if (origin.branchAnchor !== undefined) return this.owner.database.prepare("SELECT 1 FROM source_view_entries WHERE view_id=? AND anchor=? LIMIT 1").get(this.id, origin.branchAnchor) === undefined;
    return origin.sourceKeys.some(key => this.position(key) === undefined);
  }
}
