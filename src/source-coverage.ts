import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import type { SQLiteMemoryStore } from "./memory-store.ts";
import { PagedSourceView, SourceWorkPending } from "./source-view.ts";
import { DistillFailure } from "./distill-failure.ts";

const fenceTargets = [
  { table: "session_observations", path: "session_file", fields: ["session_id", "session_file", "cwd", "leaf_id", "owner_pid", "captured"] },
  { table: "session_usages", path: "session_file", fields: ["owner_id", "process_token", "session_id", "session_file", "cwd", "leaf_id", "owner_pid", "state"] },
  { table: "consolidation_jobs", path: "json_extract($row.job_json, '$.sessionFile')", fields: ["job_json", "status"] },
  { table: "distill_triggers", path: "json_extract($row.trigger_json, '$.sessionFile')", fields: ["trigger_json", "status", "compaction_record_id"] },
  { table: "clean_tasks", path: "json_extract($row.task_json, '$.sessionFile')", fields: ["task_json"] },
];
export const SOURCE_COVERAGE_SCHEMAS = [
  { name: "source_coverage", sql: "CREATE TABLE source_coverage (path TEXT PRIMARY KEY NOT NULL, progress TEXT NOT NULL) STRICT" },
  { name: "source_fences", sql: "CREATE TABLE source_fences (path TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL CHECK (revision > 0)) STRICT" },
  { name: "source_derived_gc", sql: "CREATE TABLE source_derived_gc (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton=1), phase INTEGER NOT NULL, cursor INTEGER NOT NULL, dirty INTEGER NOT NULL CHECK (dirty IN (0,1))) STRICT" },
  { name: "source_view_origins", sql: "CREATE INDEX source_view_origins ON source_view_entries (view_id, origin_key)" },
  { name: "source_index_parents", sql: "CREATE INDEX source_index_parents ON source_index_entries (generation, json_quote(json_extract(metadata, '$.parentId')))" },
  ...fenceTargets.flatMap(target => ["INSERT", "UPDATE", "DELETE"].map(event => {
    const name = `source_fence_${target.table}_${event.toLowerCase()}`;
    const rows = event === "INSERT" ? ["NEW"] : event === "DELETE" ? ["OLD"] : ["OLD", "NEW"];
    const body = rows.map(row => {
      const path = target.path.includes("$row") ? target.path.replace("$row", row) : `${row}.${target.path}`;
      return `INSERT INTO source_fences(path,revision) SELECT ${path},1 WHERE ${path} IS NOT NULL ON CONFLICT(path) DO UPDATE SET revision=revision+1;`;
    }).join(" ");
    const when = event === "UPDATE" ? ` WHEN ${target.fields.map(field => `OLD.${field} IS NOT NEW.${field}`).join(" OR ")}` : "";
    return { name, sql: `CREATE TRIGGER ${name} AFTER ${event} ON ${target.table}${when} BEGIN ${body} END` };
  })),
];
interface CoverageState {
  token: string; rootView: string; phase: "leaves" | "observations" | "usages" | "done";
  cursor: number; active: boolean; leaf: string | null; partCursor: number;
}
export interface CoverageResult {
  readonly status: "building" | "pending" | "processed";
  readonly sessionId: string;
  readonly fileVersions: ReadonlyMap<string, string>;
}
function invalid(message: string): never { throw new DistillFailure("source_invalid", message); }
function integer(value: unknown): value is number { return typeof value === "number" && Number.isSafeInteger(value) && value >= 0; }
function decode(value: unknown): unknown {
  if (typeof value !== "string") return invalid("Invalid coverage metadata.");
  try { return JSON.parse(value); } catch { return invalid("Malformed coverage metadata."); }
}
function state(value: unknown): CoverageState {
  if (typeof value !== "object" || value === null || !("token" in value) || typeof value.token !== "string" ||
    !("rootView" in value) || typeof value.rootView !== "string" || !("phase" in value) ||
    (value.phase !== "leaves" && value.phase !== "observations" && value.phase !== "usages" && value.phase !== "done") ||
    !("cursor" in value) || !integer(value.cursor) || !("active" in value) || typeof value.active !== "boolean" ||
    !("leaf" in value) || (value.leaf !== null && typeof value.leaf !== "string") ||
    !("partCursor" in value) || !integer(value.partCursor)) return invalid("Invalid coverage progress.");
  return { token: value.token, rootView: value.rootView, phase: value.phase, cursor: value.cursor, active: value.active, leaf: value.leaf, partCursor: value.partCursor };
}

/** Resumable checks over every terminal branch and recorded source boundary. */
export class SourceCoverage {
  readonly database: DatabaseSync;
  readonly store: SQLiteMemoryStore;
  constructor(database: DatabaseSync, store: SQLiteMemoryStore) { this.database = database; this.store = store; }
  current(path: string): CoverageState | undefined {
    const row = this.database.prepare("SELECT progress FROM source_coverage WHERE path=?").get(path);
    return row ? state(decode(row["progress"])) : undefined;
  }
  async advance(path: string, scope: readonly string[], version: string): Promise<CoverageResult> {
    const views = this.store.sourceViews;
    let root: PagedSourceView | undefined;
    for (let page = 0; page < 4; page++) {
      const result = await views.advance(path, null);
      if (result.ready) { root = result.view; break; }
    }
    if (!root) throw new SourceWorkPending();
    const source = root.source;
    if (!source.header) return invalid("Coverage has no validated source identity.");
    const sessionId = source.header.id;
    const fileVersions = new Map(root.state.sources.map(item => [item.path, item.version]));
    const token = createHash("sha256").update(JSON.stringify([root.id, version])).digest("hex");
    const previous = this.current(path);
    let next: CoverageState = previous?.token === token ? { ...previous } :
      { token, rootView: root.id, phase: "leaves", cursor: 0, active: false, leaf: null, partCursor: 0 };
    let status: CoverageResult["status"] = "building";
    for (let work = 0; work < 16 && next.phase !== "done"; work++) {
      if (next.active) {
        const result = await views.advance(path, next.leaf);
        if (!result.ready) continue;
        const page = await result.view.page(next.partCursor);
        const keys = page.items.map(item => item.part.key);
        const processed = new Set(this.store.processedSources(keys));
        if (keys.some(key => !processed.has(key))) { status = "pending"; break; }
        if (page.nextCursor !== undefined) next.partCursor = page.nextCursor;
        else { next.active = false; next.partCursor = 0; }
        continue;
      }
      const phase = next.phase;
      let row: Record<string, unknown> | undefined;
      if (phase === "leaves") {
        // Remove column affinity on the correlated key so SQLite can seek both
        // columns of the JSON-expression index instead of scanning the generation.
        const page = this.database.prepare("SELECT seq AS cursor, entry_key AS leaf, EXISTS (SELECT 1 FROM source_index_entries child WHERE child.generation=entries.generation AND json_quote(json_extract(child.metadata,'$.parentId'))=+entries.entry_key) AS hasChild FROM source_index_entries entries WHERE generation=? AND seq>? ORDER BY seq LIMIT 128").all(source.generation, next.cursor);
        for (const candidate of page) {
          if (candidate["hasChild"] !== 0 && candidate["hasChild"] !== 1) return invalid("Invalid terminal-branch index.");
          if (candidate["hasChild"] === 0) { row = candidate; break; }
        }
        if (!row && page.length > 0) {
          const last = page.at(-1)?.["cursor"];
          if (!integer(last) || last <= next.cursor) return invalid("Invalid terminal-branch cursor.");
          next.cursor = last;
          continue;
        }
      } else {
        row = this.database.prepare(`SELECT rowid AS cursor, json_quote(leaf_id) AS leaf, json_quote(session_id) AS session FROM ${phase === "observations" ? "session_observations" : "session_usages"} WHERE rowid>? AND (session_file=? OR session_id=?) ORDER BY rowid LIMIT 1`).get(next.cursor, path, sessionId);
      }
      if (!row) { next.phase = phase === "leaves" ? "observations" : phase === "observations" ? "usages" : "done"; next.cursor = 0; continue; }
      const leaf = decode(row["leaf"]);
      if (!integer(row["cursor"]) || row["cursor"] <= next.cursor || (leaf !== null && typeof leaf !== "string") ||
        (phase !== "leaves" && decode(row["session"]) !== sessionId)) return invalid("Recorded source identity or boundary is invalid.");
      next.cursor = row["cursor"]; next.leaf = leaf; next.active = true;
    }
    await root.unchanged();
    if (this.store.retentionVersion(scope) !== version) throw new SourceWorkPending();
    this.database.exec("SAVEPOINT source_coverage_page");
    try {
      if (JSON.stringify(this.current(path)) !== JSON.stringify(previous)) throw new SourceWorkPending();
      this.database.prepare("INSERT INTO source_coverage VALUES (?,?) ON CONFLICT(path) DO UPDATE SET progress=excluded.progress").run(path, JSON.stringify(next));
      this.database.exec("RELEASE source_coverage_page");
    } catch (error) { this.database.exec("ROLLBACK TO source_coverage_page; RELEASE source_coverage_page"); throw error; }
    if (next.phase === "done") status = "processed";
    return { status, sessionId, fileVersions };
  }
}

/** Collect each derived row separately; never delete a whole long index in one transaction. */
const derivedTables = ["source_preparation", "source_coverage", "source_views", "source_view_parts", "source_view_edits", "source_view_entries", "source_index_files", "source_index_entries", "source_fences"];
export function derivedSourceBoundaries(database: DatabaseSync): number[] {
  return derivedTables.map(table => {
    const value = database.prepare(`SELECT COALESCE(MAX(rowid),0) AS value FROM ${table}`).get()?.["value"];
    if (!integer(value)) return invalid("Invalid derived source boundary.");
    return value;
  });
}
export function collectDerivedSources(database: DatabaseSync, store: SQLiteMemoryStore, limit: number, through: readonly number[]): { removed: number; hasMore: boolean } {
  const tables = derivedTables;
  const gc = database.prepare("SELECT phase,cursor,dirty FROM source_derived_gc WHERE singleton=1").get();
  if (!gc || !integer(gc["phase"]) || gc["phase"] >= tables.length || !integer(gc["cursor"]) || (gc["dirty"] !== 0 && gc["dirty"] !== 1)) return invalid("Invalid derived metadata collection cursor.");
  let phase = gc["phase"], cursor = gc["cursor"], examined = 0, removed = 0;
  const exists = (sql: string, ...values: string[]) => database.prepare(sql).get(...values) !== undefined;
  const deleted = (path: string): boolean => {
    const receipt = store.getDeletion(path), source = store.sources.get(path);
    if (receipt?.state !== "complete" || (source && (!source.deleted || source.generation !== receipt.sourceGeneration)) || store.sources.hasDependents(path)) return false;
    return !store.sourceRetentionState(path, receipt.sessionId).pending &&
      !exists("SELECT 1 FROM session_usages WHERE session_file=? AND state<>'released' LIMIT 1", path) &&
      !exists("SELECT 1 FROM session_observations WHERE session_file=? AND captured=0 LIMIT 1", path);
  };
  const obsoleteView = (id: string): boolean => exists("SELECT 1 FROM source_views views, json_each(views.progress,'$.sources') source WHERE views.id=? AND NOT EXISTS (SELECT 1 FROM source_index_files files WHERE files.generation=json_extract(source.value,'$.generation')) LIMIT 1", id);
  database.exec("SAVEPOINT derived_source_gc");
  try {
    while (phase < tables.length && examined < limit) {
      const table = tables[phase];
      const rows = database.prepare(`SELECT rowid AS gcRow,*${table === "source_preparation" ? ",json_quote(trigger_id) AS trigger_json" : ""} FROM ${table} WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?`).all(cursor, through[phase] ?? 0, limit - examined);
      if (rows.length === 0) { phase++; cursor = 0; continue; }
      for (const row of rows) {
        if (!integer(row["gcRow"]) || row["gcRow"] <= cursor) return invalid("Invalid derived metadata row cursor.");
        cursor = row["gcRow"]; examined++;
        const field = (name: string): string => { const value = row[name]; if (typeof value !== "string") return invalid("Invalid derived metadata reference."); return value; };
        let eligible = false;
        if (table === "source_preparation") {
          const trigger = decode(row["trigger_json"]);
          if (typeof trigger !== "string") return invalid("Invalid preparation task identity.");
          eligible = !exists("SELECT 1 FROM distill_triggers WHERE id=? AND status<>'complete'", trigger);
        }
        else if (table === "source_coverage") {
          const coverage = state(decode(row["progress"]));
          eligible = deleted(field("path")) || !exists("SELECT 1 FROM source_views WHERE id=?", coverage.rootView) || obsoleteView(coverage.rootView);
        } else if (table === "source_views") {
          const id = field("id");
          // An unfinished view can be collected too: inspect its validated structural state.
          const raw = store.sourceViews.state(id), root = raw.sources[0];
          eligible = !!root && (deleted(root.path) || obsoleteView(id)) &&
            !exists("SELECT 1 FROM source_preparation WHERE view_id=? LIMIT 1", id) &&
            !exists("SELECT 1 FROM source_coverage WHERE json_extract(progress,'$.rootView')=? LIMIT 1", id);
        } else if (table === "source_view_parts" || table === "source_view_edits" || table === "source_view_entries") {
          eligible = !exists("SELECT 1 FROM source_views WHERE id=?", field("view_id"));
        } else if (table === "source_index_files") {
          const path = field("path"), index = store.sourceIndex.current(path);
          eligible = deleted(path) && index?.version === store.getDeletion(path)?.fileVersion;
        } else if (table === "source_index_entries") {
          const generation = field("generation");
          eligible = !exists("SELECT 1 FROM source_index_files WHERE generation=?", generation) &&
            !exists("SELECT 1 FROM source_views views, json_each(views.progress,'$.sources') source WHERE json_extract(source.value,'$.generation')=? LIMIT 1", generation);
        } else if (table === "source_fences") {
          const path = field("path");
          eligible = deleted(path) && !exists("SELECT 1 FROM source_index_files WHERE path=?", path) &&
            !exists("SELECT 1 FROM source_coverage WHERE path=?", path) &&
            !exists("SELECT 1 FROM managed_sources WHERE session_file=?", path);
        }
        if (eligible) removed += Number(database.prepare(`DELETE FROM ${table} WHERE rowid=?`).run(cursor).changes);
      }
    }
    let dirty = gc["dirty"] === 1 || removed > 0;
    const hasMore = phase < tables.length || dirty;
    if (phase >= tables.length) { phase = 0; cursor = 0; dirty = false; }
    database.prepare("UPDATE source_derived_gc SET phase=?,cursor=?,dirty=? WHERE singleton=1").run(phase, cursor, dirty ? 1 : 0);
    database.exec("RELEASE derived_source_gc");
    return { removed, hasMore };
  } catch (error) { database.exec("ROLLBACK TO derived_source_gc; RELEASE derived_source_gc"); throw error; }
}
