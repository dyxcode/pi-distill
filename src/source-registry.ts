import { randomUUID } from "node:crypto";
import { isAbsolute, dirname, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

export type SourceHeaderState = "unknown" | "known";
export type SourceRegistryCursorName = "headers" | "clean" | "legacy" | "gc";

export interface SourceRegistrySchema {
  readonly name: string;
  readonly sql: string;
  readonly type: "table" | "index";
}

export interface SourceRegistration {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
}

export interface SourceHeaderRecord {
  readonly sessionFile: string;
  readonly sessionId: string;
  readonly fileVersion: string;
  /** The header's parentSession value, resolved relative to the source file. */
  readonly parentSession?: string;
}

export interface SourceUnknownRecord {
  readonly sessionFile: string;
  readonly sessionId: string;
}

export interface ManagedSource {
  readonly rowId: number;
  readonly generation: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
  readonly fileVersion: string | null;
  readonly physicalIdentity: string | null;
  readonly headerState: SourceHeaderState;
  /** A fresh Pi reservation whose session file has not appeared yet. */
  readonly reserved: boolean;
  readonly parentPath: string | null;
  readonly deleted: boolean;
}

export interface SourceRegistryPage {
  readonly items: readonly ManagedSource[];
  /** Null means this page reached the end of the current catalog. */
  readonly nextCursor: number | null;
}

export interface SourceRegistryPageOptions {
  readonly afterRowId?: number;
  readonly throughRowId?: number;
  readonly limit: number;
}

export const SOURCE_REGISTRY_SCHEMAS: readonly SourceRegistrySchema[] = [
  {
    name: "managed_sources",
    type: "table",
    sql: "CREATE TABLE managed_sources (" +
      "row_id INTEGER PRIMARY KEY, " +
      "generation TEXT NOT NULL UNIQUE, " +
      "session_file TEXT NOT NULL, " +
      "session_id TEXT NOT NULL, " +
      "cwd TEXT NOT NULL, " +
      "file_version TEXT, " +
      "physical_identity TEXT, " +
      "header_state TEXT NOT NULL CHECK (header_state IN ('unknown', 'known')), " +
      "reserved INTEGER NOT NULL CHECK (reserved IN (0, 1)), " +
      "parent_path TEXT, " +
      "deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)), " +
      "CHECK (header_state <> 'known' OR reserved = 1 OR " +
      "(file_version IS NOT NULL AND physical_identity IS NOT NULL))" +
      ") STRICT",
  },
  {
    name: "managed_sources_path_session",
    type: "index",
    sql: "CREATE INDEX managed_sources_path_session " +
      "ON managed_sources (session_file, session_id, row_id)",
  },
  {
    name: "managed_sources_session_id",
    type: "index",
    sql: "CREATE INDEX managed_sources_session_id " +
      "ON managed_sources (deleted, session_id, physical_identity)",
  },
  {
    name: "managed_sources_page",
    type: "index",
    sql: "CREATE INDEX managed_sources_page ON managed_sources (deleted, row_id)",
  },
  {
    name: "managed_sources_header",
    type: "index",
    sql: "CREATE INDEX managed_sources_header " +
      "ON managed_sources (deleted, reserved, header_state, row_id)",
  },
  {
    name: "managed_sources_parent",
    type: "index",
    sql: "CREATE INDEX managed_sources_parent ON managed_sources (deleted, parent_path)",
  },
  {
    name: "managed_sources_physical",
    type: "index",
    sql: "CREATE INDEX managed_sources_physical " +
      "ON managed_sources (deleted, physical_identity)",
  },
  {
    name: "source_registry_state",
    type: "table",
    sql: "CREATE TABLE source_registry_state (" +
      "singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), " +
      "topology_version INTEGER NOT NULL CHECK (topology_version >= 0), " +
      "backfill_complete INTEGER NOT NULL CHECK (backfill_complete IN (0, 1))" +
      ") STRICT",
  },
  {
    name: "source_registry_cursors",
    type: "table",
    sql: "CREATE TABLE source_registry_cursors (" +
      "name TEXT PRIMARY KEY NOT NULL CHECK (name IN ('headers', 'clean', 'legacy', 'gc')), " +
      "cursor_row INTEGER CHECK (cursor_row IS NULL OR cursor_row >= 0)" +
      ") STRICT",
  },
] as const;
export const SOURCE_REGISTRY_SCHEMA_SQL: readonly string[] = SOURCE_REGISTRY_SCHEMAS.map(schema => schema.sql);

const validUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const validFileVersion = /^(\d+):(\d+):(\d+):(\d+):(\d+)$/;
const cursorNames: readonly SourceRegistryCursorName[] = ["headers", "clean", "legacy", "gc"];
const maxPathLength = 16_384;
const maxTextLength = Number.MAX_SAFE_INTEGER;
const maxPageSize = 1_024;

function textColumn(column: string, alias = column): string {
  // Node 22 truncates SQLite TEXT results at NUL. Preserve the complete bytes,
  // then decode according to the database encoding at the result boundary.
  return `CASE typeof(${column}) WHEN 'text' THEN CAST(${column} AS BLOB) WHEN 'null' THEN NULL ELSE 0 END AS ${alias}`;
}

const managedSourceColumns = [
  "row_id AS rowId", textColumn("generation"), textColumn("session_id", "sessionId"),
  textColumn("session_file", "sessionFile"), textColumn("cwd"),
  textColumn("file_version", "fileVersion"), textColumn("physical_identity", "physicalIdentity"),
  textColumn("header_state", "headerState"), "reserved", textColumn("parent_path", "parentPath"),
  "deleted",
].join(", ");

/** Create registry tables during a memory-store schema migration or in tests. */
export function initializeSourceRegistry(database: DatabaseSync): void {
  for (const schema of SOURCE_REGISTRY_SCHEMAS) {
    const present = database.prepare(
      "SELECT 1 AS present FROM sqlite_master WHERE type = ? AND name = ?",
    ).get(schema.type, schema.name);
    if (present === undefined) database.exec(schema.sql);
  }
  database.prepare(
    "INSERT OR IGNORE INTO source_registry_state (singleton, topology_version, backfill_complete) " +
    "VALUES (1, 0, 0)",
  ).run();
  const insertCursor = database.prepare(
    "INSERT OR IGNORE INTO source_registry_cursors (name, cursor_row) VALUES (?, NULL)",
  );
  for (const name of cursorNames) insertCursor.run(name);
}

/**
 * Durable registry of extension-managed session file generations and validated
 * parent edges. Source contents remain in Pi files; this class stores metadata only.
 */
export class SourceRegistry {
  readonly #database: DatabaseSync;
  readonly #textDecoder: TextDecoder;

  constructor(database: DatabaseSync) {
    if (!(database instanceof DatabaseSync)) throw new TypeError("Source registry requires a SQLite DatabaseSync.");
    this.#database = database;
    const encoding = database.prepare("PRAGMA encoding").get()?.["encoding"];
    if (encoding !== "UTF-8" && encoding !== "UTF-16le" && encoding !== "UTF-16be") {
      throw new Error("Unsupported source registry database text encoding.");
    }
    this.#textDecoder = new TextDecoder(encoding, { fatal: true, ignoreBOM: true });
    const state = database.prepare(
      "SELECT topology_version AS topologyVersion, backfill_complete AS backfillComplete " +
      "FROM source_registry_state WHERE singleton = 1",
    ).get();
    if (state === undefined) throw new Error("Source registry schema is not initialized.");
    parseState(state);
  }

  /** Register a managed session source, preserving its generation across appends. */
  register(input: SourceRegistration): ManagedSource {
    const record = parseRegistration(input);
    return this.#atomic(() => {
      const existing = this.#find(record.sessionFile, record.sessionId);
      if (existing) {
        if (!existing.deleted && existing.cwd !== record.cwd) {
          this.#database.prepare("UPDATE managed_sources SET cwd = ? WHERE row_id = ?")
            .run(record.cwd, existing.rowId);
          return this.#getByRowId(existing.rowId);
        }
        return existing;
      }
      const generation = randomUUID();
      this.#database.prepare(
        "INSERT INTO managed_sources (generation, session_file, session_id, cwd, file_version, " +
        "physical_identity, header_state, reserved, parent_path, deleted) " +
        "VALUES (?, ?, ?, ?, NULL, NULL, 'unknown', 0, NULL, 0)",
      ).run(generation, record.sessionFile, record.sessionId, record.cwd);
      this.#bumpTopologyVersion();
      return this.#getByGeneration(generation);
    });
  }

  /**
   * Record a validated header. A changed file version on the same device/inode
   * updates coverage without changing topology. Replacements and identity
   * mismatches invalidate the current proof and are never accepted as the old
   * generation.
   */
  recordHeader(input: SourceHeaderRecord): boolean {
    const record = parseHeaderRecord(input);
    return this.#atomic(() => {
      const source = this.#find(record.sessionFile, record.sessionId);
      if (!source || source.deleted) {
        const current = this.#find(record.sessionFile);
        if (current && !current.deleted) this.#markUnknownRow(current);
        return false;
      }
      const identity = physicalIdentity(record.fileVersion);
      if (source.physicalIdentity !== null && source.physicalIdentity !== identity) {
        this.#markUnknownRow(source);
        return false;
      }
      const parentPath = record.parentSession === undefined
        ? null
        : resolve(dirname(record.sessionFile), record.parentSession);
      const sessionConflicts = this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE deleted = 0 AND session_id = ? " +
        "AND physical_identity IS NOT NULL AND physical_identity <> ? AND row_id <> ? " +
        "AND session_file <> ?",
      ).all(record.sessionId, identity, source.rowId, record.sessionFile)
        .map(row => this.#readSource(row));
      const physicalConflicts = this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE deleted = 0 AND physical_identity = ? " +
        "AND session_id <> ? AND row_id <> ? AND session_file <> ?",
      ).all(identity, record.sessionId, source.rowId, record.sessionFile)
        .map(row => this.#readSource(row));
      if (sessionConflicts.length > 0 || physicalConflicts.length > 0) {
        this.#markUnknownRow(source);
        for (const conflict of [...sessionConflicts, ...physicalConflicts]) this.#markUnknownRow(conflict);
        return false;
      }
      const topologyChanged = source.headerState !== "known" || source.reserved ||
        source.physicalIdentity !== identity || source.parentPath !== parentPath;
      this.#database.prepare(
        "UPDATE managed_sources SET file_version = ?, physical_identity = ?, header_state = 'known', " +
        "reserved = 0, parent_path = ? WHERE row_id = ? AND deleted = 0",
      ).run(record.fileVersion, identity, parentPath, source.rowId);
      // A path has one current header generation. Keep prior generations only as
      // tombstones so stale metadata cannot block the active source forever.
      const superseded = this.#database.prepare(
        "UPDATE managed_sources SET deleted = 1 WHERE session_file = ? AND deleted = 0 AND row_id <> ?",
      ).run(record.sessionFile, source.rowId);
      if (topologyChanged || superseded.changes > 0) this.#bumpTopologyVersion();
      return true;
    });
  }

  /** Mark the header proof stale without discarding its last known physical identity or edge. */
  markUnknown(input: SourceUnknownRecord): void {
    const record = parseUnknownRecord(input);
    this.#atomic(() => {
      const source = this.#find(record.sessionFile, record.sessionId);
      if (source && !source.deleted) this.#markUnknownRow(source);
    });
  }

  /** Store a live Pi session reservation and its manager-known parent edge. */
  setReserved(sessionFile: string, sessionId: string, parentSession?: string): void {
    const path = parseSessionFile(sessionFile);
    validateSessionId(sessionId);
    const parentPath = parentSession === undefined
      ? null
      : resolve(dirname(path), parseParentSession(parentSession));
    this.#atomic(() => {
      const source = this.#find(path, sessionId);
      if (!source || source.deleted || (source.headerState === "known" && !source.reserved)) return;
      if (source.reserved && source.parentPath === parentPath) return;
      this.#database.prepare(
        "UPDATE managed_sources SET reserved = 1, file_version = NULL, header_state = 'unknown', " +
        "parent_path = ? WHERE row_id = ? AND deleted = 0",
      ).run(parentPath, source.rowId);
      this.#bumpTopologyVersion();
    });
  }

  /** Return the latest active generation for a path, or the latest matching generation. */
  get(sessionFile: string, sessionId?: string): ManagedSource | undefined {
    const path = parseSessionFile(sessionFile);
    if (sessionId !== undefined) validateSessionId(sessionId);
    const row = sessionId === undefined
      ? this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE session_file = ? ORDER BY deleted ASC, row_id DESC LIMIT 1",
      ).get(path)
      : this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE session_file = ? AND session_id = ? " +
        "ORDER BY deleted ASC, row_id DESC LIMIT 1",
      ).get(path, sessionId);
    return row === undefined ? undefined : this.#readSource(row);
  }

  /** Read a bounded row-id page; deleted generations never re-enter discovery. */
  highWater(): number {
    const value = this.#database.prepare("SELECT COALESCE(MAX(row_id),0) AS value FROM managed_sources").get()?.["value"];
    return parseNonnegativeInteger(value, "source sweep boundary");
  }

  listPage(options: SourceRegistryPageOptions): SourceRegistryPage {
    const page = parsePageOptions(options);
    const rows = this.#database.prepare(
      `SELECT ${managedSourceColumns} ` +
      "FROM managed_sources WHERE deleted = 0 AND row_id > ? AND row_id <= ? ORDER BY row_id LIMIT ?",
    ).all(page.afterRowId, page.throughRowId ?? this.highWater(), page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit).map(row => this.#readSource(row));
    const nextCursor = hasMore ? visible.at(-1)?.rowId ?? null : null;
    return { items: visible, nextCursor };
  }

  /** Return managed child paths that directly depend on this path or physical alias. */
  reverseDependencies(sessionFile: string): readonly string[] {
    const path = parseSessionFile(sessionFile);
    const aliases = this.#database.prepare(
      `SELECT DISTINCT ${textColumn("physical_identity", "physicalIdentity")} FROM managed_sources ` +
      "WHERE deleted = 0 AND session_file = ? AND physical_identity IS NOT NULL",
    ).all(path).map(row => this.#decodeRow(row)).map(row => parseNullableText(row, "physicalIdentity"))
      .filter((value): value is string => value !== null);
    const parentPaths = new Set([path]);
    if (aliases.length > 0) {
      for (const identity of aliases) {
        const aliasRows = this.#database.prepare(
          `SELECT DISTINCT ${textColumn("session_file", "sessionFile")} FROM managed_sources ` +
          "WHERE deleted = 0 AND physical_identity = ?",
        ).all(identity);
        for (const raw of aliasRows) {
          const row = this.#decodeRow(raw);
          parentPaths.add(parseAbsolutePath(row["sessionFile"], "stored session file"));
        }
      }
    }
    const targetIdentities = new Set(aliases);
    const result = new Set<string>();
    for (const parentPath of parentPaths) {
      const candidates = this.#database.prepare(
        `SELECT ${textColumn("session_file", "sessionFile")}, ` +
        `${textColumn("physical_identity", "physicalIdentity")} ` +
        "FROM managed_sources WHERE deleted = 0 AND parent_path = ? ORDER BY row_id",
      ).all(parentPath);
      for (const raw of candidates) {
        const row = this.#decodeRow(raw);
        const physical = row["physicalIdentity"] === null
          ? null
          : parsePhysicalIdentity(row["physicalIdentity"]);
        if (physical !== null && targetIdentities.has(physical)) continue;
        result.add(parseAbsolutePath(row["sessionFile"], "stored child session file"));
      }
    }
    return [...result].sort();
  }

  /**
   * Check whether a current source directly depends on this path or one of its
   * physical aliases. The existence queries stop at the first dependent instead
   * of materializing the full child catalog. Malformed target identities fail
   * closed because their aliases cannot be established safely.
   */
  hasDependents(sessionFile: string): boolean {
    const path = parseSessionFile(sessionFile);
    const malformedIdentity = this.#database.prepare(
      "SELECT 1 AS found FROM managed_sources " +
      "WHERE deleted = 0 AND session_file = ? AND physical_identity IS NOT NULL AND (" +
      "physical_identity GLOB '*[^0-9:]*' OR " +
      "length(physical_identity) - length(replace(physical_identity, ':', '')) <> 1 OR " +
      "substr(physical_identity, 1, 1) NOT GLOB '[0-9]' OR " +
      "substr(physical_identity, -1, 1) NOT GLOB '[0-9]') LIMIT 1",
    ).get(path);
    if (malformedIdentity !== undefined) return true;

    const directChild = this.#database.prepare(
      "SELECT 1 AS found FROM managed_sources AS child " +
      "WHERE child.deleted = 0 AND child.parent_path = ? AND NOT EXISTS (" +
      "SELECT 1 FROM managed_sources AS target WHERE target.deleted = 0 AND " +
      "target.session_file = ? AND target.physical_identity = child.physical_identity) LIMIT 1",
    ).get(path, path);
    if (directChild !== undefined) return true;

    const aliasChild = this.#database.prepare(
      "SELECT 1 AS found FROM managed_sources AS target " +
      "JOIN managed_sources AS alias ON alias.deleted = 0 AND " +
      "alias.physical_identity = target.physical_identity " +
      "JOIN managed_sources AS child ON child.deleted = 0 AND " +
      "child.parent_path = alias.session_file " +
      "WHERE target.deleted = 0 AND target.session_file = ? AND " +
      "target.physical_identity IS NOT NULL AND NOT EXISTS (" +
      "SELECT 1 FROM managed_sources AS same_target WHERE same_target.deleted = 0 AND " +
      "same_target.session_file = ? AND same_target.physical_identity = child.physical_identity) " +
      "LIMIT 1",
    ).get(path, path);
    return aliasChild !== undefined;
  }

  /** True only after migration backfill and validation of all current managed headers. */
  isTopologyComplete(): boolean {
    const state = parseState(this.#database.prepare(
      "SELECT topology_version AS topologyVersion, backfill_complete AS backfillComplete " +
      "FROM source_registry_state WHERE singleton = 1",
    ).get());
    if (!state.backfillComplete) return false;
    const unresolved = this.#database.prepare(
      "SELECT 1 AS found FROM managed_sources WHERE deleted = 0 AND reserved = 0 " +
      "AND header_state <> 'known' LIMIT 1",
    ).get();
    return unresolved === undefined;
  }

  /** Monotonic version of source identity, edge and header-knowledge changes only. */
  topologyVersion(): number {
    const row = this.#database.prepare(
      "SELECT topology_version AS topologyVersion FROM source_registry_state WHERE singleton = 1",
    ).get();
    const version = parseSafeInteger(row?.["topologyVersion"], "stored topology version");
    if (version < 0) throw new TypeError("Stored topology version cannot be negative.");
    return version;
  }

  /** Record that legacy source references have been completely migrated. */
  setBackfillComplete(complete: boolean): void {
    if (typeof complete !== "boolean") throw new TypeError("Backfill completion must be boolean.");
    this.#atomic(() => {
      const row = this.#database.prepare(
        "SELECT backfill_complete AS backfillComplete FROM source_registry_state WHERE singleton = 1",
      ).get();
      if (row === undefined) throw new Error("Source registry state is missing.");
      const current = parseBoolean(row["backfillComplete"], "stored backfill completion");
      if (current === complete) return;
      this.#database.prepare(
        "UPDATE source_registry_state SET backfill_complete = ? WHERE singleton = 1",
      ).run(complete ? 1 : 0);
      this.#bumpTopologyVersion();
    });
  }

  readCursor(name: SourceRegistryCursorName): number | null {
    const cursorName = parseCursorName(name);
    const row = this.#database.prepare(
      "SELECT cursor_row AS cursorRow FROM source_registry_cursors WHERE name = ?",
    ).get(cursorName);
    if (row === undefined) throw new Error("Source registry cursor is missing.");
    const value = row["cursorRow"];
    return value === null ? null : parseNonnegativeInteger(value, "stored source cursor");
  }

  writeCursor(name: SourceRegistryCursorName, cursor: number | null): void {
    const cursorName = parseCursorName(name);
    if (cursor !== null) validateNonnegativeInteger(cursor, "source cursor");
    this.#database.prepare(
      "INSERT INTO source_registry_cursors (name, cursor_row) VALUES (?, ?) " +
      "ON CONFLICT(name) DO UPDATE SET cursor_row = excluded.cursor_row",
    ).run(cursorName, cursor);
  }

  /** Mark only the exact registered generation as deleted; repeated receipts are idempotent. */
  markDeleted(sessionFile: string, sessionId: string): void {
    const path = parseSessionFile(sessionFile);
    validateSessionId(sessionId);
    this.#atomic(() => {
      const source = this.#find(path, sessionId);
      if (!source || source.deleted) return;
      this.#database.prepare("UPDATE managed_sources SET deleted = 1 WHERE row_id = ? AND deleted = 0")
        .run(source.rowId);
      this.#bumpTopologyVersion();
    });
  }

  #find(sessionFile: string, sessionId?: string): ManagedSource | undefined {
    const row = sessionId === undefined
      ? this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE session_file = ? ORDER BY deleted ASC, row_id DESC LIMIT 1",
      ).get(sessionFile)
      : this.#database.prepare(
        `SELECT ${managedSourceColumns} ` +
        "FROM managed_sources WHERE session_file = ? AND session_id = ? " +
        "ORDER BY deleted ASC, row_id DESC LIMIT 1",
      ).get(sessionFile, sessionId);
    return row === undefined ? undefined : this.#readSource(row);
  }

  #getByRowId(rowId: number): ManagedSource {
    const row = this.#database.prepare(
      `SELECT ${managedSourceColumns} ` +
      "FROM managed_sources WHERE row_id = ?",
    ).get(rowId);
    if (row === undefined) throw new Error("Registered source disappeared before it could be read.");
    return this.#readSource(row);
  }

  #getByGeneration(generation: string): ManagedSource {
    const row = this.#database.prepare(
      `SELECT ${managedSourceColumns} ` +
      "FROM managed_sources WHERE generation = ?",
    ).get(generation);
    if (row === undefined) throw new Error("Registered source generation was not persisted.");
    return this.#readSource(row);
  }

  #readSource(value: Record<string, unknown>): ManagedSource {
    return parseSource(this.#decodeRow(value));
  }

  #decodeRow(row: Record<string, unknown>): Record<string, unknown> {
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Uint8Array) row[key] = this.#textDecoder.decode(value);
    }
    return row;
  }

  #markUnknownRow(source: ManagedSource): void {
    if (source.headerState === "unknown" && source.fileVersion === null && !source.reserved) return;
    this.#database.prepare(
      "UPDATE managed_sources SET file_version = NULL, header_state = 'unknown', reserved = 0 " +
      "WHERE row_id = ?",
    ).run(source.rowId);
    this.#bumpTopologyVersion();
  }

  #bumpTopologyVersion(): void {
    const result = this.#database.prepare(
      "UPDATE source_registry_state SET topology_version = topology_version + 1 WHERE singleton = 1 " +
      "AND topology_version < 9007199254740991",
    ).run();
    if (result.changes !== 1) throw new RangeError("Source topology version is missing or exhausted.");
  }

  #atomic<T>(operation: () => T): T {
    this.#database.exec("SAVEPOINT source_registry_operation");
    try {
      const result = operation();
      this.#database.exec("RELEASE source_registry_operation");
      return result;
    } catch (error) {
      try {
        this.#database.exec("ROLLBACK TO source_registry_operation");
        this.#database.exec("RELEASE source_registry_operation");
      } catch {
        // Preserve the operation's validation or persistence error.
      }
      throw error;
    }
  }
}

function parseRegistration(value: unknown): SourceRegistration {
  if (!isRecord(value)) throw new TypeError("Source registration must be an object.");
  assertExactKeys(value, ["sessionId", "sessionFile", "cwd"]);
  const sessionId = validateSessionId(value["sessionId"]);
  const sessionFile = parseSessionFile(value["sessionFile"]);
  const cwd = parseText(value["cwd"], "source working directory", maxTextLength);
  if (cwd.trim().length === 0) throw new TypeError("Source working directory must not be empty.");
  return { sessionId, sessionFile, cwd };
}

function parseHeaderRecord(value: unknown): SourceHeaderRecord {
  if (!isRecord(value)) throw new TypeError("Source header record must be an object.");
  assertExactKeys(value, ["sessionFile", "sessionId", "fileVersion", "parentSession"]);
  const sessionFile = parseSessionFile(value["sessionFile"]);
  const sessionId = validateSessionId(value["sessionId"]);
  const fileVersion = parseFileVersion(value["fileVersion"]);
  const rawParent = value["parentSession"];
  if (rawParent === undefined) return { sessionFile, sessionId, fileVersion };
  const parentSession = parseParentSession(rawParent);
  return { sessionFile, sessionId, fileVersion, parentSession };
}

function parseParentSession(value: unknown): string {
  const parentSession = parsePathText(value, "parent session path", maxPathLength);
  if (parentSession.length === 0) throw new TypeError("Parent session path must not be empty.");
  return parentSession;
}

function parseUnknownRecord(value: unknown): SourceUnknownRecord {
  if (!isRecord(value)) throw new TypeError("Unknown source record must be an object.");
  assertExactKeys(value, ["sessionFile", "sessionId"]);
  return {
    sessionFile: parseSessionFile(value["sessionFile"]),
    sessionId: validateSessionId(value["sessionId"]),
  };
}

function parsePageOptions(value: unknown): { afterRowId: number; limit: number; throughRowId?: number } {
  if (!isRecord(value)) throw new TypeError("Source page options must be an object.");
  assertExactKeys(value, ["afterRowId", "limit", "throughRowId"]);
  const limit = value["limit"];
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > maxPageSize) {
    throw new TypeError(`Source page limit must be an integer from 1 through ${maxPageSize}.`);
  }
  const afterRowId = Object.hasOwn(value, "afterRowId") ? value["afterRowId"] : 0;
  validateNonnegativeInteger(afterRowId, "source row cursor");
  if (Object.hasOwn(value, "throughRowId")) {
    const throughRowId = value["throughRowId"]; validateNonnegativeInteger(throughRowId, "source sweep boundary");
    return { afterRowId, limit, throughRowId };
  }
  return { afterRowId, limit };
}

function parseSource(value: Record<string, unknown>): ManagedSource {
  const rowId = parsePositiveInteger(value["rowId"], "stored source row ID");
  const generation = parseText(value["generation"], "stored source generation", 64);
  if (!validUuid.test(generation)) throw new TypeError("Stored source generation is invalid.");
  const sessionId = validateSessionId(value["sessionId"]);
  const sessionFile = parseAbsolutePath(value["sessionFile"], "stored session file");
  if (resolve(sessionFile) !== sessionFile) throw new TypeError("Stored session file is not normalized.");
  const cwd = parseText(value["cwd"], "stored source working directory", maxTextLength);
  if (cwd.trim().length === 0) throw new TypeError("Stored source working directory must not be empty.");
  const fileVersion = value["fileVersion"] === null ? null : parseFileVersion(value["fileVersion"]);
  const physical = value["physicalIdentity"] === null
    ? null
    : parsePhysicalIdentity(value["physicalIdentity"]);
  const headerState = value["headerState"];
  if (headerState !== "unknown" && headerState !== "known") throw new TypeError("Stored source header state is invalid.");
  const reserved = parseBoolean(value["reserved"], "stored source reservation flag");
  const parentPath = value["parentPath"] === null ? null : parseAbsolutePath(value["parentPath"], "stored parent path");
  const deleted = parseBoolean(value["deleted"], "stored source deletion flag");
  if (headerState === "known" && !reserved && (fileVersion === null || physical === null)) {
    throw new TypeError("Known source header is missing its file identity/version.");
  }
  if (fileVersion !== null && physicalIdentity(fileVersion) !== physical) {
    throw new TypeError("Stored source physical identity does not match its file version.");
  }
  return { rowId, generation, sessionId, sessionFile, cwd, fileVersion,
    physicalIdentity: physical, headerState, reserved, parentPath, deleted };
}

function parseState(value: Record<string, unknown> | undefined): { topologyVersion: number; backfillComplete: boolean } {
  if (value === undefined) throw new Error("Source registry state is missing.");
  const topologyVersion = parseNonnegativeInteger(value["topologyVersion"], "stored topology version");
  const backfillComplete = parseBoolean(value["backfillComplete"], "stored backfill completion");
  return { topologyVersion, backfillComplete };
}

function parseCursorName(value: unknown): SourceRegistryCursorName {
  if (typeof value !== "string" || !isCursorName(value)) {
    throw new TypeError("Source registry cursor name is invalid.");
  }
  return value;
}

function isCursorName(value: string): value is SourceRegistryCursorName {
  return cursorNames.some(name => name === value);
}

function parseSessionFile(value: unknown): string {
  const path = parsePathText(value, "session file", maxPathLength);
  if (!isAbsolute(path)) throw new TypeError("Session file must be an absolute path.");
  return resolve(path);
}

function parseAbsolutePath(value: unknown, field: string): string {
  const path = parsePathText(value, field, maxPathLength);
  if (!isAbsolute(path)) throw new TypeError(`${field} must be an absolute path.`);
  if (resolve(path) !== path) throw new TypeError(`${field} must be normalized.`);
  return path;
}

function validateSessionId(value: unknown): string {
  const sessionId = parseText(value, "session ID", maxTextLength);
  if (sessionId.trim().length === 0) throw new TypeError("Session ID must not be empty.");
  return sessionId;
}

function parseFileVersion(value: unknown): string {
  const fileVersion = parseText(value, "source file version", 256);
  const match = validFileVersion.exec(fileVersion);
  if (!match) throw new TypeError("Source file version must contain device, inode, size, mtime, and ctime integers.");
  return fileVersion;
}

function physicalIdentity(fileVersion: string): string {
  const match = validFileVersion.exec(fileVersion);
  if (!match) throw new TypeError("Source file version is invalid.");
  return `${match[1]}:${match[2]}`;
}

function parsePhysicalIdentity(value: unknown): string {
  const identity = parseText(value, "stored physical identity", 128);
  if (!/^\d+:\d+$/.test(identity)) throw new TypeError("Stored physical identity is invalid.");
  return identity;
}

function parseNullableText(row: Record<string, unknown>, key: string): string | null {
  const value = row[key];
  if (value === null) return null;
  return parseText(value, `stored ${key}`, maxPathLength);
}

function parseText(value: unknown, field: string, maxLength: number): string {
  if (typeof value !== "string" || value.length > maxLength || /[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`${field} is invalid.`);
  }
  return value;
}

function parsePathText(value: unknown, field: string, maxLength: number): string {
  const path = parseText(value, field, maxLength);
  if (path.includes("\0")) throw new TypeError(`${field} cannot contain NUL.`);
  return path;
}

function assertExactKeys(value: Record<string, unknown>, allowed: readonly string[]): void {
  const allowedKeys = new Set(allowed);
  for (const key of Object.keys(value)) {
    if (!allowedKeys.has(key)) throw new TypeError(`Unexpected source registry field: ${key}.`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseBoolean(value: unknown, field: string): boolean {
  if (value === 0 || value === 0n) return false;
  if (value === 1 || value === 1n) return true;
  throw new TypeError(`${field} must be boolean.`);
}

function parsePositiveInteger(value: unknown, field: string): number {
  const integer = parseSafeInteger(value, field);
  if (integer < 1) throw new TypeError(`${field} must be positive.`);
  return integer;
}

function parseNonnegativeInteger(value: unknown, field: string): number {
  const integer = parseSafeInteger(value, field);
  if (integer < 0) throw new TypeError(`${field} cannot be negative.`);
  return integer;
}

function validateNonnegativeInteger(value: unknown, field: string): asserts value is number {
  parseNonnegativeInteger(value, field);
}

function parseSafeInteger(value: unknown, field: string): number {
  if (typeof value === "number" && Number.isSafeInteger(value)) return value;
  if (typeof value === "bigint" && value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER)) {
    return Number(value);
  }
  throw new TypeError(`${field} must be a safe integer.`);
}
