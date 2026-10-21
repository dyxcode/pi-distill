import { isAbsolute } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";

const initialColumns = `
  id TEXT PRIMARY KEY NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL`;
const legacySchema = `CREATE TABLE memories (${initialColumns}) STRICT`;
const currentSchema = `CREATE TABLE memories (${initialColumns},
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL
) STRICT`;

export interface MemoryInput {
  readonly id: string;
  readonly content: string;
  readonly createdAt: string;
}

export interface Memory extends MemoryInput {
  readonly revision: number;
  readonly updatedAt: string;
}

export interface MemoryUpdate {
  readonly id: string;
  readonly expectedRevision: number;
  readonly content: string;
  readonly updatedAt: string;
}

export interface MemoryForget {
  readonly id: string;
  readonly expectedRevision: number;
}

export class MemoryConflictError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`A different memory already exists for ID "${memoryId}".`);
    this.name = "MemoryConflictError";
    this.memoryId = memoryId;
  }
}

export class MemoryNotFoundError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`No memory exists for ID "${memoryId}".`);
    this.name = "MemoryNotFoundError";
    this.memoryId = memoryId;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fields(value: unknown, names: readonly string[]): Record<string, unknown> {
  if (
    !isRecord(value) ||
    Reflect.ownKeys(value).length !== names.length ||
    names.some((name) => !Object.hasOwn(value, name))
  ) {
    throw new TypeError(`Memory data requires exactly these fields: ${names.join(", ")}.`);
  }
  return value;
}

function validateText(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`Memory ${field} must be a nonempty string.`);
  }
  // Unicode mode matches lone surrogates, while valid pairs remain single code points.
  if (/[\uD800-\uDFFF]/u.test(value)) {
    throw new TypeError(`Memory ${field} must contain well-formed Unicode.`);
  }
}

function validateTimestamp(value: unknown, field: string): asserts value is string {
  if (
    typeof value !== "string" ||
    !Number.isFinite(Date.parse(value)) ||
    new Date(value).toISOString() !== value
  ) {
    throw new TypeError(`Memory ${field} must be a canonical UTC ISO timestamp.`);
  }
}

function validateRevision(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new TypeError("Memory revision must be a positive safe integer.");
  }
}

function parseInput(value: unknown): MemoryInput {
  const { id, content, createdAt } = fields(value, ["id", "content", "createdAt"]);
  validateText(id, "ID");
  validateText(content, "content");
  validateTimestamp(createdAt, "createdAt");
  return { id, content, createdAt };
}

function parseMemory(value: unknown): Memory {
  const { id, content, createdAt, revision, updatedAt } = fields(
    value, ["id", "content", "createdAt", "revision", "updatedAt"],
  );
  const input = parseInput({ id, content, createdAt });
  validateRevision(revision);
  validateTimestamp(updatedAt, "updatedAt");
  return { ...input, revision, updatedAt };
}

function parseUpdate(value: unknown): MemoryUpdate {
  const { id, expectedRevision, content, updatedAt } = fields(
    value, ["id", "expectedRevision", "content", "updatedAt"],
  );
  validateText(id, "ID");
  validateRevision(expectedRevision);
  validateText(content, "content");
  validateTimestamp(updatedAt, "updatedAt");
  return { id, expectedRevision, content, updatedAt };
}

function parseForget(value: unknown): MemoryForget {
  const { id, expectedRevision } = fields(value, ["id", "expectedRevision"]);
  validateText(id, "ID");
  validateRevision(expectedRevision);
  return { id, expectedRevision };
}

export class SQLiteMemoryStore {
  readonly #database: DatabaseSync;
  readonly #select: StatementSync;
  readonly #insert: StatementSync;
  readonly #update: StatementSync;
  readonly #delete: StatementSync;

  constructor(path: string) {
    if (typeof path !== "string" || !isAbsolute(path)) {
      throw new TypeError("Memory database requires an absolute file path.");
    }

    // Keep lock contention bounded; callers can retry a failed operation.
    this.#database = new DatabaseSync(path, { timeout: 1_000 });
    try {
      this.#database.exec("PRAGMA synchronous = FULL");
      this.#transaction(() => this.#initializeSchema());
      this.#select = this.#database.prepare(
        `SELECT id, content, created_at AS createdAt, revision, updated_at AS updatedAt
         FROM memories WHERE id = ?`,
      );
      this.#insert = this.#database.prepare(
        `INSERT INTO memories (id, content, created_at, revision, updated_at)
         VALUES (?, ?, ?, 1, ?)`,
      );
      this.#update = this.#database.prepare(
        "UPDATE memories SET content = ?, revision = ?, updated_at = ? WHERE id = ?",
      );
      this.#delete = this.#database.prepare("DELETE FROM memories WHERE id = ?");
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  save(memory: MemoryInput): void {
    this.#ensureOpen();
    const record = parseInput(memory);
    this.#transaction(() => {
      const existing = this.get(record.id);
      if (existing === undefined) {
        this.#insert.run(record.id, record.content, record.createdAt, record.createdAt);
      } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
        throw new MemoryConflictError(record.id);
      }
    });
  }

  update(update: MemoryUpdate): Memory {
    this.#ensureOpen();
    const request = parseUpdate(update);
    return this.#transaction(() => {
      const current = this.get(request.id);
      if (current === undefined) throw new MemoryNotFoundError(request.id);

      if (current.revision !== request.expectedRevision) {
        // Confirm an applied effect only while its resulting state is still current.
        if (
          current.revision === request.expectedRevision + 1 &&
          current.content === request.content &&
          current.updatedAt === request.updatedAt
        ) return current;
        throw new MemoryConflictError(request.id);
      }
      if (current.content === request.content) return current;
      if (current.revision === Number.MAX_SAFE_INTEGER) {
        throw new RangeError("Memory revision limit reached.");
      }

      const result = {
        ...current,
        content: request.content,
        revision: current.revision + 1,
        updatedAt: request.updatedAt,
      };
      this.#update.run(result.content, result.revision, result.updatedAt, result.id);
      return result;
    });
  }

  forget(forget: MemoryForget): void {
    this.#ensureOpen();
    const request = parseForget(forget);
    this.#transaction(() => {
      const current = this.get(request.id);
      if (current === undefined) return;
      if (current.revision !== request.expectedRevision) {
        throw new MemoryConflictError(request.id);
      }
      this.#delete.run(request.id);
    });
  }

  get(id: string): Memory | undefined {
    this.#ensureOpen();
    validateText(id, "ID");
    const row = this.#select.get(id);
    return row === undefined ? undefined : parseMemory(row);
  }

  close(): void {
    if (this.#database.isOpen) this.#database.close();
  }

  #initializeSchema(): void {
    const version = this.#database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (version !== 0 && version !== 1) {
      throw new Error(`Unsupported memory database schema version: ${String(version)}.`);
    }
    const objects = this.#database.prepare(
      "SELECT name, type, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
    ).all();
    if (version === 0 && objects.length === 0) {
      this.#database.exec(currentSchema);
      this.#database.exec("PRAGMA user_version = 1");
      return;
    }
    if (objects.length !== 1 || objects[0]?.["name"] !== "memories" || objects[0]?.["type"] !== "table") {
      throw new Error("Unsupported memory database schema.");
    }
    this.#validateSchema(version, objects[0]?.["sql"]);
    if (version === 0) {
      // Validate all legacy records before changing their schema or metadata.
      for (const row of this.#database.prepare(
        "SELECT id, content, created_at AS createdAt FROM memories",
      ).iterate()) parseInput(row);
      // Require all fields on future inserts, so an older writer fails safely after upgrade.
      this.#database.exec("ALTER TABLE memories RENAME TO memories_legacy");
      this.#database.exec(currentSchema);
      this.#database.exec(`
        INSERT INTO memories (id, content, created_at, revision, updated_at)
          SELECT id, content, created_at, 1, created_at FROM memories_legacy;
        DROP TABLE memories_legacy;
        PRAGMA user_version = 1;
      `);
    }
  }

  #validateSchema(version: number, sql: unknown): void {
    // Accept only known declarations; migration must not discard unrecognized constraints.
    const expected = version === 0 ? legacySchema : currentSchema;
    if (
      typeof sql !== "string" ||
      sql.replace(/\s+/g, "").toUpperCase() !== expected.replace(/\s+/g, "").toUpperCase()
    ) throw new Error("Unsupported memory database schema.");
    const table = this.#database.prepare(
      "SELECT strict, wr FROM pragma_table_list WHERE schema = 'main' AND name = 'memories'",
    ).get();
    const columns = this.#database.prepare("PRAGMA table_xinfo(memories)").all();
    const names = ["id", "content", "created_at"];
    if (version === 1) names.push("revision", "updated_at");
    if (
      table?.["strict"] !== 1 || table["wr"] !== 0 || columns.length !== names.length ||
      columns.some((column, index) => (
        column["name"] !== names[index] ||
        column["type"] !== (names[index] === "revision" ? "INTEGER" : "TEXT") ||
        column["notnull"] !== 1 || column["pk"] !== (index === 0 ? 1 : 0) || column["hidden"] !== 0
      ))
    ) throw new Error("Unsupported memory database schema.");
  }

  #transaction<T>(operation: () => T): T {
    // Reserve the writer before reading so competing changes cannot race our checks.
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.#database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.#database.isTransaction) {
        try {
          this.#database.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], "Memory transaction and rollback failed.");
        }
      }
      throw error;
    }
  }

  #ensureOpen(): void {
    if (!this.#database.isOpen) throw new Error("Memory store is closed.");
  }
}
