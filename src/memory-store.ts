import { isAbsolute } from "node:path";
import { DatabaseSync, type StatementSync } from "node:sqlite";

export interface Memory {
  readonly id: string;
  readonly content: string;
  readonly createdAt: string;
}

export class MemoryConflictError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`A different memory already exists for ID "${memoryId}".`);
    this.name = "MemoryConflictError";
    this.memoryId = memoryId;
  }
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

function parseMemory(value: unknown): Memory {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Memory must be an object.");
  }
  if (!("id" in value) || !("content" in value) || !("createdAt" in value)) {
    throw new TypeError("Memory requires id, content, and createdAt.");
  }
  if (Object.keys(value).some((key) => !["id", "content", "createdAt"].includes(key))) {
    throw new TypeError("Memory contains unsupported fields.");
  }

  const { id, content, createdAt } = value;
  validateText(id, "ID");
  validateText(content, "content");
  if (
    typeof createdAt !== "string" ||
    !Number.isFinite(Date.parse(createdAt)) ||
    new Date(createdAt).toISOString() !== createdAt
  ) {
    throw new TypeError("Memory createdAt must be a canonical UTC ISO timestamp.");
  }

  return { id, content, createdAt };
}

export class SQLiteMemoryStore {
  readonly #database: DatabaseSync;
  readonly #select: StatementSync;
  readonly #insert: StatementSync;

  constructor(path: string) {
    if (typeof path !== "string" || !isAbsolute(path)) {
      throw new TypeError("Memory database requires an absolute file path.");
    }

    // Keep lock contention bounded; callers can retry a failed operation.
    this.#database = new DatabaseSync(path, { timeout: 1_000 });
    try {
      this.#database.exec(`
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS memories (
          id TEXT PRIMARY KEY NOT NULL,
          content TEXT NOT NULL,
          created_at TEXT NOT NULL
        ) STRICT;
      `);
      this.#select = this.#database.prepare(
        "SELECT id, content, created_at AS createdAt FROM memories WHERE id = ?",
      );
      this.#insert = this.#database.prepare(
        "INSERT INTO memories (id, content, created_at) VALUES (?, ?, ?)",
      );
    } catch (error) {
      this.#database.close();
      throw error;
    }
  }

  save(memory: Memory): void {
    this.#ensureOpen();
    const record = parseMemory(memory);

    // Reserve the writer before checking the ID so competing saves cannot overwrite it.
    this.#database.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.get(record.id);
      if (existing === undefined) {
        this.#insert.run(record.id, record.content, record.createdAt);
      } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
        throw new MemoryConflictError(record.id);
      }
      this.#database.exec("COMMIT");
    } catch (error) {
      if (this.#database.isTransaction) {
        try {
          this.#database.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], "Memory save and rollback failed.");
        }
      }
      throw error;
    }
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

  #ensureOpen(): void {
    if (!this.#database.isOpen) throw new Error("Memory store is closed.");
  }
}
