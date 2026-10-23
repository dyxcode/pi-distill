import { type StatementSync } from "node:sqlite";
import type { Memory, MemoryForget, MemoryInput, MemorySearchRequest, MemorySearchRow, MemoryUpdate } from '../memory/types.ts';
import { MemoryConflictError, MemoryNotFoundError } from '../memory/types.ts';
import { memoryColumns, textColumn } from './columns.ts';
import { maxSearchBatchSize, parseForget, parseInput, parseMemory, parseSearchRequest, parseUpdate } from './memory-validation.ts';
import { validateText } from './validation.ts';

import type { StorageConnection } from './connection.ts';

export class Memories {
  private readonly connection: StorageConnection;
  constructor(connection: StorageConnection) { this.connection = connection; }
  #selectStatement: StatementSync | undefined;
  get selectStatement(): StatementSync {
    return this.#selectStatement ??= this.connection.database.prepare(
      `SELECT ${memoryColumns} FROM memories WHERE id = ?`,
    );
  }
  #insertStatement: StatementSync | undefined;
  get insertStatement(): StatementSync {
    return this.#insertStatement ??= this.connection.database.prepare(
      `INSERT INTO memories (id, content, created_at, revision, updated_at)
         VALUES (?, ?, ?, 1, ?)`,
    );
  }
  #updateStatement: StatementSync | undefined;
  get updateStatement(): StatementSync {
    return this.#updateStatement ??= this.connection.database.prepare(
      "UPDATE memories SET content = ?, revision = ?, updated_at = ? WHERE id = ?",
    );
  }
  #deleteStatement: StatementSync | undefined;
  get deleteStatement(): StatementSync {
    return this.#deleteStatement ??= this.connection.database.prepare("DELETE FROM memories WHERE id = ?");
  }
  #upsertOriginStatement: StatementSync | undefined;
  get upsertOriginStatement(): StatementSync {
    return this.#upsertOriginStatement ??= this.connection.database.prepare(
      "INSERT INTO memory_origins (memory_id, evidence_id, revision) VALUES (?, ?, ?) " +
      "ON CONFLICT(memory_id) DO UPDATE SET evidence_id = excluded.evidence_id, revision = excluded.revision",
    );
  }
  #deleteOriginStatement: StatementSync | undefined;
  get deleteOriginStatement(): StatementSync {
    return this.#deleteOriginStatement ??= this.connection.database.prepare("DELETE FROM memory_origins WHERE memory_id = ?");
  }
  save(memory: MemoryInput): void {
    this.connection.ensureOpen();
    const record = parseInput(memory);
    this.connection.transaction(() => {
      const existing = this.get(record.id);
      if (existing === undefined) {
        this.insertStatement.run(record.id, record.content, record.createdAt, record.createdAt);
      } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
        throw new MemoryConflictError(record.id);
      }
    });
  }

  update(update: MemoryUpdate): Memory {
    this.connection.ensureOpen();
    const request = parseUpdate(update);
    return this.connection.transaction(() => {
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
      this.updateStatement.run(result.content, result.revision, result.updatedAt, result.id);
      this.deleteOriginStatement.run(result.id);
      return result;
    });
  }

  forget(forget: MemoryForget): void {
    this.connection.ensureOpen();
    const request = parseForget(forget);
    this.connection.transaction(() => {
      const current = this.get(request.id);
      if (current === undefined) {
        this.deleteOriginStatement.run(request.id);
        return;
      }
      if (current.revision !== request.expectedRevision) {
        throw new MemoryConflictError(request.id);
      }
      this.deleteStatement.run(request.id);
      this.deleteOriginStatement.run(request.id);
    });
  }

  get(id: string): Memory | undefined {
    this.connection.ensureOpen();
    validateText(id, "ID");
    const row = this.selectStatement.get(id);
    return row === undefined ? undefined : parseMemory(this.connection.decodeRow(row));
  }

  searchBatch(request: MemorySearchRequest): readonly MemorySearchRow[] {
    this.connection.ensureOpen();
    const search = parseSearchRequest(request);
    const limit = search.limit ?? maxSearchBatchSize;
    const afterClause = search.after === undefined ? "" : "WHERE id COLLATE BINARY > ?";
    const matchedTermColumns = search.terms.map((_, index) =>
      `CASE WHEN instr(lower(content), lower(?)) > 0 THEN 1 ELSE 0 END AS matched_${index}`,
    ).join(",\n       ");
    const statement = this.connection.database.prepare(`
      WITH examined AS (
        SELECT id, content, created_at AS createdAt, revision, updated_at AS updatedAt
        FROM memories
        ${afterClause}
        ORDER BY id COLLATE BINARY
        LIMIT ?
      )
      SELECT ${[
        textColumn("id"), textColumn("content"), textColumn("createdAt"),
        "revision", textColumn("updatedAt"),
      ].join(", ")},
        ${matchedTermColumns}
      FROM examined
      ORDER BY examined.id COLLATE BINARY
    `);
    const parameters: (string | number)[] = [];
    if (search.after !== undefined) parameters.push(search.after);
    parameters.push(limit, ...search.terms);

    return statement.all(...parameters).map((rawRow) => {
      const row = this.connection.decodeRow(rawRow);
      const memory = parseMemory({
        id: row["id"],
        content: row["content"],
        createdAt: row["createdAt"],
        revision: row["revision"],
        updatedAt: row["updatedAt"],
      });
      const matchedTerms: string[] = [];
      for (let index = 0; index < search.terms.length; index += 1) {
        const flag = row[`matched_${index}`];
        if (flag === 1) {
          const term = search.terms[index];
          if (term === undefined) throw new Error("Memory search term index exceeded the validated input.");
          matchedTerms.push(term);
        } else if (flag !== 0) {
          throw new Error("SQLite returned an invalid memory search match flag.");
        }
      }
      return { memory, matchedTerms };
    });
  }
}
