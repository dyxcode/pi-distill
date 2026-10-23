import { isAbsolute } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

/** One connection and writer reservation for every repository in a store. */
export class StorageConnection {
  readonly database: DatabaseSync;
  readonly textDecoder: TextDecoder;
  constructor(path: string) {
    if (typeof path !== 'string' || !isAbsolute(path)) throw new TypeError('Memory database requires an absolute file path.');
    this.database = new DatabaseSync(path, { timeout: 1_000 });
    try {
      const encoding = this.database.prepare('PRAGMA encoding').get()?.['encoding'];
      if (encoding !== 'UTF-8' && encoding !== 'UTF-16le' && encoding !== 'UTF-16be') throw new Error('Unsupported memory database text encoding.');
      this.textDecoder = new TextDecoder(encoding, { fatal: true, ignoreBOM: true });
      this.database.exec('PRAGMA synchronous = FULL');
    } catch (error) { this.database.close(); throw error; }
  }
  close(): void {
    if (this.database.isOpen) this.database.close();
  }

  decodeRow(row: Record<string, unknown>): Record<string, unknown> {
    for (const [key, value] of Object.entries(row)) {
      if (value instanceof Uint8Array) row[key] = this.textDecoder.decode(value);
    }
    return row;
  }

  transaction<T>(operation: () => T, mode: "IMMEDIATE" | "DEFERRED" = "IMMEDIATE"): T {
    // Mutations reserve the writer; read-only assessments use a SQLite read snapshot.
    this.database.exec(mode === "IMMEDIATE" ? "BEGIN IMMEDIATE" : "BEGIN DEFERRED");
    try {
      const result = operation();
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      if (this.database.isTransaction) {
        try {
          this.database.exec("ROLLBACK");
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], "Memory transaction and rollback failed.");
        }
      }
      throw error;
    }
  }

  ensureOpen(): void {
    if (!this.database.isOpen) throw new Error("Memory store is closed.");
  }
}
