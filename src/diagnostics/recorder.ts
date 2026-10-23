import { createHmac, randomBytes, randomUUID } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { resolveDiagnosticsDatabasePath } from '../storage/path.ts';
import { operations, reasons, type DiagnosticEvent, type DiagnosticSink } from './types.ts';

const capacity = 2048;
const retentionMs = 7 * 24 * 60 * 60 * 1000;
const retryMs = 30_000;
const codes = ['EACCES', 'EPERM', 'ENOENT', 'ENOSPC', 'EIO', 'ERR_SQLITE_ERROR', 'ABORT_ERR', 'ETIMEDOUT', 'ECONNRESET', 'ECONNREFUSED'] as const;
interface Options {
  readonly agentDir: string;
  readonly extensionVersion: string;
  readonly piVersion: string;
  readonly now?: () => number;
}
type Payload = Record<string, string | number>;
function version(value: string): string { return /^[0-9][0-9A-Za-z.+-]{0,47}$/.test(value) ? value : 'unknown'; }
function member<T extends string>(list: readonly T[], value: unknown): value is T {
  return typeof value === 'string' && list.some(item => item === value);
}
function field(value: unknown, key: string): unknown {
  try {
    if (typeof value !== 'object' || value === null) return undefined;
    return Object.getOwnPropertyDescriptor(value, key)?.value;
  } catch { return undefined; }
}
function errorClass(error: unknown): string {
  const names = ['Error', 'TypeError', 'RangeError', 'SyntaxError', 'DistillFailure', 'MemoryConflictError', 'MemoryNotFoundError', 'ConsolidationConflictError'];
  try {
    for (let depth = 0; depth < 4 && typeof error === 'object' && error !== null; depth++) {
      const name = field(error, 'name');
      if (member(names, name)) return name;
      error = Object.getPrototypeOf(error);
    }
  } catch { /* Untrusted error objects cannot prevent a bounded diagnostic. */ }
  return 'operation_error';
}
function errorFields(error: unknown): Payload {
  const result: Payload = error === undefined ? {} : {errorClass: errorClass(error)};
  // Inspect data properties only, never serialize messages, stacks, getters or arbitrary causes.
  for (let depth = 0; depth < 3 && error !== undefined; depth++, error = field(error, 'cause')) {
    const code = field(error, 'code');
    if (member(codes, code)) result['errorCode'] = code;
    const sqlite = field(error, 'errcode');
    if (typeof sqlite === 'number' && Number.isSafeInteger(sqlite) && sqlite >= 0 && sqlite <= 65535) result['sqliteCode'] = sqlite;
    const status = field(error, 'status');
    if (typeof status === 'number' && Number.isInteger(status) && status >= 100 && status <= 599) result['httpStatus'] = status;
    const reason = field(error, 'reason');
    if (member(reasons, reason)) result['errorReason'] = reason;
  }
  return result;
}

/** Bounded, independent, best-effort local observations. Never an authority for application state. */
export class LocalDiagnostics implements DiagnosticSink {
  readonly #options: Options;
  readonly #runtime = randomUUID();
  readonly #suppressed = new Map<string, number>();
  #db: DatabaseSync | undefined;
  #path: string | undefined;
  #nextExpiry = 0;
  #key = '';
  #closed = false;
  #retryAt = 0;
  #dropped = 0;

  constructor(options: Options) { this.#options = options; }

  #open(): DatabaseSync {
    if (this.#db) return this.#db;
    const path = resolveDiagnosticsDatabasePath(this.#options.agentDir);
    this.#path = path;
    mkdirSync(dirname(path), {recursive: true, mode: 0o700});
    if (resolveDiagnosticsDatabasePath(this.#options.agentDir) !== path) throw new Error('Diagnostic path changed.');
    const db = new DatabaseSync(path);
    try {
      db.exec('PRAGMA busy_timeout=0');
      const schema = db.prepare('PRAGMA user_version').get()?.['user_version'];
      if (schema !== 0 && schema !== 1) throw new Error('Unknown diagnostic schema.');
      if (schema === 0 && db.prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").get()) throw new Error('Unrecognized diagnostic database.');
      // A separate DELETE journal bounds transient file growth as well as database pages.
      db.exec('PRAGMA journal_mode=DELETE; PRAGMA synchronous=NORMAL');
      const pageSize = db.prepare('PRAGMA page_size').get()?.['page_size'];
      if (typeof pageSize !== 'number' || pageSize < 512) throw new Error('Invalid diagnostic page size.');
      const pageLimit = Math.floor(8 * 1024 * 1024 / pageSize);
      const actualLimit = db.prepare(`PRAGMA max_page_count=${pageLimit}`).get()?.['max_page_count'];
      if (actualLimit !== pageLimit) throw new Error('Diagnostic database exceeds size limit.');
      db.exec('BEGIN IMMEDIATE');
      try {
        // Another process may have initialized after our first read. IF NOT EXISTS and
        // INSERT OR IGNORE preserve its key and sequence while holding the write lock.
        db.exec(`CREATE TABLE IF NOT EXISTS metadata(id INTEGER PRIMARY KEY CHECK(id=1), key TEXT NOT NULL, sequence INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS events(slot INTEGER PRIMARY KEY CHECK(slot>=0 AND slot<2048), sequence INTEGER NOT NULL UNIQUE, time INTEGER NOT NULL, payload TEXT NOT NULL CHECK(length(payload)<=2048));
          CREATE TABLE IF NOT EXISTS issues(key TEXT PRIMARY KEY, signature TEXT NOT NULL, time INTEGER NOT NULL);`);
        db.prepare('INSERT OR IGNORE INTO metadata VALUES(1, ?, 0)').run(randomBytes(32).toString('hex'));
        const key = db.prepare('SELECT key FROM metadata WHERE id=1').get()?.['key'];
        if (typeof key !== 'string' || !/^[a-f0-9]{64}$/.test(key)) throw new Error('Invalid diagnostic metadata.');
        this.#key = key;
        db.exec('PRAGMA user_version=1; COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      chmodSync(path, 0o600);
      this.#db = db;
      return db;
    } catch (error) { db.close(); throw error; }
  }

  #expire(db: DatabaseSync, now: number): void {
    if (now < this.#nextExpiry) return;
    const oldest = db.prepare('SELECT MIN(time) AS time FROM events').get()?.['time'];
    if (typeof oldest === 'number' && oldest < now - retentionMs) {
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('DELETE FROM events WHERE time < ?').run(now - retentionMs);
        db.prepare('DELETE FROM issues WHERE time < ?').run(now - retentionMs);
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
    }
    this.#nextExpiry = now + retryMs;
  }

  record(event: DiagnosticEvent): void {
    if (this.#closed) return;
    let now = 0;
    try {
      now = (this.#options.now ?? Date.now)();
      if (!Number.isSafeInteger(now) || now < 0 || now > 8.64e15) return;
      const operation = field(event, 'operation'); const outcome = field(event, 'outcome');
      if (!member(operations, operation) || !member(['ok', 'failed', 'blocked'], outcome)) return;
      const classified = errorFields(field(event, 'error'));
      const sqliteCode = classified['sqliteCode'];
      if (typeof sqliteCode === 'number' && [5, 6].includes(sqliteCode % 256)) return;
      if (outcome === 'ok' && !this.#db && !existsSync(this.#path ??= resolveDiagnosticsDatabasePath(this.#options.agentDir))) return;
      if (now < this.#retryAt) { this.#dropped++; return; }
      const db = this.#open();
      this.#expire(db, now);
      const payload: Payload = {
        schema: 1, time: new Date(now).toISOString(), runtime: this.#runtime, pid: process.pid,
        extensionVersion: version(this.#options.extensionVersion), piVersion: version(this.#options.piVersion),
        nodeVersion: process.versions.node, platform: process.platform, operation, outcome,
      };
      const reason = field(event, 'reason'); if (member(reasons, reason)) payload['reason'] = reason;
      for (const name of ['task', 'source', 'model'] as const) {
        const value = field(event, name);
        if (typeof value === 'string' && value.length <= 16384) payload[name] = createHmac('sha256', this.#key).update(name).update('\0').update(value).digest('hex').slice(0, 32);
      }
      for (const name of ['durationMs', 'retryAfterMs'] as const) {
        const value = field(event, name);
        if (typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER) payload[name] = Math.round(value);
      }
      Object.assign(payload, classified);
      const issueKey = createHmac('sha256', this.#key).update(JSON.stringify([operation, payload['task'], payload['source']])).digest('hex');
      const signature = JSON.stringify([outcome, payload['reason'], payload['errorCode'], payload['sqliteCode'], payload['httpStatus'], payload['errorReason'], payload['errorClass']]);
      // Read first so ordinary successes and repeats do not acquire the write lock.
      const previous = db.prepare('SELECT signature,time FROM issues WHERE key=?').get(issueKey);
      const recent = typeof previous?.['time'] === 'number' && previous['time'] >= now - retentionMs;
      if (outcome === 'ok' && !recent) return;
      if (outcome !== 'ok' && previous?.['signature'] === signature &&
        typeof previous['time'] === 'number' && now >= previous['time'] && now - previous['time'] < retryMs) {
        if (this.#suppressed.size >= 128 && !this.#suppressed.has(issueKey)) this.#suppressed.clear();
        this.#suppressed.set(issueKey, Math.min(Number.MAX_SAFE_INTEGER, (this.#suppressed.get(issueKey) ?? 0) + 1));
        return;
      }
      if (outcome === 'ok') payload['outcome'] = 'recovered';
      payload['suppressed'] = this.#suppressed.get(issueKey) ?? 0;
      payload['dropped'] = this.#dropped;
      const json = JSON.stringify(payload);
      if (Buffer.byteLength(json) > 2048) { this.#dropped++; return; }
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('DELETE FROM events WHERE time < ?').run(now - retentionMs);
        db.prepare('DELETE FROM issues WHERE time < ?').run(now - retentionMs);
        if (outcome === 'ok') {
          // Recheck under the lock: two Pi processes cannot emit the same recovery.
          if (db.prepare('DELETE FROM issues WHERE key=?').run(issueKey).changes === 0) { db.exec('COMMIT'); return; }
        } else {
          const current = db.prepare('SELECT signature,time FROM issues WHERE key=?').get(issueKey);
          if (current?.['signature'] === signature && typeof current['time'] === 'number' && now >= current['time'] && now - current['time'] < retryMs) {
            db.exec('COMMIT'); return;
          }
          db.prepare('INSERT OR REPLACE INTO issues VALUES(?,?,?)').run(issueKey, signature, now);
          db.exec('DELETE FROM issues WHERE key IN (SELECT key FROM issues ORDER BY time DESC, rowid DESC LIMIT -1 OFFSET 128)');
        }
        const sequence = db.prepare('UPDATE metadata SET sequence=sequence+1 WHERE id=1 RETURNING sequence').get()?.['sequence'];
        if (typeof sequence !== 'number' || !Number.isSafeInteger(sequence)) throw new Error('Invalid diagnostic sequence.');
        db.prepare('INSERT OR REPLACE INTO events VALUES(?,?,?,?)').run(sequence % capacity, sequence, now, json);
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      this.#dropped = 0;
      this.#suppressed.delete(issueKey);
    } catch (error) {
      this.#dropped++;
      const code = field(error, 'errcode');
      // Busy writers drop just this record; persistent storage faults retry at a bounded cadence.
      if (typeof code !== 'number' || ![5, 6].includes(code % 256)) this.#retryAt = now + retryMs;
      try { this.#db?.close(); } catch { /* No retry or terminal output. */ }
      this.#db = undefined;
    }
  }

  close(): void {
    this.#closed = true;
    try { this.#db?.close(); } catch { /* Disposable state. */ }
    this.#db = undefined;
    this.#suppressed.clear();
  }
}
