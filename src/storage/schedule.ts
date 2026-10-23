import type { DatabaseSync } from 'node:sqlite';
export const DISTILL_SCHEDULE_SCHEMAS = [
  { name: 'distill_schedule', sql: 'CREATE TABLE distill_schedule (session_key TEXT PRIMARY KEY NOT NULL, position INTEGER NOT NULL UNIQUE, waiting INTEGER NOT NULL CHECK (waiting IN (0,1)), probe_at INTEGER NOT NULL CHECK (probe_at >= 0)) STRICT' },
  { name: 'distill_schedule_state', sql: 'CREATE TABLE distill_schedule_state (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton=1), sequence INTEGER NOT NULL, phase INTEGER NOT NULL CHECK (phase BETWEEN 0 AND 2), cursor INTEGER NOT NULL) STRICT' },
  { name: 'distill_schedule_probe', sql: 'CREATE INDEX distill_schedule_probe ON distill_schedule (probe_at)' },
  { name: 'jobs_session_status', sql: "CREATE INDEX jobs_session_status ON consolidation_jobs (json_quote(json_extract(job_json, '$.sessionId')), status)" },
  { name: 'triggers_session_status', sql: "CREATE INDEX triggers_session_status ON distill_triggers (json_quote(json_extract(trigger_json, '$.sessionId')), status)" },
];
export interface ScheduledDistill {
  readonly sessionId: string;
  readonly position: number;
  readonly waiting: boolean;
  readonly kind: 'job' | 'trigger';
  readonly id: string;
  readonly sessionFile: string | null;
}
function integer(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}
function decode(value: unknown): unknown {
  if (typeof value !== 'string') throw new TypeError('Invalid scheduling identity.');
  return JSON.parse(value);
}
function text(value: unknown): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) throw new TypeError('Invalid scheduling text.');
}

/** Content-free shared rotation. Mutations run in the owning store transaction.
 * JSON-encoded identities preserve embedded NULs on supported Node SQLite versions.
 */
export class DistillSchedule {
  readonly database: DatabaseSync;
  constructor(database: DatabaseSync) { this.database = database; }
  position(sessionId: string): number | undefined {
    const row = this.database.prepare('SELECT position FROM distill_schedule WHERE session_key=?').get(JSON.stringify(sessionId));
    if (!row)
      return undefined;
    if (!integer(row['position']))
      throw new TypeError('Invalid schedule position.');
    return row['position'];
  }
  front(sessionId: string): Omit<ScheduledDistill, 'position' | 'waiting'> | undefined {
    text(sessionId);
    const key = JSON.stringify(sessionId);
    for (const kind of ['job', 'trigger'] as const) {
      const table = kind === 'job' ? 'consolidation_jobs' : 'distill_triggers', column = kind === 'job' ? 'job_json' : 'trigger_json';
      const row = this.database.prepare(`SELECT json_quote(id) AS id,json_quote(json_extract(${column},'$.sessionFile')) AS path FROM ${table} WHERE json_quote(json_extract(${column},'$.sessionId'))=? AND status='pending' ORDER BY rowid LIMIT 1`).get(key);
      if (!row)
        continue;
      const id = decode(row['id']), sessionFile = decode(row['path']);
      text(id);
      if (sessionFile !== null)
        text(sessionFile);
      return { sessionId, kind, id, sessionFile };
    }
    return undefined;
  }
  /** Called before inserting new work, in the owner's write transaction. */
  enqueue(sessionId: string): void {
    if (this.position(sessionId) === undefined || !this.front(sessionId))
      this.turn(sessionId);
  }
  /** One ready turn consumes one position, irrespective of the session's trigger count. */
  turn(sessionId: string): number {
    text(sessionId);
    const row = this.database.prepare('UPDATE distill_schedule_state SET sequence=sequence+1 WHERE singleton=1 RETURNING sequence').get();
    if (!row || !integer(row['sequence']))
      throw new TypeError('Invalid scheduler sequence.');
    const position = row['sequence'];
    this.database.prepare('INSERT INTO distill_schedule VALUES (?,?,0,0) ON CONFLICT(session_key) DO UPDATE SET position=excluded.position,waiting=0,probe_at=0').run(JSON.stringify(sessionId), position);
    return position;
  }
  defer(sessionId: string, position: number, probeAt: number): void {
    if (!integer(position) || !integer(probeAt))
      throw new TypeError('Invalid scheduling wait.');
    this.database.prepare('UPDATE distill_schedule SET waiting=1,probe_at=? WHERE session_key=? AND position=?').run(probeAt, JSON.stringify(sessionId), position);
  }
  page(now: number, force: boolean, after = 0, through?: number): {
    items: ScheduledDistill[];
    cursor: number;
    through: number;
    more: boolean;
  } {
    if (!integer(now) || typeof force !== 'boolean' || !integer(after) || (through !== undefined && !integer(through)))
      throw new TypeError('Invalid scheduler page.');
    const state = this.database.prepare('SELECT sequence FROM distill_schedule_state WHERE singleton=1').get();
    const end = through ?? state?.['sequence'];
    if (!integer(end))
      throw new TypeError('Invalid scheduling sweep.');
    const rows = this.database.prepare('SELECT session_key,position,waiting,probe_at FROM distill_schedule WHERE position>? AND position<=? ORDER BY position LIMIT 33').all(after, end);
    const items: ScheduledDistill[] = [];
    let cursor = after;
    for (const row of rows.slice(0, 32)) {
      const sessionId = decode(row['session_key']);
      text(sessionId);
      if (!integer(row['position']) || (row['waiting'] !== 0 && row['waiting'] !== 1))
        throw new TypeError('Invalid schedule row.');
      cursor = row['position'];
      const action = this.front(sessionId);
      if (action) {
        const probeAt = row['probe_at'];
        if (!integer(probeAt))
          throw new TypeError('Invalid scheduling probe.');
        if (force || probeAt <= now)
          items.push({ ...action, position: cursor, waiting: row['waiting'] === 1 });
        else {
          // Move an inspected sleeper out of this finite sweep without probing
          // its source. Otherwise repeated bounded scans could hide later work.
          this.defer(sessionId, this.turn(sessionId), probeAt);
        }
      }
      else
        this.database.prepare('DELETE FROM distill_schedule WHERE session_key=? AND position=?').run(JSON.stringify(sessionId), cursor);
    }
    return { items, cursor, through: end, more: rows.length > 32 };
  }
  hasReady(now: number): boolean {
    if (!integer(now)) throw new TypeError('Invalid scheduling time.');
    return this.database.prepare("SELECT 1 FROM distill_schedule s WHERE probe_at<=? AND (EXISTS (SELECT 1 FROM consolidation_jobs WHERE json_quote(json_extract(job_json,'$.sessionId'))=+s.session_key AND status='pending') OR EXISTS (SELECT 1 FROM distill_triggers WHERE json_quote(json_extract(trigger_json,'$.sessionId'))=+s.session_key AND status='pending')) LIMIT 1").get(now) !== undefined;
  }
  delay(now: number): number {
    if (!integer(now)) throw new TypeError('Invalid scheduling time.');
    const row = this.database.prepare('SELECT MIN(probe_at) AS next FROM distill_schedule WHERE probe_at>?').get(now);
    const next = row?.['next'];
    return integer(next) ? Math.max(1, Math.min(30000, next - now)) : 30000;
  }
  /** Bounded conversion of existing pending tasks; foreground enqueues join immediately. */
  backfill(): boolean {
    const row = this.database.prepare('SELECT phase,cursor FROM distill_schedule_state WHERE singleton=1').get();
    if (!row || !integer(row['phase']) || row['phase'] > 2 || !integer(row['cursor']))
      throw new TypeError('Invalid scheduling backfill.');
    let phase = row['phase'], cursor = row['cursor'], remaining = 128;
    while (phase < 2 && remaining > 0) {
      const table = phase === 0 ? 'consolidation_jobs' : 'distill_triggers', column = phase === 0 ? 'job_json' : 'trigger_json';
      const rows = this.database.prepare(`SELECT rowid AS cursor,status,json_quote(json_extract(${column},'$.sessionId')) AS session FROM ${table} WHERE rowid>? ORDER BY rowid LIMIT ?`).all(cursor, remaining);
      for (const item of rows) {
        if (!integer(item['cursor']))
          throw new TypeError('Invalid scheduling backfill position.');
        cursor = item['cursor'];
        remaining--;
        if (item['status'] === 'pending') {
          const id = decode(item['session']);
          text(id);
          if (this.position(id) === undefined)
            this.turn(id);
        }
      }
      if (rows.length === 0 || remaining > 0) {
        phase++;
        cursor = 0;
      }
    }
    this.database.prepare('UPDATE distill_schedule_state SET phase=?,cursor=? WHERE singleton=1').run(phase, cursor);
    return phase === 2;
  }
}
