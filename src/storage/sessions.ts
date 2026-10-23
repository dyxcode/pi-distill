import type {
  TaskSweepPage,
  TaskSweepPageOptions
} from "../distill/tasks.ts";
import type {
  SessionObservation
} from "../distill/types.ts";
import { ConsolidationConflictError } from '../distill/types.ts';
import type { SessionUsage } from "../retention/types.ts";
import { observationColumns, sessionUsageColumns, textColumn } from './columns.ts';
import type { StorageConnection } from './connection.ts';
import type { DistillQueue } from './distill.ts';
import { maxKnownSessionFiles } from './limits.ts';
import type { RetentionTasks } from './retention.ts';
import { parseSessionObservation, parseSessionUsage, parseStoredSessionObservation, parseStoredSessionUsage, sameSessionUsageIdentity } from './session-validation.ts';
import { SourceRegistry } from "./source-registry.ts";
import { parseTaskSweepPageOptions } from './task-validation.ts';
import { parseNullableText, validateAbsolutePath, validateNow, validateText } from './validation.ts';
interface Dependencies {
  readonly sources: SourceRegistry;
  readonly distill: Pick<DistillQueue, "taskSweepThroughCursor">;
  readonly retention: Pick<RetentionTasks, "getDeletion">;
}

export class SessionRecords {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }

  observeSession(observation: SessionObservation): void {
    this.connection.ensureOpen();
    const record = parseSessionObservation(observation);
    this.connection.transaction(() => {
      if (record.sessionFile !== null) this.registerSource({ sessionId: record.sessionId, sessionFile: record.sessionFile, cwd: record.cwd });
      this.connection.database.prepare(
        "INSERT INTO session_observations " +
        "(session_id, session_file, cwd, leaf_id, owner_pid, captured) " +
        "VALUES (?, ?, ?, ?, ?, 0) ON CONFLICT(session_id) DO UPDATE SET " +
        "session_file = excluded.session_file, cwd = excluded.cwd, " +
        "leaf_id = excluded.leaf_id, owner_pid = excluded.owner_pid, " +
        "captured = CASE WHEN session_observations.session_file = excluded.session_file " +
        "AND session_observations.cwd = excluded.cwd " +
        "AND session_observations.leaf_id IS excluded.leaf_id " +
        "THEN session_observations.captured ELSE 0 END",
      ).run(record.sessionId, record.sessionFile, record.cwd, record.leafId, record.ownerPid);
    });
  }

  saveSessionUsage(usage: SessionUsage): void {
    this.connection.ensureOpen();
    const record = parseSessionUsage(usage);
    this.connection.transaction(() => {
      if (record.sessionFile !== null) this.dependencies.sources.register({ sessionId: record.sessionId, sessionFile: record.sessionFile, cwd: record.cwd });
      const row = this.connection.database.prepare(
        `SELECT ${sessionUsageColumns} FROM session_usages WHERE owner_id = ?`,
      ).get(record.ownerId);
      if (row === undefined) {
        if (record.state !== "active") {
          throw new ConsolidationConflictError(record.ownerId, "cannot be created outside the active state");
        }
        this.connection.database.prepare(
          "INSERT INTO session_usages " +
          "(owner_id, process_token, session_id, session_file, cwd, leaf_id, owner_pid, state) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ).run(
          record.ownerId,
          record.processToken,
          record.sessionId,
          record.sessionFile,
          record.cwd,
          record.leafId,
          record.ownerPid,
          record.state,
        );
        return;
      }

      const current = parseStoredSessionUsage(this.connection.decodeRow(row));
      if (!sameSessionUsageIdentity(current, record)) {
        throw new ConsolidationConflictError(record.ownerId, "already exists with different owner identity");
      }
      const checkpointChanged = current.leafId !== record.leafId;
      const transitionAllowed = current.state === "active"
        ? record.state === "active" || record.state === "closing"
        : current.state === "closing"
          ? (record.state === "closing" || record.state === "released") && !checkpointChanged
          : record.state === "released" && !checkpointChanged;
      if (!transitionAllowed) {
        throw new ConsolidationConflictError(record.ownerId, "cannot change its session usage state or checkpoint");
      }
      if (current.state === record.state && !checkpointChanged) return;
      this.connection.database.prepare(
        "UPDATE session_usages SET leaf_id = ?, state = ? WHERE owner_id = ?",
      ).run(record.leafId, record.state, record.ownerId);
    });
  }

  acknowledgeObservation(sessionId: string, leafId: string | null): void {
    this.connection.ensureOpen();
    validateText(sessionId, "session ID");
    const currentLeafId = parseNullableText(leafId, "session leaf ID");
    this.connection.transaction(() => {
      this.connection.database.prepare(
        "UPDATE session_observations SET captured = 1 WHERE session_id = ? AND leaf_id IS ?",
      ).run(sessionId, currentLeafId);
    });
  }

  observedSessions(): readonly SessionObservation[] {
    this.connection.ensureOpen();
    return this.connection.database.prepare(
      `SELECT ${observationColumns} FROM session_observations ` +
      "WHERE captured = 0 ORDER BY session_observations.session_id COLLATE BINARY",
    ).all().map((row) => parseStoredSessionObservation(this.connection.decodeRow(row)).observation);
  }

  /** Returns a bounded stable rowid page of uncaptured session observations. */
  listObservedSessions(options: TaskSweepPageOptions): TaskSweepPage<SessionObservation> {
    this.connection.ensureOpen();
    const page = parseTaskSweepPageOptions(options);
    const cursor = page.cursor ?? 0;
    const throughCursor = this.dependencies.distill.taskSweepThroughCursor("session_observations", page.throughCursor);
    const rows = this.connection.database.prepare(
      `SELECT rowid AS cursorRow, ${observationColumns} FROM session_observations ` +
      "WHERE captured = 0 AND rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?",
    ).all(cursor, throughCursor, page.limit + 1);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.connection.decodeRow(raw);
      const { cursorRow, ...observationRow } = decoded;
      validateNow(cursorRow, "Stored session observation cursor");
      return parseStoredSessionObservation(observationRow).observation;
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.connection.decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next session observation cursor");
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  /** Startup recovery also covers a crash between the first file write and message_end. */
  listUnobservedSessionUsages(options: TaskSweepPageOptions): TaskSweepPage<SessionUsage> {
    const page = parseTaskSweepPageOptions(options);
    const throughCursor = this.dependencies.distill.taskSweepThroughCursor("session_usages", page.throughCursor);
    const rows = this.connection.database.prepare(
      `SELECT rowid AS cursorRow, ${sessionUsageColumns} FROM session_usages ` +
      "WHERE rowid > ? AND rowid <= ? AND NOT EXISTS (SELECT 1 FROM session_observations o WHERE o.session_id = session_usages.session_id) " +
      "AND NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = session_usages.session_file AND d.state = 'complete') " +
      "ORDER BY rowid LIMIT ?",
    ).all(page.cursor ?? 0, throughCursor, page.limit + 1);
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const { cursorRow, ...owner } = this.connection.decodeRow(raw);
      validateNow(cursorRow, "Session usage cursor");
      return parseStoredSessionUsage(owner);
    });
    const nextCursor = rows.length > page.limit ? visible.at(-1)?.["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next session usage cursor");
    return { items, throughCursor, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  listSessionUsages(): readonly SessionUsage[] {
    this.connection.ensureOpen();
    return this.connection.database.prepare(
      `SELECT ${sessionUsageColumns} FROM session_usages ORDER BY owner_id COLLATE BINARY`,
    ).all().map((row) => parseStoredSessionUsage(this.connection.decodeRow(row)));
  }

  /** Returns a fair lexical page of known paths after validating bounded task metadata. */
  listKnownSessionFiles(limit = maxKnownSessionFiles, afterPath?: string): readonly string[] {
    this.connection.ensureOpen();
    if (
      typeof limit !== "number" || !Number.isSafeInteger(limit) ||
      limit < 1 || limit > maxKnownSessionFiles
    ) {
      throw new TypeError(
        "Known session file limit must be an integer from 1 through " + maxKnownSessionFiles + ".",
      );
    }
    if (afterPath !== undefined) validateAbsolutePath(afterPath, "known session file cursor");
    return this.connection.database.prepare(
      "SELECT " + textColumn("session_file", "path") + " FROM managed_sources " +
      "WHERE deleted = 0 AND (? IS NULL OR session_file COLLATE BINARY > ?) " +
      "GROUP BY session_file ORDER BY session_file COLLATE BINARY LIMIT ?",
    ).all(afterPath ?? null, afterPath ?? null, limit).map(raw => {
      const row = this.connection.decodeRow(raw);
      validateAbsolutePath(row["path"], "Stored source path");
      return row["path"];
    });
  }

  private registerSource(source: { sessionId: string; sessionFile: string; cwd: string ;}): void {
    this.dependencies.sources.register(source);
  }

  assertSourceEnqueueAllowed(sessionId: string, sessionFile: string, cwd: string): void {
    this.registerSource({ sessionId, sessionFile, cwd });
    const source = this.dependencies.sources.get(sessionFile, sessionId);
    const receipt = this.dependencies.retention.getDeletion(sessionFile);
    if (source?.deleted || (receipt?.state === "complete" && receipt.sessionId === sessionId)) {
      throw new ConsolidationConflictError(sessionFile, "has a completed deletion receipt for this source generation");
    }
  }

  /** The live Pi manager supplies the header before a new file has been written. */
  reserveSessionSource(source: { sessionId: string; sessionFile: string; cwd: string; parentSession?: string ;}): void {
    this.connection.transaction(() => {
      this.dependencies.sources.register({ sessionId: source.sessionId, sessionFile: source.sessionFile, cwd: source.cwd });
      this.dependencies.sources.setReserved(source.sessionFile, source.sessionId, source.parentSession);
    });
  }
}
