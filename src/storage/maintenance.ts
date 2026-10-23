import { cleanTaskColumns, evidenceColumns, jobColumns, observationColumns, sessionUsageColumns, textColumn, triggerColumns } from './columns.ts';
import type { StorageConnection } from './connection.ts';
import { collectDerivedSources, derivedSourceBoundaries } from "./coverage.ts";
import type { DistillQueue } from './distill.ts';
import { parseEvidenceRow, parsePersistedJobRow } from './evidence-validation.ts';
import { maxMetadataGcPageSize, maxTaskPageSize } from './limits.ts';
import type { RetentionTasks } from './retention.ts';
import { parseStoredSessionObservation, parseStoredSessionUsage } from './session-validation.ts';
import type { SourceIndex } from './source-index.ts';
import { SourceRegistry } from "./source-registry.ts";
import type { SourceViews } from './source-view.ts';
import { parsePersistedCleanTask } from './task-validation.ts';
import { fields, validateAbsolutePath, validateNow, validateText } from './validation.ts';
interface Dependencies {
  readonly sourceViews: SourceViews;
  readonly sourceIndex: SourceIndex;
  readonly retention: Pick<RetentionTasks, "acquireSourceAccess" | "releaseSourceAccess" | "getDeletion" | "scopeFilter" | "sourceRetentionState">;
  readonly sources: SourceRegistry;
  readonly distill: Pick<DistillQueue, "parseTriggerRow" | "clearDistillFailure">;
}

export class MetadataMaintenance {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }
  #gcBoundaries: { core: number[]; derived: number[]; } | undefined;

  /** Bounded conversion of the old task-derived inventory; safe to repeat after interruption. */
  backfillSources(limit = 128): boolean {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw new TypeError("Invalid backfill budget.");
    return this.connection.transaction(() => this.backfillInTransaction(limit));
  }

  /** Reclaim terminal metadata in a restartable sweep; receipts, origins, and progress survive. */
  collectMetadata(limit = maxMetadataGcPageSize): { readonly removed: number; readonly hasMore: boolean; readonly contended: boolean; } {
    this.connection.ensureOpen();
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > maxMetadataGcPageSize) throw new TypeError("Invalid metadata collection page size.");
    const access = this.dependencies.retention.acquireSourceAccess();
    if (!access) return { removed: 0, hasMore: true, contended: true };
    try {
      return this.connection.transaction(() => {
        // Bits 1/2 latch completed collectors; bit 4 requests a fresh sweep after
        // removals. Retained rows do not require both collectors to end together.
        let state = this.dependencies.sources.readCursor("gc") ?? 0;
        if (state > 7) throw new TypeError("Invalid metadata sweep completion state.");
        this.#gcBoundaries ??= {
          core: ["clean_tasks", "distill_trigger_jobs", "distill_triggers", "consolidation_jobs", "session_observations", "session_usages", "managed_sources", "memory_evidence"].map(table => {
            const column = table === "managed_sources" ? "row_id" : "rowid";
            const end = this.connection.database.prepare(`SELECT COALESCE(MAX(${column}),0) AS value FROM ${table}`).get()?.["value"];
            validateNow(end, "Metadata sweep boundary"); return end;
          }),
          derived: derivedSourceBoundaries(this.connection.database),
        };
        let removed = 0;
        const coreBudget = state & 2 ? limit : Math.max(1, Math.floor(limit / 2));
        const corePending = !(state & 1);
        if (corePending) {
          const core = this.collectOperationalMetadata(coreBudget, this.#gcBoundaries.core);
          removed += core.removed;
          if (!core.hasMore) state |= 1;
        }
        if (!(state & 2) && (limit > 1 || (state & 1 && removed === 0))) {
          const derived = collectDerivedSources(this.connection.database, {
            sources: this.dependencies.sources,
            sourceViews: this.dependencies.sourceViews,
            sourceIndex: this.dependencies.sourceIndex,
            getDeletion: path => this.dependencies.retention.getDeletion(path),
            sourceRetentionState: (path, id) => this.dependencies.retention.sourceRetentionState(path, id),
          }, corePending ? Math.max(1, limit - coreBudget) : limit, this.#gcBoundaries.derived);
          removed += derived.removed;
          if (!derived.hasMore) state |= 2;
        }
        if (removed > 0) state |= 4;
        const finished = (state & 3) === 3;
        const hasMore = !finished || (state & 4) !== 0;
        this.dependencies.sources.writeCursor("gc", finished ? null : state);
        if (finished) this.#gcBoundaries = undefined;
        return { removed, hasMore, contended: false };
      });
    } finally { this.dependencies.retention.releaseSourceAccess(access); }
  }

  private collectOperationalMetadata(limit: number, through: readonly number[]): { readonly removed: number; readonly hasMore: boolean; } {
    this.connection.ensureOpen();
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > maxMetadataGcPageSize) {
      throw new TypeError("Metadata collection limit must be an integer from 1 through 128.");
    }

    if (!this.backfillInTransaction(maxTaskPageSize)) return { removed: 0, hasMore: true };

    const state = this.connection.database.prepare(
      "SELECT phase, cursor_row AS cursorRow FROM metadata_gc_state WHERE singleton = 1",
    ).get();
    if (state === undefined) throw new Error("Metadata collection state is missing.");
    let phase = state["phase"];
    let cursor = state["cursorRow"];
    if (typeof phase !== "number" || !Number.isSafeInteger(phase) || phase < 0 || phase > 7 ||
      typeof cursor !== "number" || !Number.isSafeInteger(cursor) || cursor < 0) {
      throw new TypeError("Stored metadata collection progress is invalid.");
    }

    let examined = 0;
    let removed = 0;
    while (phase < 8 && examined < limit) {
      const remaining = limit - examined;
      const rows = this.metadataGcRows(phase, cursor, remaining, through[phase] ?? 0);
      if (rows.length === 0) {
        phase += 1;
        cursor = 0;
        continue;
      }
      for (const raw of rows) {
        const row = this.connection.decodeRow(raw);
        const rowId = row["gcRow"];
        if (typeof rowId !== "number" || !Number.isSafeInteger(rowId) || rowId <= cursor) {
          throw new TypeError("Stored metadata collection row cursor is invalid.");
        }
        cursor = rowId;
        examined += 1;
        if (this.collectMetadataRow(phase, row)) removed += 1;
      }
      if (rows.length < remaining) {
        phase += 1;
        cursor = 0;
      }
    }

    if (phase >= 8) {
      this.connection.database.prepare("UPDATE metadata_gc_state SET phase = 0, cursor_row = 0 WHERE singleton = 1").run();
      return { removed, hasMore: removed > 0 };
    }
    this.connection.database.prepare("UPDATE metadata_gc_state SET phase = ?, cursor_row = ? WHERE singleton = 1")
      .run(phase, cursor);
    return { removed, hasMore: true };
  }

  private metadataGcRows(phase: number, cursor: number, limit: number, through: number): Record<string, unknown>[] {
    const queries: readonly string[] = [
      `SELECT rowid AS gcRow, ${cleanTaskColumns} FROM clean_tasks WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ${textColumn("job_id", "jobId")} FROM distill_trigger_jobs WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${triggerColumns} FROM distill_triggers WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${jobColumns} FROM consolidation_jobs WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${observationColumns} FROM session_observations WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT rowid AS gcRow, ${sessionUsageColumns} FROM session_usages WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
      `SELECT row_id AS gcRow, ${textColumn("generation")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("session_id", "sessionId")}, deleted FROM managed_sources WHERE row_id > ? AND row_id <= ? ORDER BY row_id LIMIT ?`,
      `SELECT rowid AS gcRow, ${evidenceColumns} FROM memory_evidence WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`,
    ];
    const query = queries[phase];
    if (query === undefined) throw new Error("Metadata collection phase is invalid.");
    return this.connection.database.prepare(query).all(cursor, through, limit);
  }

  private collectMetadataRow(phase: number, row: Record<string, unknown>): boolean {
    const rowId = row["gcRow"];
    if (typeof rowId !== "number" || !Number.isSafeInteger(rowId) || rowId < 1) {
      throw new TypeError("Stored metadata row ID is invalid.");
    }
    if (phase === 0) {
      const record = parsePersistedCleanTask(this.metadataPayload(row));
      if (record.task.sessionFile === null || !this.metadataSourceEligible(record.task.sessionFile, record.task.sessionId)) return false;
      return this.connection.database.prepare("DELETE FROM clean_tasks WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 1) {
      const link = fields(this.metadataPayload(row), ["id", "triggerId", "jobId"]);
      validateText(link["id"], "stored trigger batch link ID");
      validateText(link["triggerId"], "stored trigger batch ID");
      validateText(link["jobId"], "stored trigger batch job ID");
      const triggerRow = this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers WHERE id = ?`).get(link["triggerId"]);
      const jobRow = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(link["jobId"]);
      if (triggerRow === undefined || jobRow === undefined) throw new TypeError("Stored trigger batch link is orphaned.");
      const trigger = this.dependencies.distill.parseTriggerRow(triggerRow);
      const job = parsePersistedJobRow(this.connection.decodeRow(jobRow));
      if (trigger.status !== "complete" || job.status !== "complete" || job.job.sessionFile === null ||
        job.job.sessionFile !== trigger.trigger.sessionFile || job.job.sessionId !== trigger.trigger.sessionId ||
        !this.metadataSourceEligible(trigger.trigger.sessionFile, trigger.trigger.sessionId)) return false;
      return this.connection.database.prepare("DELETE FROM distill_trigger_jobs WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 2) {
      const trigger = this.dependencies.distill.parseTriggerRow(this.metadataPayload(row));
      if (trigger.status !== "complete" || trigger.jobIds.length > 0 || trigger.trigger.sessionFile === null ||
        !this.metadataSourceEligible(trigger.trigger.sessionFile, trigger.trigger.sessionId)) return false;
      const removed = this.connection.database.prepare("DELETE FROM distill_triggers WHERE rowid = ?").run(rowId).changes === 1;
      if (removed) this.dependencies.distill.clearDistillFailure("trigger", trigger.trigger.id);
      return removed;
    }
    if (phase === 3) {
      const job = parsePersistedJobRow(this.metadataPayload(row));
      if (job.status !== "complete" || job.job.sessionFile === null ||
        this.connection.database.prepare("SELECT 1 AS present FROM distill_trigger_jobs WHERE job_id = ? LIMIT 1").get(job.job.id) !== undefined ||
        !this.metadataSourceEligible(job.job.sessionFile, job.job.sessionId)) return false;
      const removed = this.connection.database.prepare("DELETE FROM consolidation_jobs WHERE rowid = ?").run(rowId).changes === 1;
      if (removed) this.dependencies.distill.clearDistillFailure("job", job.job.id);
      return removed;
    }
    if (phase === 4) {
      const stored = parseStoredSessionObservation(this.metadataPayload(row));
      const observation = stored.observation;
      if (!stored.captured || !this.metadataSourceEligible(observation.sessionFile, observation.sessionId)) return false;
      return this.connection.database.prepare("DELETE FROM session_observations WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 5) {
      const usage = parseStoredSessionUsage(this.metadataPayload(row));
      if (usage.state !== "released" || !this.metadataSourceEligible(usage.sessionFile, usage.sessionId)) return false;
      return this.connection.database.prepare("DELETE FROM session_usages WHERE rowid = ?").run(rowId).changes === 1;
    }
    if (phase === 6) {
      validateAbsolutePath(row["sessionFile"], "stored managed source path");
      validateText(row["sessionId"], "stored managed source session ID");
      validateText(row["generation"], "stored managed source generation");
      if (row["deleted"] !== 1) return false;
      const eligible = this.metadataSourceEligible(row["sessionFile"], row["sessionId"]);
      if (!eligible || eligible.generation !== row["generation"] ||
        this.hasOperationalSourceMetadata(row["sessionFile"])) return false;
      return this.connection.database.prepare(
        "DELETE FROM managed_sources WHERE row_id = ? AND generation = ? AND deleted = 1",
      ).run(rowId, eligible.generation).changes === 1;
    }
    if (phase === 7) {
      const evidence = parseEvidenceRow(this.metadataPayload(row));
      if (this.connection.database.prepare("SELECT 1 AS present FROM memory_origins WHERE evidence_id = ? LIMIT 1").get(evidence.evidenceId) !== undefined) return false;
      return this.connection.database.prepare("DELETE FROM memory_evidence WHERE rowid = ?").run(rowId).changes === 1;
    }
    throw new Error("Metadata collection phase is invalid.");
  }

  private metadataPayload(row: Record<string, unknown>): Record<string, unknown> {
    const { gcRow: _cursor, ...payload } = row;
    return payload;
  }

  private metadataSourceEligible(sessionFile: string, sessionId: string): { readonly generation: string; } | undefined {
    const source = this.dependencies.sources.get(sessionFile, sessionId);
    if (!source || !source.deleted) return undefined;
    const receipt = this.dependencies.retention.getDeletion(sessionFile);
    if (receipt?.state !== "complete" || receipt.sessionId !== sessionId || receipt.sourceGeneration !== source.generation) return undefined;
    if (this.dependencies.sources.hasDependents(sessionFile) || this.hasLiveSourceReferences(sessionFile)) return undefined;
    return { generation: source.generation };
  }

  private hasLiveSourceReferences(sessionFile: string): boolean {
    const filter = this.dependencies.retention.scopeFilter([sessionFile]);
    for (const [table, column, condition] of [
      ["consolidation_jobs", "json_extract(job_json, '$.sessionFile')", "status = 'pending'"],
      ["distill_triggers", "json_extract(trigger_json, '$.sessionFile')", "status <> 'complete'"],
      ["session_observations", "session_file", "captured = 0"],
      ["session_usages", "session_file", "state <> 'released'"],
    ] as const) {
      if (this.connection.database.prepare(`SELECT 1 AS present FROM ${table} WHERE ${condition} AND ${filter.sql(column)} LIMIT 1`)
        .get(...filter.values) !== undefined) return true;
    }
    return false;
  }

  private hasOperationalSourceMetadata(sessionFile: string): boolean {
    const filter = this.dependencies.retention.scopeFilter([sessionFile]);
    const queries = [
      ["clean_tasks", "json_extract(task_json, '$.sessionFile')"],
      ["consolidation_jobs", "json_extract(job_json, '$.sessionFile')"],
      ["distill_triggers", "json_extract(trigger_json, '$.sessionFile')"],
      ["session_observations", "session_file"],
      ["session_usages", "session_file"],
    ] as const;
    for (const [table, column] of queries) {
      if (this.connection.database.prepare(`SELECT 1 AS present FROM ${table} WHERE ${filter.sql(column)} LIMIT 1`)
        .get(...filter.values) !== undefined) return true;
    }
    return this.connection.database.prepare(
      "SELECT 1 AS present FROM distill_trigger_jobs AS links " +
      "JOIN distill_triggers AS triggers ON triggers.id = links.trigger_id WHERE " +
      filter.sql("json_extract(triggers.trigger_json, '$.sessionFile')") + " LIMIT 1",
    ).get(...filter.values) !== undefined;
  }

  backfillInTransaction(limit: number): boolean {
    const state = this.connection.database.prepare("SELECT phase, cursor_row FROM source_backfill WHERE singleton = 1").get();
    if (!state || typeof state["phase"] !== "number" || typeof state["cursor_row"] !== "number") throw new Error("Invalid source backfill progress.");
    let phase = state["phase"], cursor = state["cursor_row"], remaining = limit;
    const tables = ["consolidation_jobs", "distill_triggers", "clean_tasks", "session_observations", "session_usages"];
    if (!Number.isSafeInteger(phase) || phase < 0 || phase > tables.length || !Number.isSafeInteger(cursor) || cursor < 0) throw new Error("Invalid source backfill progress.");
    while (phase < tables.length && remaining > 0) {
      const table = tables[phase]!;
      const columns = [jobColumns, triggerColumns, cleanTaskColumns, observationColumns, sessionUsageColumns][phase]!;
      const rows = this.connection.database.prepare(`SELECT rowid AS cursorRow, ${columns} FROM ${table} WHERE rowid > ? ORDER BY rowid LIMIT ?`).all(cursor, remaining);
      for (const raw of rows) {
        const { cursorRow, ...fields } = this.connection.decodeRow(raw);
        validateNow(cursorRow, "Source backfill cursor");
        let source: { sessionId: string; sessionFile: string | null; cwd: string ;};
        if (phase === 0) source = parsePersistedJobRow(fields).job;
        else if (phase === 1) source = this.dependencies.distill.parseTriggerRow(fields).trigger;
        else if (phase === 2) source = parsePersistedCleanTask(fields).task;
        else if (phase === 3) source = parseStoredSessionObservation(fields).observation;
        else source = parseStoredSessionUsage(fields);
        if (source.sessionFile !== null) {
          this.dependencies.sources.register({ sessionId: source.sessionId, sessionFile: source.sessionFile, cwd: source.cwd });
          const receipt = this.dependencies.retention.getDeletion(source.sessionFile);
          if (receipt?.state === "complete" && receipt.sessionId === source.sessionId) this.dependencies.sources.markDeleted(source.sessionFile, source.sessionId);
        }
        cursor = cursorRow;
      }
      remaining -= rows.length;
      if (rows.length === 0) { phase++; cursor = 0; }
    }
    this.connection.database.prepare("UPDATE source_backfill SET phase = ?, cursor_row = ? WHERE singleton = 1").run(phase, cursor);
    if (phase === tables.length) this.dependencies.sources.setBackfillComplete(true);
    return phase === tables.length;
  }
}
