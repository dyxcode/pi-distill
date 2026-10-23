import { createHash } from "node:crypto";
import type {
  DistillTrigger
} from "../distill/tasks.ts";
import type {
  ConsolidationCommit,
  ConsolidationEvidenceBoundary,
  ConsolidationJob,
  SessionObservation,
} from "../distill/types.ts";
import { ConsolidationConflictError } from '../distill/types.ts';
import type { MemoryOrigin } from '../memory/types.ts';
import { MemoryConflictError, MemoryNotFoundError } from '../memory/types.ts';
import type { SessionUsage } from "../retention/types.ts";
import { evidenceColumns, jobColumns, observationColumns, progressColumns, sessionUsageColumns, textColumn, triggerColumns } from './columns.ts';
import type { StorageConnection } from './connection.ts';
import type { DistillQueue } from './distill.ts';
import { parseConsolidationCommit, parseEvidenceRow, parseMemoryOriginRow, parsePersistedJobRow, parseProgressRow } from './evidence-validation.ts';
import { maxSessionMetadataRows } from './limits.ts';
import type { Memories } from './memories.ts';
import { parseMemory } from './memory-validation.ts';
import type { MemoryEvidence } from './records.ts';
import type { RetentionTasks } from './retention.ts';
import { parseStoredSessionObservation, parseStoredSessionUsage } from './session-validation.ts';
import type { SessionRecords } from './sessions.ts';
import { SourceRegistry } from "./source-registry.ts";
import { fields, parseSourceKeys, validateText } from './validation.ts';
interface Dependencies {
  readonly retention: Pick<RetentionTasks, "scopeFilter" | "getDeletion">;
  readonly distill: Pick<DistillQueue, "distillTriggerItem" | "parseTriggerRow" | "assertCurrentDistillLease" | "clearDistillFailure" | "finishConsolidationJob" | "clearDistillLease">;
  readonly memories: Pick<Memories, "get" | "insertStatement" | "upsertOriginStatement" | "updateStatement" | "deleteOriginStatement" | "deleteStatement">;
  readonly sessions: Pick<SessionRecords, "assertSourceEnqueueAllowed">;
  readonly sources: SourceRegistry;
}

export class ConsolidationCommits {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }

  memoryOrigin(id: string): MemoryOrigin | undefined {
    this.connection.ensureOpen();
    validateText(id, "ID");
    const row = this.connection.database.prepare(
      "SELECT " + [
        textColumn("m.id", "currentId"), textColumn("m.content", "content"),
        textColumn("m.created_at", "createdAt"), "m.revision AS currentRevision",
        textColumn("m.updated_at", "updatedAt"), textColumn("o.memory_id", "memoryId"),
        textColumn("o.evidence_id", "originEvidenceId"), "o.revision AS originRevision",
        textColumn("e.evidence_id", "evidenceId"), textColumn("e.session_id", "sessionId"),
        textColumn("e.session_file", "sessionFile"), textColumn("e.source_generation", "sourceGeneration"),
        textColumn("e.cwd", "cwd"), textColumn("e.leaf_id", "leafId"),
        textColumn("e.source_keys", "sourceKeys"), textColumn("e.lineage_session_ids", "lineageSessionIds"),
        textColumn("e.source_time", "sourceTime"), textColumn("e.branch_anchor", "branchAnchor"),
      ].join(", ") + " " +
      "FROM memories AS m " +
      "LEFT JOIN memory_origins AS o ON o.memory_id = m.id " +
      "LEFT JOIN memory_evidence AS e ON e.evidence_id = o.evidence_id " +
      "WHERE m.id = ?",
    ).get(id);
    if (row === undefined) return undefined;
    const {
      currentId,
      content,
      createdAt,
      currentRevision,
      updatedAt,
      memoryId,
      originEvidenceId,
      originRevision,
      evidenceId,
      sessionId,
      sessionFile,
      sourceGeneration,
      cwd,
      leafId,
      sourceKeys,
      lineageSessionIds,
      sourceTime,
      branchAnchor,
    } = fields(this.connection.decodeRow(row), [
      "currentId",
      "content",
      "createdAt",
      "currentRevision",
      "updatedAt",
      "memoryId",
      "originEvidenceId",
      "originRevision",
      "evidenceId",
      "sessionId",
      "sessionFile",
      "sourceGeneration",
      "cwd",
      "leafId",
      "sourceKeys",
      "lineageSessionIds",
      "sourceTime",
      "branchAnchor",
    ]);
    const current = parseMemory({
      id: currentId,
      content,
      createdAt,
      revision: currentRevision,
      updatedAt,
    });
    if (memoryId === null) {
      if (originEvidenceId !== null || originRevision !== null || evidenceId !== null || sessionId !== null ||
        sessionFile !== null || sourceGeneration !== null || cwd !== null || leafId !== null || sourceKeys !== null ||
        lineageSessionIds !== null || sourceTime !== null || branchAnchor !== null) {
        throw new TypeError("Stored memory origin row is inconsistent.");
      }
      return undefined;
    }
    const origin = parseMemoryOriginRow({ memoryId, evidenceId: originEvidenceId, revision: originRevision });
    if (origin.memoryId !== current.id || origin.revision !== current.revision) return undefined;
    if (evidenceId === null) throw new TypeError("Stored memory origin references missing evidence.");
    const evidence = parseEvidenceRow({
      evidenceId, sessionId, sessionFile, sourceGeneration, cwd, leafId, sourceKeys,
      lineageSessionIds, sourceTime, branchAnchor,
    });
    return {
      sessionId: evidence.sessionId,
      sourceKeys: [...evidence.sourceKeys],
      ...(evidence.lineageSessionIds === undefined
        ? {}
        : { lineageSessionIds: [...evidence.lineageSessionIds] }),
      ...(evidence.sourceTime === undefined ? {} : { sourceTime: evidence.sourceTime }),
      ...(evidence.branchAnchor === undefined ? {} : { branchAnchor: evidence.branchAnchor }),
    };
  }

  processedSources(keys: readonly string[]): readonly string[] {
    this.connection.ensureOpen();
    const requested = parseSourceKeys(keys, "processed source keys", true);
    const select = this.connection.database.prepare(
      `SELECT ${progressColumns} FROM consolidation_progress WHERE source_key = ?`,
    );
    const result: string[] = [];
    for (const key of requested) {
      const row = select.get(key);
      if (row !== undefined) {
        const processed = parseProgressRow(this.connection.decodeRow(row));
        result.push(processed.sourceKey);
      }
    }
    return result;
  }

  /** One snapshot; queued source triggers count as unresolved retention evidence. */
  readConsolidationStatus(keys: readonly string[], scope?: readonly string[]): {
    readonly processedSources: readonly string[];
    readonly pendingJobs: readonly ConsolidationJob[];
    readonly pendingTriggers: readonly DistillTrigger[];
    readonly observations: readonly SessionObservation[];
    readonly usages: readonly SessionUsage[];
  } {
    this.connection.ensureOpen();
    return this.connection.transaction(() => {
      const filter = this.dependencies.retention.scopeFilter(scope);
      const rows = (table: string, columns: string, path: string, condition = "1") => {
        const items = this.connection.database.prepare(`SELECT ${columns} FROM ${table} WHERE ${condition} AND ${filter.sql(path)} ORDER BY rowid LIMIT ?`)
          .all(...filter.values, maxSessionMetadataRows + 1);
        if (items.length > maxSessionMetadataRows) throw new RangeError("Source metadata exceeds its assessment budget.");
        return items;
      };
      return {
        processedSources: this.processedSources(keys),
        pendingJobs: rows("consolidation_jobs", jobColumns, "json_extract(job_json, '$.sessionFile')", "status = 'pending'")
          .map(row => parsePersistedJobRow(this.connection.decodeRow(row)).job),
        pendingTriggers: rows("distill_triggers", triggerColumns, "json_extract(trigger_json, '$.sessionFile')", "status <> 'complete'")
          .map(row => this.dependencies.distill.distillTriggerItem(this.dependencies.distill.parseTriggerRow(row))),
        observations: rows("session_observations", observationColumns, "session_file")
          .map(row => parseStoredSessionObservation(this.connection.decodeRow(row)).observation),
        usages: rows("session_usages", sessionUsageColumns, "session_file")
          .map(row => parseStoredSessionUsage(this.connection.decodeRow(row))),
      };
    }, "DEFERRED");
  }

  commitConsolidation(commit: ConsolidationCommit): boolean {
    this.connection.ensureOpen();
    const request = parseConsolidationCommit(commit);
    return this.connection.transaction(() => {
      const row = this.connection.database.prepare(
        `SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`,
      ).get(request.jobId);
      if (row === undefined) {
        throw new ConsolidationConflictError(request.jobId, "does not exist");
      }
      const current = parsePersistedJobRow(this.connection.decodeRow(row));
      if (current.status === "complete") return false;
      if (
        current.leaseOwner !== request.owner ||
        current.leaseUntil === null ||
        current.leaseUntil <= request.now
      ) {
        throw new ConsolidationConflictError(request.jobId, "has no current lease for this owner");
      }
      this.dependencies.distill.assertCurrentDistillLease("job", request.jobId, request.owner, request.now);

      const selectProgress = this.connection.database.prepare(
        `SELECT ${progressColumns} FROM consolidation_progress WHERE source_key = ?`,
      );
      const processed = new Set<string>();
      for (const key of current.job.sourceKeys) {
        const processedRow = selectProgress.get(key);
        if (processedRow !== undefined) processed.add(parseProgressRow(this.connection.decodeRow(processedRow)).sourceKey);
      }
      const remaining = current.job.sourceKeys.filter((key) => !processed.has(key));
      const requestedSources = request.sourceKeys ?? remaining;
      if (remaining.length === 0) {
        if (requestedSources.length !== 0) {
          throw new ConsolidationConflictError(request.jobId, "source set changed after the batch was prepared");
        }
        if (request.creates.length > 0 || request.updates.length > 0 || request.deletes.length > 0 ||
          request.evidence !== undefined) {
          throw new ConsolidationConflictError(request.jobId, "has no remaining source keys for memory changes");
        }
        const completion = this.connection.database.prepare(
          "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL, " +
          "retry_category = NULL, retry_at = NULL " +
          "WHERE id = ? AND status = 'pending'",
        ).run(request.jobId);
        if (completion.changes !== 1) {
          throw new ConsolidationConflictError(request.jobId, "changed before completion");
        }
        this.dependencies.distill.clearDistillFailure("job", request.jobId);
        this.dependencies.distill.finishConsolidationJob(current.job, request.owner);
        return false;
      }
      if (
        requestedSources.length === 0 ||
        requestedSources.length > remaining.length ||
        requestedSources.some((key, index) => key !== remaining[index])
      ) {
        throw new ConsolidationConflictError(request.jobId, "source set changed after the batch was prepared");
      }
      if (requestedSources.length < remaining.length && request.evidence === undefined) {
        throw new ConsolidationConflictError(request.jobId, "partial source progress requires an evidence boundary");
      }

      const evidence = this.persistMemoryEvidence(
        current.job,
        requestedSources,
        request.evidence ?? current.job,
      );

      for (const record of request.creates) {
        const existing = this.dependencies.memories.get(record.id);
        let revision = 1;
        if (existing === undefined) {
          this.dependencies.memories.insertStatement.run(record.id, record.content, record.createdAt, record.createdAt);
        } else if (existing.content !== record.content || existing.createdAt !== record.createdAt) {
          throw new MemoryConflictError(record.id);
        } else {
          revision = existing.revision;
        }
        this.dependencies.memories.upsertOriginStatement.run(record.id, evidence.evidenceId, revision);
      }
      for (const update of request.updates) {
        const existing = this.dependencies.memories.get(update.id);
        if (existing === undefined) throw new MemoryNotFoundError(update.id);
        if (existing.revision !== update.expectedRevision) {
          if (
            existing.revision === update.expectedRevision + 1 &&
            existing.content === update.content &&
            existing.updatedAt === update.updatedAt
          ) {
            this.dependencies.memories.upsertOriginStatement.run(update.id, evidence.evidenceId, existing.revision);
            continue;
          }
          throw new MemoryConflictError(update.id);
        }
        if (existing.content === update.content) {
          this.dependencies.memories.upsertOriginStatement.run(update.id, evidence.evidenceId, existing.revision);
          continue;
        }
        if (existing.revision === Number.MAX_SAFE_INTEGER) {
          throw new RangeError("Memory revision limit reached.");
        }
        const revision = existing.revision + 1;
        this.dependencies.memories.updateStatement.run(
          update.content,
          revision,
          update.updatedAt,
          existing.id,
        );
        this.dependencies.memories.upsertOriginStatement.run(update.id, evidence.evidenceId, revision);
      }
      for (const removal of request.deletes) {
        const existing = this.dependencies.memories.get(removal.id);
        if (existing === undefined) {
          this.dependencies.memories.deleteOriginStatement.run(removal.id);
          continue;
        }
        if (existing.revision !== removal.expectedRevision) {
          throw new MemoryConflictError(removal.id);
        }
        this.dependencies.memories.deleteStatement.run(removal.id);
        this.dependencies.memories.deleteOriginStatement.run(removal.id);
      }

      const insertProgress = this.connection.database.prepare(
        "INSERT INTO consolidation_progress (source_key, processed_at) VALUES (?, ?)",
      );
      for (const key of requestedSources) insertProgress.run(key, request.now);
      const finalPrefix = requestedSources.length === remaining.length;
      const jobUpdate = finalPrefix
        ? this.connection.database.prepare(
          "UPDATE consolidation_jobs SET status = 'complete', lease_owner = NULL, lease_until = NULL, " +
          "retry_category = NULL, retry_at = NULL " +
          "WHERE id = ? AND status = 'pending' AND lease_owner = ? AND lease_until > ?",
        ).run(request.jobId, request.owner, request.now)
        : this.connection.database.prepare(
          "UPDATE consolidation_jobs SET lease_owner = NULL, lease_until = NULL, retry_category = NULL, retry_at = NULL " +
          "WHERE id = ? AND status = 'pending' AND lease_owner = ? AND lease_until > ?",
        ).run(request.jobId, request.owner, request.now);
      if (jobUpdate.changes !== 1) {
        throw new ConsolidationConflictError(request.jobId, "changed before prefix progress was committed");
      }
      this.dependencies.distill.clearDistillFailure("job", request.jobId);
      if (finalPrefix) this.dependencies.distill.finishConsolidationJob(current.job, request.owner);
      else this.dependencies.distill.clearDistillLease();
      return true;
    });
  }

  persistMemoryEvidence(
    job: ConsolidationJob,
    sourceKeys: readonly string[] = job.sourceKeys,
    boundary: ConsolidationEvidenceBoundary = job,
    allowDeletedSource = false,
  ): MemoryEvidence {
    if (job.sessionFile !== null && !allowDeletedSource) {
      this.dependencies.sessions.assertSourceEnqueueAllowed(job.sessionId, job.sessionFile, job.cwd);
    }
    let source = job.sessionFile === null ? undefined : this.dependencies.sources.get(job.sessionFile, job.sessionId);
    if (job.sessionFile !== null && !source) {
      source = this.dependencies.sources.register({ sessionId: job.sessionId, sessionFile: job.sessionFile, cwd: job.cwd });
    }
    if (allowDeletedSource && job.sessionFile !== null) {
      const receipt = this.dependencies.retention.getDeletion(job.sessionFile);
      if (receipt?.state === "complete" && receipt.sessionId === job.sessionId) {
        this.dependencies.sources.markDeleted(job.sessionFile, job.sessionId);
        source = this.dependencies.sources.get(job.sessionFile, job.sessionId);
      }
    } else if (source?.deleted) {
      throw new ConsolidationConflictError(job.id, "belongs to a deleted source generation");
    }
    const evidence: MemoryEvidence = {
      evidenceId: createHash("sha256").update(JSON.stringify([
        job.id,
        source?.generation ?? null,
        sourceKeys,
        boundary.sourceTime ?? null,
        boundary.branchAnchor ?? null,
      ])).digest("hex"),
      sessionId: job.sessionId,
      sessionFile: job.sessionFile,
      sourceGeneration: source?.generation ?? null,
      cwd: job.cwd,
      leafId: job.leafId,
      sourceKeys: [...sourceKeys],
      ...(job.lineageSessionIds === undefined ? {} : { lineageSessionIds: job.lineageSessionIds }),
      ...(boundary.sourceTime === undefined ? {} : { sourceTime: boundary.sourceTime }),
      ...(boundary.branchAnchor === undefined ? {} : { branchAnchor: boundary.branchAnchor }),
    };
    if ((evidence.sessionFile === null) !== (evidence.sourceGeneration === null)) {
      throw new ConsolidationConflictError(job.id, "has no stable source generation for memory provenance");
    }
    const raw = this.connection.database.prepare(
      `SELECT ${evidenceColumns} FROM memory_evidence WHERE evidence_id = ?`,
    ).get(evidence.evidenceId);
    if (raw !== undefined) {
      const stored = parseEvidenceRow(this.connection.decodeRow(raw));
      if (JSON.stringify(stored) !== JSON.stringify(evidence)) {
        throw new ConsolidationConflictError(job.id, "already has different immutable memory provenance");
      }
      return stored;
    }
    this.connection.database.prepare(
      "INSERT INTO memory_evidence " +
      "(evidence_id, session_id, session_file, source_generation, cwd, leaf_id, source_keys, " +
      "lineage_session_ids, source_time, branch_anchor) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    ).run(
      evidence.evidenceId,
      evidence.sessionId,
      evidence.sessionFile,
      evidence.sourceGeneration,
      evidence.cwd,
      evidence.leafId,
      JSON.stringify(evidence.sourceKeys),
      evidence.lineageSessionIds === undefined ? null : JSON.stringify(evidence.lineageSessionIds),
      evidence.sourceTime ?? null,
      evidence.branchAnchor ?? null,
    );
    return evidence;
  }
}
