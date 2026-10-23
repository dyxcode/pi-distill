import { dirname } from "node:path";
import type {
  ConsolidationJob
} from "../distill/types.ts";
import { cleanTaskColumns, distillFailureColumns, evidenceColumns, jobColumns, legacyJobColumns, memoryColumns, observationColumns, originColumns, originColumnsV6, progressColumns, progressColumnsV6, schemaColumns, sessionUsageColumns, textColumn, triggerColumns } from './columns.ts';
import type { ConsolidationCommits } from './commits.ts';
import type { StorageConnection } from './connection.ts';
import { SOURCE_COVERAGE_SCHEMAS } from "./coverage.ts";
import type { DistillQueue } from './distill.ts';
import { parseEvidenceRow, parseMemoryOriginRow, parseMemoryOriginRowV6, parsePersistedJobRow, parseProgressRow, parseProgressRowV6 } from './evidence-validation.ts';
import type { MetadataMaintenance } from './maintenance.ts';
import { parseInput, parseMemory } from './memory-validation.ts';
import type { MemoryEvidence, PersistedDistillTrigger, SchemaColumn } from './records.ts';
import type { RetentionTasks } from './retention.ts';
import { DISTILL_SCHEDULE_SCHEMAS } from "./schedule.ts";
import { cleanTasksSchema, consolidationJobsSchema, consolidationJobsV3Schema, consolidationProgressSchema, consolidationProgressSchemaV6, currentSchema, deletionsSchema, deletionsSchemaV5, distillFailuresSchema, distillLeaseSchema, distillTriggersSchema, latestSchemaVersion, legacySchema, memoryEvidenceSchema, memoryOriginsSchema, memoryOriginsSchemaV6, metadataGcSchema, sessionObservationsSchema, sessionUsagesSchema, sourceAccessSchema, sourceBackfillSchema, storeIndexes, storeIndexesV7, storeSchemas, storeSchemasV7, storeSchemasV8, triggerJobsSchema } from './schema-definitions.ts';
import { parseStoredSessionObservation, parseStoredSessionUsage } from './session-validation.ts';
import { SOURCE_INDEX_SCHEMAS } from "./source-index.ts";
import { SOURCE_REGISTRY_SCHEMAS, SourceRegistry, initializeSourceRegistry } from "./source-registry.ts";
import { SOURCE_VIEW_SCHEMAS } from "./source-view.ts";
import { parsePersistedCleanTask } from './task-validation.ts';
import { fields, validateAbsolutePath, validateText } from './validation.ts';
interface Dependencies {
  readonly retention: Pick<RetentionTasks, "sourceAccess">;
  readonly maintenance: Pick<MetadataMaintenance, "backfillInTransaction">;
  readonly commits: Pick<ConsolidationCommits, "persistMemoryEvidence">;
  readonly sources: SourceRegistry;
  readonly distill: Pick<DistillQueue, "parseTriggerRow" | "parseDistillFailureRow" | "readDistillLease">;
}

export class SchemaManager {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }

  initializeSchema(): void {
    const version = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (version !== 0 && version !== 1 && version !== 2 && version !== 3 && version !== 4 && version !== 5 && version !== 6 && version !== 7 && version !== 8 && version !== 9 && version !== 10 && version !== 11 && version !== latestSchemaVersion) {
      throw new Error("Unsupported memory database schema version: " + String(version) + ".");
    }
    if (version === latestSchemaVersion) {
      this.validateLatestSchema();
      this.validateMemoryRows();
      this.dependencies.retention.sourceAccess();
      return;
    }
    const objects = this.connection.database.prepare(
      `SELECT ${schemaColumns} FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`,
    ).all().map((row) => this.connection.decodeRow(row));
    if (version === 0 && objects.length === 0) {
      this.connection.database.exec(currentSchema);
      this.createAuxiliaryTables();
      this.connection.database.exec("PRAGMA user_version = 3");
    } else if (version === 0) {
      if (
        objects.length !== 1 ||
        objects[0]?.["name"] !== "memories" ||
        objects[0]?.["type"] !== "table"
      ) throw new Error("Unsupported memory database schema.");
      this.validateTable(
        "memories",
        objects[0]?.["sql"],
        legacySchema,
        [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      );
      for (const row of this.connection.database.prepare(
        `SELECT ${textColumn("id")}, ${textColumn("content")}, ${textColumn("created_at", "createdAt")} FROM memories`,
      ).iterate()) parseInput(this.connection.decodeRow(row));
      this.connection.database.exec("ALTER TABLE memories RENAME TO memories_legacy");
      this.connection.database.exec(currentSchema);
      this.connection.database.exec(
        "INSERT INTO memories (id, content, created_at, revision, updated_at) " +
        "SELECT id, content, created_at, 1, created_at FROM memories_legacy; " +
        "DROP TABLE memories_legacy",
      );
      this.createAuxiliaryTables();
      this.connection.database.exec("PRAGMA user_version = 3");
    } else if (version === 1) {
      if (
        objects.length !== 1 ||
        objects[0]?.["name"] !== "memories" ||
        objects[0]?.["type"] !== "table"
      ) throw new Error("Unsupported memory database schema.");
      this.validateTable(
        "memories",
        objects[0]?.["sql"],
        currentSchema,
        [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "updated_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      );
      this.validateMemoryRows();
      this.createAuxiliaryTables();
      this.connection.database.exec("PRAGMA user_version = 3");
    } else if (version === 2) {
      this.validateSchema(false, true);
      this.validateAllRows(false, true);
      this.connection.database.exec(sessionUsagesSchema);
      this.connection.database.exec("PRAGMA user_version = 3");
    } else if (version === 3) {
      this.validateSchema(true, true);
      this.validateAllRows(true, true);
    }
    if (version <= 3) this.migrateV3ToV4();
    if (version < 5) {
      this.validateSchema(true);
      this.validateAllRows();
      this.connection.database.exec(sourceAccessSchema);
      this.connection.database.exec(deletionsSchemaV5);
      this.connection.database.exec("PRAGMA user_version = 5");
    }
    if (version < 6) {
      this.validateLatestSchema();
      this.validateAllRows();
      initializeSourceRegistry(this.connection.database);
      for (const sql of [sourceBackfillSchema, ...storeIndexes]) this.connection.database.exec(sql);
      this.connection.database.prepare("INSERT INTO source_backfill VALUES (1,0,0)").run();
      this.dependencies.maintenance.backfillInTransaction(128);
      this.connection.database.exec("PRAGMA user_version = 6");
    } else {
      this.validateSchema(true);
      this.validateAllRows();
      initializeSourceRegistry(this.connection.database);
    }
    if (version <= 6) this.migrateV6ToV7();
    if (version < 8) this.migrateV7ToV8();
    if (version < 9) {
      for (const schema of SOURCE_INDEX_SCHEMAS) this.connection.database.exec(schema.sql);
      this.connection.database.exec("PRAGMA user_version = 9");
    }
    if (version < 10) {
      for (const schema of SOURCE_VIEW_SCHEMAS) this.connection.database.exec(schema.sql);
      this.connection.database.exec("PRAGMA user_version = 10");
    }
    if (version < 11) {
      for (const schema of SOURCE_COVERAGE_SCHEMAS) this.connection.database.exec(schema.sql);
      this.connection.database.exec("INSERT INTO source_derived_gc VALUES (1,0,0,0); PRAGMA user_version = 11");
    }
    for (const schema of DISTILL_SCHEDULE_SCHEMAS) this.connection.database.exec(schema.sql);
    this.connection.database.exec("INSERT INTO distill_schedule_state VALUES (1,0,0,0); PRAGMA user_version = 12");
    this.validateLatestSchema();
    this.validateAllRows();
    this.dependencies.retention.sourceAccess();
  }

  private migrateV6ToV7(): void {
    const currentVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 6) throw new Error("Memory schema migration expected version 6.");
    this.validateSchema(true);
    this.validateAllRows();

    this.connection.database.exec(
      "ALTER TABLE memory_origins RENAME TO memory_origins_v6; " +
      "ALTER TABLE consolidation_progress RENAME TO consolidation_progress_v6; " +
      "ALTER TABLE session_deletions RENAME TO session_deletions_v6; " +
      memoryEvidenceSchema + "; " + memoryOriginsSchema + "; " + consolidationProgressSchema + "; " + deletionsSchema + "; " +
      metadataGcSchema + "; " + storeIndexesV7.join("; "),
    );
    this.connection.database.prepare("INSERT INTO metadata_gc_state VALUES (1, 0, 0)").run();

    const originRows = this.connection.database.prepare(
      `SELECT ${originColumnsV6} FROM memory_origins_v6 ORDER BY rowid`,
    ).all();
    const insertOrigin = this.connection.database.prepare(
      "INSERT INTO memory_origins (memory_id, evidence_id, revision) VALUES (?, ?, ?)",
    );
    const originsToMigrate: { memoryId: string; job: ConsolidationJob; revision: number; }[] = [];
    for (const raw of originRows) {
      const origin = parseMemoryOriginRowV6(this.connection.decodeRow(raw));
      const jobRow = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(origin.jobId);
      if (jobRow === undefined) throw new TypeError("Stored memory origin references a missing consolidation job.");
      const job = parsePersistedJobRow(this.connection.decodeRow(jobRow));
      if (job.status !== "complete") throw new TypeError("Stored memory origin references an incomplete consolidation job.");
      originsToMigrate.push({ memoryId: origin.memoryId, job: job.job, revision: origin.revision });
    }
    for (const origin of originsToMigrate) {
      const evidence = this.dependencies.commits.persistMemoryEvidence(origin.job, origin.job.sourceKeys, origin.job, true);
      insertOrigin.run(origin.memoryId, evidence.evidenceId, origin.revision);
    }

    const progressRows = this.connection.database.prepare(
      `SELECT ${progressColumnsV6} FROM consolidation_progress_v6 ORDER BY rowid`,
    ).all();
    const insertProgress = this.connection.database.prepare(
      "INSERT INTO consolidation_progress (source_key, processed_at) VALUES (?, ?)",
    );
    for (const raw of progressRows) {
      const progress = parseProgressRowV6(this.connection.decodeRow(raw));
      const jobRow = this.connection.database.prepare(`SELECT ${jobColumns} FROM consolidation_jobs WHERE id = ?`).get(progress.jobId);
      if (jobRow === undefined || !parsePersistedJobRow(this.connection.decodeRow(jobRow)).job.sourceKeys.includes(progress.sourceKey)) {
        throw new TypeError("Stored source progress does not match its consolidation job.");
      }
      insertProgress.run(progress.sourceKey, progress.processedAt);
    }

    const deletionRows = this.connection.database.prepare(
      "SELECT " + [textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
      textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"), textColumn("state")].join(", ") +
      " FROM session_deletions_v6 ORDER BY rowid",
    ).all();
    const insertDeletion = this.connection.database.prepare(
      "INSERT INTO session_deletions (session_file, session_id, intent_id, file_version, source_generation, state) " +
      "VALUES (?, ?, ?, ?, ?, ?)",
    );
    for (const raw of deletionRows) {
      const row = this.connection.decodeRow(raw);
      validateAbsolutePath(row["sessionFile"], "Stored deletion path");
      validateText(row["sessionId"], "Stored deletion session ID");
      validateText(row["intentId"], "Stored deletion intent ID");
      validateText(row["fileVersion"], "Stored deletion file version");
      if (!/^\d+:\d+:\d+:\d+:\d+$/.test(row["fileVersion"])) throw new TypeError("Invalid deletion file version.");
      if (row["state"] !== "deleting" && row["state"] !== "complete") throw new TypeError("Invalid deletion state.");
      let source = this.dependencies.sources.get(row["sessionFile"], row["sessionId"]);
      if (!source) {
        source = this.dependencies.sources.register({
          sessionId: row["sessionId"],
          sessionFile: row["sessionFile"],
          cwd: dirname(row["sessionFile"]),
        });
      }
      if (row["state"] === "complete") this.dependencies.sources.markDeleted(row["sessionFile"], row["sessionId"]);
      insertDeletion.run(row["sessionFile"], row["sessionId"], row["intentId"], row["fileVersion"], source.generation, row["state"]);
    }

    this.connection.database.exec(
      "DROP TABLE memory_origins_v6; DROP TABLE consolidation_progress_v6; DROP TABLE session_deletions_v6; " +
      "PRAGMA user_version = 7",
    );
  }

  private migrateV7ToV8(): void {
    const currentVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 7) throw new Error("Memory schema migration expected version 7.");
    this.validateSchema(true);
    this.validateAllRows();
    this.connection.database.exec(`${distillFailuresSchema}; PRAGMA user_version = 8`);
  }

  private migrateV3ToV4(): void {
    const currentVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (currentVersion !== 3) throw new Error("Memory schema migration expected version 3.");
    this.connection.database.exec(
      "ALTER TABLE consolidation_jobs RENAME TO consolidation_jobs_v3; " +
      consolidationJobsSchema + "; " +
      "INSERT INTO consolidation_jobs (rowid, id, job_json, status, lease_owner, lease_until, retry_category, retry_at) " +
      "SELECT rowid, id, job_json, status, lease_owner, lease_until, NULL, NULL " +
      "FROM consolidation_jobs_v3 ORDER BY rowid; " +
      "DROP TABLE consolidation_jobs_v3",
    );
    this.createTaskTables();
    this.connection.database.exec("PRAGMA user_version = 4");
  }

  private createAuxiliaryTables(): void {
    this.connection.database.exec(consolidationJobsV3Schema);
    this.connection.database.exec(consolidationProgressSchemaV6);
    this.connection.database.exec(memoryOriginsSchemaV6);
    this.connection.database.exec(sessionObservationsSchema);
    this.connection.database.exec(sessionUsagesSchema);
  }

  private createTaskTables(): void {
    this.connection.database.exec(distillTriggersSchema);
    this.connection.database.exec(triggerJobsSchema);
    this.connection.database.exec(distillLeaseSchema);
    this.connection.database.exec(cleanTasksSchema);
    this.connection.database.prepare(
      "INSERT INTO distill_lease (singleton, claim_token, worker_id, task_kind, task_id, lease_until) " +
      "VALUES (1, NULL, NULL, NULL, NULL, NULL)",
    ).run();
  }

  private validateLatestSchema(): void {
    this.validateSchema(true);
  }

  private validateSchema(includeSessionUsages: boolean, useLegacyJobSchema = false): void {
    const objects = this.connection.database.prepare(
      `SELECT ${schemaColumns} FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'`,
    ).all().map((row) => this.connection.decodeRow(row));
    const schemaVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    const usesV7Metadata = typeof schemaVersion === "number" && schemaVersion >= 7;
    const usesV8Metadata = typeof schemaVersion === "number" && schemaVersion >= 8;
    const schemas = new Map<string, { readonly sql: string; readonly columns: readonly SchemaColumn[]; }>([
      ["memories", {
        sql: currentSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "content", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "created_at", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "updated_at", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["consolidation_jobs", {
        sql: useLegacyJobSchema ? consolidationJobsV3Schema : consolidationJobsSchema,
        columns: useLegacyJobSchema ? [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ] : [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      }],
      ["consolidation_progress", {
        sql: usesV7Metadata ? consolidationProgressSchema : consolidationProgressSchemaV6,
        columns: usesV7Metadata ? [
          { name: "source_key", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "processed_at", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ] : [
          { name: "source_key", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "processed_at", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["memory_origins", {
        sql: usesV7Metadata ? memoryOriginsSchema : memoryOriginsSchemaV6,
        columns: usesV7Metadata ? [
          { name: "memory_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "evidence_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ] : [
          { name: "memory_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "revision", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
      ["session_observations", {
        sql: sessionObservationsSchema,
        columns: [
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "captured", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      }],
    ]);
    if (usesV7Metadata) {
      schemas.set("memory_evidence", {
        sql: memoryEvidenceSchema,
        columns: [
          { name: "evidence_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_file", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_generation", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_keys", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "lineage_session_ids", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "source_time", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "branch_anchor", type: "TEXT", notNull: 0, primaryKey: 0 },
        ],
      });
    }
    if (includeSessionUsages) {
      schemas.set("session_usages", {
        sql: sessionUsagesSchema,
        columns: [
          { name: "owner_id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "process_token", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "cwd", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "leaf_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
          { name: "state", type: "TEXT", notNull: 1, primaryKey: 0 },
        ],
      });
    }
    if (!useLegacyJobSchema && includeSessionUsages) {
      schemas.set("distill_triggers", {
        sql: distillTriggersSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "trigger_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "compaction_record_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
      schemas.set("distill_trigger_jobs", {
        sql: triggerJobsSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "trigger_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "job_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "ordinal", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ],
      });
      schemas.set("distill_lease", {
        sql: distillLeaseSchema,
        columns: [
          { name: "singleton", type: "INTEGER", notNull: 1, primaryKey: 1 },
          { name: "claim_token", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "worker_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "task_kind", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "task_id", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
      schemas.set("clean_tasks", {
        sql: cleanTasksSchema,
        columns: [
          { name: "id", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "task_json", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "status", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "wait_reason", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_category", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "retry_at", type: "INTEGER", notNull: 0, primaryKey: 0 },
          { name: "lease_owner", type: "TEXT", notNull: 0, primaryKey: 0 },
          { name: "lease_until", type: "INTEGER", notNull: 0, primaryKey: 0 },
        ],
      });
    }
    if (typeof schemaVersion === "number" && schemaVersion >= 5) {
      schemas.set("source_access", {
        sql: sourceAccessSchema, columns: [
          { name: "singleton", type: "INTEGER", notNull: 1, primaryKey: 1 },
          { name: "token", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "owner_pid", type: "INTEGER", notNull: 1, primaryKey: 0 },
        ]
      });
      schemas.set("session_deletions", {
        sql: usesV7Metadata ? deletionsSchema : deletionsSchemaV5, columns: [
          { name: "session_file", type: "TEXT", notNull: 1, primaryKey: 1 },
          { name: "session_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "intent_id", type: "TEXT", notNull: 1, primaryKey: 0 },
          { name: "file_version", type: "TEXT", notNull: 1, primaryKey: 0 },
          ...(usesV7Metadata ? [{ name: "source_generation", type: "TEXT" as const, notNull: 0 as const, primaryKey: 0 as const }] : []),
          { name: "state", type: "TEXT", notNull: 1, primaryKey: 0 },
        ]
      });
    }
    const additional = typeof schemaVersion === "number" && schemaVersion >= 6
      ? [...SOURCE_REGISTRY_SCHEMAS, ...(schemaVersion >= 9 ? SOURCE_INDEX_SCHEMAS : []), ...(schemaVersion >= 10 ? SOURCE_VIEW_SCHEMAS : []), ...(schemaVersion >= 11 ? SOURCE_COVERAGE_SCHEMAS : []), ...(schemaVersion >= 12 ? DISTILL_SCHEDULE_SCHEMAS : []), ...(usesV8Metadata ? storeSchemasV8 : usesV7Metadata ? storeSchemasV7 : storeSchemas)]
      : [];
    const extraSchemas = new Map(additional.map(schema => [schema.name, schema.sql]));
    if (objects.length !== schemas.size + extraSchemas.size) throw new Error("Unsupported memory database schema.");
    for (const object of objects) {
      const name = object["name"];
      if (typeof name === "string" && extraSchemas.has(name)) {
        const expected = extraSchemas.get(name)!;
        if (typeof object["sql"] !== "string" || object["sql"].replace(/\s+/g, "").toUpperCase() !== expected.replace(/\s+/g, "").toUpperCase()) throw new Error("Unsupported registry schema.");
        continue;
      }
      if (typeof name !== "string" || object["type"] !== "table") {
        throw new Error("Unsupported memory database schema.");
      }
      const schema = schemas.get(name);
      if (schema === undefined) throw new Error("Unsupported memory database schema.");
      this.validateTable(name, object["sql"], schema.sql, schema.columns);
    }
  }

  private validateTable(
    name: string,
    sql: unknown,
    expectedSql: string,
    expectedColumns: readonly SchemaColumn[],
  ): void {
    if (
      typeof sql !== "string" ||
      sql.replace(/\s+/g, "").toUpperCase() !== expectedSql.replace(/\s+/g, "").toUpperCase()
    ) throw new Error("Unsupported memory database schema.");
    const table = this.connection.database.prepare(
      "SELECT strict, wr FROM pragma_table_list WHERE schema = 'main' AND name = ?",
    ).get(name);
    const columns = this.connection.database.prepare(
      `SELECT ${textColumn("name")}, ${textColumn("type")}, "notnull", pk, hidden FROM pragma_table_xinfo(?)`,
    ).all(name).map((row) => this.connection.decodeRow(row));
    if (
      table?.["strict"] !== 1 || table["wr"] !== 0 || columns.length !== expectedColumns.length ||
      columns.some((column, index) => {
        const expected = expectedColumns[index];
        return expected === undefined ||
          column["name"] !== expected.name ||
          column["type"] !== expected.type ||
          column["notnull"] !== expected.notNull ||
          column["pk"] !== expected.primaryKey ||
          column["hidden"] !== 0;
      })
    ) throw new Error("Unsupported memory database schema.");
  }

  private validateMemoryRows(): void {
    for (const row of this.connection.database.prepare(
      `SELECT ${memoryColumns} FROM memories`,
    ).iterate()) parseMemory(this.connection.decodeRow(row));
  }

  private validateAllRows(includeSessionUsages = true, useLegacyJobColumns = false): void {
    this.validateMemoryRows();
    const schemaVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    const usesV7Metadata = typeof schemaVersion === "number" && schemaVersion >= 7;
    const jobs = this.connection.database.prepare(
      `SELECT ${useLegacyJobColumns ? legacyJobColumns : jobColumns} FROM consolidation_jobs`,
    ).all().map((row) => parsePersistedJobRow(this.connection.decodeRow(row)));
    const jobsById = new Map(jobs.map((record) => [record.job.id, record]));
    for (const row of this.connection.database.prepare(
      `SELECT ${usesV7Metadata ? progressColumns : progressColumnsV6} FROM consolidation_progress`,
    ).iterate()) {
      if (usesV7Metadata) {
        parseProgressRow(this.connection.decodeRow(row));
      } else {
        const progress = parseProgressRowV6(this.connection.decodeRow(row));
        const sourceJob = jobsById.get(progress.jobId);
        if (sourceJob === undefined || !sourceJob.job.sourceKeys.includes(progress.sourceKey)) {
          throw new TypeError("Stored source progress does not match its consolidation job.");
        }
      }
    }
    const evidence = new Map<string, MemoryEvidence>();
    if (usesV7Metadata) {
      for (const row of this.connection.database.prepare(`SELECT ${evidenceColumns} FROM memory_evidence`).iterate()) {
        const record = parseEvidenceRow(this.connection.decodeRow(row));
        if (evidence.has(record.evidenceId)) throw new TypeError("Stored memory evidence ID is duplicated.");
        evidence.set(record.evidenceId, record);
      }
    }
    for (const row of this.connection.database.prepare(
      `SELECT ${usesV7Metadata ? originColumns : originColumnsV6} FROM memory_origins`,
    ).iterate()) {
      if (usesV7Metadata) {
        const origin = parseMemoryOriginRow(this.connection.decodeRow(row));
        if (!evidence.has(origin.evidenceId)) throw new TypeError("Stored memory origin references missing evidence.");
      } else {
        const origin = parseMemoryOriginRowV6(this.connection.decodeRow(row));
        const sourceJob = jobsById.get(origin.jobId);
        if (sourceJob === undefined || sourceJob.status !== "complete") {
          throw new TypeError("Stored memory origin does not match a completed consolidation job.");
        }
      }
    }
    for (const row of this.connection.database.prepare(
      `SELECT ${observationColumns} FROM session_observations`,
    ).iterate()) parseStoredSessionObservation(this.connection.decodeRow(row));
    if (includeSessionUsages) {
      for (const row of this.connection.database.prepare(
        `SELECT ${sessionUsageColumns} FROM session_usages`,
      ).iterate()) parseStoredSessionUsage(this.connection.decodeRow(row));
      if (!useLegacyJobColumns) {
        const triggers = new Map<string, PersistedDistillTrigger>();
        for (const row of this.connection.database.prepare(`SELECT ${triggerColumns} FROM distill_triggers`).iterate()) {
          const trigger = this.dependencies.distill.parseTriggerRow(row);
          triggers.set(trigger.trigger.id, trigger);
          for (const jobId of trigger.jobIds) {
            const linkedJob = jobsById.get(jobId);
            if (
              linkedJob === undefined ||
              linkedJob.job.sessionId !== trigger.trigger.sessionId ||
              linkedJob.job.sessionFile !== trigger.trigger.sessionFile
            ) throw new TypeError("Stored distill trigger batch does not match its session source.");
            if (trigger.status === "complete" && linkedJob.status !== "complete") {
              throw new TypeError("Stored completed distill trigger has pending batches.");
            }
          }
        }
        if (typeof schemaVersion === "number" && schemaVersion >= 8) {
          for (const raw of this.connection.database.prepare(`SELECT ${distillFailureColumns} FROM distill_failures`).iterate()) {
            const failure = this.dependencies.distill.parseDistillFailureRow(raw);
            const status = failure.kind === "job"
              ? jobsById.get(failure.id)?.status
              : triggers.get(failure.id)?.status;
            if (status !== "pending" && !(failure.kind === "trigger" && status === "candidate")) {
              throw new TypeError("Stored distill failure does not match pending work.");
            }
          }
        }
        for (const raw of this.connection.database.prepare(
          `SELECT ${textColumn("id")}, ${textColumn("trigger_id", "triggerId")}, ` +
          `${textColumn("job_id", "jobId")} ` +
          "FROM distill_trigger_jobs ORDER BY rowid",
        ).iterate()) {
          const link = fields(this.connection.decodeRow(raw), ["id", "triggerId", "jobId"]);
          validateText(link["id"], "stored trigger batch link ID");
          validateText(link["triggerId"], "stored trigger batch trigger ID");
          validateText(link["jobId"], "stored trigger batch job ID");
          if (!triggers.has(link["triggerId"]) || !jobsById.has(link["jobId"])) {
            throw new TypeError("Stored distill trigger batch link is orphaned.");
          }
        }
        for (const row of this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks`).iterate()) {
          parsePersistedCleanTask(this.connection.decodeRow(row));
        }
        const globalLease = this.dependencies.distill.readDistillLease();
        if (globalLease.claimToken !== null && globalLease.taskKind !== null && globalLease.taskId !== null) {
          if (globalLease.taskKind === "job") {
            const leasedJob = jobsById.get(globalLease.taskId);
            if (
              leasedJob === undefined || leasedJob.status !== "pending" ||
              leasedJob.leaseOwner !== globalLease.claimToken ||
              leasedJob.leaseUntil !== globalLease.leaseUntil
            ) throw new TypeError("Stored global distill lease does not match its consolidation job.");
          } else {
            const leasedTrigger = triggers.get(globalLease.taskId);
            if (leasedTrigger === undefined || (leasedTrigger.status !== "pending" && leasedTrigger.status !== "candidate")) {
              throw new TypeError("Stored global distill lease does not match its trigger.");
            }
          }
        }
      }
    }
  }
}
