export const initialColumns = `
  id TEXT PRIMARY KEY NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL`;

export const legacySchema = `CREATE TABLE memories (${initialColumns}) STRICT`;

export const currentSchema = `CREATE TABLE memories (${initialColumns},
  revision INTEGER NOT NULL,
  updated_at TEXT NOT NULL
) STRICT`;

export const consolidationJobsV3Schema =
  "CREATE TABLE consolidation_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "job_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";

export const consolidationJobsSchema =
  "CREATE TABLE consolidation_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "job_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL)), " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL))" +
  ") STRICT";

export const consolidationProgressSchemaV6 =
  "CREATE TABLE consolidation_progress (" +
  "source_key TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "processed_at INTEGER NOT NULL" +
  ") STRICT";

export const consolidationProgressSchema =
  "CREATE TABLE consolidation_progress (" +
  "source_key TEXT PRIMARY KEY NOT NULL, " +
  "processed_at INTEGER NOT NULL" +
  ") STRICT";

export const memoryOriginsSchemaV6 =
  "CREATE TABLE memory_origins (" +
  "memory_id TEXT PRIMARY KEY NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "revision INTEGER NOT NULL" +
  ") STRICT";

export const memoryOriginsSchema =
  "CREATE TABLE memory_origins (" +
  "memory_id TEXT PRIMARY KEY NOT NULL, " +
  "evidence_id TEXT NOT NULL, " +
  "revision INTEGER NOT NULL" +
  ") STRICT";

export const memoryEvidenceSchema =
  "CREATE TABLE memory_evidence (" +
  "evidence_id TEXT PRIMARY KEY NOT NULL, " +
  "session_id TEXT NOT NULL, " +
  "session_file TEXT, " +
  "source_generation TEXT, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "source_keys TEXT NOT NULL, " +
  "lineage_session_ids TEXT, " +
  "source_time TEXT, " +
  "branch_anchor TEXT, " +
  "CHECK ((session_file IS NULL AND source_generation IS NULL) OR " +
  "(session_file IS NOT NULL AND source_generation IS NOT NULL))" +
  ") STRICT";

export const sessionObservationsSchema =
  "CREATE TABLE session_observations (" +
  "session_id TEXT PRIMARY KEY NOT NULL, " +
  "session_file TEXT NOT NULL, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "owner_pid INTEGER NOT NULL, " +
  "captured INTEGER NOT NULL DEFAULT 0 CHECK (captured IN (0, 1))" +
  ") STRICT";

export const sessionUsagesSchema =
  "CREATE TABLE session_usages (" +
  "owner_id TEXT PRIMARY KEY NOT NULL, " +
  "process_token TEXT NOT NULL, " +
  "session_id TEXT NOT NULL, " +
  "session_file TEXT NOT NULL, " +
  "cwd TEXT NOT NULL, " +
  "leaf_id TEXT, " +
  "owner_pid INTEGER NOT NULL CHECK (owner_pid > 0), " +
  "state TEXT NOT NULL CHECK (state IN ('active', 'closing', 'released'))" +
  ") STRICT";

export const distillTriggersSchema =
  "CREATE TABLE distill_triggers (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "trigger_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('candidate', 'pending', 'prepared', 'complete')), " +
  "compaction_record_id TEXT, " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL))" +
  ") STRICT";

export const triggerJobsSchema =
  "CREATE TABLE distill_trigger_jobs (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "trigger_id TEXT NOT NULL, " +
  "job_id TEXT NOT NULL, " +
  "ordinal INTEGER NOT NULL CHECK (ordinal >= 0)" +
  ") STRICT";

export const distillLeaseSchema =
  "CREATE TABLE distill_lease (" +
  "singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), " +
  "claim_token TEXT, " +
  "worker_id TEXT, " +
  "task_kind TEXT CHECK (task_kind IS NULL OR task_kind IN ('trigger', 'job')), " +
  "task_id TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((claim_token IS NULL AND worker_id IS NULL AND task_kind IS NULL AND " +
  "task_id IS NULL AND lease_until IS NULL) OR " +
  "(claim_token IS NOT NULL AND worker_id IS NOT NULL AND task_kind IS NOT NULL AND " +
  "task_id IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";

export const distillFailuresSchema =
  "CREATE TABLE distill_failures (" +
  "task_kind TEXT NOT NULL CHECK (task_kind IN ('trigger', 'job')), " +
  "task_id TEXT NOT NULL, " +
  "reason TEXT NOT NULL CHECK (reason IN ('source_invalid', 'model_unavailable', 'model_invalid', 'model_budget', 'transient', 'conflict')), " +
  "source_version TEXT NOT NULL, " +
  "model_key TEXT NOT NULL, " +
  "policy_version TEXT NOT NULL, " +
  "attempts INTEGER NOT NULL CHECK (attempts > 0), " +
  "next_attempt_at INTEGER, " +
  "PRIMARY KEY (task_kind, task_id)" +
  ") STRICT";

export const cleanTasksSchema =
  "CREATE TABLE clean_tasks (" +
  "id TEXT PRIMARY KEY NOT NULL, " +
  "task_json TEXT NOT NULL, " +
  "status TEXT NOT NULL CHECK (status IN ('pending', 'waiting')), " +
  "wait_reason TEXT CHECK (wait_reason IS NULL OR wait_reason IN " +
  "('host_coordination', 'unprocessed', 'usage', 'dependencies', 'unavailable')), " +
  "retry_category TEXT CHECK (retry_category IS NULL OR retry_category IN ('transient', 'unavailable', 'invalid')), " +
  "retry_at INTEGER, " +
  "lease_owner TEXT, " +
  "lease_until INTEGER, " +
  "CHECK ((status = 'pending' AND wait_reason IS NULL) OR status = 'waiting'), " +
  "CHECK ((retry_category IS NULL AND retry_at IS NULL) OR " +
  "(retry_category IS NOT NULL AND retry_at IS NOT NULL)), " +
  "CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR " +
  "(lease_owner IS NOT NULL AND lease_until IS NOT NULL))" +
  ") STRICT";

export const sourceAccessSchema = "CREATE TABLE source_access (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), token TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0)) STRICT";

export const deletionsSchemaV5 = "CREATE TABLE session_deletions (session_file TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL, file_version TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))) STRICT";

export const deletionsSchema = "CREATE TABLE session_deletions (session_file TEXT PRIMARY KEY NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL, file_version TEXT NOT NULL, source_generation TEXT, state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))) STRICT";

export const sourceBackfillSchema = "CREATE TABLE source_backfill (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), phase INTEGER NOT NULL, cursor_row INTEGER NOT NULL) STRICT";

export const metadataGcSchema = "CREATE TABLE metadata_gc_state (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), phase INTEGER NOT NULL CHECK (phase BETWEEN 0 AND 7), cursor_row INTEGER NOT NULL CHECK (cursor_row >= 0)) STRICT";

export const storeIndexes = [
  "CREATE INDEX jobs_source_status ON consolidation_jobs (json_extract(job_json, '$.sessionFile'), status)",
  "CREATE INDEX triggers_source_status ON distill_triggers (json_extract(trigger_json, '$.sessionFile'), status)",
  "CREATE INDEX clean_source ON clean_tasks (json_extract(task_json, '$.sessionFile'))",
  "CREATE INDEX usages_source ON session_usages (session_file)",
  "CREATE INDEX observations_source ON session_observations (session_file)",
  "CREATE INDEX trigger_links_job ON distill_trigger_jobs (job_id)",
];

export const storeIndexesV7 = [
  "CREATE INDEX memory_origins_evidence ON memory_origins (evidence_id)",
  "CREATE INDEX session_deletions_generation ON session_deletions (source_generation, state)",
];

export const storeSchemas = [sourceBackfillSchema, ...storeIndexes].map(sql => {
  const match = /^CREATE (TABLE|INDEX) (\w+)/.exec(sql);
  if (!match?.[2]) throw new Error("Invalid store schema declaration.");
  return { name: match[2], sql };
});

export const storeSchemasV7 = [...storeSchemas, ...[metadataGcSchema, ...storeIndexesV7].map(sql => {
  const match = /^CREATE (TABLE|INDEX) (\w+)/.exec(sql);
  if (!match?.[2]) throw new Error("Invalid store schema declaration.");
  return { name: match[2], sql };
})];

export const storeSchemasV8 = [...storeSchemasV7, { name: "distill_failures", sql: distillFailuresSchema }];

export const latestSchemaVersion = 12;
