// Node 22 truncates TEXT results at NUL. Return bytes only at the result boundary;
// predicates and ordering must continue to use the original TEXT columns.
export function textColumn(column: string, alias = column): string {
  // Keep NULL distinct and make other storage classes fail text validation.
  return `CASE typeof(${column}) WHEN 'text' THEN CAST(${column} AS BLOB) WHEN 'null' THEN NULL ELSE 0 END AS ${alias}`;
}

export const memoryColumns = [
  textColumn("id"), textColumn("content"), textColumn("created_at", "createdAt"),
  "revision", textColumn("updated_at", "updatedAt"),
].join(", ");

export const jobColumns = [
  textColumn("id"), textColumn("job_json"), textColumn("status"),
  textColumn("lease_owner"), "lease_until", textColumn("retry_category", "retryCategory"), "retry_at AS retryAt",
].join(", ");

export const legacyJobColumns = [
  textColumn("id"), textColumn("job_json"), textColumn("status"),
  textColumn("lease_owner"), "lease_until", "NULL AS retryCategory", "NULL AS retryAt",
].join(", ");

export const triggerColumns = [
  textColumn("id"), textColumn("trigger_json", "triggerJson"), textColumn("status"),
  textColumn("compaction_record_id", "compactionRecordId"),
  textColumn("retry_category", "retryCategory"), "retry_at AS retryAt",
].join(", ");

export const cleanTaskColumns = [
  textColumn("id"), textColumn("task_json", "taskJson"), textColumn("status"),
  textColumn("wait_reason", "waitReason"), textColumn("retry_category", "retryCategory"),
  "retry_at AS retryAt", textColumn("lease_owner", "leaseOwner"), "lease_until AS leaseUntil",
].join(", ");

export const progressColumnsV6 = [textColumn("source_key"), textColumn("job_id"), "processed_at"].join(", ");

export const progressColumns = [textColumn("source_key"), "processed_at"].join(", ");

export const originColumnsV6 = [textColumn("memory_id", "memoryId"), textColumn("job_id", "jobId"), "revision"].join(", ");

export const originColumns = [textColumn("memory_id", "memoryId"), textColumn("evidence_id", "evidenceId"), "revision"].join(", ");

export const evidenceColumns = [
  textColumn("evidence_id", "evidenceId"), textColumn("session_id", "sessionId"),
  textColumn("session_file", "sessionFile"), textColumn("source_generation", "sourceGeneration"),
  textColumn("cwd"), textColumn("leaf_id", "leafId"), textColumn("source_keys", "sourceKeys"),
  textColumn("lineage_session_ids", "lineageSessionIds"), textColumn("source_time", "sourceTime"),
  textColumn("branch_anchor", "branchAnchor"),
].join(", ");

export const deletionColumns = [
  textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
  textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"),
  textColumn("source_generation", "sourceGeneration"), textColumn("state"),
].join(", ");

export const observationColumns = [
  textColumn("session_id"), textColumn("session_file"), textColumn("cwd"),
  textColumn("leaf_id"), "owner_pid", "captured",
].join(", ");

export const sessionUsageColumns = [
  textColumn("owner_id", "ownerId"), textColumn("process_token", "processToken"),
  textColumn("session_id", "sessionId"), textColumn("session_file", "sessionFile"),
  textColumn("cwd"), textColumn("leaf_id", "leafId"), "owner_pid AS ownerPid",
  textColumn("state"),
].join(", ");

export const schemaColumns = [textColumn("name"), textColumn("type"), textColumn("sql")].join(", ");

export const distillFailureColumns = [
  textColumn("task_kind", "kind"), textColumn("task_id", "id"), textColumn("reason"),
  textColumn("source_version", "sourceVersion"), textColumn("model_key", "modelKey"),
  textColumn("policy_version", "policyVersion"), "attempts", "next_attempt_at AS nextAttemptAt",
].join(", ");
