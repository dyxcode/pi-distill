import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { SQLiteMemoryStore } from "../../src/storage/store.ts";

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-source-backfill-"));
  const path = join(root, "memories.sqlite");
  const stores = [];
  t.after(async () => {
    for (const store of stores) store.close();
    await rm(root, { recursive: true, force: true });
  });
  return {
    root,
    path,
    open() {
      const store = new SQLiteMemoryStore(path);
      stores.push(store);
      return store;
    },
  };
}

function oldV5Schema(path) {
  const db = new DatabaseSync(path);
  try {
    db.exec("PRAGMA foreign_keys = OFF");
    const v5Tables = new Set([
      "clean_tasks",
      "consolidation_jobs",
      "consolidation_progress",
      "distill_lease",
      "distill_trigger_jobs",
      "distill_triggers",
      "memories",
      "memory_origins",
      "session_deletions",
      "session_observations",
      "session_usages",
      "source_access",
    ]);
    const extras = db.prepare(
      "SELECT type, name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
    ).all().filter(row => !v5Tables.has(row.name));
    for (const row of extras.filter(value => value.type === "index")) {
      db.exec(`DROP INDEX IF EXISTS "${row.name.replaceAll('"', '""')}"`);
    }
    for (const row of extras.filter(value => value.type === "table")) {
      db.exec(`DROP TABLE IF EXISTS "${row.name.replaceAll('"', '""')}"`);
    }
    for (const row of extras.filter(value => value.type === "trigger" || value.type === "view")) {
      const objectType = row.type === "trigger" ? "TRIGGER" : "VIEW";
      db.exec(`DROP ${objectType} IF EXISTS "${row.name.replaceAll('"', '""')}"`);
    }
    for (const table of ["consolidation_progress", "memory_origins", "session_deletions"]) {
      assert.equal(db.prepare(`SELECT COUNT(*) AS count FROM "${table}"`).get().count, 0,
        `the v5 fixture only reconstructs empty legacy ${table} tables`);
    }
    db.exec(`
      DROP TABLE consolidation_progress;
      CREATE TABLE consolidation_progress (
        source_key TEXT PRIMARY KEY NOT NULL,
        job_id TEXT NOT NULL,
        processed_at INTEGER NOT NULL
      ) STRICT;
      DROP TABLE memory_origins;
      CREATE TABLE memory_origins (
        memory_id TEXT PRIMARY KEY NOT NULL,
        job_id TEXT NOT NULL,
        revision INTEGER NOT NULL
      ) STRICT;
      DROP TABLE session_deletions;
      CREATE TABLE session_deletions (
        session_file TEXT PRIMARY KEY NOT NULL,
        session_id TEXT NOT NULL,
        intent_id TEXT NOT NULL,
        file_version TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))
      ) STRICT;
      PRAGMA user_version = 5;
    `);

    const remaining = db.prepare(
      "SELECT type, name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name",
    ).all();
    assert.deepEqual(remaining.map(({ type, name }) => `${type}:${name}`), [
      "table:clean_tasks",
      "table:consolidation_jobs",
      "table:consolidation_progress",
      "table:distill_lease",
      "table:distill_trigger_jobs",
      "table:distill_triggers",
      "table:memories",
      "table:memory_origins",
      "table:session_deletions",
      "table:session_observations",
      "table:session_usages",
      "table:source_access",
    ]);
    assert.equal(db.prepare("PRAGMA user_version").get().user_version, 5);
  } finally {
    db.close();
  }
}

function oldV6Schema(path) {
  const db = new DatabaseSync(path);
  try {
    db.exec("PRAGMA foreign_keys = OFF");
    for (const table of ["consolidation_progress", "memory_origins", "session_deletions", "memory_evidence"]) {
      assert.equal(db.prepare(`SELECT COUNT(*) AS count FROM "${table}"`).get().count, 0,
        `the v6 fixture only reconstructs empty legacy ${table} tables`);
    }
    db.exec(`
      DROP INDEX IF EXISTS memory_origins_evidence;
      DROP INDEX IF EXISTS session_deletions_generation;
      DROP TABLE memory_origins;
      CREATE TABLE memory_origins (
        memory_id TEXT PRIMARY KEY NOT NULL,
        job_id TEXT NOT NULL,
        revision INTEGER NOT NULL
      ) STRICT;
      DROP TABLE consolidation_progress;
      CREATE TABLE consolidation_progress (
        source_key TEXT PRIMARY KEY NOT NULL,
        job_id TEXT NOT NULL,
        processed_at INTEGER NOT NULL
      ) STRICT;
      DROP TABLE session_deletions;
      CREATE TABLE session_deletions (
        session_file TEXT PRIMARY KEY NOT NULL,
        session_id TEXT NOT NULL,
        intent_id TEXT NOT NULL,
        file_version TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('deleting', 'complete'))
      ) STRICT;
      DROP TABLE memory_evidence;
      DROP TABLE metadata_gc_state;
      DROP TRIGGER source_fence_session_observations_insert; DROP TRIGGER source_fence_session_observations_update; DROP TRIGGER source_fence_session_observations_delete; DROP TRIGGER source_fence_session_usages_insert; DROP TRIGGER source_fence_session_usages_update; DROP TRIGGER source_fence_session_usages_delete; DROP TRIGGER source_fence_consolidation_jobs_insert; DROP TRIGGER source_fence_consolidation_jobs_update; DROP TRIGGER source_fence_consolidation_jobs_delete; DROP TRIGGER source_fence_distill_triggers_insert; DROP TRIGGER source_fence_distill_triggers_update; DROP TRIGGER source_fence_distill_triggers_delete; DROP TRIGGER source_fence_clean_tasks_insert; DROP TRIGGER source_fence_clean_tasks_update; DROP TRIGGER source_fence_clean_tasks_delete; DROP INDEX source_index_parents; DROP INDEX distill_schedule_probe; DROP INDEX jobs_session_status; DROP INDEX triggers_session_status; DROP TABLE distill_schedule; DROP TABLE distill_schedule_state; DROP TABLE source_coverage; DROP TABLE source_fences; DROP TABLE source_derived_gc; DROP TABLE source_view_parts; DROP TABLE source_view_edits; DROP TABLE source_view_entries; DROP TABLE source_preparation; DROP TABLE source_views; DROP TABLE source_index_entries; DROP TABLE source_index_files; DROP TABLE distill_failures;
      DELETE FROM managed_sources WHERE row_id > 128;
      UPDATE source_registry_state SET backfill_complete = 0 WHERE singleton = 1;
      UPDATE source_registry_cursors SET cursor_row = NULL;
      UPDATE source_backfill SET phase = 0, cursor_row = 128 WHERE singleton = 1;
      PRAGMA user_version = 6;
    `);
    assert.equal(db.prepare("PRAGMA user_version").get().user_version, 6);
    assert.equal(db.prepare("SELECT COUNT(*) AS count FROM managed_sources").get().count, 128);
    assert.equal(db.prepare("SELECT backfill_complete FROM source_registry_state WHERE singleton = 1").get()
      .backfill_complete, 0);
  } finally {
    db.close();
  }
}

function legacyJob(id, root, index) {
  return {
    id,
    sessionId: `legacy-session-${String(index).padStart(3, "0")}`,
    sessionFile: join(root, `legacy-session-${String(index).padStart(3, "0")}.jsonl`),
    leafId: `leaf-${index}`,
    cwd: root,
    sourceKeys: [`legacy-source-${index}`],
  };
}

function listSources(store) {
  const result = [];
  let afterRowId;
  for (;;) {
    const page = store.sources.listPage({ limit: 128, ...(afterRowId === undefined ? {} : { afterRowId }) });
    result.push(...page.items);
    if (page.nextCursor === null) return result;
    afterRowId = page.nextCursor;
  }
}

for (const legacyVersion of [5, 6]) {
test(`v${legacyVersion} legacy source backfill resumes in bounded passes across reopen and preserves current work`, async t => {
  const f = await fixture(t);
  let store = f.open();
  const memory = {
    id: "preserved-memory",
    content: "Current memory survives source-registry migration.",
    createdAt: "2026-10-23T00:00:00.000Z",
  };
  store.save(memory);
  const jobs = Array.from({ length: 130 }, (_, index) => legacyJob(`legacy-job-${index}`, f.root, index));
  for (const job of jobs) store.enqueueConsolidation(job);
  store.close();

  if (legacyVersion === 5) oldV5Schema(f.path);
  else oldV6Schema(f.path);
  store = f.open();
  const version = new DatabaseSync(f.path);
  try {
    assert.equal(version.prepare("PRAGMA user_version").get().user_version, 12);
  } finally {
    version.close();
  }
  assert.deepEqual(store.get(memory.id), { ...memory, revision: 1, updatedAt: memory.createdAt });
  assert.deepEqual(store.listConsolidationJobs(), jobs);
  assert.equal(store.sources.isTopologyComplete(), false);
  assert.equal(listSources(store).length, 128, "the migration performs one bounded 128-row backfill pass");
  store.close();

  store = f.open();
  assert.equal(store.sources.isTopologyComplete(), false);
  assert.equal(store.backfillSources(1), false);
  assert.equal(listSources(store).length, 129, "the resumed pass advances its durable row cursor");
  store.close();

  store = f.open();
  assert.equal(store.backfillSources(128), true);
  let sources = listSources(store);
  assert.equal(sources.length, jobs.length);
  assert.equal(store.sources.isTopologyComplete(), false, "backfill completion does not bless unvalidated headers");
  for (const source of sources) {
    assert.equal(store.sources.recordHeader({
      sessionFile: source.sessionFile,
      sessionId: source.sessionId,
      fileVersion: `1:${source.rowId}:0:1:1`,
    }), true);
  }
  assert.equal(store.sources.isTopologyComplete(), true);
  const topologyVersion = store.sources.topologyVersion();
  const generations = sources.map(source => source.generation);

  assert.equal(store.backfillSources(128), true);
  sources = listSources(store);
  assert.deepEqual(sources.map(source => source.generation), generations);
  assert.equal(store.sources.topologyVersion(), topologyVersion, "repeating completed backfill is idempotent");
  assert.deepEqual(store.get(memory.id), { ...memory, revision: 1, updatedAt: memory.createdAt });
  assert.deepEqual(store.listConsolidationJobs(), jobs);
});
}
