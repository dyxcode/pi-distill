import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  ConsolidationConflictError,
  MemoryNotFoundError,
  SQLiteMemoryStore,
} from "../src/memory-store.ts";

function memory(id, overrides = {}) {
  return {
    id,
    content: "Synthetic memory " + id + " with retained conditions.",
    createdAt: "2026-10-21T08:00:00.000Z",
    ...overrides,
  };
}

function job(id, overrides = {}) {
  return {
    id,
    sessionId: "session-" + id,
    sessionFile: null,
    leafId: "leaf-" + id,
    cwd: process.cwd(),
    sourceKeys: [id + ":entry-1"],
    ...overrides,
  };
}

function commit(jobId, owner, overrides = {}) {
  return {
    jobId,
    owner,
    now: 100,
    creates: [],
    updates: [],
    deletes: [],
    ...overrides,
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-consolidation-"));
  const stores = [];
  t.after(async () => {
    for (const store of stores) store.close();
    await rm(root, { recursive: true, force: true });
  });
  const path = join(root, "memories.sqlite");
  function open() {
    const store = new SQLiteMemoryStore(path);
    stores.push(store);
    return store;
  }
  return { path, root, open };
}

function database(path) {
  return new DatabaseSync(path);
}

test("jobs enqueue idempotently, reject a changed payload, and list only pending records", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = job("job-b", { sourceKeys: ["s:2", "s:1"] });
  const second = job("job-a");
  store.enqueueConsolidation(first);
  store.enqueueConsolidation(structuredClone(first));
  store.enqueueConsolidation(second);
  assert.deepEqual(store.listConsolidationJobs(), [first, second]);
  assert.throws(
    () => store.enqueueConsolidation(job("job-b", { sourceKeys: ["s:changed"] })),
    ConsolidationConflictError,
  );

  assert.equal(store.claimConsolidation(second.id, "worker", 100, 100), true);
  assert.equal(store.commitConsolidation(commit(second.id, "worker")), true);
  assert.deepEqual(store.listConsolidationJobs(), [first]);
  store.close();
  assert.deepEqual(f.open().listConsolidationJobs(), [first]);
});

test("pending job order follows enqueue order rather than job ID order", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const earlier = job("z-enqueued-first");
  const later = job("a-enqueued-later");
  store.enqueueConsolidation(earlier);
  store.enqueueConsolidation(later);

  assert.deepEqual(store.listConsolidationJobs(), [earlier, later]);
});

test("global distill claims serialize across sessions while preserving per-session job order", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const secondStore = f.open();
  const earlier = job("z-earlier", { sessionId: "same-session" });
  const later = job("a-later", { sessionId: "same-session" });
  const unrelated = job("m-unrelated", { sessionId: "other-session" });
  store.enqueueConsolidation(earlier);
  store.enqueueConsolidation(later);
  store.enqueueConsolidation(unrelated);

  assert.equal(secondStore.claimConsolidation(later.id, "worker-later", 100, 50), false);
  assert.equal(secondStore.claimConsolidation(unrelated.id, "worker-other", 100, 50), true);
  assert.equal(store.claimConsolidation(earlier.id, "worker-earlier", 100, 50), false);

  assert.equal(secondStore.commitConsolidation(commit(unrelated.id, "worker-other")), true);
  assert.equal(store.claimConsolidation(earlier.id, "worker-earlier", 101, 50), true);
  assert.equal(secondStore.claimConsolidation(later.id, "worker-later", 102, 50), false);
  assert.equal(store.commitConsolidation(commit(earlier.id, "worker-earlier", { now: 103 })), true);
  assert.equal(secondStore.claimConsolidation(later.id, "worker-later", 104, 50), true);
});

test("claims serialize across connections, expire at the boundary, and release only for their owner", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const pending = job("claim");
  first.enqueueConsolidation(pending);

  assert.equal(first.claimConsolidation(pending.id, "worker-a", 100, 10), true);
  assert.equal(second.claimConsolidation(pending.id, "worker-b", 109, 10), false);
  second.releaseConsolidation(pending.id, "worker-b");
  assert.equal(second.claimConsolidation(pending.id, "worker-b", 109, 10), false);
  assert.equal(second.claimConsolidation(pending.id, "worker-b", 110, 10), true);
  first.releaseConsolidation(pending.id, "worker-a");
  assert.equal(first.claimConsolidation(pending.id, "worker-a", 110, 10), false);

  second.releaseConsolidation(pending.id, "worker-b");
  assert.equal(first.claimConsolidation(pending.id, "worker-a", 111, 10), true);
});

test("one commit applies creates, updates, deletes, and source progress atomically", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("batch", { sourceKeys: ["session:entry-a", "session:entry-b"] });
  const original = memory("keep");
  const removed = memory("remove");
  store.save(original);
  store.save(removed);
  store.enqueueConsolidation(pending);
  assert.equal(store.claimConsolidation(pending.id, "worker", 100, 50), true);

  const update = {
    id: original.id,
    expectedRevision: 1,
    content: "The corrected synthetic memory preserves the current condition.",
    updatedAt: "2026-10-21T09:00:00.000Z",
  };
  const created = memory("new");
  assert.equal(store.commitConsolidation(commit(pending.id, "worker", {
    creates: [created],
    updates: [update],
    deletes: [{ id: removed.id, expectedRevision: 1 }],
  })), true);

  assert.equal(store.get(original.id).content, update.content);
  assert.equal(store.get(original.id).revision, 2);
  assert.equal(store.get(created.id).content, created.content);
  assert.equal(store.get(removed.id), undefined);
  assert.deepEqual(store.memoryOrigin(original.id), {
    sessionId: pending.sessionId,
    sourceKeys: pending.sourceKeys,
  });
  assert.deepEqual(store.memoryOrigin(created.id), {
    sessionId: pending.sessionId,
    sourceKeys: pending.sourceKeys,
  });
  assert.equal(store.memoryOrigin(removed.id), undefined);
  assert.deepEqual(store.processedSources(pending.sourceKeys), pending.sourceKeys);
  assert.deepEqual(store.listConsolidationJobs(), []);
});

test("memory origins follow the latest batch and disappear on external changes or deletion", async (t) => {
  const f = await fixture(t);
  let store = f.open();
  const original = memory("origin-target");
  const firstJob = job("origin-first", { sourceKeys: ["lineage:first"] });
  store.enqueueConsolidation(firstJob);
  store.claimConsolidation(firstJob.id, "worker-a", 100, 50);
  assert.equal(store.commitConsolidation(commit(firstJob.id, "worker-a", {
    creates: [original],
  })), true);
  assert.deepEqual(store.memoryOrigin(original.id), {
    sessionId: firstJob.sessionId,
    sourceKeys: firstJob.sourceKeys,
  });
  assert.deepEqual(store.memoryOrigin("unknown"), undefined);

  const reader = database(f.path);
  reader.prepare("UPDATE memories SET revision = 2 WHERE id = ?").run(original.id);
  reader.close();
  assert.equal(store.memoryOrigin(original.id), undefined);
  store.close();
  store = f.open();
  assert.equal(store.memoryOrigin(original.id), undefined);

  const secondJob = job("origin-second", { sourceKeys: ["lineage:second"] });
  store.enqueueConsolidation(secondJob);
  store.claimConsolidation(secondJob.id, "worker-b", 100, 50);
  const secondUpdate = {
    id: original.id,
    expectedRevision: 2,
    content: "A later synthetic batch replaces the original origin.",
    updatedAt: "2026-10-21T10:00:00.000Z",
  };
  assert.equal(store.commitConsolidation(commit(secondJob.id, "worker-b", {
    updates: [secondUpdate],
  })), true);
  assert.deepEqual(store.memoryOrigin(original.id), {
    sessionId: secondJob.sessionId,
    sourceKeys: secondJob.sourceKeys,
  });

  const corrected = store.get(original.id);
  const external = store.update({
    id: corrected.id,
    expectedRevision: corrected.revision,
    content: "An external correction no longer has source lineage.",
    updatedAt: "2026-10-21T11:00:00.000Z",
  });
  assert.equal(store.memoryOrigin(original.id), undefined);

  const thirdJob = job("origin-third", { sourceKeys: ["lineage:third"] });
  store.enqueueConsolidation(thirdJob);
  store.claimConsolidation(thirdJob.id, "worker-c", 100, 50);
  assert.equal(store.commitConsolidation(commit(thirdJob.id, "worker-c", {
    updates: [{
      id: original.id,
      expectedRevision: external.revision,
      content: "A fresh batch restores current source lineage.",
      updatedAt: "2026-10-21T12:00:00.000Z",
    }],
  })), true);
  assert.deepEqual(store.memoryOrigin(original.id), {
    sessionId: thirdJob.sessionId,
    sourceKeys: thirdJob.sourceKeys,
  });
  store.forget({ id: original.id, expectedRevision: 5 });
  assert.equal(store.memoryOrigin(original.id), undefined);
});

test("memory origins preserve validated lineage session IDs", async (t) => {
  const f = await fixture(t);
  let store = f.open();
  const source = job("lineage-origin", {
    lineageSessionIds: ["root-session", "current-session"],
  });
  store.enqueueConsolidation(source);
  store.claimConsolidation(source.id, "worker", 100, 50);
  store.commitConsolidation(commit(source.id, "worker", { creates: [memory("lineage-memory")] }));

  assert.deepEqual(store.memoryOrigin("lineage-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    lineageSessionIds: source.lineageSessionIds,
  });
  store.close();
  store = f.open();
  assert.deepEqual(store.memoryOrigin("lineage-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    lineageSessionIds: source.lineageSessionIds,
  });
  for (const invalid of [[], ["duplicate", "duplicate"], ["valid", ""], ["bad-\ud800"]]) {
    assert.throws(() => store.enqueueConsolidation(job("invalid-lineage", {
      lineageSessionIds: invalid,
    })), TypeError);
  }
});

test("memory origins preserve only canonical source timestamps", async (t) => {
  const f = await fixture(t);
  let store = f.open();
  const source = job("dated-origin", { sourceTime: "2026-10-21T09:15:00.000Z" });
  store.enqueueConsolidation(source);
  store.claimConsolidation(source.id, "worker", 100, 50);
  store.commitConsolidation(commit(source.id, "worker", { creates: [memory("dated-memory")] }));

  assert.deepEqual(store.memoryOrigin("dated-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    sourceTime: source.sourceTime,
  });
  store.close();
  store = f.open();
  assert.deepEqual(store.memoryOrigin("dated-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    sourceTime: source.sourceTime,
  });
  for (const sourceTime of [null, "not-a-date", "2026-10-21T09:15:00Z", "2026-10-21T10:15:00.000+01:00"]) {
    assert.throws(() => store.enqueueConsolidation(job("invalid-source-time", { sourceTime })), TypeError);
  }
});

test("memory origins preserve nonempty branch anchors", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const branchAnchor = "a".repeat(64);
  const source = job("anchored-origin", { branchAnchor });
  store.enqueueConsolidation(source);
  store.claimConsolidation(source.id, "worker", 100, 50);
  store.commitConsolidation(commit(source.id, "worker", { creates: [memory("anchored-memory")] }));

  assert.deepEqual(store.memoryOrigin("anchored-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    branchAnchor,
  });
  assert.throws(() => store.enqueueConsolidation(job("empty-anchor", { branchAnchor: "" })), TypeError);
});

test("an empty change set still marks every source processed and completes the job", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("no-change", { sourceKeys: ["s:a", "s:b"] });
  store.enqueueConsolidation(pending);
  assert.equal(store.claimConsolidation(pending.id, "worker", 100, 50), true);
  assert.equal(store.commitConsolidation(commit(pending.id, "worker")), true);
  assert.deepEqual(store.processedSources(pending.sourceKeys), pending.sourceKeys);
  assert.deepEqual(store.listConsolidationJobs(), []);
});

test("processing assessments read committed progress while another connection holds the writer reservation", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = job("finished");
  const second = job("waiting");
  store.enqueueConsolidation(first);
  store.claimConsolidation(first.id, "worker", 100, 100);
  store.commitConsolidation(commit(first.id, "worker"));
  store.enqueueConsolidation(second);
  const writer = database(f.path);
  try {
    writer.exec("BEGIN IMMEDIATE");
    writer.prepare("DELETE FROM consolidation_progress WHERE source_key = ?").run(first.sourceKeys[0]);
    assert.deepEqual(store.readConsolidationStatus([...first.sourceKeys, ...second.sourceKeys]), {
      processedSources: first.sourceKeys,
      pendingJobs: [second],
      pendingTriggers: [],
      observations: [],
      usages: [],
    });
    writer.exec("ROLLBACK");
  } finally {
    if (writer.isTransaction) writer.exec("ROLLBACK");
    writer.close();
  }
  assert.throws(() => store.readConsolidationStatus(["duplicate", "duplicate"]), TypeError);
  assert.deepEqual(store.readConsolidationStatus(first.sourceKeys).processedSources, first.sourceKeys);
});

test("a stale owner cannot commit after a lease takeover", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("fenced");
  store.enqueueConsolidation(pending);
  assert.equal(store.claimConsolidation(pending.id, "worker-a", 100, 10), true);
  assert.equal(store.claimConsolidation(pending.id, "worker-b", 109, 10), false);
  assert.equal(store.claimConsolidation(pending.id, "worker-b", 110, 10), true);

  assert.throws(
    () => store.commitConsolidation(commit(pending.id, "worker-a", { creates: [memory("stale")] })),
    ConsolidationConflictError,
  );
  assert.equal(store.get("stale"), undefined);
  assert.equal(store.commitConsolidation(commit(pending.id, "worker-b", { now: 111 })), true);
  assert.deepEqual(store.processedSources(pending.sourceKeys), pending.sourceKeys);
});

test("source subset fencing lets a later batch process only entries that remain", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const secondStore = f.open();
  const first = job("first", { sourceKeys: ["shared"] });
  store.enqueueConsolidation(first);
  store.claimConsolidation(first.id, "worker-a", 100, 50);
  assert.equal(store.commitConsolidation(commit(first.id, "worker-a", {
    creates: [memory("first-memory")],
  })), true);
  store.forget({ id: "first-memory", expectedRevision: 1 });

  const partial = job("partial", { sourceKeys: ["shared", "new"] });
  secondStore.enqueueConsolidation(partial);
  secondStore.claimConsolidation(partial.id, "worker-b", 100, 50);
  assert.throws(
    () => secondStore.commitConsolidation(commit(partial.id, "worker-b", {
      sourceKeys: ["shared", "new"],
      creates: [memory("stale-batch-must-not-create")],
    })),
    ConsolidationConflictError,
  );
  assert.equal(secondStore.get("stale-batch-must-not-create"), undefined);
  assert.deepEqual(secondStore.processedSources(partial.sourceKeys), ["shared"]);
  assert.deepEqual(secondStore.listConsolidationJobs(), [partial]);

  assert.equal(secondStore.commitConsolidation(commit(partial.id, "worker-b", {
    sourceKeys: ["new"],
    creates: [memory("remaining-only")],
  })), true);
  assert.equal(store.get("remaining-only").content, "Synthetic memory remaining-only with retained conditions.");
  assert.deepEqual(store.processedSources(partial.sourceKeys), partial.sourceKeys);
  assert.deepEqual(store.listConsolidationJobs(), []);

  const alreadyProcessed = job("fully-covered", { sourceKeys: ["shared"] });
  secondStore.enqueueConsolidation(alreadyProcessed);
  secondStore.claimConsolidation(alreadyProcessed.id, "worker-c", 100, 50);
  const droppedMutation = commit(alreadyProcessed.id, "worker-c", {
    sourceKeys: [],
    creates: [memory("first-memory")],
  });
  assert.throws(() => secondStore.commitConsolidation(droppedMutation), ConsolidationConflictError);
  assert.equal(secondStore.get("first-memory"), undefined);
  assert.deepEqual(secondStore.listConsolidationJobs(), [alreadyProcessed]);
  assert.equal(secondStore.commitConsolidation(commit(alreadyProcessed.id, "worker-c", {
    sourceKeys: [],
  })), false);
  assert.deepEqual(secondStore.listConsolidationJobs(), []);
});

test("replaying a completed job returns false and cannot recreate a later-forgotten memory", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("replay");
  const forgotten = memory("forgotten-after-batch");
  store.enqueueConsolidation(pending);
  store.claimConsolidation(pending.id, "worker", 100, 50);
  const request = commit(pending.id, "worker", { creates: [forgotten] });
  assert.equal(store.commitConsolidation(request), true);
  store.forget({ id: forgotten.id, expectedRevision: 1 });
  assert.equal(store.commitConsolidation(request), false);
  assert.equal(store.get(forgotten.id), undefined);
  assert.deepEqual(store.processedSources(pending.sourceKeys), pending.sourceKeys);
});

test("a later invalid mutation rolls back earlier changes and leaves the job pending", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("invalid-late");
  const original = memory("existing");
  store.save(original);
  store.enqueueConsolidation(pending);
  store.claimConsolidation(pending.id, "worker", 100, 50);

  assert.throws(() => store.commitConsolidation(commit(pending.id, "worker", {
    creates: [memory("must-rollback")],
    updates: [{
      id: "missing",
      expectedRevision: 1,
      content: "A validly shaped request to an absent record.",
      updatedAt: "2026-10-21T09:00:00.000Z",
    }],
  })), MemoryNotFoundError);
  assert.equal(store.get("must-rollback"), undefined);
  assert.deepEqual(store.get(original.id), { ...original, revision: 1, updatedAt: original.createdAt });
  assert.deepEqual(store.processedSources(pending.sourceKeys), []);
  assert.deepEqual(store.listConsolidationJobs(), [pending]);
});

test("a failed SQLite COMMIT rolls back memory writes, progress, and job completion", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const pending = job("commit-failure");
  const created = memory("retry-after-commit-failure");
  store.enqueueConsolidation(pending);
  store.claimConsolidation(pending.id, "worker", 100, 50);
  const request = commit(pending.id, "worker", { creates: [created] });
  const reader = database(f.path);
  try {
    reader.exec("BEGIN");
    reader.prepare("SELECT name FROM sqlite_schema").all();
    assert.throws(() => store.commitConsolidation(request));
    reader.exec("ROLLBACK");

    assert.equal(store.get(created.id), undefined);
    assert.deepEqual(store.processedSources(pending.sourceKeys), []);
    assert.deepEqual(store.listConsolidationJobs(), [pending]);
    assert.equal(store.commitConsolidation(request), true);
    assert.deepEqual(store.get(created.id), { ...created, revision: 1, updatedAt: created.createdAt });
  } finally {
    reader.close();
  }
});

test("v6 migration preserves shared current origins and the deletion receipt for an already cleaned source", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = job("deleted-origin-source", {
    sessionId: "deleted-origin-session",
    sessionFile: join(f.root, "deleted-origin-session.jsonl"),
    cwd: f.root,
    sourceKeys: ["deleted-origin:entry-1", "deleted-origin:entry-2"],
    lineageSessionIds: ["root-session", "deleted-origin-session"],
    sourceTime: "2026-10-22T07:30:00.000Z",
    branchAnchor: "b".repeat(64),
  });
  const created = memory("origin-survives-v6-migration");
  const shared = memory("shared-origin-survives-v6-migration");
  store.enqueueConsolidation(source);
  assert.equal(store.claimConsolidation(source.id, "origin-worker", 100, 100), true);
  assert.equal(store.commitConsolidation(commit(source.id, "origin-worker", { creates: [created, shared] })), true);

  const expectedMemory = { ...created, revision: 1, updatedAt: created.createdAt };
  const expectedOrigin = {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: source.sourceTime,
    branchAnchor: source.branchAnchor,
  };
  assert.deepEqual(store.get(created.id), expectedMemory);
  assert.deepEqual(store.memoryOrigin(created.id), expectedOrigin);
  const expectedGeneration = store.sources.get(source.sessionFile, source.sessionId)?.generation;
  assert.ok(expectedGeneration);

  const clean = store.listCleanTasks({ limit: 128 }).items.find(item => item.sessionFile === source.sessionFile);
  assert.ok(clean);
  const owner = "origin-source-cleaner";
  const deletionStartedAt = Date.now();
  assert.equal(store.claimCleanTask(clean.id, owner, deletionStartedAt, 60_000), true);
  const access = store.acquireSourceAccess();
  assert.ok(access);
  try {
    assert.equal(store.prepareDeletion(clean.id, owner, access, store.retentionVersion(), "1:2:3:4:5"), true);
    assert.equal(store.finishDeletion(clean.id, owner, access, store.retentionVersion(), () => true), true);
  } finally {
    store.releaseSourceAccess(access);
  }
  const expectedReceipt = store.getDeletion(source.sessionFile);
  assert.ok(expectedReceipt);
  assert.equal(expectedReceipt.state, "complete");
  assert.equal(expectedReceipt.sourceGeneration, expectedGeneration);
  store.close();

  // Rebuild the actual v6 relation and receipt tables. The v6 database has no
  // evidence table or GC cursor, and its origin/progress rows point to jobs.
  let expectedProgress;
  const legacy = database(f.path);
  try {
    legacy.exec("PRAGMA foreign_keys = OFF");
    const origins = legacy.prepare(
      "SELECT memory_id, revision FROM memory_origins ORDER BY rowid",
    ).all();
    const progress = legacy.prepare(
      "SELECT source_key, processed_at FROM consolidation_progress ORDER BY source_key",
    ).all();
    const receipts = legacy.prepare(
      "SELECT session_file, session_id, intent_id, file_version, state FROM session_deletions ORDER BY rowid",
    ).all();
    assert.deepEqual(origins.map(row => ({ ...row })), [
      { memory_id: created.id, revision: 1 },
      { memory_id: shared.id, revision: 1 },
    ]);
    assert.deepEqual(progress.map(row => row.source_key), source.sourceKeys);
    expectedProgress = progress.map(row => ({ source_key: row.source_key, processed_at: row.processed_at }));
    assert.deepEqual(receipts.map(row => row.session_file), [source.sessionFile]);
    assert.equal(legacy.prepare("SELECT status FROM consolidation_jobs WHERE id = ?").get(source.id).status, "complete");
    legacy.exec(`
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
      DROP TABLE source_index_entries; DROP TABLE source_index_files; DROP TABLE distill_failures;
    `);
    const insertOrigin = legacy.prepare("INSERT INTO memory_origins VALUES (?, ?, ?)");
    for (const origin of origins) insertOrigin.run(origin.memory_id, source.id, origin.revision);
    const insertProgress = legacy.prepare("INSERT INTO consolidation_progress VALUES (?, ?, ?)");
    for (const row of progress) insertProgress.run(row.source_key, source.id, row.processed_at);
    const insertReceipt = legacy.prepare("INSERT INTO session_deletions VALUES (?, ?, ?, ?, ?)");
    for (const row of receipts) {
      insertReceipt.run(row.session_file, row.session_id, row.intent_id, row.file_version, row.state);
    }
    legacy.exec("PRAGMA user_version = 6");
  } finally {
    legacy.close();
  }

  const upgraded = f.open();
  assert.deepEqual(upgraded.get(created.id), expectedMemory);
  assert.deepEqual(upgraded.memoryOrigin(created.id), expectedOrigin);
  assert.deepEqual(upgraded.get(shared.id), { ...shared, revision: 1, updatedAt: shared.createdAt });
  assert.deepEqual(upgraded.memoryOrigin(shared.id), expectedOrigin);
  assert.equal(upgraded.getDeletion(source.sessionFile)?.state, "complete");
  assert.equal(upgraded.getDeletion(source.sessionFile)?.sourceGeneration, expectedGeneration);
  assert.equal(upgraded.sources.get(source.sessionFile, source.sessionId)?.generation, expectedGeneration);
  assert.equal(upgraded.sources.get(source.sessionFile, source.sessionId)?.deleted, true);
  assert.equal(upgraded.listConsolidationJobs().length, 0);

  const migrated = database(f.path);
  try {
    assert.equal(migrated.prepare("PRAGMA user_version").get().user_version, 9);
    assert.deepEqual(migrated.prepare(
      "SELECT o.memory_id AS memory_id, e.session_id AS session_id, e.source_keys AS source_keys " +
      "FROM memory_origins AS o JOIN memory_evidence AS e ON e.evidence_id = o.evidence_id ORDER BY o.rowid",
    ).all().map(row => ({ ...row })), [
      { memory_id: created.id, session_id: source.sessionId, source_keys: JSON.stringify(source.sourceKeys) },
      { memory_id: shared.id, session_id: source.sessionId, source_keys: JSON.stringify(source.sourceKeys) },
    ]);
    const evidence = migrated.prepare(
      "SELECT session_id, session_file, source_generation, cwd, leaf_id, source_keys, " +
      "lineage_session_ids, source_time, branch_anchor FROM memory_evidence WHERE source_keys = ?",
    ).get(JSON.stringify(source.sourceKeys));
    assert.deepEqual({ ...evidence }, {
      session_id: source.sessionId,
      session_file: source.sessionFile,
      source_generation: expectedGeneration,
      cwd: source.cwd,
      leaf_id: source.leafId,
      source_keys: JSON.stringify(source.sourceKeys),
      lineage_session_ids: JSON.stringify(source.lineageSessionIds),
      source_time: source.sourceTime,
      branch_anchor: source.branchAnchor,
    });
    assert.deepEqual(migrated.prepare(
      "SELECT source_key, processed_at FROM consolidation_progress ORDER BY source_key",
    ).all().map(row => ({ ...row })), expectedProgress);
    assert.equal(migrated.prepare(
      "SELECT status FROM consolidation_jobs WHERE id = ?",
    ).get(source.id).status, "complete", "migration preserves the completed v6 work record");
    const receipt = migrated.prepare(
      "SELECT session_file, session_id, source_generation, state FROM session_deletions",
    ).get();
    assert.deepEqual({ ...receipt }, {
      session_file: source.sessionFile,
      session_id: source.sessionId,
      source_generation: expectedGeneration,
      state: "complete",
    });
  } finally {
    migrated.close();
  }
});

test("schema v0 and v1 migrations preserve memory fields and create the current store", async (t) => {
  for (const version of [0, 1]) {
    const f = await fixture(t);
    const records = [memory("v" + version + "-a"), memory("v" + version + "-b", {
      content: "Existing content stays byte-for-byte during the migration. 漢字🧠\u0000end",
    })];
    const legacy = database(f.path);
    try {
      if (version === 0) {
        legacy.exec("CREATE TABLE memories (" +
          "id TEXT PRIMARY KEY NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL" +
          ") STRICT");
        const insert = legacy.prepare("INSERT INTO memories VALUES (?, ?, ?)");
        for (const record of records) insert.run(record.id, record.content, record.createdAt);
      } else {
        legacy.exec("CREATE TABLE memories (" +
          "id TEXT PRIMARY KEY NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL, " +
          "revision INTEGER NOT NULL, updated_at TEXT NOT NULL) STRICT");
        legacy.exec("PRAGMA user_version = 1");
        const insert = legacy.prepare("INSERT INTO memories VALUES (?, ?, ?, ?, ?)");
        for (const record of records) {
          insert.run(record.id, record.content, record.createdAt, 3, "2026-10-21T09:00:00.000Z");
        }
      }
    } finally {
      legacy.close();
    }

    const store = f.open();
    for (const record of records) {
      const expected = version === 0
        ? { ...record, revision: 1, updatedAt: record.createdAt }
        : { ...record, revision: 3, updatedAt: "2026-10-21T09:00:00.000Z" };
      assert.deepEqual(store.get(record.id), expected);
    }
    const check = database(f.path);
    try {
      assert.equal(check.prepare("PRAGMA user_version").get()["user_version"], 9);
      const names = check.prepare("SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name")
        .all().map((row) => row["name"]);
      assert.deepEqual(names, [
        "clean_source",
        "clean_tasks",
        "consolidation_jobs",
        "consolidation_progress",
        "distill_failures",
        "distill_lease",
        "distill_trigger_jobs",
        "distill_triggers",
        "jobs_source_status",
        "managed_sources",
        "managed_sources_header",
        "managed_sources_page",
        "managed_sources_parent",
        "managed_sources_path_session",
        "managed_sources_physical",
        "managed_sources_session_id",
        "memories",
        "memory_evidence",
        "memory_origins",
        "memory_origins_evidence",
        "metadata_gc_state",
        "observations_source",
        "session_deletions",
        "session_deletions_generation",
        "session_observations",
        "session_usages",
        "source_access",
        "source_backfill",
        "source_index_entries",
        "source_index_files",
        "source_registry_cursors",
        "source_registry_state",
        "trigger_links_job",
        "triggers_source_status",
        "usages_source",
      ]);
      assert.deepEqual(check.prepare("PRAGMA table_info(memory_origins)").all().map((row) => row["name"]), [
        "memory_id",
        "evidence_id",
        "revision",
      ]);
      assert.deepEqual(check.prepare("PRAGMA table_info(memory_evidence)").all().map((row) => row["name"]), [
        "evidence_id",
        "session_id",
        "session_file",
        "source_generation",
        "cwd",
        "leaf_id",
        "source_keys",
        "lineage_session_ids",
        "source_time",
        "branch_anchor",
      ]);
      assert.deepEqual(check.prepare("PRAGMA table_info(consolidation_progress)").all().map((row) => row["name"]), [
        "source_key",
        "processed_at",
      ]);
      assert.deepEqual(check.prepare("PRAGMA table_info(session_deletions)").all().map((row) => row["name"]), [
        "session_file",
        "session_id",
        "intent_id",
        "file_version",
        "source_generation",
        "state",
      ]);
      assert.deepEqual(check.prepare("PRAGMA table_info(metadata_gc_state)").all().map((row) => row["name"]), [
        "singleton",
        "phase",
        "cursor_row",
      ]);
      const observationColumns = check.prepare("PRAGMA table_info(session_observations)").all();
      assert.deepEqual(observationColumns.map((row) => row["name"]), [
        "session_id",
        "session_file",
        "cwd",
        "leaf_id",
        "owner_pid",
        "captured",
      ]);
      assert.equal(observationColumns[5]["notnull"], 1);
      assert.equal(observationColumns[5]["dflt_value"], "0");
    } finally {
      check.close();
    }
  }
});

test("future and unknown schemas fail without being reset", async (t) => {
  const f = await fixture(t);
  const raw = database(f.path);
  raw.exec("CREATE TABLE future_data (value TEXT NOT NULL) STRICT; PRAGMA user_version = 99");
  raw.prepare("INSERT INTO future_data VALUES (?)").run("preserve");
  raw.close();

  assert.throws(() => new SQLiteMemoryStore(f.path), /unsupported.*schema version/i);
  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("PRAGMA user_version").get()["user_version"], 99);
    assert.equal(verify.prepare("SELECT value FROM future_data").get()["value"], "preserve");
  } finally {
    verify.close();
  }
});

test("a version-three database with an unrecognized object is rejected and preserved", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.save(memory("preserve-current"));
  store.close();
  const raw = database(f.path);
  raw.exec("CREATE TABLE unexpected (value TEXT NOT NULL) STRICT");
  raw.prepare("INSERT INTO unexpected VALUES (?)").run("keep");
  raw.close();

  assert.throws(() => new SQLiteMemoryStore(f.path), /unsupported memory database schema/i);
  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("SELECT content FROM memories WHERE id = ?").get("preserve-current")["content"],
      "Synthetic memory preserve-current with retained conditions.");
    assert.equal(verify.prepare("SELECT value FROM unexpected").get()["value"], "keep");
  } finally {
    verify.close();
  }
});

test("current memory origins reject unexpected content columns", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = job("origin-content-source");
  const created = memory("metadata-only");
  store.enqueueConsolidation(source);
  assert.equal(store.claimConsolidation(source.id, "origin-content-worker", 100, 100), true);
  assert.equal(store.commitConsolidation(commit(source.id, "origin-content-worker", { creates: [created] })), true);
  store.close();
  const raw = database(f.path);
  raw.exec("ALTER TABLE memory_origins ADD COLUMN content TEXT");
  raw.prepare("UPDATE memory_origins SET content = ? WHERE memory_id = ?")
    .run("must not persist content here", created.id);
  raw.close();

  assert.throws(() => new SQLiteMemoryStore(f.path), /unsupported memory database schema/i);
  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("SELECT content FROM memory_origins WHERE memory_id = ?")
      .get(created.id)["content"], "must not persist content here");
  } finally {
    verify.close();
  }
});

test("session observations upsert by session and persist validated paths and branch metadata", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = {
    sessionId: "session-中文🧠",
    sessionFile: join(f.root, "session.jsonl"),
    cwd: f.root,
    leafId: "leaf-old",
    ownerPid: 456,
  };
  const latest = { ...first, leafId: null, ownerPid: 789 };
  store.observeSession(first);
  store.observeSession(latest);
  assert.deepEqual(store.observedSessions(), [latest]);
  store.close();
  assert.deepEqual(f.open().observedSessions(), [latest]);
});

test("session observations require exact-leaf acknowledgment and recapture changed boundaries", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = {
    sessionId: "observed-session",
    sessionFile: join(f.root, "session.jsonl"),
    cwd: f.root,
    leafId: "leaf-one",
    ownerPid: 100,
  };
  store.observeSession(first);
  assert.deepEqual(store.observedSessions(), [first]);

  store.acknowledgeObservation(first.sessionId, first.leafId);
  assert.deepEqual(store.observedSessions(), []);
  store.observeSession({ ...first, ownerPid: 200 });
  assert.deepEqual(store.observedSessions(), []);

  const secondLeaf = { ...first, leafId: "leaf-two", ownerPid: 300 };
  store.observeSession(secondLeaf);
  assert.deepEqual(store.observedSessions(), [secondLeaf]);
  store.acknowledgeObservation(first.sessionId, first.leafId);
  assert.deepEqual(store.observedSessions(), [secondLeaf]);
  store.acknowledgeObservation(secondLeaf.sessionId, secondLeaf.leafId);
  assert.deepEqual(store.observedSessions(), []);

  const movedFile = { ...secondLeaf, sessionFile: join(f.root, "moved-session.jsonl"), ownerPid: 400 };
  store.observeSession(movedFile);
  assert.deepEqual(store.observedSessions(), [movedFile]);
  store.acknowledgeObservation(movedFile.sessionId, movedFile.leafId);
  const movedCwd = { ...movedFile, cwd: join(f.root, "different-cwd"), ownerPid: 500 };
  store.observeSession(movedCwd);
  assert.deepEqual(store.observedSessions(), [movedCwd]);
  store.acknowledgeObservation("missing-session", null);
  store.acknowledgeObservation(movedCwd.sessionId, movedCwd.leafId);
  assert.deepEqual(store.observedSessions(), []);
});

test("external job, lease, observation, change, and stored JSON inputs are validated", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const valid = job("validate");
  for (const invalid of [
    null,
    { ...valid, extra: "transcript content is not permitted" },
    { ...valid, id: "bad-\ud800" },
    { ...valid, cwd: "relative/path" },
    { ...valid, sessionFile: "/tmp/session.jsonl", sourceKeys: ["", "valid"] },
    { ...valid, sourceKeys: ["duplicate", "duplicate"] },
  ]) assert.throws(() => store.enqueueConsolidation(invalid), TypeError);

  assert.throws(() => store.claimConsolidation("missing", "", 0, 1), TypeError);
  for (const [now, leaseMs] of [[-1, 1], [1.5, 1], [0, 0], [1, Number.MAX_SAFE_INTEGER], [Infinity, 1]]) {
    assert.throws(() => store.claimConsolidation("missing", "worker", now, leaseMs), TypeError);
  }
  assert.throws(() => store.observeSession({
    sessionId: "s", sessionFile: "relative.jsonl", cwd: f.root, leafId: null, ownerPid: 1,
  }), TypeError);
  assert.throws(() => store.observeSession({
    sessionId: "s", sessionFile: join(f.root, "s"), cwd: f.root, leafId: null, ownerPid: 0,
  }), TypeError);
  assert.throws(() => store.acknowledgeObservation("", null), TypeError);
  assert.throws(() => store.acknowledgeObservation("s", ""), TypeError);

  store.enqueueConsolidation(valid);
  store.claimConsolidation(valid.id, "worker", 100, 50);
  assert.throws(() => store.commitConsolidation(commit(valid.id, "worker", {
    creates: [memory("bad-content", { content: "lone-\ud800" })],
  })), TypeError);
  assert.equal(store.get("bad-content"), undefined);
  assert.deepEqual(store.listConsolidationJobs(), [valid]);

  store.close();
  const raw = database(f.path);
  raw.prepare("UPDATE consolidation_jobs SET job_json = ? WHERE id = ?")
    .run('{"id":"validate"}', valid.id);
  raw.close();
  const reopened = f.open();
  assert.throws(() => reopened.listConsolidationJobs(), TypeError);
});
