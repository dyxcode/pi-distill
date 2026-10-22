import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  ConsolidationConflictError,
  SQLiteMemoryStore,
} from "../src/memory-store.ts";

function trigger(id, overrides = {}) {
  return {
    id,
    sessionId: "session-" + id,
    sessionFile: join(process.cwd(), id + ".jsonl"),
    leafId: "leaf-" + id,
    cwd: process.cwd(),
    reason: "close",
    ...overrides,
  };
}

function job(id, sourceKey = id + ":entry", overrides = {}) {
  return {
    id,
    sessionId: "session-" + id,
    sessionFile: join(process.cwd(), id + ".jsonl"),
    leafId: "leaf-" + id,
    cwd: process.cwd(),
    sourceKeys: [sourceKey],
    ...overrides,
  };
}

function memory(id, content = "Synthetic retained fact.") {
  return { id, content, createdAt: "2026-10-22T00:00:00.000Z" };
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
  const root = await mkdtemp(join(tmpdir(), "pi-distill-task-store-"));
  const stores = [];
  t.after(async () => {
    for (const store of stores) store.close();
    await rm(root, { recursive: true, force: true });
  });
  const path = join(root, "memory.sqlite");
  function open() {
    const store = new SQLiteMemoryStore(path);
    stores.push(store);
    return store;
  }
  return { root, path, open };
}

test("close triggers queue immediately while compaction candidates require durable confirmation", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const close = trigger("close-a");
  const compact = trigger("compact-a", { reason: "compact" });
  store.enqueueDistillTrigger(close);
  store.enqueueDistillTrigger(structuredClone(close));
  store.enqueueDistillTrigger(compact);

  const firstPage = store.listDistillTriggers({ limit: 1 });
  assert.equal(firstPage.items[0].status, "pending");
  assert.equal(firstPage.items[0].sessionFile, close.sessionFile);
  assert.equal(firstPage.nextCursor, 1);
  const secondPage = store.listDistillTriggers({ limit: 1, cursor: firstPage.nextCursor });
  assert.equal(secondPage.items[0].status, "candidate");
  const claim = store.claimDistillTrigger({ workerId: "pi-a", now: 10, leaseMs: 20 });
  assert.equal(claim.trigger.id, close.id);
  store.releaseDistillClaim({ claimToken: claim.claimToken, now: 11 });

  assert.equal(store.confirmCompactionTrigger({ id: compact.id, compactionRecordId: "compact-record-1" }), true);
  assert.equal(store.confirmCompactionTrigger({ id: compact.id, compactionRecordId: "compact-record-1" }), true);
  assert.throws(
    () => store.confirmCompactionTrigger({ id: compact.id, compactionRecordId: "different-record" }),
    ConsolidationConflictError,
  );
  const confirmed = store.listDistillTriggers({ limit: 10 }).items.find(item => item.id === compact.id);
  assert.equal(confirmed.status, "pending");
  assert.equal(confirmed.compactionRecordId, "compact-record-1");
  assert.deepEqual(confirmed.jobIds, []);
});

test("failed compaction removes only its candidate and cannot cancel a confirmed trigger", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const candidate = trigger("candidate", { reason: "compact" });
  const confirmed = trigger("confirmed", { reason: "compact" });
  store.enqueueDistillTrigger(candidate);
  store.enqueueDistillTrigger(confirmed);
  assert.equal(store.cancelCompactionTrigger(candidate.id), true);
  assert.equal(store.cancelCompactionTrigger(candidate.id), false);
  store.confirmCompactionTrigger({ id: confirmed.id, compactionRecordId: "record" });
  assert.equal(store.cancelCompactionTrigger(confirmed.id), false);
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(item => item.id), [confirmed.id]);
});

test("one database-wide distill claim serializes trigger preparation and all consolidation jobs", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const queued = trigger("queued");
  const other = trigger("other");
  first.enqueueDistillTrigger(queued);
  first.enqueueDistillTrigger(other);
  const claim = first.claimDistillTrigger({ workerId: "pi-a", now: 100, leaseMs: 50 });
  assert.equal(claim.trigger.id, queued.id);
  assert.equal(second.claimDistillTrigger({ workerId: "pi-b", now: 101, leaseMs: 50 }), undefined);

  first.prepareDistillTrigger({ id: queued.id, claimToken: claim.claimToken, now: 102, jobs: [] });
  const next = second.claimDistillTrigger({ workerId: "pi-b", now: 103, leaseMs: 50 });
  assert.equal(next.trigger.id, other.id);
  assert.equal(first.renewDistillClaim({ claimToken: next.claimToken, now: 104, leaseMs: 80 }), true);
  assert.equal(first.releaseDistillClaim({ claimToken: next.claimToken, now: 105 }), true);
  assert.equal(second.releaseDistillClaim({ claimToken: next.claimToken, now: 105 }), false);
});

test("expired trigger claims are fenced and preparation atomically links its batches", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("prepared");
  const batchA = job("batch-a", "source:a", { sessionId: source.sessionId, sessionFile: source.sessionFile });
  const batchB = job("batch-b", "source:b", { sessionId: source.sessionId, sessionFile: source.sessionFile });
  store.enqueueDistillTrigger(source);
  const first = store.claimDistillTrigger({ workerId: "pi", now: 10, leaseMs: 5 });
  const second = store.claimDistillTrigger({ workerId: "pi", now: 15, leaseMs: 5 });
  assert.notEqual(first.claimToken, second.claimToken);
  assert.throws(
    () => store.prepareDistillTrigger({ id: source.id, claimToken: first.claimToken, now: 15, jobs: [batchA] }),
    ConsolidationConflictError,
  );
  store.prepareDistillTrigger({ id: source.id, claimToken: second.claimToken, now: 16, jobs: [batchA, batchB] });

  const prepared = store.listDistillTriggers({ limit: 10 }).items[0];
  assert.equal(prepared.status, "prepared");
  assert.deepEqual(prepared.jobIds, [batchA.id, batchB.id]);
  assert.deepEqual(store.listConsolidationJobs(), [batchA, batchB]);
  assert.deepEqual(store.readConsolidationStatus([]).pendingTriggers.map(item => item.id), [source.id]);
});

test("a new trigger can link an exact batch already queued by an overlapping range", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = trigger("overlap-first");
  const second = trigger("overlap-second", {
    sessionId: first.sessionId,
    sessionFile: first.sessionFile,
    leafId: "later-leaf",
  });
  const shared = job("overlap-batch", "session:shared", {
    sessionId: first.sessionId,
    sessionFile: first.sessionFile,
  });
  store.enqueueDistillTrigger(first);
  const firstClaim = store.claimDistillTrigger({ workerId: "pi", now: 10, leaseMs: 50 });
  store.prepareDistillTrigger({ id: first.id, claimToken: firstClaim.claimToken, now: 11, jobs: [shared] });

  store.enqueueDistillTrigger(second);
  const secondClaim = store.claimDistillTrigger({ workerId: "pi", now: 12, leaseMs: 50 });
  store.prepareDistillTrigger({ id: second.id, claimToken: secondClaim.claimToken, now: 13, jobs: [structuredClone(shared)] });

  const triggers = store.listDistillTriggers({ limit: 10 }).items;
  assert.deepEqual(triggers.map(item => item.jobIds), [[shared.id], [shared.id]]);
  assert.deepEqual(store.listConsolidationJobs(), [shared]);
  const batchClaim = store.claimDistillJob({ id: shared.id, workerId: "pi", now: 14, leaseMs: 50 });
  store.commitConsolidation(commit(shared.id, batchClaim.claimToken, { now: 15 }));
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(item => item.status), ["complete", "complete"]);
  assert.equal(store.listCleanTasks({ limit: 10 }).items.length, 1);
});

test("final associated batch atomically completes its trigger and queues clean while preserving memory provenance", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("two-batches");
  const first = job("batch-one", "source:one", { sessionId: source.sessionId, sessionFile: source.sessionFile });
  const second = job("batch-two", "source:two", { sessionId: source.sessionId, sessionFile: source.sessionFile });
  store.enqueueDistillTrigger(source);
  const preparation = store.claimDistillTrigger({ workerId: "pi", now: 10, leaseMs: 50 });
  store.prepareDistillTrigger({ id: source.id, claimToken: preparation.claimToken, now: 11, jobs: [first, second] });

  const claimOne = store.claimDistillJob({ id: first.id, workerId: "pi", now: 20, leaseMs: 50 });
  assert.ok(claimOne);
  store.commitConsolidation(commit(first.id, claimOne.claimToken, {
    now: 21,
    creates: [memory("origin", "First source fact.")],
  }));
  assert.equal(store.listDistillTriggers({ limit: 10 }).items[0].status, "prepared");
  assert.equal(store.listCleanTasks({ limit: 10 }).items.length, 0);

  const claimTwo = store.claimDistillJob({ id: second.id, workerId: "pi", now: 30, leaseMs: 50 });
  assert.ok(claimTwo);
  store.commitConsolidation(commit(second.id, claimTwo.claimToken, {
    now: 31,
    updates: [{
      id: "origin",
      expectedRevision: 1,
      content: "Second source corrects the retained fact.",
      updatedAt: "2026-10-22T00:01:00.000Z",
    }],
  }));

  const completed = store.listDistillTriggers({ limit: 10 }).items[0];
  assert.equal(completed.status, "complete");
  assert.equal(store.readConsolidationStatus([]).pendingTriggers.length, 0);
  assert.equal(store.get("origin").revision, 2);
  assert.deepEqual(store.memoryOrigin("origin").sourceKeys, second.sourceKeys);
  const tasks = store.listCleanTasks({ limit: 10 }).items;
  assert.equal(tasks.length, 1);
  assert.equal(tasks[0].sessionId, source.sessionId);
  assert.equal(tasks[0].sessionFile, source.sessionFile);
  assert.equal(tasks[0].waitReason, undefined);
  assert.equal(tasks[0].intentId, source.id);
});

test("empty completed ranges create clean intents, and clean assessment never completes or deletes them", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("empty-range");
  store.enqueueDistillTrigger(source);
  const claim = store.claimDistillTrigger({ workerId: "pi", now: 10, leaseMs: 50 });
  store.prepareDistillTrigger({ id: source.id, claimToken: claim.claimToken, now: 11, jobs: [] });

  let page = store.listCleanTasks({ limit: 10 });
  assert.equal(page.items[0].status, "pending");
  assert.equal(store.claimCleanTask(page.items[0].id, "clean-a", 20, 50), true);
  assert.equal(store.recordCleanAssessment(page.items[0].id, "clean-a", 21, "host_coordination", 100), true);
  assert.equal(store.claimCleanTask(page.items[0].id, "clean-b", 50, 50), false);
  page = store.listCleanTasks({ limit: 10, now: 50 });
  assert.equal(page.items.length, 0);
  page = store.listCleanTasks({ limit: 10, now: 100 });
  assert.equal(page.items[0].waitReason, "host_coordination");
  assert.equal(store.claimCleanTask(page.items[0].id, "clean-b", 100, 50), true);
  assert.equal(store.recordCleanAssessment(page.items[0].id, "clean-b", 101, "usage", 150), true);
  assert.equal(store.get(source.id), undefined);
  assert.equal(store.listCleanTasks({ limit: 10 }).items[0].status, "waiting");
});

test("a newer clean intent fences a running assessment and waiting tasks can be explicitly released", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = store.enqueueCleanTask({
    sessionId: "session-clean",
    sessionFile: join(f.root, "clean.jsonl"),
    leafId: "leaf-one",
    cwd: f.root,
    reason: "close",
    intentId: "intent-one",
  });
  assert.equal(store.claimCleanTask(first.id, "old-owner", 10, 100), true);
  const refreshed = store.enqueueCleanTask({
    sessionId: "session-clean",
    sessionFile: first.sessionFile,
    leafId: "leaf-two",
    cwd: f.root,
    reason: "compact",
    intentId: "intent-two",
  });
  assert.equal(refreshed.id, first.id);
  assert.equal(refreshed.leafId, "leaf-two");
  assert.equal(store.recordCleanAssessment(first.id, "old-owner", 11, "dependencies", 500), false);

  assert.equal(store.claimCleanTask(first.id, "new-owner", 20, 100), true);
  assert.equal(store.releaseCleanTask(first.id, "new-owner"), true);
  assert.equal(store.claimCleanTask(first.id, "next-owner", 21, 100), true);
});

test("legacy consolidation completion enqueues cleanup and v3 migration preserves existing records", async (t) => {
  const f = await fixture(t);
  const legacy = new DatabaseSync(f.path);
  legacy.exec(`
    CREATE TABLE memories (id TEXT PRIMARY KEY NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL, revision INTEGER NOT NULL, updated_at TEXT NOT NULL) STRICT;
    CREATE TABLE consolidation_jobs (id TEXT PRIMARY KEY NOT NULL, job_json TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), lease_owner TEXT, lease_until INTEGER, CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL))) STRICT;
    CREATE TABLE consolidation_progress (source_key TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL, processed_at INTEGER NOT NULL) STRICT;
    CREATE TABLE memory_origins (memory_id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL, revision INTEGER NOT NULL) STRICT;
    CREATE TABLE session_observations (session_id TEXT PRIMARY KEY NOT NULL, session_file TEXT NOT NULL, cwd TEXT NOT NULL, leaf_id TEXT, owner_pid INTEGER NOT NULL, captured INTEGER NOT NULL DEFAULT 0 CHECK (captured IN (0, 1))) STRICT;
    CREATE TABLE session_usages (owner_id TEXT PRIMARY KEY NOT NULL, process_token TEXT NOT NULL, session_id TEXT NOT NULL, session_file TEXT NOT NULL, cwd TEXT NOT NULL, leaf_id TEXT, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0), state TEXT NOT NULL CHECK (state IN ('active', 'closing', 'released'))) STRICT;
    PRAGMA user_version = 3;
  `);
  const completed = job("legacy-complete", "legacy:processed");
  const pending = job("legacy-pending", "legacy:pending");
  legacy.prepare("INSERT INTO memories VALUES (?, ?, ?, ?, ?)").run(
    "legacy-memory", "Preserved memory", "2026-10-20T00:00:00.000Z", 1, "2026-10-20T00:00:00.000Z",
  );
  legacy.prepare("INSERT INTO consolidation_jobs VALUES (?, ?, 'complete', NULL, NULL)").run(
    completed.id, JSON.stringify(completed),
  );
  legacy.prepare("INSERT INTO consolidation_jobs VALUES (?, ?, 'pending', ?, ?)").run(
    pending.id, JSON.stringify(pending), "old-owner", 9_999,
  );
  legacy.prepare("INSERT INTO consolidation_progress VALUES (?, ?, ?)").run(
    completed.sourceKeys[0], completed.id, 123,
  );
  legacy.prepare("INSERT INTO memory_origins VALUES (?, ?, ?)").run("legacy-memory", completed.id, 1);
  legacy.prepare("INSERT INTO session_observations VALUES (?, ?, ?, ?, ?, ?)").run(
    "observed", join(f.root, "observed.jsonl"), f.root, "leaf", 99, 0,
  );
  legacy.prepare("INSERT INTO session_usages VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(
    "owner", "process-token", "observed", join(f.root, "observed.jsonl"), f.root, "leaf", 99, "active",
  );
  legacy.close();

  const store = f.open();
  const version = new DatabaseSync(f.path);
  assert.equal(version.prepare("PRAGMA user_version").get().user_version, 5);
  version.close();
  assert.equal(store.get("legacy-memory").content, "Preserved memory");
  assert.deepEqual(store.memoryOrigin("legacy-memory").sourceKeys, completed.sourceKeys);
  assert.deepEqual(store.listConsolidationJobs(), [pending]);
  assert.deepEqual(store.processedSources(completed.sourceKeys), completed.sourceKeys);
  assert.equal(store.listSessionUsages()[0].state, "active");

  const newTrigger = trigger("new-after-v3-migration");
  store.enqueueDistillTrigger(newTrigger);
  assert.equal(store.claimDistillTrigger({ workerId: "new-worker", now: 9_998, leaseMs: 50 }), undefined);
  const afterLegacyLease = store.claimDistillTrigger({ workerId: "new-worker", now: 9_999, leaseMs: 50 });
  assert.equal(afterLegacyLease.trigger.id, newTrigger.id);
  assert.equal(store.releaseDistillClaim({ claimToken: afterLegacyLease.claimToken, now: 10_000 }), true);

  const reconciliation = store.reconcileLegacyCleanTasks(1);
  assert.equal(reconciliation.created, 1);
  assert.equal(reconciliation.nextCursor, undefined);
  const legacyClean = store.listCleanTasks({ limit: 10 }).items[0];
  assert.equal(legacyClean.reason, "legacy");
  assert.equal(store.claimCleanTask(legacyClean.id, "clean-owner", 10, 50), true);
  assert.equal(store.recordCleanAssessment(legacyClean.id, "clean-owner", 11, "usage", 500), true);
  assert.deepEqual(store.reconcileLegacyCleanTasks(1), { created: 0 });
  assert.deepEqual(store.listCleanTasks({ limit: 10 }).items[0], {
    ...legacyClean,
    status: "waiting",
    waitReason: "usage",
    retryCategory: "transient",
    retryAt: 500,
  });
});

test("legacy jobs enqueue cleanup atomically on their own successful commit", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const source = job("legacy-run", "legacy:entry");
  store.enqueueConsolidation(source);
  const claim = store.claimDistillJob({ id: source.id, workerId: "pi", now: 10, leaseMs: 50 });
  assert.ok(claim);
  store.commitConsolidation(commit(source.id, claim.claimToken, { now: 20 }));
  assert.equal(store.listCleanTasks({ limit: 10 }).items.length, 1);
  assert.equal(store.listCleanTasks({ limit: 10 }).items[0].sessionFile, source.sessionFile);
});

test("known session file discovery pages through trigger and clean sources", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const paths = ["a.jsonl", "b.jsonl", "c.jsonl"].map(name => join(f.root, name));
  store.enqueueDistillTrigger(trigger("path-a", { sessionFile: paths[0] }));
  store.enqueueDistillTrigger(trigger("path-b", { sessionFile: paths[1] }));
  store.enqueueCleanTask({
    sessionId: "session-c",
    sessionFile: paths[2],
    leafId: null,
    cwd: f.root,
    reason: "legacy",
    intentId: "legacy-c",
  });
  const first = store.listKnownSessionFiles(2);
  const second = store.listKnownSessionFiles(2, first.at(-1));
  assert.deepEqual([...first, ...second], paths);
});

test("pending session observations page fairly by durable row cursor", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  for (const sessionId of ["observed-c", "observed-a", "observed-b"]) {
    store.observeSession({
      sessionId,
      sessionFile: join(f.root, sessionId + ".jsonl"),
      cwd: f.root,
      leafId: null,
      ownerPid: 123,
    });
  }
  store.acknowledgeObservation("observed-a", null);

  const first = store.listObservedSessions({ limit: 1 });
  assert.deepEqual(first.items.map(item => item.sessionId), ["observed-c"]);
  assert.equal(first.nextCursor, 1);
  const second = store.listObservedSessions({ limit: 1, cursor: first.nextCursor });
  assert.deepEqual(second.items.map(item => item.sessionId), ["observed-b"]);
  assert.equal(second.nextCursor, undefined);
  assert.deepEqual(store.observedSessions().map(item => item.sessionId), ["observed-b", "observed-c"]);
});
