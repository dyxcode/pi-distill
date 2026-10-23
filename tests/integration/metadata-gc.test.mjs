import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { ConsolidationConflictError, SQLiteMemoryStore } from "../../src/storage/store.ts";

function job(id, sessionId, sessionFile, sourceKeys, overrides = {}) {
  return {
    id,
    sessionId,
    sessionFile,
    leafId: "leaf-" + sessionId,
    cwd: process.cwd(),
    sourceKeys,
    ...overrides,
  };
}

function memory(id) {
  return {
    id,
    content: "Synthetic fact with its original lineage retained.",
    createdAt: "2026-10-22T08:00:00.000Z",
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-metadata-gc-"));
  const path = join(root, "memories.sqlite");
  const stores = [];
  t.after(async () => {
    for (const store of stores) store.close();
    await rm(root, { recursive: true, force: true });
  });
  const open = () => {
    const store = new SQLiteMemoryStore(path);
    stores.push(store);
    return store;
  };
  return { root, path, open };
}

function hasJob(path, id) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT 1 AS present FROM consolidation_jobs WHERE id = ?").get(id) !== undefined;
  } finally {
    database.close();
  }
}

function hasCleanTask(path, sessionFile) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare(
      "SELECT 1 AS present FROM clean_tasks WHERE json_extract(task_json, '$.sessionFile') = ?",
    ).get(sessionFile) !== undefined;
  } finally {
    database.close();
  }
}

function hasTrigger(path, id) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT 1 AS present FROM distill_triggers WHERE id = ?").get(id) !== undefined;
  } finally {
    database.close();
  }
}

function hasTriggerLink(path, triggerId) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT 1 AS present FROM distill_trigger_jobs WHERE trigger_id = ?").get(triggerId) !== undefined;
  } finally {
    database.close();
  }
}

function hasObservation(path, sessionId) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT 1 AS present FROM session_observations WHERE session_id = ?").get(sessionId) !== undefined;
  } finally {
    database.close();
  }
}

function hasUsage(path, ownerId) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT 1 AS present FROM session_usages WHERE owner_id = ?").get(ownerId) !== undefined;
  } finally {
    database.close();
  }
}

function hasManagedSource(path, sessionFile, sessionId) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare(
      "SELECT 1 AS present FROM managed_sources WHERE session_file = ? AND session_id = ?",
    ).get(sessionFile, sessionId) !== undefined;
  } finally {
    database.close();
  }
}

function hasEvidenceForSource(path, sessionId, sourceKeys) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare(
      "SELECT 1 AS present FROM memory_evidence WHERE session_id = ? AND source_keys = ? LIMIT 1",
    ).get(sessionId, JSON.stringify(sourceKeys)) !== undefined;
  } finally {
    database.close();
  }
}

function totalRows(path) {
  const database = new DatabaseSync(path);
  try {
    const tables = database.prepare(
      "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*'",
    ).all();
    return tables.reduce((sum, { name }) => {
      const escapedName = name.replaceAll('"', '""');
      return sum + database.prepare(`SELECT COUNT(*) AS count FROM "${escapedName}"`).get().count;
    }, 0);
  } finally {
    database.close();
  }
}

function completeJob(store, source, createdMemory) {
  store.enqueueConsolidation(source);
  assert.equal(store.claimConsolidation(source.id, "worker-" + source.id, 100, 100), true);
  assert.equal(store.commitConsolidation({
    jobId: source.id,
    owner: "worker-" + source.id,
    now: 101,
    creates: createdMemory === undefined ? [] : [createdMemory],
    updates: [],
    deletes: [],
  }), true);
}

function completeJobFromTrigger(store, source, triggerId, createdMemory) {
  const trigger = {
    id: triggerId,
    sessionId: source.sessionId,
    sessionFile: source.sessionFile,
    leafId: source.leafId,
    cwd: source.cwd,
    reason: "close",
  };
  store.enqueueDistillTrigger(trigger);
  const claim = store.claimDistillTrigger({ workerId: "trigger-worker-" + triggerId, now: 100, leaseMs: 100 });
  assert.ok(claim);
  assert.equal(claim.trigger.id, triggerId);
  store.prepareDistillTrigger({ id: triggerId, claimToken: claim.claimToken, now: 101, jobs: [source] });
  const owner = "job-worker-" + source.id;
  assert.equal(store.claimConsolidation(source.id, owner, 102, 100), true);
  assert.equal(store.commitConsolidation({
    jobId: source.id,
    owner,
    now: 103,
    creates: createdMemory === undefined ? [] : [createdMemory],
    updates: [],
    deletes: [],
  }), true);
}

function addCapturedObservationAndReleasedUsage(store, source, ownerId) {
  const observation = {
    sessionId: source.sessionId,
    sessionFile: source.sessionFile,
    cwd: source.cwd,
    leafId: source.leafId,
    ownerPid: process.pid,
  };
  store.observeSession(observation);
  store.acknowledgeObservation(observation.sessionId, observation.leafId);
  const usage = {
    ...observation,
    ownerId,
    processToken: "process-token-" + ownerId,
    state: "active",
  };
  store.saveSessionUsage(usage);
  store.saveSessionUsage({ ...usage, state: "closing" });
  store.saveSessionUsage({ ...usage, state: "released" });
}

function collectAtMost(store, path, limit) {
  const before = totalRows(path);
  const result = store.collectMetadata(limit);
  const after = totalRows(path);
  assert.ok(result.removed <= limit, `collectMetadata(${limit}) removed ${result.removed} rows`);
  assert.equal(before - after, result.removed, "removed reports the actual durable rows reclaimed");
  return result;
}

function deleteSource(store, sessionFile, owner) {
  const task = store.listCleanTasks({ limit: 128 }).items.find(item => item.sessionFile === sessionFile);
  assert.ok(task, `completion should have queued cleanup for ${sessionFile}`);
  const now = Date.now();
  assert.equal(store.claimCleanTask(task.id, owner, now, 60_000), true);
  const access = store.acquireSourceAccess();
  assert.ok(access);
  try {
    assert.equal(store.prepareDeletion(task.id, owner, access, store.retentionVersion(), "1:2:3:4:5"), true);
    let mutationCalls = 0;
    assert.equal(store.finishDeletion(task.id, owner, access, store.retentionVersion(), () => {
      mutationCalls++;
      return true;
    }), true);
    assert.equal(mutationCalls, 1);
  } finally {
    store.releaseSourceAccess(access);
  }
}

test("metadata collection reclaims deleted-source jobs without losing memory facts or pending work", async t => {
  const f = await fixture(t);
  let store = f.open();
  const completedFile = join(f.root, "completed.jsonl");
  const secondFile = join(f.root, "second-completed.jsonl");
  const completed = job("completed-job", "completed-session", completedFile,
    ["shared:entry-1", "completed:entry-2"], {
      lineageSessionIds: ["root-session", "completed-session"],
      sourceTime: "2026-10-22T07:30:00.000Z",
      branchAnchor: "a".repeat(64),
    });
  const secondCompleted = job("second-completed-job", "second-session", secondFile, ["second:entry-1"]);
  const created = memory("retained-memory");
  const completedTriggerId = "completed-trigger";
  completeJobFromTrigger(store, completed, completedTriggerId, created);
  completeJob(store, secondCompleted);
  const completedUsageId = "released-completed-owner";
  addCapturedObservationAndReleasedUsage(store, completed, completedUsageId);

  const pendingFile = join(f.root, "pending.jsonl");
  const pending = job("pending-job", "pending-session", pendingFile,
    ["shared:entry-1", "pending:entry-1"]);
  store.enqueueConsolidation(pending);
  const pendingTrigger = {
    id: "pending-trigger",
    sessionId: pending.sessionId,
    sessionFile: pendingFile,
    leafId: pending.leafId,
    cwd: pending.cwd,
    reason: "close",
  };
  store.enqueueDistillTrigger(pendingTrigger);

  const expectedOrigin = {
    sessionId: completed.sessionId,
    sourceKeys: completed.sourceKeys,
    lineageSessionIds: completed.lineageSessionIds,
    sourceTime: completed.sourceTime,
    branchAnchor: completed.branchAnchor,
  };
  assert.deepEqual(store.memoryOrigin(created.id), expectedOrigin);
  const completedGeneration = store.sources.get(completedFile, completed.sessionId)?.generation;
  assert.ok(completedGeneration);
  const processedBefore = store.processedSources(completed.sourceKeys);
  assert.deepEqual(processedBefore, completed.sourceKeys);
  assert.equal(hasJob(f.path, completed.id), true);
  assert.equal(hasJob(f.path, secondCompleted.id), true);
  assert.equal(hasTrigger(f.path, completedTriggerId), true);
  assert.equal(hasTriggerLink(f.path, completedTriggerId), true);
  assert.equal(hasObservation(f.path, completed.sessionId), true);
  assert.equal(hasUsage(f.path, completedUsageId), true);
  const preDeletionCollection = collectAtMost(store, f.path, 128);
  assert.equal(hasJob(f.path, completed.id), true,
    "orphan evidence groups can be collected without reclaiming pending source work");
  assert.equal(hasJob(f.path, secondCompleted.id), true);
  assert.deepEqual(store.memoryOrigin(created.id), expectedOrigin,
    "evidence referenced by a current memory origin remains durable before deletion");
  assert.equal(preDeletionCollection.removed <= 1, true,
    "only the completed no-memory job's unreferenced evidence group is eligible here");

  deleteSource(store, completedFile, "delete-completed");
  deleteSource(store, secondFile, "delete-second-completed");
  assert.equal(store.getDeletion(completedFile)?.state, "complete");
  assert.equal(store.getDeletion(completedFile)?.sourceGeneration, completedGeneration,
    "the deletion receipt is bound to the exact managed source generation");
  assert.equal(store.listCleanTasks({ limit: 128 }).items.length, 0,
    "metadata collection must run after deletion leaves no clean tasks");

  const firstPage = collectAtMost(store, f.path, 1);
  assert.equal(typeof firstPage.removed, "number");
  assert.ok(firstPage.removed > 0);
  assert.equal(firstPage.hasMore, true, "the second deleted source remains beyond the one-item page");
  let page = firstPage;
  for (let attempt = 0; page.hasMore && attempt < 20; attempt++) {
    page = collectAtMost(store, f.path, 128);
  }
  assert.equal(page.hasMore, false, "all eligible metadata should be collected in bounded passes");

  assert.equal(hasJob(f.path, completed.id), false);
  assert.equal(hasJob(f.path, secondCompleted.id), false);
  assert.equal(hasTrigger(f.path, completedTriggerId), false);
  assert.equal(hasTriggerLink(f.path, completedTriggerId), false);
  assert.equal(hasObservation(f.path, completed.sessionId), false);
  assert.equal(hasUsage(f.path, completedUsageId), false);
  assert.equal(hasJob(f.path, pending.id), true);
  assert.deepEqual(store.listConsolidationJobs(), [pending]);
  assert.deepEqual(store.memoryOrigin(created.id), expectedOrigin);
  assert.equal(hasEvidenceForSource(f.path, completed.sessionId, completed.sourceKeys), true,
    "evidence referenced by a current memory origin survives operational metadata collection");
  assert.deepEqual(store.processedSources(completed.sourceKeys), processedBefore);
  assert.equal(store.getDeletion(completedFile)?.state, "complete",
    "the completed deletion receipt remains a fence against stale work");
  assert.equal(store.listDistillTriggers({ limit: 10 }).items.some(item => item.id === pendingTrigger.id), true);

  store.close();
  store = f.open();
  assert.deepEqual(store.memoryOrigin(created.id), expectedOrigin);
  assert.deepEqual(store.processedSources(completed.sourceKeys), processedBefore);
  assert.deepEqual(store.listConsolidationJobs(), [pending]);
  assert.equal(store.getDeletion(completedFile)?.state, "complete");
  assert.deepEqual(collectAtMost(store, f.path, 128), { removed: 0, hasMore: false, contended: false });
  assert.throws(() => store.enqueueConsolidation(completed), ConsolidationConflictError);
  assert.throws(() => store.enqueueConsolidation(job("late-job", completed.sessionId, completedFile, ["late:entry"])), ConsolidationConflictError);
  assert.throws(() => store.enqueueDistillTrigger({
    ...pendingTrigger,
    id: "late-trigger",
    sessionId: completed.sessionId,
    sessionFile: completedFile,
  }), ConsolidationConflictError);

  const now = Date.now();
  assert.equal(store.claimConsolidation(pending.id, "pending-worker", now, 60_000), true);
  assert.equal(store.commitConsolidation({
    jobId: pending.id,
    owner: "pending-worker",
    now: now + 1,
    sourceKeys: ["pending:entry-1"],
    creates: [],
    updates: [],
    deletes: [],
  }), true, "the preserved shared source fact keeps pending work from replaying it");
  assert.deepEqual(store.processedSources(pending.sourceKeys), pending.sourceKeys);
});
test("embedded-NUL trigger and job identities survive reopen and bounded metadata collection", async t => {
  const f = await fixture(t);
  let store = f.open();
  const sessionFile = join(f.root, "nul-identities.jsonl");
  const source = job("nul\0job", "nul\0session", sessionFile, ["nul:source-key"]);
  const triggerId = "nul\0trigger";
  const created = memory("nul-origin-memory");
  completeJobFromTrigger(store, source, triggerId, created);

  assert.equal(hasJob(f.path, source.id), true);
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(({ id, jobIds }) => ({ id, jobIds })), [
    { id: triggerId, jobIds: [source.id] },
  ]);
  const origin = store.memoryOrigin(created.id);
  assert.deepEqual(origin, { sessionId: source.sessionId, sourceKeys: source.sourceKeys });
  assert.equal(hasTriggerLink(f.path, triggerId), true);

  store.close();
  store = f.open();
  assert.equal(hasJob(f.path, source.id), true);
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(({ id, jobIds }) => ({ id, jobIds })), [
    { id: triggerId, jobIds: [source.id] },
  ]);
  assert.deepEqual(store.memoryOrigin(created.id), origin,
    "the evidence ID, session ID, and source keys retain their complete text values after reopen");

  deleteSource(store, sessionFile, "nul-source-delete");
  let page = collectAtMost(store, f.path, 1);
  for (let attempt = 0; page.hasMore && attempt < 64; attempt += 1) {
    page = collectAtMost(store, f.path, 1);
  }
  assert.equal(page.hasMore, false);
  assert.equal(hasTriggerLink(f.path, triggerId), false,
    "phase 1 validates and reclaims the trigger/job link using full embedded-NUL IDs");
  assert.equal(hasTrigger(f.path, triggerId), false);
  assert.equal(hasJob(f.path, source.id), false);
  assert.equal(hasManagedSource(f.path, sessionFile, source.sessionId), false,
    "phase 6 decodes the full embedded-NUL session ID when reclaiming the source generation");
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, source.sourceKeys), true,
    "phase 7 preserves evidence referenced by the current memory origin");
  assert.deepEqual(store.memoryOrigin(created.id), origin);
  assert.deepEqual(store.processedSources(source.sourceKeys), source.sourceKeys);
  assert.equal(store.getDeletion(sessionFile)?.state, "complete");
});

test("pending and uncertain references keep terminal metadata after a deletion receipt", async t => {
  const f = await fixture(t);
  const store = f.open();
  const sessionFile = join(f.root, "blocked-source.jsonl");
  const completed = job("blocked-completed-job", "blocked-session", sessionFile, ["blocked:completed"]);
  const pending = job("blocked-pending-job", completed.sessionId, sessionFile, ["blocked:pending"]);
  completeJob(store, completed);
  store.enqueueConsolidation(pending);

  const pendingTrigger = {
    id: "blocked-pending-trigger",
    sessionId: completed.sessionId,
    sessionFile,
    leafId: completed.leafId,
    cwd: completed.cwd,
    reason: "close",
  };
  store.enqueueDistillTrigger(pendingTrigger);
  store.observeSession({
    sessionId: completed.sessionId,
    sessionFile,
    cwd: completed.cwd,
    leafId: completed.leafId,
    ownerPid: process.pid,
  });
  const activeUsage = {
    ownerId: "blocked-active-owner",
    processToken: "blocked-process-token",
    sessionId: completed.sessionId,
    sessionFile,
    cwd: completed.cwd,
    leafId: completed.leafId,
    ownerPid: process.pid,
    state: "active",
  };
  store.saveSessionUsage(activeUsage);

  // Exercise the collector's own fail-closed checks even if a stale or externally
  // produced receipt coexists with newer same-source references.
  deleteSource(store, sessionFile, "blocked-source-delete");
  assert.equal(store.getDeletion(sessionFile)?.state, "complete");
  let page = collectAtMost(store, f.path, 4);
  for (let attempt = 0; page.hasMore && attempt < 20; attempt++) {
    page = collectAtMost(store, f.path, 4);
  }
  assert.equal(page.hasMore, false, "blocked records do not keep a completed sweep busy forever");
  assert.equal(hasJob(f.path, completed.id), true, "pending work for the same source protects its terminal job history");
  assert.equal(hasJob(f.path, pending.id), true);
  assert.equal(hasTrigger(f.path, pendingTrigger.id), true);
  assert.equal(hasObservation(f.path, completed.sessionId), true, "uncaptured observation remains unresolved");
  assert.equal(hasUsage(f.path, activeUsage.ownerId), true, "active usage remains a live source reference");
  assert.equal(store.sources.get(sessionFile, completed.sessionId)?.deleted, true,
    "source metadata stays available for resolving a completed receipt with newer references");
});

test("a replacement session at a receipted path does not inherit the old source generation's receipt", async t => {
  const f = await fixture(t);
  const store = f.open();
  const sessionFile = join(f.root, "replaced-source.jsonl");
  const oldJob = job("replaced-old-job", "old-session", sessionFile, ["old:entry"]);
  completeJob(store, oldJob);
  deleteSource(store, sessionFile, "delete-old-source");

  const replacement = {
    sessionId: "replacement-session",
    sessionFile,
    cwd: f.root,
  };
  const oldSource = store.sources.get(sessionFile, oldJob.sessionId);
  assert.ok(oldSource);
  assert.equal(store.getDeletion(sessionFile)?.sourceGeneration, oldSource.generation);
  const newSource = store.sources.register(replacement);
  assert.notEqual(newSource.generation, oldSource.generation);
  assert.equal(store.sources.recordHeader({
    sessionFile: replacement.sessionFile,
    sessionId: replacement.sessionId,
    fileVersion: "9:42:0:1:1",
  }), true);
  assert.equal(store.getDeletion(sessionFile)?.sessionId, oldJob.sessionId);

  let page = collectAtMost(store, f.path, 2);
  for (let attempt = 0; page.hasMore && attempt < 20; attempt++) {
    page = collectAtMost(store, f.path, 2);
  }
  const stillCurrent = store.sources.get(sessionFile, replacement.sessionId);
  assert.ok(stillCurrent);
  assert.equal(stillCurrent.generation, newSource.generation);
  assert.equal(stillCurrent.deleted, false, "a path-matched old receipt cannot delete a replacement source");
  assert.equal(store.getDeletion(sessionFile)?.sessionId, oldJob.sessionId);
  assert.equal(store.sources.get(sessionFile, oldJob.sessionId), undefined);
  assert.equal(hasManagedSource(f.path, sessionFile, oldJob.sessionId), false,
    "the collector really reclaimed the old source row before the stale enqueue retry");

  assert.throws(() => store.enqueueConsolidation(oldJob), ConsolidationConflictError);
  assert.throws(() => store.enqueueDistillTrigger({
    id: "stale-old-trigger-after-source-gc",
    sessionId: oldJob.sessionId,
    sessionFile,
    leafId: oldJob.leafId,
    cwd: oldJob.cwd,
    reason: "close",
  }), ConsolidationConflictError);
  assert.equal(hasManagedSource(f.path, sessionFile, oldJob.sessionId), false,
    "a stale enqueue cannot register a fresh generation and bypass its durable receipt");
  assert.equal(store.sources.get(sessionFile, replacement.sessionId)?.deleted, false);
  assert.equal(store.getDeletion(sessionFile)?.sourceGeneration, oldSource.generation);

  const replacementJob = job("replacement-job", replacement.sessionId, sessionFile, ["replacement:entry"]);
  const replacementMemory = memory("replacement-memory");
  completeJob(store, replacementJob, replacementMemory);
  assert.deepEqual(store.memoryOrigin(replacementMemory.id), {
    sessionId: replacement.sessionId,
    sourceKeys: replacementJob.sourceKeys,
  });
  assert.equal(hasCleanTask(f.path, sessionFile), true,
    "new work may reach a durable clean intent at this path");
  assert.equal(store.listCleanTasks({ limit: 128 }).items.some(task => task.sessionFile === sessionFile), false,
    "the immutable path receipt prevents exposing deletion work for a later session");
  assert.equal(store.getDeletion(sessionFile)?.sessionId, oldJob.sessionId);
  assert.equal(store.sources.get(sessionFile, replacement.sessionId)?.deleted, false,
    "the new generation remains protected without inheriting old delete authorization");
});

test("metadata collection bounds row deletions across two stores and resumes after reopen", async t => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const jobs = [];
  for (let index = 0; index < 9; index += 1) {
    const source = job(`many-completed-${index}`, `many-session-${index}`,
      join(f.root, `many-source-${index}.jsonl`), [`many:${index}`]);
    jobs.push(source);
    completeJob(first, source);
    deleteSource(first, source.sessionFile, `many-delete-${index}`);
  }

  let page = collectAtMost(first, f.path, 2);
  assert.ok(page.removed > 0);
  assert.equal(page.hasMore, true, "more terminal source metadata remains beyond the first bounded page");
  const secondPage = collectAtMost(second, f.path, 2);
  assert.ok(secondPage.removed <= 2);
  first.close();
  second.close();

  let resumed = f.open();
  page = collectAtMost(resumed, f.path, 2);
  for (let attempt = 0; page.hasMore && attempt < 100; attempt += 1) {
    page = collectAtMost(resumed, f.path, 2);
  }
  assert.equal(page.hasMore, false, "a reopened store resumes and finishes the durable collection sweep");
  for (const source of jobs) {
    assert.equal(hasJob(f.path, source.id), false);
    assert.equal(resumed.getDeletion(source.sessionFile)?.state, "complete");
  }
  assert.deepEqual(collectAtMost(resumed, f.path, 2), { removed: 0, hasMore: false, contended: false },
    "repeating collection after completion is idempotent");
});

test("a deletion between the jobs and registry phases leaves the tombstone until operational rows are collected", async t => {
  const f = await fixture(t);
  const store = f.open();
  const sessionFile = join(f.root, "interleaved-cleanup.jsonl");
  const source = job("interleaved-cleanup-job", "interleaved-cleanup-session", sessionFile,
    ["interleaved:entry-1"]);
  completeJob(store, source);

  assert.deepEqual(collectAtMost(store, f.path, 1), { removed: 0, hasMore: true, contended: false },
    "the first bounded pass examines the clean task before a deletion receipt exists");
  assert.deepEqual(collectAtMost(store, f.path, 1), { removed: 0, hasMore: true, contended: false },
    "the next bounded pass advances through the completed job row");
  assert.equal(hasJob(f.path, source.id), true);
  assert.equal(store.listCleanTasks({ limit: 10 }).items.some(item => item.sessionFile === sessionFile), true);

  deleteSource(store, sessionFile, "interleaved-source-delete");
  assert.equal(store.getDeletion(sessionFile)?.state, "complete");
  collectAtMost(store, f.path, 1);

  assert.equal(hasManagedSource(f.path, sessionFile, source.sessionId), true,
    "the registry-phase check retains a receipted source while its job and task rows remain");
  assert.equal(hasJob(f.path, source.id), true);
  assert.equal(hasCleanTask(f.path, sessionFile), true,
    "public clean-task listings hide work behind a complete receipt, but its durable row still needs collection");

  for (let attempt = 0; attempt < 32; attempt += 1) {
    if (!hasJob(f.path, source.id) &&
      !hasCleanTask(f.path, sessionFile) &&
      !hasManagedSource(f.path, sessionFile, source.sessionId)) break;
    collectAtMost(store, f.path, 1);
  }
  assert.equal(hasJob(f.path, source.id), false);
  assert.equal(hasCleanTask(f.path, sessionFile), false);
  assert.equal(hasManagedSource(f.path, sessionFile, source.sessionId), false,
    "a later sweep removes the job and clean intent before reclaiming the source tombstone");
  assert.equal(store.getDeletion(sessionFile)?.state, "complete");
});

test("unreferenced memory evidence is collected after its current memory is forgotten", async t => {
  const f = await fixture(t);
  const store = f.open();
  const sessionFile = join(f.root, "forgotten-origin.jsonl");
  const source = job("forgotten-origin-job", "forgotten-origin-session", sessionFile, ["forgotten:entry"]);
  const created = memory("forgotten-origin-memory");
  completeJob(store, source, created);
  assert.ok(store.memoryOrigin(created.id));
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, source.sourceKeys), true);
  const processedBefore = store.processedSources(source.sourceKeys);
  assert.deepEqual(processedBefore, source.sourceKeys);

  store.forget({ id: created.id, expectedRevision: 1 });
  assert.equal(store.get(created.id), undefined);
  assert.equal(store.memoryOrigin(created.id), undefined);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, source.sourceKeys), true,
    "forgetting the last memory origin leaves evidence for bounded metadata collection");
  deleteSource(store, sessionFile, "forgotten-origin-delete");

  let page = collectAtMost(store, f.path, 1);
  for (let attempt = 0; page.hasMore && attempt < 64; attempt += 1) {
    page = collectAtMost(store, f.path, 1);
  }
  assert.equal(page.hasMore, false);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, source.sourceKeys), false);
  assert.equal(hasJob(f.path, source.id), false);
  assert.deepEqual(store.processedSources(source.sourceKeys), processedBefore,
    "collecting orphaned provenance does not allow processed source evidence to replay");
  assert.equal(store.getDeletion(sessionFile)?.state, "complete");
});

test("metadata collection retains only prefix evidence groups referenced by current memories", async t => {
  const f = await fixture(t);
  const store = f.open();
  const sessionFile = join(f.root, "prefix-evidence.jsonl");
  const source = job("prefix-evidence-job", "prefix-evidence-session", sessionFile,
    ["prefix-evidence:a", "prefix-evidence:b"], {
      sourceTime: "2026-10-22T07:30:00.000Z",
      branchAnchor: "f".repeat(64),
    });
  const firstTime = "2026-10-22T07:00:00.000Z";
  const secondTime = "2026-10-22T07:15:00.000Z";
  const firstMemory = memory("prefix-evidence-memory-a");
  const secondMemory = memory("prefix-evidence-memory-b");
  store.enqueueConsolidation(source);
  assert.equal(store.claimConsolidation(source.id, "prefix-worker-a", 100, 100), true);
  assert.equal(store.commitConsolidation({
    jobId: source.id,
    owner: "prefix-worker-a",
    now: 101,
    sourceKeys: ["prefix-evidence:a"],
    evidence: { sourceTime: firstTime, branchAnchor: "a".repeat(64) },
    creates: [firstMemory],
    updates: [],
    deletes: [],
  }), true);
  assert.equal(store.claimConsolidation(source.id, "prefix-worker-b", 102, 100), true);
  assert.equal(store.commitConsolidation({
    jobId: source.id,
    owner: "prefix-worker-b",
    now: 103,
    sourceKeys: ["prefix-evidence:b"],
    evidence: { sourceTime: secondTime, branchAnchor: "b".repeat(64) },
    creates: [secondMemory],
    updates: [],
    deletes: [],
  }), true);

  const firstOrigin = {
    sessionId: source.sessionId,
    sourceKeys: ["prefix-evidence:a"],
    sourceTime: firstTime,
    branchAnchor: "a".repeat(64),
  };
  const secondOrigin = {
    sessionId: source.sessionId,
    sourceKeys: ["prefix-evidence:b"],
    sourceTime: secondTime,
    branchAnchor: "b".repeat(64),
  };
  assert.deepEqual(store.memoryOrigin(firstMemory.id), firstOrigin);
  assert.deepEqual(store.memoryOrigin(secondMemory.id), secondOrigin);
  deleteSource(store, sessionFile, "prefix-evidence-delete");

  let page = collectAtMost(store, f.path, 1);
  for (let attempt = 0; page.hasMore && attempt < 64; attempt += 1) {
    page = collectAtMost(store, f.path, 1);
  }
  assert.equal(page.hasMore, false);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, firstOrigin.sourceKeys), true);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, secondOrigin.sourceKeys), true);
  assert.deepEqual(store.memoryOrigin(firstMemory.id), firstOrigin);
  assert.deepEqual(store.memoryOrigin(secondMemory.id), secondOrigin);

  store.forget({ id: firstMemory.id, expectedRevision: 1 });
  let firstOrphan = collectAtMost(store, f.path, 128);
  for (let attempt = 0; firstOrphan.hasMore && attempt < 8; attempt += 1) {
    firstOrphan = collectAtMost(store, f.path, 128);
  }
  assert.equal(firstOrphan.hasMore, false);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, firstOrigin.sourceKeys), false);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, secondOrigin.sourceKeys), true);
  assert.deepEqual(store.memoryOrigin(secondMemory.id), secondOrigin);

  store.forget({ id: secondMemory.id, expectedRevision: 1 });
  let secondOrphan = collectAtMost(store, f.path, 128);
  for (let attempt = 0; secondOrphan.hasMore && attempt < 8; attempt += 1) {
    secondOrphan = collectAtMost(store, f.path, 128);
  }
  assert.equal(secondOrphan.hasMore, false);
  assert.equal(hasEvidenceForSource(f.path, source.sessionId, secondOrigin.sourceKeys), false);
});

test('a metadata round with unequal retained core and derived sweeps finishes across reopen',async t=>{
  const f=await fixture(t);let store=f.open();
  const pending={id:'pending-gc',sessionId:'pending-gc',sessionFile:join(f.root,'pending.jsonl'),cwd:f.root,leafId:null,reason:'close'};
  store.enqueueDistillTrigger(pending);
  const raw=new DatabaseSync(f.path);
  // Persistent synthetic preparation references remain live, while orphan rows
  // exercise a different-sized derived sweep than the operational records.
  raw.prepare('INSERT INTO source_preparation VALUES (?,?,?)').run(pending.id,'a'.repeat(64),0);
  for(let i=0;i<17;i++)raw.prepare('INSERT INTO source_preparation VALUES (?,?,?)').run(`orphan-${i}`,'b'.repeat(64),0);
  raw.close();
  let result,removed=0;
  for(let i=0;i<100;i++){
    result=collectAtMost(store,f.path,2);removed+=result.removed;
    if(i===3){store.close();store=f.open();}
    if(!result.hasMore)break;
  }
  assert.equal(result.hasMore,false,'retained metadata must not require coincident sweep completion');
  assert.equal(removed,17);assert.equal(store.readDistillTrigger(pending.id).status,'pending');
  assert.equal(store.readSourcePreparation(pending.id,'a'.repeat(64)),0);
});

test('source access contention is reported separately from completed metadata work',async t=>{
  const f=await fixture(t);const owner=f.open(),worker=f.open(),token=owner.acquireSourceAccess();
  assert.ok(token);assert.deepEqual(worker.collectMetadata(128),{removed:0,hasMore:true,contended:true});
  owner.releaseSourceAccess(token);
  assert.deepEqual(worker.collectMetadata(128),{removed:0,hasMore:false,contended:false});
});

test('finite metadata sweeps finish despite new retained work arriving after every page',async t=>{
  const f=await fixture(t),store=f.open();
  const add=id=>store.enqueueConsolidation({id,sessionId:id,sessionFile:null,leafId:null,cwd:f.root,sourceKeys:[id+':key']});
  for(let i=0;i<20;i++)add(`old-${i}`);
  let result;
  for(let i=0;i<100;i++){
    result=collectAtMost(store,f.path,2);
    for(let j=0;j<3;j++)add(`new-${i}-${j}`);
    if(!result.hasMore)break;
  }
  assert.equal(result.hasMore,false,'new arrivals cannot indefinitely extend the current finite sweep');
  assert.ok(store.listConsolidationJobs().some(job=>job.id==='old-19'));
});
