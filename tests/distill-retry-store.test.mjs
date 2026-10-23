import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { SQLiteMemoryStore } from "../src/memory-store.ts";

const context = (overrides = {}) => ({
  sourceVersion: "source-v1",
  modelKey: "provider/model-a",
  policyVersion: "policy-v1",
  ...overrides,
});

function trigger(id) {
  return {
    id,
    sessionId: `session-${id}`,
    sessionFile: join(process.cwd(), `${id}.jsonl`),
    leafId: `leaf-${id}`,
    cwd: process.cwd(),
    reason: "close",
  };
}

function job(id, source) {
  return {
    id,
    sessionId: source.sessionId,
    sessionFile: source.sessionFile,
    leafId: source.leafId,
    cwd: source.cwd,
    sourceKeys: [`${source.sessionId}:entry`],
  };
}

function observation(id) {
  return {
    sessionId: `session-${id}`,
    sessionFile: join(process.cwd(), `${id}.jsonl`),
    cwd: process.cwd(),
    leafId: `leaf-${id}`,
    ownerPid: process.pid,
  };
}

function usage(id) {
  return {
    ...observation(id),
    ownerId: `owner-${id}`,
    processToken: `process-${id}`,
    state: "active",
  };
}

function completeJob(store, record, now) {
  store.enqueueConsolidation(record);
  const claim = store.claimDistillJob({
    id: record.id, workerId: `complete-${record.id}`, now, leaseMs: 50,
  });
  assert.ok(claim);
  assert.equal(store.commitConsolidation({
    jobId: record.id,
    owner: claim.claimToken,
    now: now + 1,
    creates: [],
    updates: [],
    deletes: [],
  }), true);
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-retry-store-"));
  const path = join(root, "memory.sqlite");
  const stores = [];
  const open = () => {
    const store = new SQLiteMemoryStore(path);
    stores.push(store);
    return store;
  };
  t.after(async () => {
    for (const store of stores) store.close();
    await rm(root, { recursive: true, force: true });
  });
  return { path, open };
}

test("readDistillTrigger returns the public state without changing claimability", async t => {
  const f = await fixture(t);
  const store = f.open();
  const pending = trigger("read-pending");
  const candidate = { ...trigger("read-candidate"), reason: "compact" };
  store.enqueueDistillTrigger(pending);
  store.enqueueDistillTrigger(candidate);

  assert.deepEqual(store.readDistillTrigger(pending.id), {
    ...pending,
    status: "pending",
    compactionRecordId: null,
    jobIds: [],
    retryCategory: null,
    retryAt: null,
  });
  assert.deepEqual(store.readDistillTrigger(candidate.id), {
    ...candidate,
    status: "candidate",
    compactionRecordId: null,
    jobIds: [],
    retryCategory: null,
    retryAt: null,
  });
  assert.equal(store.readDistillTrigger("missing-trigger"), undefined);
  assert.ok(store.claimDistillTrigger({ id: pending.id, workerId: "reader-test", now: 10, leaseMs: 20 }));
});

test("pending job pages preserve enqueue order across completed rows without skipping work", async t => {
  const f = await fixture(t);
  const store = f.open();
  const completedBefore = job("complete-before", trigger("complete-before-source"));
  const first = job("pending-first", trigger("pending-first-source"));
  const completedBetween = job("complete-between", trigger("complete-between-source"));
  const second = job("pending-second", trigger("pending-second-source"));
  const completedAfter = job("complete-after", trigger("complete-after-source"));
  const third = job("pending-third", trigger("pending-third-source"));

  completeJob(store, completedBefore, 10);
  store.enqueueConsolidation(first);
  completeJob(store, completedBetween, 20);
  store.enqueueConsolidation(second);
  completeJob(store, completedAfter, 30);
  store.enqueueConsolidation(third);

  const firstPage = store.listPendingConsolidationJobs({ limit: 1 });
  assert.deepEqual(firstPage.items, [first]);
  assert.ok(firstPage.nextCursor);
  const secondPage = store.listPendingConsolidationJobs({ limit: 1, cursor: firstPage.nextCursor });
  assert.deepEqual(secondPage.items, [second]);
  assert.ok(secondPage.nextCursor);
  const thirdPage = store.listPendingConsolidationJobs({ limit: 1, cursor: secondPage.nextCursor });
  assert.deepEqual(thirdPage.items, [third]);
  assert.equal(thirdPage.nextCursor, undefined);
});

test("rowid task sweeps exclude rows appended after their first page", async t => {
  const f = await fixture(t);
  const store = f.open();
  const jobOne = job("sweep-job-one", trigger("sweep-job-one-source"));
  const jobTwo = job("sweep-job-two", trigger("sweep-job-two-source"));
  const triggerOne = trigger("sweep-trigger-one");
  const triggerTwo = trigger("sweep-trigger-two");
  const observationOne = observation("sweep-observation-one");
  const observationTwo = observation("sweep-observation-two");
  const usageOne = usage("sweep-usage-one");
  const usageTwo = usage("sweep-usage-two");
  store.enqueueConsolidation(jobOne);
  store.enqueueConsolidation(jobTwo);
  store.enqueueDistillTrigger(triggerOne);
  store.enqueueDistillTrigger(triggerTwo);
  store.observeSession(observationOne);
  store.observeSession(observationTwo);
  store.saveSessionUsage(usageOne);
  store.saveSessionUsage(usageTwo);

  const firstJobs = store.listPendingConsolidationJobs({ limit: 1 });
  const firstTriggers = store.listDistillTriggers({ limit: 1 });
  const firstObservations = store.listObservedSessions({ limit: 1 });
  const firstUsages = store.listUnobservedSessionUsages({ limit: 1 });
  assert.ok(firstJobs.nextCursor);
  assert.ok(firstTriggers.nextCursor);
  assert.ok(firstObservations.nextCursor);
  assert.ok(firstUsages.nextCursor);

  const jobThree = job("sweep-job-three", trigger("sweep-job-three-source"));
  const triggerThree = trigger("sweep-trigger-three");
  const observationThree = observation("sweep-observation-three");
  const usageThree = usage("sweep-usage-three");
  store.enqueueConsolidation(jobThree);
  store.enqueueDistillTrigger(triggerThree);
  store.observeSession(observationThree);
  store.saveSessionUsage(usageThree);

  const oldJobs = store.listPendingConsolidationJobs({
    limit: 1, cursor: firstJobs.nextCursor, throughCursor: firstJobs.throughCursor,
  });
  const oldTriggers = store.listDistillTriggers({
    limit: 1, cursor: firstTriggers.nextCursor, throughCursor: firstTriggers.throughCursor,
  });
  const oldObservations = store.listObservedSessions({
    limit: 1, cursor: firstObservations.nextCursor, throughCursor: firstObservations.throughCursor,
  });
  const oldUsages = store.listUnobservedSessionUsages({
    limit: 1, cursor: firstUsages.nextCursor, throughCursor: firstUsages.throughCursor,
  });
  assert.deepEqual(oldJobs.items.map(item => item.id), [jobTwo.id]);
  assert.deepEqual(oldTriggers.items.map(item => item.id), [triggerTwo.id]);
  assert.deepEqual(oldObservations.items.map(item => item.sessionId), [observationTwo.sessionId]);
  assert.deepEqual(oldUsages.items.map(item => item.ownerId), [usageTwo.ownerId]);
  for (const [oldPage, firstPage] of [
    [oldJobs, firstJobs], [oldTriggers, firstTriggers],
    [oldObservations, firstObservations], [oldUsages, firstUsages],
  ]) {
    assert.equal(oldPage.throughCursor, firstPage.throughCursor);
    assert.equal(oldPage.nextCursor, undefined);
  }

  assert.deepEqual(store.listPendingConsolidationJobs({ limit: 128 }).items.map(item => item.id), [
    jobOne.id, jobTwo.id, jobThree.id,
  ]);
  assert.deepEqual(store.listDistillTriggers({ limit: 128 }).items.map(item => item.id), [
    triggerOne.id, triggerTwo.id, triggerThree.id,
  ]);
  assert.deepEqual(store.listObservedSessions({ limit: 128 }).items.map(item => item.sessionId), [
    observationOne.sessionId, observationTwo.sessionId, observationThree.sessionId,
  ]);
  assert.deepEqual(store.listUnobservedSessionUsages({ limit: 128 }).items.map(item => item.ownerId), [
    usageOne.ownerId, usageTwo.ownerId, usageThree.ownerId,
  ]);
});

test("source validation failures persist and only a source or policy change can release the hold", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("invalid-source");
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const claim = store.claimDistillTrigger({
    id: source.id, workerId: "worker", now: 100, leaseMs: 50, retryContext: attemptedWith,
  });
  assert.ok(claim);
  assert.equal(store.releaseDistillClaim({
    claimToken: claim.claimToken,
    now: 101,
    failure: { reason: "source_invalid", context: attemptedWith },
  }), true);

  assert.deepEqual(store.readDistillFailure("trigger", source.id), {
    kind: "trigger",
    id: source.id,
    reason: "source_invalid",
    context: attemptedWith,
    attempts: 1,
    nextAttemptAt: null,
  });
  assert.equal(store.canAttemptDistill("trigger", source.id, attemptedWith, 102), false);
  assert.equal(store.claimDistillTrigger({ workerId: "wake", now: 102, leaseMs: 50 }), undefined);
  assert.equal(store.claimDistillTrigger({
    id: source.id,
    workerId: "same-source-new-model",
    now: 103,
    leaseMs: 50,
    retryContext: context({ modelKey: "provider/model-b" }),
  }), undefined);

  const repairedSource = context({ sourceVersion: "source-v2" });
  assert.equal(store.canAttemptDistill("trigger", source.id, repairedSource, 104), true);
  const repaired = store.claimDistillTrigger({
    id: source.id, workerId: "repaired", now: 104, leaseMs: 50, retryContext: repairedSource,
  });
  assert.ok(repaired);
  store.prepareDistillTrigger({ id: source.id, claimToken: repaired.claimToken, now: 105, jobs: [] });
  assert.equal(store.readDistillFailure("trigger", source.id), undefined);
});

test("transient retry-after and backoff attempts survive reopening the database", async t => {
  const f = await fixture(t);
  let store = f.open();
  const source = trigger("transient-source");
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const first = store.claimDistillTrigger({
    id: source.id, workerId: "first", now: 1_000, leaseMs: 100, retryContext: attemptedWith,
  });
  assert.ok(first);
  store.releaseDistillClaim({
    claimToken: first.claimToken,
    now: 1_001,
    failure: { reason: "transient", context: attemptedWith, retryAfterMs: 200_000 },
  });
  assert.equal(store.readDistillFailure("trigger", source.id)?.attempts, 1);
  assert.equal(store.readDistillFailure("trigger", source.id)?.nextAttemptAt, 201_001);
  assert.equal(store.canAttemptDistill("trigger", source.id, attemptedWith, 201_000), false);
  assert.equal(store.canAttemptDistill("trigger", source.id, context({ modelKey: "provider/model-b" }), 1_002), false);
  store.close();

  store = f.open();
  assert.equal(store.claimDistillTrigger({
    id: source.id, workerId: "early", now: 201_000, leaseMs: 100, retryContext: attemptedWith,
  }), undefined);
  assert.equal(store.claimDistillTrigger({
    id: source.id, workerId: "changed-context-early", now: 1_002, leaseMs: 100,
    retryContext: context({ modelKey: "provider/model-b" }),
  }), undefined);
  const second = store.claimDistillTrigger({
    id: source.id, workerId: "second", now: 201_001, leaseMs: 100, retryContext: attemptedWith,
  });
  assert.ok(second);
  store.releaseDistillClaim({
    claimToken: second.claimToken,
    now: 201_002,
    failure: { reason: "transient", context: attemptedWith },
  });
  assert.deepEqual(store.readDistillFailure("trigger", source.id), {
    kind: "trigger",
    id: source.id,
    reason: "transient",
    context: attemptedWith,
    attempts: 2,
    nextAttemptAt: 321_002,
  });
});

test("a maximum retry-after hint saturates and persists instead of losing the failure", async t => {
  const f = await fixture(t);
  let store = f.open();
  const source = trigger("maximum-retry-after");
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const claim = store.claimDistillTrigger({
    id: source.id, workerId: "max-delay", now: 1_000, leaseMs: 100, retryContext: attemptedWith,
  });
  assert.ok(claim);
  assert.equal(store.releaseDistillClaim({
    claimToken: claim.claimToken,
    now: 1_001,
    failure: { reason: "transient", context: attemptedWith, retryAfterMs: Number.MAX_SAFE_INTEGER },
  }), true);
  assert.deepEqual(store.readDistillFailure("trigger", source.id), {
    kind: "trigger",
    id: source.id,
    reason: "transient",
    context: attemptedWith,
    attempts: 1,
    nextAttemptAt: Number.MAX_SAFE_INTEGER,
  });
  assert.equal(store.canAttemptDistill("trigger", source.id, attemptedWith, Number.MAX_SAFE_INTEGER - 1), false);
  store.close();

  store = f.open();
  assert.equal(store.readDistillFailure("trigger", source.id)?.nextAttemptAt, Number.MAX_SAFE_INTEGER);
  assert.equal(store.canAttemptDistill("trigger", source.id, attemptedWith, Number.MAX_SAFE_INTEGER), true);
});

test("model budget failures move from one short retry to a durable six-hour cooldown", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("budget-source");
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const first = store.claimDistillTrigger({
    id: source.id, workerId: "first", now: 10_000, leaseMs: 100, retryContext: attemptedWith,
  });
  assert.ok(first);
  store.releaseDistillClaim({
    claimToken: first.claimToken,
    now: 10_001,
    failure: { reason: "model_budget", context: attemptedWith },
  });
  assert.equal(store.readDistillFailure("trigger", source.id)?.nextAttemptAt, 40_001);

  const secondAttemptContext = context({ sourceVersion: "source-v2" });
  const second = store.claimDistillTrigger({
    id: source.id, workerId: "second", now: 40_001, leaseMs: 100, retryContext: secondAttemptContext,
  });
  assert.ok(second);
  store.releaseDistillClaim({
    claimToken: second.claimToken,
    now: 40_002,
    failure: { reason: "model_invalid", context: secondAttemptContext },
  });
  const heldUntil = 40_002 + 6 * 60 * 60 * 1_000;
  assert.equal(store.readDistillFailure("trigger", source.id)?.attempts, 2);
  assert.equal(store.readDistillFailure("trigger", source.id)?.nextAttemptAt, heldUntil);
  assert.equal(store.canAttemptDistill("trigger", source.id, secondAttemptContext, heldUntil - 1), false);
  assert.equal(store.canAttemptDistill(
    "trigger", source.id, context({ sourceVersion: "source-v3" }), 40_003,
  ), false);
  assert.equal(store.canAttemptDistill("trigger", source.id, context({ policyVersion: "policy-v2" }), 40_003), true);
});

test("job claims use the same failure gate and stale lease owners cannot write failure metadata", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("job-source");
  const batch = job("job-batch", source);
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const preparation = store.claimDistillTrigger({
    id: source.id, workerId: "prepare", now: 100, leaseMs: 100, retryContext: attemptedWith,
  });
  assert.ok(preparation);
  store.prepareDistillTrigger({ id: source.id, claimToken: preparation.claimToken, now: 101, jobs: [batch] });

  const expired = store.claimDistillJob({
    id: batch.id, workerId: "expired", now: 200, leaseMs: 10, retryContext: attemptedWith,
  });
  assert.ok(expired);
  const current = store.claimDistillJob({
    id: batch.id, workerId: "current", now: 210, leaseMs: 20, retryContext: attemptedWith,
  });
  assert.ok(current);
  assert.equal(store.releaseDistillClaim({
    claimToken: expired.claimToken,
    now: 210,
    failure: { reason: "model_invalid", context: attemptedWith },
  }), false);
  assert.equal(store.readDistillFailure("job", batch.id), undefined);
  assert.equal(store.releaseDistillClaim({
    claimToken: current.claimToken,
    now: 211,
    failure: { reason: "model_invalid", context: attemptedWith },
  }), true);
  assert.equal(store.canAttemptDistill("job", batch.id, attemptedWith, 212), false);
  assert.equal(store.canAttemptDistill("job", batch.id, context({ modelKey: "provider/model-b" }), 212), true);
});

test("cancellation does not create a failure and successful preparation clears prior failure metadata", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("cancelled-source");
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const cancelled = store.claimDistillTrigger({
    id: source.id, workerId: "cancelled", now: 100, leaseMs: 50, retryContext: attemptedWith,
  });
  assert.ok(cancelled);
  assert.equal(store.releaseDistillClaim({ claimToken: cancelled.claimToken, now: 101 }), true);
  assert.equal(store.readDistillFailure("trigger", source.id), undefined);

  const claim = store.claimDistillTrigger({
    id: source.id, workerId: "failure", now: 102, leaseMs: 50, retryContext: attemptedWith,
  });
  assert.ok(claim);
  store.releaseDistillClaim({
    claimToken: claim.claimToken,
    now: 103,
    failure: { reason: "transient", context: attemptedWith },
  });
  const retry = store.claimDistillTrigger({
    id: source.id, workerId: "recovery", now: 30_103, leaseMs: 50, retryContext: attemptedWith,
  });
  assert.ok(retry);
  store.prepareDistillTrigger({ id: source.id, claimToken: retry.claimToken, now: 30_104, jobs: [] });
  assert.equal(store.readDistillFailure("trigger", source.id), undefined);
});

test("candidate recovery requires an explicit fenced ID and preserves a hold after confirmation", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("candidate-recovery");
  const attemptedWith = context();
  const compact = { ...source, reason: "compact" };
  store.enqueueDistillTrigger(compact);
  assert.throws(() => store.claimDistillTrigger({
    workerId: "broad-candidate", now: 100, leaseMs: 50, allowCandidate: true,
  }), /require a trigger ID/);
  assert.equal(store.claimDistillTrigger({
    id: compact.id, workerId: "not-explicit", now: 100, leaseMs: 50,
  }), undefined);

  const candidate = store.claimDistillTrigger({
    id: compact.id, allowCandidate: true, workerId: "candidate-reader", now: 100, leaseMs: 50,
    retryContext: attemptedWith,
  });
  assert.ok(candidate);
  assert.equal(store.releaseDistillClaim({
    claimToken: candidate.claimToken,
    now: 101,
    failure: { reason: "source_invalid", context: attemptedWith },
  }), true);
  assert.equal(store.canAttemptDistill("trigger", compact.id, attemptedWith, 102), false);
  assert.equal(store.claimDistillTrigger({
    id: compact.id, allowCandidate: true, workerId: "same-candidate", now: 102, leaseMs: 50,
    retryContext: attemptedWith,
  }), undefined);

  const repaired = store.claimDistillTrigger({
    id: compact.id, allowCandidate: true, workerId: "repaired-candidate", now: 103, leaseMs: 50,
    retryContext: context({ sourceVersion: "source-v2" }),
  });
  assert.ok(repaired);
  assert.equal(store.confirmCompactionTrigger({ id: compact.id, compactionRecordId: "record" }), true);
  assert.equal(store.releaseDistillClaim({ claimToken: repaired.claimToken, now: 104 }), true);
  assert.deepEqual(store.readDistillFailure("trigger", compact.id)?.context, attemptedWith);
  assert.equal(store.canAttemptDistill(
    "trigger", compact.id, context({ sourceVersion: "source-v2" }), 105,
  ), true);
});

test("cancelling a failed unconfirmed candidate also removes its failure metadata", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = { ...trigger("cancel-failed-candidate"), reason: "compact" };
  const attemptedWith = context();
  store.enqueueDistillTrigger(source);
  const claim = store.claimDistillTrigger({
    id: source.id, allowCandidate: true, workerId: "candidate", now: 100, leaseMs: 50,
    retryContext: attemptedWith,
  });
  assert.ok(claim);
  assert.equal(store.releaseDistillClaim({
    claimToken: claim.claimToken,
    now: 101,
    failure: { reason: "source_invalid", context: attemptedWith },
  }), true);
  assert.ok(store.readDistillFailure("trigger", source.id));
  assert.equal(store.cancelCompactionTrigger(source.id), true);
  assert.equal(store.readDistillFailure("trigger", source.id), undefined);
  assert.equal(store.readDistillTrigger(source.id), undefined);
});

test("opening a v7 database preserves memories and pending distillation work during the v8 migration", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = trigger("v7-pending");
  store.save({ id: "preserved-memory", content: "Synthetic retained memory.", createdAt: "2026-10-23T00:00:00.000Z" });
  store.enqueueDistillTrigger(source);
  store.close();

  const raw = new DatabaseSync(f.path);
  raw.exec("DROP TABLE distill_failures; PRAGMA user_version = 7");
  raw.close();

  const migrated = f.open();
  const check = new DatabaseSync(f.path);
  try {
    assert.equal(check.prepare("PRAGMA user_version").get().user_version, 8);
  } finally {
    check.close();
  }
  assert.equal(migrated.get("preserved-memory")?.content, "Synthetic retained memory.");
  assert.equal(migrated.listDistillTriggers({ limit: 10 }).items[0]?.status, "pending");
});
