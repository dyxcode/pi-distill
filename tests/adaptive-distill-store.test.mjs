import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  ConsolidationConflictError,
  SQLiteMemoryStore,
} from "../src/memory-store.ts";

const timeOne = "2026-10-22T08:00:00.000Z";
const timeTwo = "2026-10-22T08:05:00.000Z";
const anchorOne = "a".repeat(64);
const anchorTwo = "b".repeat(64);

function memory(id) {
  return {
    id,
    content: "Synthetic memory " + id + " keeps its conditions and source boundary.",
    createdAt: timeOne,
  };
}

function job(id, overrides = {}) {
  return {
    id,
    sessionId: "session-" + id,
    sessionFile: null,
    leafId: "leaf-" + id,
    cwd: process.cwd(),
    sourceKeys: [id + ":entry-a", id + ":entry-b", id + ":entry-c"],
    sourceTime: "2026-10-22T08:59:00.000Z",
    branchAnchor: "f".repeat(64),
    ...overrides,
  };
}

function commit(jobId, owner, overrides = {}) {
  return {
    jobId,
    owner,
    now: 101,
    creates: [],
    updates: [],
    deletes: [],
    ...overrides,
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-adaptive-store-"));
  const path = join(root, "memories.sqlite");
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
  return { path, root, open };
}

test("prefix commits advance exact progress and keep trigger cleanup pending until the final prefix", async t => {
  const f = await fixture(t);
  let store = f.open();
  const sourceFile = join(f.root, "source.jsonl");
  const source = job("prefix-lifecycle", {
    sessionId: "prefix-session",
    sessionFile: sourceFile,
    sourceKeys: ["entry-a", "entry-b", "entry-c"],
    lineageSessionIds: ["root-session", "prefix-session"],
  });
  const updateTarget = memory("prefix-update-target");
  const deleteTarget = memory("prefix-delete-target");
  store.save(updateTarget);
  store.save(deleteTarget);
  const triggers = [
    {
      id: "prefix-close-trigger",
      sessionId: source.sessionId,
      sessionFile: source.sessionFile,
      leafId: source.leafId,
      cwd: source.cwd,
      reason: "close",
    },
    {
      id: "prefix-compact-trigger",
      sessionId: source.sessionId,
      sessionFile: source.sessionFile,
      leafId: source.leafId,
      cwd: source.cwd,
      reason: "compact",
    },
  ];
  for (const trigger of triggers) store.enqueueDistillTrigger(trigger);
  assert.equal(store.confirmCompactionTrigger({
    id: triggers[1].id,
    compactionRecordId: "prefix-compaction-record",
  }), true);
  for (let index = 0; index < triggers.length; index += 1) {
    const trigger = triggers[index];
    assert.ok(trigger);
    const triggerClaim = store.claimDistillTrigger({
      id: trigger.id,
      workerId: "prepare-prefix-trigger-" + index,
      now: 10 + index * 2,
      leaseMs: 100,
    });
    assert.ok(triggerClaim);
    store.prepareDistillTrigger({
      id: trigger.id,
      claimToken: triggerClaim.claimToken,
      now: 11 + index * 2,
      jobs: [source],
    });
  }

  assert.equal(store.claimConsolidation(source.id, "worker-one", 100, 1_000), true);
  const firstRequest = commit(source.id, "worker-one", {
    now: 101,
    sourceKeys: ["entry-a"],
    evidence: { sourceTime: timeOne, branchAnchor: anchorOne },
    creates: [memory("prefix-memory-one")],
    updates: [{
      id: updateTarget.id,
      expectedRevision: 1,
      content: "The prefix corrected this synthetic fact with its original condition.",
      updatedAt: timeOne,
    }],
    deletes: [{ id: deleteTarget.id, expectedRevision: 1 }],
  });
  assert.equal(store.commitConsolidation(firstRequest), true);
  assert.deepEqual(store.processedSources(source.sourceKeys), ["entry-a"]);
  assert.deepEqual(store.get(updateTarget.id), {
    ...updateTarget,
    content: "The prefix corrected this synthetic fact with its original condition.",
    revision: 2,
    updatedAt: timeOne,
  }, "updates commit within the selected prefix");
  assert.equal(store.get(deleteTarget.id), undefined, "deletes commit within the selected prefix");
  assert.deepEqual(store.listConsolidationJobs(), [source], "the immutable parent job remains pending");
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(({ id, status, jobIds }) => ({
    id, status, jobIds,
  })), triggers.map(trigger => ({ id: trigger.id, status: "prepared", jobIds: [source.id] })),
  "both public trigger associations to the shared job remain intact until all source keys finish");
  assert.deepEqual(store.listCleanTasks({ limit: 10 }).items, [], "a prefix does not authorize cleanup");
  assert.deepEqual(store.memoryOrigin("prefix-memory-one"), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-a"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeOne,
    branchAnchor: anchorOne,
  });
  assert.deepEqual(store.memoryOrigin(updateTarget.id), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-a"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeOne,
    branchAnchor: anchorOne,
  }, "the updated memory points at the prefix's actual evidence boundary");
  assert.equal(store.memoryOrigin(deleteTarget.id), undefined);

  store.close();
  store = f.open();
  assert.deepEqual(store.processedSources(source.sourceKeys), ["entry-a"],
    "the committed prefix survives reopening the store");
  assert.deepEqual(store.listConsolidationJobs(), [source]);
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(({ id, status, jobIds }) => ({
    id, status, jobIds,
  })), triggers.map(trigger => ({ id: trigger.id, status: "prepared", jobIds: [source.id] })));
  assert.equal(store.claimConsolidation(source.id, "worker-two", 102, 1_000), true,
    "a successful prefix releases the job and global worker leases");
  assert.throws(() => store.commitConsolidation({ ...firstRequest, now: 103 }), ConsolidationConflictError,
    "the previous owner cannot replay a prefix after a new claim");
  assert.deepEqual(store.processedSources(source.sourceKeys), ["entry-a"]);

  const secondRequest = commit(source.id, "worker-two", {
    now: 104,
    sourceKeys: ["entry-b", "entry-c"],
    evidence: { sourceTime: timeTwo, branchAnchor: anchorTwo },
    creates: [memory("prefix-memory-two")],
    updates: [{
      id: "missing-memory",
      expectedRevision: 1,
      content: "This later invalid mutation must roll back this prefix only.",
      updatedAt: timeTwo,
    }],
  });
  assert.throws(() => store.commitConsolidation(secondRequest), /memory/i,
    "the current prefix rolls back if a later memory mutation fails");
  assert.deepEqual(store.processedSources(source.sourceKeys), ["entry-a"]);
  assert.equal(store.get("prefix-memory-two"), undefined);
  assert.deepEqual(store.memoryOrigin("prefix-memory-one"), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-a"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeOne,
    branchAnchor: anchorOne,
  }, "a failed later prefix leaves the prior immutable origin group intact");
  assert.deepEqual(store.listCleanTasks({ limit: 10 }).items, []);

  assert.equal(store.commitConsolidation({
    ...secondRequest,
    creates: [memory("prefix-memory-two")],
    updates: [],
  }), true);
  assert.deepEqual(store.processedSources(source.sourceKeys), source.sourceKeys);
  assert.deepEqual(store.listConsolidationJobs(), []);
  assert.deepEqual(store.listDistillTriggers({ limit: 10 }).items.map(({ id, status, jobIds }) => ({
    id, status, jobIds,
  })), triggers.map(trigger => ({ id: trigger.id, status: "complete", jobIds: [source.id] })));
  const cleanTasks = store.listCleanTasks({ limit: 10 }).items;
  assert.equal(cleanTasks.length, 1, "cleanup starts only after final coverage and stays deduplicated by source file");
  assert.ok(triggers.some(trigger => trigger.id === cleanTasks[0]?.intentId),
    "the final cleanup remains associated with one of the shared trigger intents");
  assert.deepEqual(store.memoryOrigin("prefix-memory-one"), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-a"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeOne,
    branchAnchor: anchorOne,
  });
  assert.deepEqual(store.memoryOrigin(updateTarget.id), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-a"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeOne,
    branchAnchor: anchorOne,
  }, "the updated memory keeps its earlier prefix origin after later work");
  assert.deepEqual(store.memoryOrigin("prefix-memory-two"), {
    sessionId: source.sessionId,
    sourceKeys: ["entry-b", "entry-c"],
    lineageSessionIds: source.lineageSessionIds,
    sourceTime: timeTwo,
    branchAnchor: anchorTwo,
  });
});

test("only a nonempty exact ordered prefix is accepted and partial work requires an evidence boundary", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = job("prefix-validation", { sourceKeys: ["key-a", "key-b", "key-c"] });
  store.enqueueConsolidation(source);
  assert.equal(store.claimConsolidation(source.id, "worker", 100, 1_000), true);

  const rejected = [
    { label: "empty", sourceKeys: [], evidence: {} },
    { label: "skipped", sourceKeys: ["key-a", "key-c"], evidence: {} },
    { label: "suffix", sourceKeys: ["key-b"], evidence: {} },
    { label: "reordered", sourceKeys: ["key-b", "key-a"], evidence: {} },
    { label: "foreign", sourceKeys: ["other-key"], evidence: {} },
    { label: "partial without evidence", sourceKeys: ["key-a"] },
  ];
  for (const { label, ...request } of rejected) {
    assert.throws(() => store.commitConsolidation(commit(source.id, "worker", {
      ...request,
      creates: [memory("rejected-" + label.replaceAll(" ", "-"))],
    })), ConsolidationConflictError, label);
  }
  assert.deepEqual(store.processedSources(source.sourceKeys), []);
  assert.deepEqual(store.listConsolidationJobs(), [source]);
  for (const { label } of rejected) {
    assert.equal(store.get("rejected-" + label.replaceAll(" ", "-")), undefined);
  }
  for (const evidence of [
    null,
    { unexpected: true },
    { sourceTime: "not-a-canonical-timestamp" },
    { branchAnchor: "" },
  ]) {
    assert.throws(() => store.commitConsolidation(commit(source.id, "worker", { evidence })), TypeError);
  }

  assert.equal(store.commitConsolidation(commit(source.id, "worker", {
    creates: [memory("full-boundary-memory")],
  })), true, "the existing full-range call remains valid without an evidence object");
  assert.deepEqual(store.memoryOrigin("full-boundary-memory"), {
    sessionId: source.sessionId,
    sourceKeys: source.sourceKeys,
    sourceTime: source.sourceTime,
    branchAnchor: source.branchAnchor,
  }, "the omitted boundary uses the durable job boundary");
});

test("an already-covered pending job rejects dropped mutations, then can finish safely", async t => {
  const f = await fixture(t);
  const store = f.open();
  const first = job("overlap-first", { sourceKeys: ["shared-key"] });
  store.enqueueConsolidation(first);
  assert.equal(store.claimConsolidation(first.id, "first-worker", 100, 1_000), true);
  assert.equal(store.commitConsolidation(commit(first.id, "first-worker")), true);

  const alreadyCovered = job("overlap-second", { sourceKeys: ["shared-key"] });
  store.enqueueConsolidation(alreadyCovered);
  assert.equal(store.claimConsolidation(alreadyCovered.id, "second-worker", 102, 1_000), true);
  assert.throws(() => store.commitConsolidation(commit(alreadyCovered.id, "second-worker", {
    creates: [memory("must-not-be-dropped")],
  })), ConsolidationConflictError);
  assert.equal(store.get("must-not-be-dropped"), undefined);
  assert.deepEqual(store.listConsolidationJobs(), [alreadyCovered]);

  assert.equal(store.commitConsolidation(commit(alreadyCovered.id, "second-worker", {
    sourceKeys: [],
  })), false, "an empty covered job finishes without changing memory");
  assert.deepEqual(store.listConsolidationJobs(), []);
});

test("a successful prefix clears a prior durable distillation failure", async t => {
  const f = await fixture(t);
  const store = f.open();
  const source = job("prefix-clears-failure", { sourceKeys: ["failure-a", "failure-b"] });
  const firstContext = {
    sourceVersion: "source-v1",
    modelKey: "provider/model-a",
    policyVersion: "policy-v1",
  };
  const retryContext = { ...firstContext, sourceVersion: "source-v2" };
  store.enqueueConsolidation(source);
  const failed = store.claimDistillJob({
    id: source.id,
    workerId: "first-worker",
    now: 100,
    leaseMs: 100,
    retryContext: firstContext,
  });
  assert.ok(failed);
  assert.equal(store.releaseDistillClaim({
    claimToken: failed.claimToken,
    now: 101,
    failure: { reason: "source_invalid", context: firstContext },
  }), true);
  assert.ok(store.readDistillFailure("job", source.id));

  const retry = store.claimDistillJob({
    id: source.id,
    workerId: "retry-worker",
    now: 102,
    leaseMs: 100,
    retryContext,
  });
  assert.ok(retry);
  assert.equal(store.commitConsolidation(commit(source.id, retry.claimToken, {
    now: 103,
    sourceKeys: ["failure-a"],
    evidence: {},
    creates: [memory("failure-prefix-memory")],
  })), true);
  assert.equal(store.readDistillFailure("job", source.id), undefined);
  assert.deepEqual(store.processedSources(source.sourceKeys), ["failure-a"]);
  assert.deepEqual(store.listConsolidationJobs(), [source]);
});
