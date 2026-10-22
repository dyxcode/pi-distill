import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { ConsolidationConflictError, SQLiteMemoryStore } from "../src/memory-store.ts";

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
  return { jobId, owner, now: 100, creates: [], updates: [], deletes: [], ...overrides };
}

function usage(root, ownerId, overrides = {}) {
  return {
    sessionId: "session-shared",
    sessionFile: join(root, "shared-session.jsonl"),
    cwd: root,
    leafId: "leaf-one",
    ownerPid: process.pid,
    ownerId,
    processToken: "process-token-" + ownerId,
    state: "active",
    ...overrides,
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-session-usage-"));
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
  return { root, path, open };
}

function database(path) {
  return new DatabaseSync(path);
}

test("session usage registrations are independent by owner across SQLite connections and survive reopen", async (t) => {
  const f = await fixture(t);
  const firstStore = f.open();
  const secondStore = f.open();
  const first = usage(f.root, "owner-a", { processToken: "process-token-shared" });
  const second = usage(f.root, "owner-b", { processToken: "process-token-shared" });

  firstStore.saveSessionUsage(first);
  secondStore.saveSessionUsage(second);
  secondStore.saveSessionUsage(structuredClone(second));
  const closingFirst = { ...first, state: "closing" };
  secondStore.saveSessionUsage(closingFirst);

  assert.deepEqual(firstStore.listSessionUsages(), [closingFirst, second]);
  assert.deepEqual(secondStore.listSessionUsages(), [closingFirst, second]);
  firstStore.close();
  secondStore.close();
  assert.deepEqual(f.open().listSessionUsages(), [closingFirst, second]);
});

test("session usage text uses the database byte boundary for embedded NUL values", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = usage(f.root, "owner\u0000tail", {
    processToken: "process\u0000tail",
    sessionId: "session\u0000tail",
    leafId: "leaf\u0000tail",
  });
  store.saveSessionUsage(record);
  assert.deepEqual(store.listSessionUsages(), [record]);
  store.close();
  assert.deepEqual(f.open().listSessionUsages(), [record]);
});

test("session usage identity is immutable and owner checkpoint state is fenced", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const initial = usage(f.root, "owner-fenced");
  store.saveSessionUsage(initial);

  for (const changed of [
    { processToken: "other-process" },
    { sessionId: "other-session" },
    { sessionFile: join(f.root, "other-session.jsonl") },
    { cwd: join(f.root, "other-cwd") },
    { ownerPid: process.pid + 1 },
  ]) {
    assert.throws(
      () => store.saveSessionUsage({ ...initial, ...changed }),
      ConsolidationConflictError,
    );
  }

  const activeAtNextLeaf = { ...initial, leafId: "leaf-two" };
  store.saveSessionUsage(activeAtNextLeaf);
  const closing = { ...activeAtNextLeaf, leafId: "leaf-final", state: "closing" };
  store.saveSessionUsage(closing);
  store.saveSessionUsage(structuredClone(closing));
  assert.throws(
    () => store.saveSessionUsage({ ...closing, leafId: "late-leaf" }),
    ConsolidationConflictError,
  );
  const released = { ...closing, state: "released" };
  store.saveSessionUsage(released);
  store.saveSessionUsage(structuredClone(released));
  assert.throws(
    () => store.saveSessionUsage({ ...released, state: "active" }),
    ConsolidationConflictError,
  );
  assert.throws(
    () => store.saveSessionUsage({ ...released, leafId: "new-checkpoint" }),
    ConsolidationConflictError,
  );
  assert.deepEqual(store.listSessionUsages(), [released]);
});

test("new owners can only be created active and malformed usage inputs are rejected", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const valid = usage(f.root, "owner-valid");

  for (const state of ["closing", "released"]) {
    assert.throws(() => store.saveSessionUsage({ ...valid, state }), ConsolidationConflictError);
  }
  for (const invalid of [
    null,
    { ...valid, extra: "transcript content is not metadata" },
    { ...valid, ownerId: "" },
    { ...valid, processToken: "\ud800" },
    { ...valid, sessionFile: "relative/session.jsonl" },
    { ...valid, cwd: f.root + "\u0000invalid" },
    { ...valid, ownerPid: 0 },
    { ...valid, ownerPid: 1.5 },
    { ...valid, leafId: "\ud800" },
    { ...valid, state: "paused" },
  ]) assert.throws(() => store.saveSessionUsage(invalid), TypeError);

  assert.deepEqual(store.listSessionUsages(), []);
});

test("stored usage rows are validated on reads and on reopen", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.saveSessionUsage(usage(f.root, "owner-stored"));
  const raw = database(f.path);
  try {
    raw.prepare("UPDATE session_usages SET session_file = ? WHERE owner_id = ?")
      .run("relative/session.jsonl", "owner-stored");
    assert.throws(() => store.listSessionUsages(), TypeError);
  } finally {
    raw.close();
  }
  store.close();
  assert.throws(() => f.open(), TypeError);
});

test("consolidation status reads usages and observations from one read snapshot", async (t) => {
  const f = await fixture(t);
  const reader = f.open();
  const active = usage(f.root, "owner-snapshot");
  const observation = {
    sessionId: active.sessionId,
    sessionFile: active.sessionFile,
    cwd: active.cwd,
    leafId: active.leafId,
    ownerPid: active.ownerPid,
  };
  reader.saveSessionUsage(active);
  reader.observeSession(observation);

  const writer = database(f.path);
  try {
    writer.exec("BEGIN IMMEDIATE");
    writer.prepare("UPDATE session_usages SET state = 'closing', leaf_id = ? WHERE owner_id = ?")
      .run("leaf-uncommitted", active.ownerId);
    writer.prepare("UPDATE session_observations SET leaf_id = ? WHERE session_id = ?")
      .run("leaf-uncommitted", observation.sessionId);

    assert.deepEqual(reader.readConsolidationStatus([]), {
      processedSources: [],
      pendingJobs: [],
      pendingTriggers: [],
      observations: [observation],
      usages: [active],
    });
    writer.exec("ROLLBACK");
  } finally {
    if (writer.isTransaction) writer.exec("ROLLBACK");
    writer.close();
  }
  assert.deepEqual(reader.readConsolidationStatus([]).usages, [active]);
});

test("known session files include completed jobs, all observations and retained released usages", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const paths = [
    join(f.root, "a-complete.jsonl"),
    join(f.root, "b-pending.jsonl"),
    join(f.root, "c-observed.jsonl"),
    join(f.root, "d-released.jsonl"),
  ];
  const complete = job("known-complete", { sessionFile: paths[0] });
  store.enqueueConsolidation(complete);
  store.claimConsolidation(complete.id, "worker", 100, 100);
  store.commitConsolidation(commit(complete.id, "worker"));
  store.enqueueConsolidation(job("known-pending", { sessionFile: paths[1] }));
  store.observeSession({
    sessionId: "known-observed",
    sessionFile: paths[2],
    cwd: f.root,
    leafId: "captured-leaf",
    ownerPid: process.pid,
  });
  store.acknowledgeObservation("known-observed", "captured-leaf");
  store.observeSession({
    sessionId: "known-duplicate-observed",
    sessionFile: paths[0],
    cwd: f.root,
    leafId: null,
    ownerPid: process.pid,
  });
  const released = usage(f.root, "owner-released", {
    sessionFile: paths[3],
    state: "active",
  });
  store.saveSessionUsage(released);
  store.saveSessionUsage({ ...released, state: "closing" });
  store.saveSessionUsage({ ...released, state: "closing", leafId: released.leafId });
  store.saveSessionUsage({ ...released, state: "released" });

  const expected = [...paths].sort();
  assert.deepEqual(store.listKnownSessionFiles(), expected);
  assert.deepEqual(store.listKnownSessionFiles(2), expected.slice(0, 2));
  for (const limit of [0, -1, 1.5, 1025, Infinity, NaN, "2"]) {
    assert.throws(() => store.listKnownSessionFiles(limit), TypeError);
  }
});

test("known session file results stay bounded and malformed stored job paths fail closed", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const first = job("path-first", { sessionFile: join(f.root, "a-session.jsonl") });
  const malformed = job("path-malformed", { sessionFile: join(f.root, "z-session.jsonl") });
  store.enqueueConsolidation(first);
  store.enqueueConsolidation(malformed);

  const raw = database(f.path);
  raw.prepare("UPDATE consolidation_jobs SET job_json = ? WHERE id = ?")
    .run(JSON.stringify({ ...malformed, sessionFile: "relative/session.jsonl" }), malformed.id);
  raw.close();
  assert.throws(() => store.listKnownSessionFiles(1), TypeError);
});

test("session discovery limits SQL results and status snapshots fail closed above 16,384 metadata rows", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const raw = database(f.path);
  const insert = raw.prepare(
    "INSERT INTO session_usages " +
    "(owner_id, process_token, session_id, session_file, cwd, leaf_id, owner_pid, state) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
  );
  const expectedPaths = [];
  try {
    raw.exec("BEGIN IMMEDIATE");
    for (let index = 0; index < 16_384; index += 1) {
      const suffix = String(index).padStart(5, "0");
      const sessionFile = join(f.root, "session-" + suffix + ".jsonl");
      expectedPaths.push(sessionFile);
      insert.run("owner-" + suffix, "process-shared", "session-shared", sessionFile,
        f.root, "leaf-shared", process.pid);
    }
    raw.exec("COMMIT");
  } finally {
    if (raw.isTransaction) raw.exec("ROLLBACK");
    raw.close();
  }

  assert.deepEqual(store.listKnownSessionFiles(17), expectedPaths.sort().slice(0, 17));
  assert.equal(store.readConsolidationStatus([]).usages.length, 16_384);

  const complete = job("over-discovery-limit");
  store.enqueueConsolidation(complete);
  store.claimConsolidation(complete.id, "worker", 100, 100);
  store.commitConsolidation(commit(complete.id, "worker"));
  assert.throws(() => store.listKnownSessionFiles(1), RangeError);
  assert.equal(store.readConsolidationStatus([]).usages.length, 16_384);

  const extra = database(f.path);
  try {
    extra.prepare(
      "INSERT INTO session_usages " +
      "(owner_id, process_token, session_id, session_file, cwd, leaf_id, owner_pid, state) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, 'active')",
    ).run("owner-over-cap", "process-shared", "session-shared", join(f.root, "over-cap.jsonl"),
      f.root, "leaf-shared", process.pid);
  } finally {
    extra.close();
  }
  assert.throws(() => store.readConsolidationStatus([]), RangeError);
  assert.throws(() => store.listKnownSessionFiles(1), RangeError);
});

test("schema v2 migration preserves memories, jobs, progress, origins and observations", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const retainedMemory = memory("v2-existing-memory");
  store.save(retainedMemory);
  const pending = job("v2-pending", { sessionFile: join(f.root, "v2-pending.jsonl") });
  store.enqueueConsolidation(pending);
  const complete = job("v2-complete", {
    sessionFile: join(f.root, "v2-complete.jsonl"),
    sourceKeys: ["v2:processed-source"],
  });
  store.enqueueConsolidation(complete);
  store.claimConsolidation(complete.id, "worker-v2", 100, 100);
  const generated = memory("v2-generated-memory");
  store.commitConsolidation(commit(complete.id, "worker-v2", { creates: [generated] }));
  const observation = {
    sessionId: "v2-observation",
    sessionFile: join(f.root, "v2-observation.jsonl"),
    cwd: f.root,
    leafId: "v2-leaf",
    ownerPid: process.pid,
  };
  store.observeSession(observation);
  store.close();

  const downgrade = database(f.path);
  restoreV2Schema(downgrade);
  downgrade.close();

  const upgraded = f.open();
  assert.deepEqual(upgraded.get(retainedMemory.id), {
    ...retainedMemory,
    revision: 1,
    updatedAt: retainedMemory.createdAt,
  });
  assert.deepEqual(upgraded.get(generated.id), {
    ...generated,
    revision: 1,
    updatedAt: generated.createdAt,
  });
  assert.deepEqual(upgraded.listConsolidationJobs(), [pending]);
  assert.deepEqual(upgraded.processedSources(complete.sourceKeys), complete.sourceKeys);
  assert.deepEqual(upgraded.observedSessions(), [observation]);
  assert.deepEqual(upgraded.listKnownSessionFiles(), [
    pending.sessionFile,
    complete.sessionFile,
    observation.sessionFile,
  ].sort());

  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("PRAGMA user_version").get()["user_version"], 4);
    assert.deepEqual(verify.prepare("SELECT name FROM sqlite_schema WHERE name = 'session_usages'").all()
      .map(row => row["name"]), ["session_usages"]);
  } finally {
    verify.close();
  }
  upgraded.close();
  assert.deepEqual(f.open().listSessionUsages(), []);
});

test("invalid v2 state aborts migration without changing the database version or adding usage storage", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.observeSession({
    sessionId: "invalid-v2-observation",
    sessionFile: join(f.root, "session.jsonl"),
    cwd: f.root,
    leafId: null,
    ownerPid: process.pid,
  });
  store.close();

  const downgrade = database(f.path);
  restoreV2Schema(downgrade);
  downgrade.prepare("UPDATE session_observations SET session_file = 'relative/session.jsonl'").run();
  downgrade.close();

  assert.throws(() => f.open(), TypeError);
  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("PRAGMA user_version").get()["user_version"], 2);
    assert.equal(verify.prepare("SELECT name FROM sqlite_schema WHERE name = 'session_usages'").get(), undefined);
    assert.equal(verify.prepare("SELECT session_file FROM session_observations").get()["session_file"],
      "relative/session.jsonl");
  } finally {
    verify.close();
  }
});

test("v2 migration rejects an unexpected schema object without changing existing data", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.save(memory("unexpected-v2-schema"));
  store.close();

  const raw = database(f.path);
  restoreV2Schema(raw);
  raw.exec("CREATE TABLE unexpected (value TEXT NOT NULL) STRICT");
  raw.prepare("INSERT INTO unexpected VALUES (?)").run("preserve");
  raw.close();

  assert.throws(() => f.open(), /unsupported memory database schema/i);
  const verify = database(f.path);
  try {
    assert.equal(verify.prepare("PRAGMA user_version").get()["user_version"], 2);
    assert.equal(verify.prepare("SELECT name FROM sqlite_schema WHERE name = 'session_usages'").get(), undefined);
    assert.equal(verify.prepare("SELECT content FROM memories WHERE id = ?").get("unexpected-v2-schema")["content"],
      "Synthetic memory unexpected-v2-schema with retained conditions.");
    assert.equal(verify.prepare("SELECT value FROM unexpected").get()["value"], "preserve");
  } finally {
    verify.close();
  }
});

// Reconstruct the historical fixture rather than relabeling current schema as v2.
function restoreV2Schema(db) {
  db.exec(`
    DROP TABLE session_usages;
    DROP TABLE clean_tasks;
    DROP TABLE distill_trigger_jobs;
    DROP TABLE distill_triggers;
    DROP TABLE distill_lease;
    ALTER TABLE consolidation_jobs RENAME TO jobs_current;
    CREATE TABLE consolidation_jobs (id TEXT PRIMARY KEY NOT NULL, job_json TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending', 'complete')), lease_owner TEXT, lease_until INTEGER, CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL))) STRICT;
    INSERT INTO consolidation_jobs SELECT id, job_json, status, lease_owner, lease_until FROM jobs_current;
    DROP TABLE jobs_current;
    PRAGMA user_version = 2;
  `);
}
