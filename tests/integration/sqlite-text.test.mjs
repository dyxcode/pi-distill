import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { MemoryConflictError, SQLiteMemoryStore } from "../../src/storage/store.ts";
import { MemoryTools } from "../../src/memory/retrieval.ts";

const time = "2026-10-21T08:00:00.000Z";
const updatedAt = "2026-10-22T08:00:00.000Z";

async function fixture(t, encoding = "UTF-8") {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-text-"));
  const path = join(root, "memories.sqlite");
  const seed = new DatabaseSync(path);
  // Materialize the database encoding before the store creates its schema.
  seed.exec(`PRAGMA encoding = '${encoding}'; CREATE TABLE seed (id INTEGER); DROP TABLE seed`);
  seed.close();
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

for (const encoding of ["UTF-8", "UTF-16le", "UTF-16be"]) {
  test(`${encoding}: exact text, distinct IDs, corrections and retrieval survive reopening`, async (t) => {
    const f = await fixture(t, encoding);
    let store = f.open();
    const ids = ["a", "a\0", "a\0b"];
    const content = "\ufeffOriginal\0条件：保留🧠 café。\0END";
    for (const id of ids) store.save({ id, content, createdAt: time });
    store.close();
    store = f.open();
    for (const id of ids) {
      const record = { id, content, createdAt: time };
      assert.deepEqual(store.get(id), { ...record, revision: 1, updatedAt: time });
      store.save(record);
    }
    assert.equal(store.memoryOrigin(ids[2]), undefined, "An existing memory without provenance keeps its NULL join fields");

    const seen = [];
    let after;
    for (let page = 0; page <= ids.length; page++) {
      const rows = store.searchBatch({
        terms: ["条件", "\0end"], match: "any", limit: 1,
        ...(after === undefined ? {} : { after }),
      });
      if (rows.length === 0) break;
      assert.deepEqual(rows[0].matchedTerms, ["条件", "\0end"]);
      assert.equal(rows[0].memory.content, content);
      after = rows[0].memory.id;
      seen.push(after);
    }
    assert.deepEqual(seen, ids, "Pagination must terminate without duplicates or omissions");

    const correction = { id: ids[1], expectedRevision: 1, content: "\ufeffChanged\0新条件🧠END", updatedAt };
    const corrected = store.update(correction);
    assert.equal(corrected.revision, 2);
    assert.deepEqual(store.update(correction), corrected);
    assert.throws(() => store.update({ ...correction, content: "conflicting" }), MemoryConflictError);
    assert.throws(() => store.forget({ id: ids[1], expectedRevision: 1 }), MemoryConflictError);
    store.forget({ id: ids[1], expectedRevision: 2 });
    store.close();
    store = f.open();
    assert.equal(store.get(ids[1]), undefined);
    assert.equal(store.get(ids[0]).content, content);
    assert.equal(store.get(ids[2]).content, content);

    const tools = new MemoryTools(() => new SQLiteMemoryStore(f.path));
    t.after(() => tools.close());
    const search = (await tools.search({ terms: ["条件"] })).details;
    assert.equal(search.status, "ok");
    assert.deepEqual(search.candidates.map(({ id }) => id), [ids[0], ids[2]]);
    const read = (await tools.read({ id: ids[2], expectedRevision: 1 })).details;
    assert.equal(read.status, "ok");
    assert.equal(read.content, content);
  });

  test(`${encoding}: durable jobs, leases, source progress and origins preserve full identities`, async (t) => {
    const f = await fixture(t, encoding);
    let store = f.open();
    const job = {
      id: "job\0one", sessionId: "session\0one", sessionFile: null,
      leafId: "leaf\0one", cwd: f.root, sourceKeys: ["source", "source\0one"],
      lineageSessionIds: ["ancestor\0one", "session\0one"], branchAnchor: "branch\0one",
    };
    const owner = "worker\0one";
    const record = { id: "memory\0one", content: "\ufeffEvidence\0中文🧠", createdAt: time };
    store.enqueueConsolidation(job);
    store.close();
    store = f.open();
    assert.deepEqual(store.listConsolidationJobs(), [job]);
    store.enqueueConsolidation(structuredClone(job));
    assert.equal(store.claimConsolidation(job.id, owner, 100, 100), true);
    store.close();
    store = f.open();
    store.releaseConsolidation(job.id, "worker");
    assert.equal(store.claimConsolidation(job.id, "other", 101, 100), false);
    store.releaseConsolidation(job.id, owner);
    assert.equal(store.claimConsolidation(job.id, owner, 102, 100), true);
    const changes = { jobId: job.id, owner, now: 103, creates: [record], updates: [], deletes: [] };
    assert.equal(store.commitConsolidation(changes), true);
    store.close();
    store = f.open();
    assert.deepEqual(store.processedSources([...job.sourceKeys, "source\0missing"]), job.sourceKeys);
    assert.deepEqual(store.memoryOrigin(record.id), {
      sessionId: job.sessionId, sourceKeys: job.sourceKeys,
      lineageSessionIds: job.lineageSessionIds, branchAnchor: job.branchAnchor,
    });
    assert.equal(store.memoryOrigin("unrelated"), undefined);
    store.forget({ id: record.id, expectedRevision: 1 });
    assert.equal(store.commitConsolidation(changes), false);
    assert.equal(store.get(record.id), undefined);
    assert.deepEqual(store.listConsolidationJobs(), []);

    const observation = {
      sessionId: "observed\0one", sessionFile: join(f.root, "session.jsonl"),
      cwd: f.root, leafId: "leaf\0one", ownerPid: process.pid,
    };
    store.observeSession(observation);
    store.observeSession({ ...observation, sessionId: "observed", leafId: null });
    store.close();
    store = f.open();
    assert.deepEqual(store.observedSessions(), [{ ...observation, sessionId: "observed", leafId: null }, observation]);
    store.acknowledgeObservation(observation.sessionId, "leaf");
    assert.equal(store.observedSessions().length, 2);
    store.acknowledgeObservation(observation.sessionId, observation.leafId);
    assert.deepEqual(store.observedSessions(), [{ ...observation, sessionId: "observed", leafId: null }]);
  });

  for (const version of [0, 1]) {
    test(`${encoding}: schema ${version} upgrades without changing embedded NUL text`, async (t) => {
      const f = await fixture(t, encoding);
      const db = new DatabaseSync(f.path);
      const extra = version === 0 ? "" : ", revision INTEGER NOT NULL, updated_at TEXT NOT NULL";
      db.exec(`CREATE TABLE memories (id TEXT PRIMARY KEY NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL${extra}) STRICT`);
      db.exec(`PRAGMA user_version = ${version}`);
      const record = { id: "legacy\0one", content: "\ufeffOld\0中文🧠END", createdAt: time };
      const values = version === 0 ? [record.id, record.content, time] : [record.id, record.content, time, 3, updatedAt];
      db.prepare(`INSERT INTO memories VALUES (${values.map(() => "?").join(",")})`).run(...values);
      db.close();
      const expected = { ...record, revision: version === 0 ? 1 : 3, updatedAt: version === 0 ? time : updatedAt };
      let store = f.open();
      assert.deepEqual(store.get(record.id), expected);
      store.close();
      store = f.open();
      assert.deepEqual(store.get(record.id), expected);
    });
  }
}

test("invalid stored UTF-8 is rejected instead of replaced with a plausible character", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.save({ id: "invalid", content: "valid", createdAt: time });
  const db = new DatabaseSync(f.path);
  db.exec("UPDATE memories SET content = CAST(x'76616c696480' AS TEXT)");
  db.close();
  assert.throws(() => store.get("invalid"), TypeError);
  store.close();
  assert.throws(() => f.open(), TypeError);
});

test("a NUL cannot hide invalid timestamp or filesystem-path suffixes during validation", async (t) => {
  const f = await fixture(t);
  let store = f.open();
  store.save({ id: "invalid-time", content: "valid", createdAt: time });
  const db = new DatabaseSync(f.path);
  try {
    db.prepare("UPDATE memories SET created_at = ?").run(time + "\0invalid");
    assert.throws(() => store.get("invalid-time"), TypeError);
    store.close();
    assert.throws(() => f.open(), TypeError);
    db.prepare("UPDATE memories SET created_at = ?").run(time);
    store = f.open();
    store.observeSession({ sessionId: "observed", sessionFile: join(f.root, "session.jsonl"), cwd: f.root, leafId: null, ownerPid: process.pid });
    db.prepare("UPDATE session_observations SET session_file = ?").run(join(f.root, "session.jsonl") + "\0invalid");
    assert.throws(() => store.observedSessions(), TypeError);
    store.close();
    const reopened = f.open();
    assert.throws(() => reopened.observedSessions(), TypeError);
  } finally {
    db.close();
  }
});
