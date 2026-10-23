import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  MemoryConflictError,
  MemoryNotFoundError,
  SQLiteMemoryStore,
} from "../../src/storage/store.ts";

const moduleUrl = new URL("../../src/storage/store.ts", import.meta.url).href;

function memory(overrides = {}) {
  return {
    id: randomUUID(),
    content: "Synthetic 项目中，memory 可以通过明确请求删除。🧠",
    createdAt: "2026-10-21T08:00:00.000Z",
    ...overrides,
  };
}

function correction(record) {
  return {
    id: record.id,
    expectedRevision: record.revision ?? 1,
    content: "The synthetic project now uses the revised preference.",
    updatedAt: "2026-10-21T09:00:00.000Z",
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-forget-"));
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
  return { path, open };
}

function child(source, ...args) {
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", source, ...args], {
    encoding: "utf8",
    timeout: 10_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return result.stdout;
}

test("forget removes the corrected memory across reopening and preserves other IDs", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const target = memory({ id: "synthetic-记忆🧠\u0000end" });
  const other = memory();
  store.save(target);
  store.save(other);
  const current = store.update(correction(target));
  const neighbor = store.get(other.id);
  assert.equal(store.forget({ id: target.id, expectedRevision: current.revision }), undefined);
  assert.equal(store.get(target.id), undefined);
  assert.deepEqual(store.get(other.id), neighbor);
  store.close();
  const reopened = f.open();
  assert.equal(reopened.get(target.id), undefined);
  assert.deepEqual(reopened.get(other.id), neighbor);
});

test("successful forgetting survives the writer exiting without close and an independent reader", async (t) => {
  const f = await fixture(t);
  const record = memory();
  const store = f.open();
  store.save(record);
  store.close();
  child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    store.forget({ id: process.argv[2], expectedRevision: 1 });
    process.exit(0);
  `, f.path, record.id);
  const output = child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    console.log(JSON.stringify({ absent: store.get(process.argv[2]) === undefined }));
    store.close();
  `, f.path, record.id);
  assert.deepEqual(JSON.parse(output), { absent: true });
});

test("repeat and unknown forget requests succeed without reserving IDs", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const request = { id: record.id, expectedRevision: 1 };
  store.forget(request);
  store.forget(request);
  store.close();
  const reopened = f.open();
  reopened.forget(request);
  const unknown = memory();
  assert.equal(reopened.forget({ id: unknown.id, expectedRevision: 17 }), undefined);
  reopened.save(unknown);
  assert.equal(reopened.get(unknown.id).content, unknown.content);
});

test("a stale revision cannot delete a newer correction committed by another connection", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const record = memory();
  first.save(record);
  const current = second.update(correction(record));
  assert.throws(
    () => first.forget({ id: record.id, expectedRevision: 1 }),
    (error) => error instanceof MemoryConflictError && error.memoryId === record.id,
  );
  assert.deepEqual(first.get(record.id), current);
  assert.deepEqual(second.get(record.id), current);
  first.forget({ id: record.id, expectedRevision: current.revision });
  assert.equal(second.get(record.id), undefined);
});

test("a future expected revision also conflicts and leaves the store usable", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const before = store.get(record.id);
  assert.throws(
    () => store.forget({ id: record.id, expectedRevision: 2 }),
    MemoryConflictError,
  );
  assert.deepEqual(store.get(record.id), before);
  store.forget({ id: record.id, expectedRevision: 1 });
  assert.equal(store.get(record.id), undefined);
});

test("an update prepared on another connection fails as not found after forgetting", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const record = memory();
  first.save(record);
  const request = correction(second.get(record.id));
  first.forget({ id: record.id, expectedRevision: 1 });
  assert.throws(() => second.update(request), MemoryNotFoundError);
  assert.equal(first.get(record.id), undefined);
  assert.equal(second.get(record.id), undefined);
});

test("save can recreate the same ID with original or new content after forgetting", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  store.forget({ id: record.id, expectedRevision: 1 });
  store.close();
  const reopened = f.open();
  reopened.save(record);
  assert.deepEqual(reopened.get(record.id), { ...record, revision: 1, updatedAt: record.createdAt });
  reopened.forget({ id: record.id, expectedRevision: 1 });
  const replacement = { ...record, content: "A newly created synthetic memory.", createdAt: "2026-10-22T08:00:00.000Z" };
  reopened.save(replacement);
  assert.deepEqual(reopened.get(record.id), { ...replacement, revision: 1, updatedAt: replacement.createdAt });
  const updated = reopened.update(correction(replacement));
  assert.equal(updated.revision, 2);
});

test("invalid forget input is rejected without deleting or changing the target", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const before = store.get(record.id);
  const valid = { id: record.id, expectedRevision: 1 };
  for (const invalid of [
    undefined, null, [], {}, { id: record.id }, { expectedRevision: 1 },
    ...["", "  ", 123, "invalid-\ud800"].map((id) => ({ ...valid, id })),
    ...[undefined, null, "1", 0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]
      .map((expectedRevision) => ({ ...valid, expectedRevision })),
    { ...valid, unexpected: true },
    { ...valid, [Symbol("unsupported")]: true },
    Object.assign(Object.create({ id: record.id }), { expectedRevision: 1 }),
  ]) {
    assert.throws(() => store.forget(invalid), TypeError);
    assert.deepEqual(store.get(record.id), before);
  }
  store.forget(valid);
  assert.equal(store.get(record.id), undefined);
});

test("forget on a closed store fails clearly and preserves the record", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const before = store.get(record.id);
  store.close();
  assert.throws(() => store.forget({ id: record.id, expectedRevision: 1 }), /closed/i);
  assert.deepEqual(f.open().get(record.id), before);
});

test("a writer lock blocks forgetting without data loss and permits a later retry", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const before = store.get(record.id);
  const request = { id: record.id, expectedRevision: 1 };
  const blocker = new DatabaseSync(f.path);
  try {
    blocker.exec("BEGIN IMMEDIATE");
    assert.throws(() => store.forget(request));
    assert.deepEqual(store.get(record.id), before);
    blocker.exec("ROLLBACK");
    store.forget(request);
    assert.equal(store.get(record.id), undefined);
  } finally {
    blocker.close();
  }
});

test("a failed delete commit rolls back the corrected record and allows retry", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  const current = store.update(correction(record));
  const request = { id: record.id, expectedRevision: current.revision };
  const reader = new DatabaseSync(f.path);
  try {
    reader.exec("BEGIN");
    reader.prepare("SELECT name FROM sqlite_schema").all();
    assert.throws(() => store.forget(request));
    reader.exec("ROLLBACK");
    assert.deepEqual(store.get(record.id), current);
    store.close();
    const reopened = f.open();
    assert.deepEqual(reopened.get(record.id), current);
    reopened.forget(request);
    reopened.close();
    assert.equal(f.open().get(record.id), undefined);
  } finally {
    reader.close();
  }
});

test("legacy records remain correctable and forgettable after the existing upgrade", async (t) => {
  const f = await fixture(t);
  const target = memory();
  const other = memory();
  const legacy = new DatabaseSync(f.path);
  try {
    // Historical three-column format; this fixture deliberately encodes the shipped schema.
    legacy.exec(`CREATE TABLE IF NOT EXISTS memories (
      id TEXT PRIMARY KEY NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL
    ) STRICT`);
    const insert = legacy.prepare("INSERT INTO memories VALUES (?, ?, ?)");
    for (const record of [target, other]) insert.run(record.id, record.content, record.createdAt);
  } finally {
    legacy.close();
  }
  const store = f.open();
  const current = store.update(correction(target));
  const neighbor = store.get(other.id);
  store.forget({ id: target.id, expectedRevision: current.revision });
  store.close();
  const reopened = f.open();
  assert.equal(reopened.get(target.id), undefined);
  assert.deepEqual(reopened.get(other.id), neighbor);
});
