import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { MemoryConflictError, SQLiteMemoryStore } from "../../src/storage/store.ts";

const moduleUrl = new URL("../../src/storage/store.ts", import.meta.url).href;

function memory(overrides = {}) {
  return {
    id: randomUUID(),
    content: "In the synthetic demo project, tests remain local by user request.",
    createdAt: "2026-10-21T08:00:00.000Z",
    ...overrides,
  };
}

function stored(record) {
  return { ...record, revision: 1, updatedAt: record.createdAt };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-memory-"));
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

function child(source, ...args) {
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", source, ...args], {
    encoding: "utf8",
    timeout: 10_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  return result.stdout;
}

test("a new store saves and reads a complete memory without classifying it", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  assert.deepEqual(store.get(record.id), stored(record));
});

test("a successful save survives the writer process exiting without close", async (t) => {
  const f = await fixture(t);
  const record = memory();
  child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    store.save(JSON.parse(process.argv[2]));
    process.exit(0);
  `, f.path, JSON.stringify(record));
  const output = child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    console.log(JSON.stringify(store.get(process.argv[2])));
    store.close();
  `, f.path, record.id);
  assert.deepEqual(JSON.parse(output), stored(record));
});

test("Chinese, English, quotes, and multiline context survive reopening exactly", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory({
    content: "在 synthetic-project 中，用户选择 'local tests'。\nReason 未确认；其他项目可参考。🧠\u0000End.",
  });
  store.save(record);
  store.close();
  assert.deepEqual(f.open().get(record.id), stored(record));
});

test("an unknown ID returns undefined", async (t) => {
  const f = await fixture(t);
  assert.equal(f.open().get(randomUUID()), undefined);
});

test("retrying the same complete record succeeds across reopening", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  store.save(record);
  store.save({ ...record });
  store.close();
  const reopened = f.open();
  reopened.save({ ...record });
  assert.deepEqual(reopened.get(record.id), stored(record));
});

test("conflicting content or creation time preserves the original and leaves the store usable", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  for (const changed of [
    { ...original, content: "A different, unconfirmed instruction." },
    { ...original, createdAt: "2026-10-22T08:00:00.000Z" },
  ]) {
    assert.throws(() => store.save(changed), MemoryConflictError);
    assert.deepEqual(store.get(original.id), stored(original));
  }
  const next = memory();
  store.save(next);
  store.close();
  const reopened = f.open();
  assert.deepEqual(reopened.get(original.id), stored(original));
  assert.deepEqual(reopened.get(next.id), stored(next));
});

test("invalid external records are rejected without changing existing data", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  for (const invalid of [
    null,
    [],
    {},
    memory({ id: "" }),
    memory({ id: "   " }),
    memory({ id: 123 }),
    memory({ content: " \n\t" }),
    memory({ content: 123 }),
    memory({ createdAt: "not-a-date" }),
    memory({ createdAt: "2026-02-30T08:00:00.000Z" }),
    memory({ createdAt: "2026-10-21" }),
    memory({ createdAt: 123 }),
    memory({ unexpectedField: "must not be silently lost" }),
  ]) {
    assert.throws(() => store.save(invalid), TypeError);
    if (typeof invalid?.id === "string" && invalid.id.trim() !== "") {
      assert.equal(store.get(invalid.id), undefined);
    }
  }
  store.close();
  assert.deepEqual(f.open().get(original.id), stored(original));
});

test("malformed Unicode IDs and content are rejected instead of silently rewritten", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  for (const invalid of [
    { ...original, id: "invalid-\ud800" },
    { ...original, content: "invalid high surrogate: \ud800" },
    { ...original, content: "invalid low surrogate: \udc00" },
  ]) {
    assert.throws(() => store.save(invalid), TypeError);
    assert.deepEqual(store.get(original.id), stored(original));
  }
});

test("two connections see committed memories and enforce the same retry rules", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const record = memory();
  first.save(record);
  assert.deepEqual(second.get(record.id), stored(record));
  second.save({ ...record });
  assert.throws(() => second.save({ ...record, content: "Conflicting value." }), MemoryConflictError);
  const next = memory();
  second.save(next);
  assert.deepEqual(first.get(next.id), stored(next));
});

test("an unreadable database is reported and its bytes are preserved", async (t) => {
  const f = await fixture(t);
  const bytes = Buffer.from("This is not a SQLite database.\n");
  await writeFile(f.path, bytes);
  assert.throws(() => f.open());
  assert.deepEqual(await readFile(f.path), bytes);
});

test("opening an invalid destination reports failure", async (t) => {
  const f = await fixture(t);
  assert.throws(() => new SQLiteMemoryStore(f.root));
});

test("a file path must be explicit and absolute", () => {
  for (const path of ["", "memories.sqlite", ":memory:", null]) {
    assert.throws(() => new SQLiteMemoryStore(path), TypeError);
  }
});

test("a blocked save reports failure and can be retried after the lock is released", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  const blocker = new DatabaseSync(f.path);
  try {
    blocker.exec("BEGIN IMMEDIATE");
    assert.throws(() => store.save(record));
    assert.equal(store.get(record.id), undefined);
    blocker.exec("ROLLBACK");
    store.save(record);
    assert.deepEqual(store.get(record.id), stored(record));
  } finally {
    blocker.close();
  }
});

test("a failed commit rolls back the inserted record and allows a later retry", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const record = memory();
  const reader = new DatabaseSync(f.path);
  try {
    reader.exec("BEGIN");
    // Hold a read lock without depending on the store's table layout.
    reader.prepare("SELECT name FROM sqlite_schema").all();
    assert.throws(() => store.save(record));
    reader.exec("ROLLBACK");
    assert.equal(store.get(record.id), undefined);
    store.save(record);
    store.close();
    assert.deepEqual(f.open().get(record.id), stored(record));
  } finally {
    reader.close();
  }
});

test("close is repeatable and operations on a closed store fail clearly", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.close();
  store.close();
  assert.throws(() => store.save(memory()), /closed/i);
  assert.throws(() => store.get(randomUUID()), /closed/i);
});
