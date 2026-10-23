import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { MemoryConflictError, SQLiteMemoryStore } from "../../src/storage/store.ts";

const moduleUrl = new URL("../../src/storage/store.ts", import.meta.url).href;

function memory(overrides = {}) {
  return {
    id: randomUUID(),
    content: "The synthetic project initially requires a separate push request.",
    createdAt: "2026-10-21T08:00:00.000Z",
    ...overrides,
  };
}

function stored(record) {
  return { ...record, revision: 1, updatedAt: record.createdAt };
}

function correction(record, overrides = {}) {
  return {
    id: record.id,
    expectedRevision: record.revision ?? 1,
    content: "In the synthetic project, a commit request also authorizes a push.",
    updatedAt: "2026-10-21T09:00:00.000Z",
    ...overrides,
  };
}

function corrected(record, request) {
  return {
    id: record.id,
    content: request.content,
    createdAt: record.createdAt,
    revision: request.expectedRevision + 1,
    updatedAt: request.updatedAt,
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-update-"));
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

function legacyDatabase(path, records) {
  const db = new DatabaseSync(path);
  try {
    // This is the historical format shipped in ada973e, not the new implementation layout.
    db.exec(`CREATE TABLE memories (
      id TEXT PRIMARY KEY NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL
    ) STRICT`);
    const insert = db.prepare("INSERT INTO memories VALUES (?, ?, ?)");
    for (const record of records) insert.run(record.id, record.content, record.createdAt);
  } finally {
    db.close();
  }
}

test("correction returns and persists the current record with stable identity and creation time", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const request = correction(original, {
    content: "Synthetic 项目规则更新：commit 包含 push。\n语境保留，其他项目自行判断。🧠\u0000End.",
  });
  const expected = corrected(original, request);
  assert.deepEqual(store.update(request), expected);
  assert.deepEqual(store.get(original.id), expected);
  store.close();
  assert.deepEqual(f.open().get(original.id), expected);
});

test("multiple corrections increment the counter and expose only current content", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const first = store.update(correction(original));
  const request = correction(first, {
    content: "The synthetic project now pauses publication until a release request.",
    updatedAt: "2026-10-21T10:00:00.000Z",
  });
  const second = store.update(request);
  assert.deepEqual(second, corrected(first, request));
  assert.equal(second.revision, 3);
  store.close();
  assert.deepEqual(f.open().get(original.id), second);
});

test("a successful correction survives the writer process exiting without close", async (t) => {
  const f = await fixture(t);
  const original = memory();
  const request = correction(original);
  function child(source, ...args) {
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", source, ...args], {
      encoding: "utf8",
      timeout: 10_000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr || result.stdout);
    return result.stdout;
  }
  child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    store.save(JSON.parse(process.argv[2]));
    store.update(JSON.parse(process.argv[3]));
    process.exit(0);
  `, f.path, JSON.stringify(original), JSON.stringify(request));
  const output = child(`
    import { SQLiteMemoryStore } from ${JSON.stringify(moduleUrl)};
    const store = new SQLiteMemoryStore(process.argv[1]);
    console.log(JSON.stringify(store.get(process.argv[2])));
    store.close();
  `, f.path, original.id);
  assert.deepEqual(JSON.parse(output), corrected(original, request));
});

test("unchanged content preserves the counter and last modification time", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const request = correction(original, { content: original.content });
  assert.deepEqual(store.update(request), stored(original));
  assert.deepEqual(store.update(request), stored(original));
  store.close();
  assert.deepEqual(f.open().get(original.id), stored(original));
});

test("an identical successful update can be retried after reopening without another increment", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const request = correction(original);
  const result = store.update(request);
  assert.deepEqual(store.update({ ...request }), result);
  store.close();
  const reopened = f.open();
  assert.deepEqual(reopened.update({ ...request }), result);
  assert.deepEqual(reopened.get(original.id), result);
});

test("competing connections reject a stale payload even if only its timestamp differs", async (t) => {
  const f = await fixture(t);
  const first = f.open();
  const second = f.open();
  const original = memory();
  first.save(original);
  const seenBySecond = second.get(original.id);
  const request = correction(original);
  const current = first.update(request);
  for (const different of [
    correction(seenBySecond, { content: "Another synthetic correction." }),
    { ...request, updatedAt: "2026-10-21T09:01:00.000Z" },
  ]) {
    assert.throws(() => second.update(different), MemoryConflictError);
    assert.deepEqual(second.get(original.id), current);
  }
  const nextRequest = correction(second.get(original.id), {
    content: "A correction based on the fresh version.",
    updatedAt: "2026-10-21T10:00:00.000Z",
  });
  assert.deepEqual(second.update(nextRequest), corrected(current, nextRequest));
});

test("a retry after later changes reports conflict and cannot restore old content", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const oldRequest = correction(original);
  const first = store.update(oldRequest);
  const latest = store.update(correction(first, {
    content: "The latest synthetic rule.",
    updatedAt: "2026-10-21T10:00:00.000Z",
  }));
  assert.throws(() => store.update(oldRequest), MemoryConflictError);
  assert.deepEqual(store.get(original.id), latest);
});

test("returning to earlier text does not make an old version current again", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const first = store.update(correction(original));
  const latest = store.update(correction(first, {
    content: original.content,
    updatedAt: "2026-10-21T10:00:00.000Z",
  }));
  assert.equal(latest.revision, 3);
  for (const stale of [
    correction(original),
    correction(original, { content: original.content, updatedAt: latest.updatedAt }),
  ]) assert.throws(() => store.update(stale), MemoryConflictError);
  assert.deepEqual(store.get(original.id), latest);
});

test("pre-correction save retries cannot overwrite corrected content or reset metadata", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const current = store.update(correction(original));
  assert.throws(() => store.save(original), MemoryConflictError);
  store.save({ id: current.id, content: current.content, createdAt: current.createdAt });
  assert.deepEqual(store.get(original.id), current);
});

test("updating an unknown ID reports not found without creating it", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const request = correction(memory());
  assert.throws(() => store.update(request), (error) => {
    assert.equal(error.name, "MemoryNotFoundError");
    assert.equal(error.memoryId, request.id);
    return true;
  });
  assert.equal(store.get(request.id), undefined);
});

test("invalid correction inputs leave the current record intact", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const invalid = [null, [], {}, correction(original, { unexpected: true })];
  for (const id of ["", " ", 1, "invalid-\ud800"]) invalid.push(correction(original, { id }));
  for (const content of ["", " \n", 1, "invalid-\udc00"]) invalid.push(correction(original, { content }));
  for (const expectedRevision of [0, -1, 1.5, "1", NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    invalid.push(correction(original, { expectedRevision }));
  }
  for (const updatedAt of ["", 1, "2026-10-21", "2026-02-30T08:00:00.000Z"]) {
    invalid.push(correction(original, { updatedAt }));
  }
  for (const request of invalid) {
    assert.throws(() => store.update(request), TypeError);
    assert.deepEqual(store.get(original.id), stored(original));
  }
});

test("failed update commit preserves all fields and permits a later retry", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  const original = memory();
  store.save(original);
  const request = correction(original);
  const reader = new DatabaseSync(f.path);
  try {
    reader.exec("BEGIN");
    reader.prepare("SELECT name FROM sqlite_schema").all();
    assert.throws(() => store.update(request));
    reader.exec("ROLLBACK");
    assert.deepEqual(store.get(original.id), stored(original));
    const result = store.update(request);
    store.close();
    assert.deepEqual(f.open().get(original.id), result);
  } finally {
    reader.close();
  }
});

test("updating a closed store reports closure", async (t) => {
  const f = await fixture(t);
  const store = f.open();
  store.close();
  assert.throws(() => store.update(correction(memory())), /closed/i);
});

test("legacy records upgrade exactly and remain correct after updating and reopening", async (t) => {
  const f = await fixture(t);
  const records = [memory(), memory({ content: "Synthetic 中文 context。🧠\u0000End." })];
  legacyDatabase(f.path, records);
  const store = f.open();
  for (const record of records) assert.deepEqual(store.get(record.id), stored(record));
  const current = store.update(correction(records[0]));
  store.close();
  const reopened = f.open();
  assert.deepEqual(reopened.get(records[0].id), current);
  assert.deepEqual(reopened.get(records[1].id), stored(records[1]));
});

test("an already-open legacy writer cannot insert records missing the new metadata", async (t) => {
  const f = await fixture(t);
  const original = memory();
  legacyDatabase(f.path, [original]);
  const legacy = new DatabaseSync(f.path);
  try {
    const insert = legacy.prepare("INSERT INTO memories (id, content, created_at) VALUES (?, ?, ?)");
    const store = f.open();
    const next = memory();
    assert.throws(() => insert.run(next.id, next.content, next.createdAt));
    assert.equal(store.get(next.id), undefined);
    assert.deepEqual(store.get(original.id), stored(original));
    store.save(next);
    assert.deepEqual(store.get(next.id), stored(next));
  } finally {
    legacy.close();
  }
});

test("invalid legacy data aborts upgrade without changing the original database", async (t) => {
  const f = await fixture(t);
  legacyDatabase(f.path, [memory(), memory({ createdAt: "invalid date" })]);
  const before = await readFile(f.path);
  assert.throws(() => f.open(), TypeError);
  assert.deepEqual(await readFile(f.path), before);
});

test("a failed migration commit rolls back and leaves legacy data usable for a later upgrade", async (t) => {
  const f = await fixture(t);
  const original = memory();
  legacyDatabase(f.path, [original]);
  const before = await readFile(f.path);
  const reader = new DatabaseSync(f.path);
  try {
    reader.exec("BEGIN");
    reader.prepare("SELECT name FROM sqlite_schema").all();
    assert.throws(() => f.open());
    reader.exec("ROLLBACK");
    assert.deepEqual(await readFile(f.path), before);
    const store = f.open();
    assert.deepEqual(store.get(original.id), stored(original));
    const request = correction(original);
    assert.deepEqual(store.update(request), corrected(original, request));
  } finally {
    reader.close();
  }
});

test("a future schema version is rejected without resetting its contents or version", async (t) => {
  const f = await fixture(t);
  const original = memory();
  legacyDatabase(f.path, [original]);
  const db = new DatabaseSync(f.path);
  db.exec("PRAGMA user_version = 999");
  db.close();
  const before = await readFile(f.path);
  assert.throws(() => f.open(), /schema|version/i);
  assert.deepEqual(await readFile(f.path), before);
});

test("an incompatible unversioned schema is rejected without modifying it", async (t) => {
  const f = await fixture(t);
  const db = new DatabaseSync(f.path);
  db.exec("CREATE TABLE memories (id TEXT, content TEXT, created_at TEXT)");
  db.close();
  const before = await readFile(f.path);
  assert.throws(() => f.open(), /schema/i);
  assert.deepEqual(await readFile(f.path), before);
});

test("extra legacy constraints and defaults are rejected instead of silently removed by migration", async (t) => {
  for (const declaration of [
    "content TEXT NOT NULL CHECK (length(content) < 1000)",
    "content TEXT NOT NULL UNIQUE",
    "content TEXT NOT NULL DEFAULT 'unrecognized default'",
    "content TEXT NOT NULL COLLATE NOCASE",
  ]) {
    const f = await fixture(t);
    const db = new DatabaseSync(f.path);
    db.exec(`CREATE TABLE memories (
      id TEXT PRIMARY KEY NOT NULL,
      ${declaration},
      created_at TEXT NOT NULL
    ) STRICT`);
    db.close();
    const before = await readFile(f.path);
    assert.throws(() => f.open(), /schema/i);
    assert.deepEqual(await readFile(f.path), before);
  }
});

test("an incompatible current schema is rejected even when its column metadata matches", async (t) => {
  for (const revisionDeclaration of [
    "revision INTEGER NOT NULL CHECK (revision = 1)",
    "revision INTEGER NOT NULL DEFAULT 1",
  ]) {
    const f = await fixture(t);
    const db = new DatabaseSync(f.path);
    db.exec(`CREATE TABLE memories (
      id TEXT PRIMARY KEY NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL,
      ${revisionDeclaration},
      updated_at TEXT NOT NULL
    ) STRICT; PRAGMA user_version = 1`);
    db.close();
    const before = await readFile(f.path);
    assert.throws(() => f.open(), /schema/i);
    assert.deepEqual(await readFile(f.path), before);
  }
});
