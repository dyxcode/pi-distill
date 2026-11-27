import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm, readFile, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import * as api from "../../src/host-storage.ts";
import { SQLiteMemoryStore } from "../../src/storage/store.ts";
const createdAt = "2026-11-17T00:00:00.000Z";
async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "distill-host-reader-"));
  const path = join(root, "memories.sqlite");
  const writer = new SQLiteMemoryStore(path);
  t.after(async () => {
    writer.close();
    await rm(root, { recursive: true, force: true });
  });
  const save = (id, content) => writer.save({ id, content, createdAt });
  return {
    root,
    path,
    writer,
    save,
    open() {
      const reader = api.openMemoryReaderV1(path);
      t.after(() => reader.close());
      return reader;
    },
  };
}
test("host reader is exported and searches literal bilingual text with bounded pages", async (t) => {
  const f = await fixture(t);
  f.save("a", "SQLite storage");
  f.save("b", "保留知识 SQLite");
  f.save("c", "unrelated");
  const r = f.open();
  assert.equal(r.version, 1);
  const first = r.search({ query: " sqlite ", limit: 1 });
  assert.equal(first.items.length, 1);
  assert.equal(first.items[0].id, "a");
  assert.ok(first.nextCursor);
  const second = r.search({ query: "sqlite", limit: 1, cursor: first.nextCursor });
  assert.equal(second.items[0].id, "b");
  assert.equal(r.search({ query: "保留" }).items[0].id, "b");
  assert.deepEqual(r.search({ query: "not present" }), { items: [] });
  assert.throws(() => r.search({ query: "" }), { code: "invalid_input" });
  assert.throws(() => r.search({ query: "x".repeat(129) }), { code: "invalid_input" });
  assert.throws(() => r.search({ query: "SQLite", limit: 21 }), { code: "invalid_input" });
});
test("recent memories page by update time and then ascending ID", async (t) => {
  const f = await fixture(t);
  const r = f.open();
  assert.deepEqual(r.list(), { items: [] });
  f.save("z", "old");
  f.save("b", "second");
  f.save("a", "first");
  const updatedAt = "2026-11-18T00:00:00.000Z";
  f.writer.update({ id: "z", expectedRevision: 1, content: "知识😀".repeat(200), updatedAt });
  const first = r.list({ limit: 2 });
  assert.deepEqual(first.items, [
    { id: "z", revision: 2, updatedAt, preview: [..."知识😀".repeat(200)].slice(0, 500).join("") },
    { id: "a", revision: 1, updatedAt: createdAt, preview: "first" },
  ]);
  assert.ok(first.nextCursor);
  assert.deepEqual(r.list({ cursor: first.nextCursor }), {
    items: [{ id: "b", revision: 1, updatedAt: createdAt, preview: "second" }],
  });
  for (const limit of [0, 21, 1.5, null]) assert.throws(() => r.list({ limit }), { code: "invalid_input" });
  const search = r.search({ query: "s", limit: 1 });
  assert.ok(search.nextCursor);
  for (const cursor of ["", "invalid", first.nextCursor + "x", search.nextCursor])
    assert.throws(() => r.list({ cursor }), { code: "invalid_cursor" });
  assert.throws(() => f.open().list({ cursor: first.nextCursor }), { code: "invalid_cursor" });
  assert.throws(() => r.search({ query: "first", cursor: first.nextCursor }), { code: "invalid_cursor" });
  r.close();
  assert.throws(() => r.list(), { code: "memory_unavailable" });
});
test("search resumes after an empty bounded scan and never loses matching rows at a limit", async (t) => {
  const f = await fixture(t);
  for (let i = 0; i < 140; i++) f.save(String(i).padStart(3, "0"), i >= 129 ? "needle" : "noise");
  const r = f.open();
  let page = r.search({ query: "needle", limit: 3 });
  assert.deepEqual(page.items, []);
  assert.ok(page.nextCursor);
  const ids = [];
  for (let i = 0; i < 10; i++) {
    if (!page.nextCursor) break;
    page = r.search({ query: "needle", limit: 3, cursor: page.nextCursor });
    ids.push(...page.items.map((x) => x.id));
  }
  assert.equal(ids.length, 11);
  assert.equal(new Set(ids).size, 11);
  assert.equal(page.nextCursor, undefined);
});
test("read chunks preserve Unicode and enforce revision on each request", async (t) => {
  const f = await fixture(t);
  const content = "知识😀".repeat(4000);
  f.save("long", content);
  const r = f.open();
  const hit = r.search({ query: "知识" }).items[0];
  assert.ok([...hit.preview].length <= 500);
  let page = r.read({ id: hit.id, expectedRevision: hit.revision });
  let text = page.text;
  assert.equal([...page.text].length, 8000);
  assert.ok(page.nextCursor);
  page = r.read({ id: hit.id, expectedRevision: hit.revision, cursor: page.nextCursor });
  text += page.text;
  assert.equal(text, content);
  assert.equal(page.nextCursor, undefined);
  f.writer.update({ id: "long", expectedRevision: 1, content: "updated", updatedAt: createdAt });
  assert.throws(() => r.read({ id: "long", expectedRevision: 1 }), { code: "memory_changed" });
  assert.equal(r.read({ id: "long", expectedRevision: 2 }).text, "updated");
  f.writer.forget({ id: "long", expectedRevision: 2 });
  assert.throws(() => r.read({ id: "long", expectedRevision: 2 }), { code: "memory_not_found" });
});
test("cursors are bound to reader, operation, query and revision", async (t) => {
  const f = await fixture(t);
  f.save("a", "needle ".repeat(1400));
  f.save("b", "needle");
  const r = f.open();
  const s = r.search({ query: "needle", limit: 1 });
  const p = r.read({ id: "a", expectedRevision: 1 });
  assert.ok(s.nextCursor);
  assert.ok(p.nextCursor);
  for (const cursor of [s.nextCursor + "x", p.nextCursor])
    assert.throws(() => r.search({ query: "needle", cursor }), { code: "invalid_cursor" });
  assert.throws(() => r.search({ query: "other", cursor: s.nextCursor }), {
    code: "invalid_cursor",
  });
  assert.throws(() => f.open().search({ query: "needle", cursor: s.nextCursor }), {
    code: "invalid_cursor",
  });
  assert.throws(() => r.read({ id: "b", expectedRevision: 1, cursor: p.nextCursor }), {
    code: "invalid_cursor",
  });
  assert.throws(() => r.read({ id: "a", expectedRevision: 2, cursor: p.nextCursor }), {
    code: "invalid_cursor",
  });
  r.close();
  r.close();
  assert.throws(() => r.search({ query: "needle" }), { code: "memory_unavailable" });
});
test("reader opens only existing compatible stores and does not mutate data", async (t) => {
  const f = await fixture(t);
  f.save("a", "content");
  f.writer.close();
  const before = await readFile(f.path);
  const r = f.open();
  r.search({ query: "content" });
  assert.equal(r.list().items[0].id, "a");
  r.read({ id: "a", expectedRevision: 1 });
  r.close();
  assert.deepEqual(await readFile(f.path), before);
  const missing = join(f.root, "missing.sqlite");
  assert.throws(() => api.openMemoryReaderV1(missing), { code: "memory_unavailable" });
  await assert.rejects(access(missing));
  const db = new DatabaseSync(f.path);
  db.exec("PRAGMA user_version=999");
  db.close();
  const unsupported = await readFile(f.path);
  assert.throws(() => api.openMemoryReaderV1(f.path), { code: "memory_unavailable" });
  assert.deepEqual(await readFile(f.path), unsupported);
});


test("escaped identifiers keep generated cursors usable", async (t) => {
  const f = await fixture(t);
  const id = "\u0001".repeat(512);
  const query = "\u0002".repeat(128);
  f.save(id, query + "x".repeat(9000));
  f.save("z", query);
  const reader = f.open();
  const page = reader.search({ query, limit: 1 });
  assert.ok(page.nextCursor);
  assert.equal(reader.search({ query, cursor: page.nextCursor }).items[0].id, "z");
  const read = reader.read({ id, expectedRevision: 1 });
  assert.ok(read.nextCursor);
  assert.equal(reader.read({ id, expectedRevision: 1, cursor: read.nextCursor }).text.length, 1128);
});
