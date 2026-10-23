import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  initializeSourceRegistry,
  SourceRegistry,
} from "../src/source-registry.ts";

const version = (device, inode, size = 1, mtime = 1, ctime = 1) =>
  `${device}:${inode}:${size}:${mtime}:${ctime}`;

async function fixture(t, encoding = "UTF-8") {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-source-registry-"));
  const databasePath = join(directory, "registry.sqlite");
  let database = new DatabaseSync(databasePath);
  database.exec(`PRAGMA encoding = '${encoding}'; CREATE TABLE encoding_seed (id INTEGER); DROP TABLE encoding_seed`);
  initializeSourceRegistry(database);
  let registry = new SourceRegistry(database);
  const reopen = () => {
    database.close();
    database = new DatabaseSync(databasePath);
    registry = new SourceRegistry(database);
    return registry;
  };
  t.after(async () => {
    if (database.isOpen) database.close();
    await rm(directory, { recursive: true, force: true });
  });
  return { directory, databasePath, database: () => database, registry: () => registry, reopen };
}

for (const encoding of ["UTF-8", "UTF-16le", "UTF-16be"]) {
  test(`${encoding}: source registry preserves embedded-NUL identities across reopening`, async (t) => {
    const f = await fixture(t, encoding);
    const source = {
      sessionId: "session\0🧠tail",
      sessionFile: join(f.directory, "source.jsonl"),
      cwd: `${f.directory}\0cwd-tail`,
    };
    f.registry().setBackfillComplete(true);
    assert.throws(() => f.registry().register({
      ...source,
      sessionFile: `${source.sessionFile}\0invalid-tail`,
    }), TypeError);
    const registered = f.registry().register(source);
    assert.equal(registered.sessionId, source.sessionId);
    assert.equal(registered.cwd, source.cwd);

    assert.equal(f.registry().recordHeader(header(source, version(12, 80))), true);
    const reopened = f.reopen();
    assert.deepEqual(reopened.get(source.sessionFile, source.sessionId), {
      ...registered,
      fileVersion: version(12, 80),
      physicalIdentity: "12:80",
      headerState: "known",
      reserved: false,
    });
    assert.equal(reopened.listPage({ limit: 1 }).items[0].sessionId, source.sessionId);
    f.database().prepare("UPDATE managed_sources SET session_file = ? WHERE generation = ?")
      .run(`${source.sessionFile}\0invalid-tail`, registered.generation);
    assert.throws(() => reopened.listPage({ limit: 1 }), TypeError);
  });
}

function input(directory, name, sessionId = `session-${name}`) {
  return {
    sessionId,
    sessionFile: join(directory, name),
    cwd: directory,
  };
}

function header(source, fileVersion, parentSession) {
  return {
    sessionFile: source.sessionFile,
    sessionId: source.sessionId,
    fileVersion,
    ...(parentSession === undefined ? {} : { parentSession }),
  };
}

test("registration is durable, idempotent, and preserves a known header on repeated observation", async (t) => {
  const f = await fixture(t);
  const source = input(f.directory, "session-a.jsonl");
  f.registry().setBackfillComplete(true);
  const first = f.registry().register(source);
  assert.match(first.generation, /^[0-9a-f-]{36}$/i);
  assert.equal(first.headerState, "unknown");
  assert.equal(f.registry().isTopologyComplete(), false);

  assert.equal(f.registry().recordHeader(header(source, version(1, 10))), true);
  const known = f.registry().get(source.sessionFile, source.sessionId);
  assert.equal(known.headerState, "known");
  const topologyVersion = f.registry().topologyVersion();

  const repeated = f.registry().register(source);
  assert.equal(repeated.generation, first.generation);
  assert.equal(repeated.headerState, "known");
  assert.equal(f.registry().topologyVersion(), topologyVersion);

  const reopened = f.reopen();
  assert.equal(reopened.get(source.sessionFile, source.sessionId).generation, first.generation);
  assert.equal(reopened.get(source.sessionFile, source.sessionId).headerState, "known");
  assert.equal(reopened.isTopologyComplete(), true);
});

test("bounded catalog pages resume from durable cursors beyond the former 1,024-source ceiling", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const count = 1_050;
  for (let index = 0; index < count; index += 1) {
    registry.register(input(f.directory, `session-${String(index).padStart(4, "0")}.jsonl`));
  }

  const first = registry.listPage({ limit: 128 });
  assert.equal(first.items.length, 128);
  assert.notEqual(first.nextCursor, null);
  registry.writeCursor("headers", first.nextCursor);

  let resumed = f.reopen();
  assert.equal(resumed.readCursor("headers"), first.nextCursor);
  const seen = new Set(first.items.map(source => source.sessionFile));
  let cursor = resumed.readCursor("headers");
  while (cursor !== null) {
    const page = resumed.listPage({ afterRowId: cursor, limit: 128 });
    for (const source of page.items) assert.equal(seen.add(source.sessionFile), seen);
    cursor = page.nextCursor;
    resumed.writeCursor("headers", cursor);
  }
  assert.equal(seen.size, count);
  assert.equal(resumed.readCursor("headers"), null);
});

test("source file versions update without changing topology, while parent-edge changes do", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const parent = input(f.directory, "parent.jsonl");
  const child = input(f.directory, "child.jsonl");
  registry.register(parent);
  registry.register(child);
  registry.recordHeader(header(parent, version(7, 11)));
  registry.recordHeader(header(child, version(7, 12), "parent.jsonl"));

  assert.equal(registry.get(child.sessionFile, child.sessionId).parentPath, parent.sessionFile);
  assert.deepEqual(registry.reverseDependencies(parent.sessionFile), [child.sessionFile]);
  const edgeVersion = registry.topologyVersion();

  assert.equal(registry.recordHeader(header(child, version(7, 12, 2, 2, 2), "parent.jsonl")), true);
  assert.equal(registry.get(child.sessionFile, child.sessionId).fileVersion, version(7, 12, 2, 2, 2));
  assert.equal(registry.topologyVersion(), edgeVersion);

  assert.equal(registry.recordHeader(header(child, version(7, 12, 3, 3, 3))), true);
  assert.equal(registry.get(child.sessionFile, child.sessionId).parentPath, null);
  assert.deepEqual(registry.reverseDependencies(parent.sessionFile), []);
  assert.ok(registry.topologyVersion() > edgeVersion);
});

test("hasDependents detects registered children through source paths and physical aliases", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const parent = input(f.directory, "parent.jsonl", "shared-session");
  const alias = input(f.directory, "parent-alias.jsonl", "shared-session");
  const child = input(f.directory, "child.jsonl");
  registry.register(parent);
  registry.register(alias);
  registry.register(child);
  registry.recordHeader(header(parent, version(13, 80)));
  registry.recordHeader(header(alias, version(13, 80)));
  registry.recordHeader(header(child, version(13, 81), "parent.jsonl"));

  assert.equal(registry.hasDependents(parent.sessionFile), true);
  assert.equal(registry.hasDependents(alias.sessionFile), true);
  assert.equal(registry.hasDependents(child.sessionFile), false);

  registry.markDeleted(child.sessionFile, child.sessionId);
  assert.equal(registry.hasDependents(parent.sessionFile), false);
  assert.equal(registry.hasDependents(alias.sessionFile), false);

  f.database().prepare("UPDATE managed_sources SET physical_identity = 'not-valid' WHERE session_file = ?")
    .run(parent.sessionFile);
  assert.equal(registry.hasDependents(parent.sessionFile), true);
});

test("unknown headers block topology globally until repaired, without blessing a replacement inode", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const source = input(f.directory, "source.jsonl");
  registry.register(source);
  assert.equal(registry.recordHeader(header(source, version(3, 20), "parent.jsonl")), true);
  const oldTopologyVersion = registry.topologyVersion();

  registry.markUnknown({ sessionFile: source.sessionFile, sessionId: source.sessionId });
  assert.equal(registry.isTopologyComplete(), false);
  assert.equal(registry.get(source.sessionFile, source.sessionId).physicalIdentity, "3:20");
  assert.ok(registry.topologyVersion() > oldTopologyVersion);
  const unknownTopologyVersion = registry.topologyVersion();
  registry.markUnknown({ sessionFile: source.sessionFile, sessionId: source.sessionId });
  assert.equal(registry.topologyVersion(), unknownTopologyVersion);

  assert.equal(registry.recordHeader(header(source, version(4, 21))), false);
  const unresolved = registry.get(source.sessionFile, source.sessionId);
  assert.equal(unresolved.headerState, "unknown");
  assert.equal(unresolved.physicalIdentity, "3:20");
  assert.equal(registry.isTopologyComplete(), false);

  assert.equal(registry.recordHeader(header(source, version(3, 20), "parent.jsonl")), true);
  assert.equal(registry.isTopologyComplete(), true);
});

test("backfill readiness and durable maintenance cursors are validated and persisted", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  assert.equal(registry.isTopologyComplete(), false);
  for (const name of ["headers", "clean", "legacy", "gc"]) {
    assert.equal(registry.readCursor(name), null);
    registry.writeCursor(name, 27);
  }
  const reopened = f.reopen();
  for (const name of ["headers", "clean", "legacy", "gc"]) {
    assert.equal(reopened.readCursor(name), 27);
    reopened.writeCursor(name, null);
    assert.equal(reopened.readCursor(name), null);
  }
  reopened.setBackfillComplete(true);
  assert.equal(reopened.isTopologyComplete(), true);
  assert.throws(() => reopened.readCursor("other"), /cursor/i);
  assert.throws(() => reopened.writeCursor("headers", -1), /cursor/i);
});

test("fresh session reservations persist manager-known parent edges without blocking unrelated topology", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const parent = input(f.directory, "parent.jsonl");
  const child = input(f.directory, "new-child.jsonl");
  registry.register(parent);
  registry.recordHeader(header(parent, version(9, 50)));
  registry.register(child);

  registry.setReserved(child.sessionFile, child.sessionId, "parent.jsonl");
  const reservation = registry.get(child.sessionFile, child.sessionId);
  assert.equal(reservation.reserved, true);
  assert.equal(reservation.parentPath, parent.sessionFile);
  assert.equal(registry.isTopologyComplete(), true);
  assert.deepEqual(registry.reverseDependencies(parent.sessionFile), [child.sessionFile]);
  const topologyVersion = registry.topologyVersion();

  registry.setReserved(child.sessionFile, child.sessionId, "parent.jsonl");
  assert.equal(registry.topologyVersion(), topologyVersion);
  const reopened = f.reopen();
  assert.equal(reopened.get(child.sessionFile, child.sessionId).reserved, true);
  assert.deepEqual(reopened.reverseDependencies(parent.sessionFile), [child.sessionFile]);

  reopened.markUnknown({ sessionFile: child.sessionFile, sessionId: child.sessionId });
  assert.equal(reopened.get(child.sessionFile, child.sessionId).reserved, false);
  assert.equal(reopened.isTopologyComplete(), false);
  assert.deepEqual(reopened.reverseDependencies(parent.sessionFile), [child.sessionFile]);
  assert.equal(reopened.recordHeader(header(child, version(9, 51), "parent.jsonl")), true);
  assert.equal(reopened.get(child.sessionFile, child.sessionId).reserved, false);
  assert.equal(reopened.isTopologyComplete(), true);
});

test("duplicate live session identities with different file inodes fail closed", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const original = input(f.directory, "original.jsonl", "duplicate-id");
  const copy = input(f.directory, "copy.jsonl", "duplicate-id");
  registry.register(original);
  registry.register(copy);
  assert.equal(registry.recordHeader(header(original, version(10, 60))), true);
  assert.equal(registry.recordHeader(header(copy, version(10, 61))), false);
  assert.equal(registry.get(original.sessionFile).headerState, "unknown");
  assert.equal(registry.get(copy.sessionFile).headerState, "unknown");
  assert.equal(registry.isTopologyComplete(), false);
});

test("deletion removes only the matching generation from discovery and dependencies", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const parent = input(f.directory, "parent.jsonl");
  const child = input(f.directory, "child.jsonl");
  registry.register(parent);
  registry.register(child);
  registry.recordHeader(header(parent, version(5, 30)));
  registry.recordHeader(header(child, version(5, 31), "parent.jsonl"));
  const before = registry.topologyVersion();

  registry.markDeleted(child.sessionFile, "different-session");
  assert.equal(registry.listPage({ limit: 10 }).items.length, 2);
  assert.deepEqual(registry.reverseDependencies(parent.sessionFile), [child.sessionFile]);
  registry.markDeleted(child.sessionFile, child.sessionId);
  assert.equal(registry.listPage({ limit: 10 }).items.length, 1);
  assert.deepEqual(registry.reverseDependencies(parent.sessionFile), []);
  assert.ok(registry.topologyVersion() > before);
  registry.markDeleted(child.sessionFile, child.sessionId);
  assert.equal(registry.listPage({ limit: 10 }).items.length, 1);
});

test("reusing a path creates a new generation without reviving or overwriting the old one", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const original = input(f.directory, "reused.jsonl", "session-original");
  registry.register(original);
  registry.recordHeader(header(original, version(8, 40)));
  const oldGeneration = registry.get(original.sessionFile, original.sessionId).generation;

  registry.markDeleted(original.sessionFile, original.sessionId);
  const retried = registry.register(original);
  assert.equal(retried.generation, oldGeneration);
  assert.equal(retried.deleted, true);
  assert.equal(registry.listPage({ limit: 10 }).items.length, 0);

  const replacement = input(f.directory, "reused.jsonl", "session-replacement");
  const current = registry.register(replacement);
  assert.notEqual(current.generation, oldGeneration);
  assert.equal(current.headerState, "unknown");
  assert.equal(registry.get(replacement.sessionFile).generation, current.generation);
  assert.equal(registry.get(original.sessionFile, original.sessionId).generation, oldGeneration);
  assert.equal(registry.recordHeader(header(replacement, version(8, 41))), true);
  assert.equal(registry.get(replacement.sessionFile).physicalIdentity, "8:41");
  assert.equal(registry.get(original.sessionFile, original.sessionId).physicalIdentity, "8:40");
  assert.equal(registry.isTopologyComplete(), true);
});

test("a validated new session generation supersedes stale path metadata", async (t) => {
  const f = await fixture(t);
  const registry = f.registry();
  registry.setBackfillComplete(true);
  const previous = input(f.directory, "rotated.jsonl", "session-before-rotation");
  registry.register(previous);
  registry.recordHeader(header(previous, version(11, 70)));
  const previousGeneration = registry.get(previous.sessionFile, previous.sessionId).generation;

  const current = input(f.directory, "rotated.jsonl", "session-after-rotation");
  registry.register(current);
  assert.equal(registry.isTopologyComplete(), false);
  assert.equal(registry.recordHeader(header(current, version(11, 70))), true);

  const old = registry.get(previous.sessionFile, previous.sessionId);
  assert.equal(old.generation, previousGeneration);
  assert.equal(old.deleted, true);
  assert.notEqual(registry.get(current.sessionFile).generation, previousGeneration);
  assert.equal(registry.get(current.sessionFile).generation,
    registry.get(current.sessionFile, current.sessionId).generation);
  assert.equal(registry.isTopologyComplete(), true);
});
