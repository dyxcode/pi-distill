import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { openHostStorageV1 } from "../../src/host-storage.ts";
import { SQLiteMemoryStore } from "../../src/storage/store.ts";

test("schema v13 migration preserves opening and attached host uses", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-host-storage-migration-"));
  const databasePath = join(directory, "memory.sqlite");
  t.after(() => rm(directory, { recursive: true, force: true }));

  const storage = openHostStorageV1(databasePath);
  t.after(() => storage.close());
  const opening = storage.beginHostUse({
    sessionId: "synthetic-opening-session",
    sessionFile: join(directory, "opening.jsonl"),
    cwd: directory,
  });
  const attached = storage.beginHostUse({
    sessionId: "synthetic-attached-session",
    sessionFile: join(directory, "attached.jsonl"),
    cwd: directory,
  });
  assert.ok(opening);
  assert.ok(attached);
  const child = attached.attachChild(process.pid);
  const byId = (left, right) => left.id.localeCompare(right.id);
  const before = [...storage.listPendingHostUses()].sort(byId);
  assert.equal(before.find(({ id }) => id === opening.id)?.state, "opening");
  assert.equal(before.find(({ id }) => id === attached.id)?.state, "attached");
  assert.deepEqual(before.find(({ id }) => id === attached.id)?.child, child);
  storage.close();

  const database = new DatabaseSync(databasePath);
  database.exec(
    "DROP INDEX host_session_uses_by_file; " +
      "DROP INDEX host_session_uses_by_session; " +
      "ALTER TABLE host_session_uses RENAME TO host_session_uses_v14; " +
      "CREATE TABLE host_session_uses (" +
      "id TEXT PRIMARY KEY NOT NULL, " +
      "session_id TEXT NOT NULL, " +
      "session_file TEXT NOT NULL, " +
      "cwd TEXT NOT NULL, " +
      "source_generation TEXT, " +
      "owner_pid INTEGER NOT NULL CHECK (owner_pid > 0), " +
      "created_at INTEGER NOT NULL CHECK (created_at >= 0), " +
      "state TEXT NOT NULL CHECK (state IN ('opening', 'attached')), " +
      "child_pid INTEGER CHECK (child_pid IS NULL OR child_pid > 0), " +
      "child_token TEXT, " +
      "CHECK ((state = 'opening' AND child_pid IS NULL AND child_token IS NULL) OR " +
      "(state = 'attached' AND child_pid IS NOT NULL AND child_token IS NOT NULL))" +
      ") STRICT; " +
      "INSERT INTO host_session_uses " +
      "SELECT id, session_id, session_file, cwd, source_generation, owner_pid, created_at, state, child_pid, child_token " +
      "FROM host_session_uses_v14; " +
      "DROP TABLE host_session_uses_v14; " +
      "CREATE INDEX host_session_uses_by_file ON host_session_uses (session_file, id); " +
      "CREATE INDEX host_session_uses_by_session ON host_session_uses (session_id, id); " +
      "DROP INDEX IF EXISTS source_access_by_file; DROP TABLE source_access; CREATE TABLE source_access (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), token TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0)) STRICT; PRAGMA user_version = 13",
  );
  database.close();

  const migrated = openHostStorageV1(databasePath);
  t.after(() => migrated.close());
  const version = new DatabaseSync(databasePath);
  assert.equal(version.prepare("PRAGMA user_version").get().user_version, 15);
  version.close();
  assert.deepEqual([...migrated.listPendingHostUses()].sort(byId), before);
});

test("per-session source access isolates distinct sessions and allows shared reads", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-per-session-access-"));
  const databasePath = join(directory, "memory.sqlite");
  t.after(() => rm(directory, { recursive: true, force: true }));

  const store = new SQLiteMemoryStore(databasePath);
  const host = openHostStorageV1(databasePath);
  t.after(() => {
    host.close();
    store.close();
  });

  const fileA = join(directory, "session-a.jsonl");
  const fileB = join(directory, "session-b.jsonl");
  const refA = { sessionId: "session-a", sessionFile: fileA, cwd: directory };
  const refB = { sessionId: "session-b", sessionFile: fileB, cwd: directory };

  // Exclusive delete lock on session A must not block reads or host uses on session B.
  const deleteA = store.acquireSourceAccess(fileA, "delete");
  assert.ok(deleteA);
  assert.equal(host.acquireSourceRead(refA), undefined);
  assert.equal(host.beginHostUse(refA), undefined);

  const readB1 = host.acquireSourceRead(refB);
  const readB2 = host.acquireSourceRead(refB);
  assert.ok(readB1, "unrelated session read must not be blocked by session A delete");
  assert.ok(readB2, "concurrent reads on the same session must share access");
  const useB = host.beginHostUse(refB);
  assert.ok(useB, "unrelated session host use must not be blocked by session A delete");
  assert.equal(host.recoverHostUse(useB.id, { kind: "opening-abandoned" }), true);

  // Exclusive delete lock on session B must be blocked while either read lease is held.
  assert.equal(store.acquireSourceAccess(fileB, "delete"), undefined);
  readB1.release();
  assert.equal(store.acquireSourceAccess(fileB, "delete"), undefined);
  readB2.release();

  const deleteB = store.acquireSourceAccess(fileB, "delete");
  assert.ok(deleteB, "delete lock succeeds once all readers on session B release");
  assert.equal(store.releaseSourceAccess(deleteB), true);
  assert.equal(store.releaseSourceAccess(deleteA), true);

  const readA = host.acquireSourceRead(refA);
  assert.ok(readA);
  readA.release();
});
