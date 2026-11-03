import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { openHostStorageV1 } from "../../src/host-storage.ts";

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
      "PRAGMA user_version = 13",
  );
  database.close();

  const migrated = openHostStorageV1(databasePath);
  t.after(() => migrated.close());
  const version = new DatabaseSync(databasePath);
  assert.equal(version.prepare("PRAGMA user_version").get().user_version, 14);
  version.close();
  assert.deepEqual([...migrated.listPendingHostUses()].sort(byId), before);
});
