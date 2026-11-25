import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { openHostStorageV1 } from "../../src/host-storage.ts";
import { SQLiteMemoryStore } from "../../src/storage/store.ts";
import { SOURCE_COVERAGE_SCHEMAS } from "../../src/storage/coverage.ts";

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
      SOURCE_COVERAGE_SCHEMAS.filter(s => s.name.startsWith("source_fence_session_usages_")).map(s => `DROP TRIGGER IF EXISTS ${s.name}; `).join("") + "DROP INDEX IF EXISTS usages_source; ALTER TABLE session_usages RENAME TO session_usages_v16; CREATE TABLE session_usages (owner_id TEXT PRIMARY KEY NOT NULL, process_token TEXT NOT NULL, session_id TEXT NOT NULL, session_file TEXT NOT NULL, cwd TEXT NOT NULL, leaf_id TEXT, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0), state TEXT NOT NULL CHECK (state IN ('active', 'closing', 'released'))) STRICT; INSERT INTO session_usages SELECT * FROM session_usages_v16; DROP TABLE session_usages_v16; CREATE INDEX usages_source ON session_usages (session_file); " + SOURCE_COVERAGE_SCHEMAS.filter(s => s.name.startsWith("source_fence_session_usages_")).map(s => `${s.sql}; `).join("") + "DROP INDEX IF EXISTS host_workspace_holds_by_file; DROP TABLE IF EXISTS host_workspace_holds; DROP INDEX IF EXISTS source_access_by_file; DROP TABLE source_access; CREATE TABLE source_access (singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1), token TEXT NOT NULL, owner_pid INTEGER NOT NULL CHECK (owner_pid > 0)) STRICT; PRAGMA user_version = 13",
  );
  database.close();

  const migrated = openHostStorageV1(databasePath);
  t.after(() => migrated.close());
  const version = new DatabaseSync(databasePath);
  assert.equal(version.prepare("PRAGMA user_version").get().user_version, 16);
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

test("durable host workspace holds survive restart, block retention and child shutdown distill, and enqueue distill on explicit release", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-workspace-hold-"));
  const databasePath = join(directory, "memory.sqlite");
  const sessionFile = join(directory, "session.jsonl");
  t.after(() => rm(directory, { recursive: true, force: true }));

  const { writeFile } = await import("node:fs/promises");
  const { ConsolidationCoordinator } = await import("../../src/distill/coordinator.ts");
  await writeFile(
    sessionFile,
    JSON.stringify({ type: "session", version: 3, id: "ws-hold-1", timestamp: "2026-11-25T00:00:00.000Z", cwd: directory }) + "\n" +
    JSON.stringify({ type: "message", id: "m1", parentId: null, timestamp: "2026-11-25T00:00:01.000Z", message: { role: "user", content: "hello" } }) + "\n",
  );

  const host = openHostStorageV1(databasePath);
  const hold1 = host.holdWorkspaceSession({ sessionId: "ws-hold-1", sessionFile, cwd: directory });
  assert.ok(hold1);
  assert.equal(hold1.sessionId, "ws-hold-1");
  assert.equal(hold1.sessionFile, sessionFile);
  // Idempotent hold
  const hold2 = host.holdWorkspaceSession({ sessionId: "ws-hold-1", sessionFile, cwd: directory });
  assert.ok(hold2);
  assert.equal(host.listWorkspaceHolds().length, 1);
  host.close();

  // Survives host storage close/reopen (redeploy / reboot)
  const reopened = openHostStorageV1(databasePath);
  t.after(() => reopened.close());
  assert.equal(reopened.listWorkspaceHolds().length, 1);
  assert.equal(reopened.listWorkspaceHolds()[0].sessionId, "ws-hold-1");

  // Blocks retention deletion even when process usage is released
  const store = new SQLiteMemoryStore(databasePath);
  t.after(() => store.close());
  store.saveSessionUsage({
    ownerId: "owner-1",
    processToken: "proc-1",
    sessionId: "ws-hold-1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: process.pid,
    state: "active",
  });
  store.saveSessionUsage({
    ownerId: "owner-1",
    processToken: "proc-1",
    sessionId: "ws-hold-1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: process.pid,
    state: "closing",
  });
  store.saveSessionUsage({
    ownerId: "owner-1",
    processToken: "proc-1",
    sessionId: "ws-hold-1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: process.pid,
    state: "released",
  });
  for (const state of ["active", "interrupted"]) {
    const usage = {
      ownerId: `crashed-${state}`, processToken: `crashed-${state}`, sessionId: "ws-hold-1",
      sessionFile, cwd: directory, leafId: "m1", ownerPid: 2_147_483_647,
    };
    store.saveSessionUsage({ ...usage, state: "active" });
    if (state === "interrupted") store.saveSessionUsage({ ...usage, state });
  }
  const retentionWhileHeld = store.sourceRetentionState(sessionFile, "ws-hold-1");
  assert.equal(retentionWhileHeld.released, false);
  assert.equal(retentionWhileHeld.hostUse, true);

  // Child shutdown while workspace hold is active must NOT enqueue a close distill trigger
  const coordinator = new ConsolidationCoordinator({ openStore: () => new SQLiteMemoryStore(databasePath) });
  await coordinator.shutdown({
    cwd: directory,
    sessionManager: {
      getSessionFile: () => sessionFile,
      getSessionId: () => "ws-hold-1",
      getCwd: () => directory,
      getLeafId: () => "m1",
    },
  }, "quit");
  assert.equal(store.sourceRetentionState(sessionFile, "ws-hold-1").pending, false);

  // Explicit user close releases hold and atomically enqueues distillation
  const released = reopened.releaseWorkspaceHold(
    { sessionId: "ws-hold-1", sessionFile, cwd: directory },
    { enqueueDistill: true, leafId: "m1" },
  );
  assert.equal(released, true);
  assert.equal(reopened.listWorkspaceHolds().length, 0);
  // Idempotent second release returns false
  assert.equal(reopened.releaseWorkspaceHold({ sessionId: "ws-hold-1", sessionFile, cwd: directory }), false);
  const retentionAfterRelease = store.sourceRetentionState(sessionFile, "ws-hold-1");
  assert.equal(retentionAfterRelease.hostUse, false);
  assert.equal(retentionAfterRelease.pending, true);
  assert.equal(retentionAfterRelease.released, true, "explicit close supersedes exited owners' interruption protection");
});
