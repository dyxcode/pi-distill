import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { ConsolidationCoordinator } from "../../src/distill/coordinator.ts";
import { SessionUsageCoordinator } from "../../src/retention/usage.ts";
import { SQLiteMemoryStore } from "../../src/storage/store.ts";

test("unexpected crash (dead active PID) marks usage interrupted and does not distill or release for deletion", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-crash-no-distill-"));
  const databasePath = join(directory, "memory.sqlite");
  const sessionFile = join(directory, "crashed.jsonl");
  t.after(() => rm(directory, { recursive: true, force: true }));

  await writeFile(
    sessionFile,
    JSON.stringify({ type: "session", version: 3, id: "crash-s1", timestamp: "2026-11-25T00:00:00.000Z", cwd: directory }) + "\n" +
    JSON.stringify({ type: "message", id: "m1", parentId: null, timestamp: "2026-11-25T00:00:01.000Z", message: { role: "user", content: "unfinished work" } }) + "\n",
  );

  const store = new SQLiteMemoryStore(databasePath);
  t.after(() => store.close());

  // Simulate a Pi process that was active and crashed (dead PID 999999)
  store.saveSessionUsage({
    ownerId: "crashed-owner",
    processToken: "proc-crashed",
    sessionId: "crash-s1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: 999999,
    state: "active",
  });
  store.observeSession({
    sessionId: "crash-s1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: 999999,
  });

  // Reconcile dead owners: active dead PID becomes interrupted, NOT released
  store.reconcileExitedSessionOwners([sessionFile]);
  const usages = store.listSessionUsages();
  assert.equal(usages.length, 1);
  assert.equal(usages[0].state, "interrupted");
  assert.equal(store.sourceRetentionState(sessionFile, "crash-s1").released, false);

  // Starting a coordinator must NOT enqueue distillation for the crashed session
  const coordinator = new ConsolidationCoordinator({ openStore: () => new SQLiteMemoryStore(databasePath) });
  await coordinator.start({
    cwd: directory,
    sessionManager: {
      getSessionFile: () => join(directory, "other.jsonl"),
      getSessionId: () => "other-s",
      getCwd: () => directory,
      getLeafId: () => null,
    },
  });
  await coordinator.whenIdle();
  await coordinator.shutdown({
    cwd: directory,
    sessionManager: {
      getSessionFile: () => join(directory, "other.jsonl"),
      getSessionId: () => "other-s",
      getCwd: () => directory,
      getLeafId: () => null,
    },
  }, "reload");

  assert.equal(store.sourceRetentionState(sessionFile, "crash-s1").pending, false);
  assert.equal(store.sourceRetentionState(sessionFile, "crash-s1").released, false);

  // Later, user resumes and explicitly closes the session -> prior interrupted row is released
  store.saveSessionUsage({
    ownerId: "resumed-owner",
    processToken: "proc-resumed",
    sessionId: "crash-s1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: 999998,
    state: "active",
  });
  store.saveSessionUsage({
    ownerId: "resumed-owner",
    processToken: "proc-resumed",
    sessionId: "crash-s1",
    sessionFile,
    cwd: directory,
    leafId: "m1",
    ownerPid: 999998,
    state: "closing",
  });
  store.reconcileExitedSessionOwners([sessionFile]);
  assert.equal(store.sourceRetentionState(sessionFile, "crash-s1").released, true);
});

test("signal-interrupted shutdown (OS reboot / SIGTERM) marks usage interrupted and skips close distillation", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-signal-no-distill-"));
  const databasePath = join(directory, "memory.sqlite");
  const sessionFile = join(directory, "rebooted.jsonl");
  t.after(() => rm(directory, { recursive: true, force: true }));

  await writeFile(
    sessionFile,
    JSON.stringify({ type: "session", version: 3, id: "sig-s1", timestamp: "2026-11-25T00:00:00.000Z", cwd: directory }) + "\n" +
    JSON.stringify({ type: "message", id: "m1", parentId: null, timestamp: "2026-11-25T00:00:01.000Z", message: { role: "user", content: "before reboot" } }) + "\n",
  );

  const ctx = {
    cwd: directory,
    sessionManager: {
      getSessionFile: () => sessionFile,
      getSessionId: () => "sig-s1",
      getCwd: () => directory,
      getLeafId: () => "m1",
      getHeader: () => null,
    },
  };

  const usage = new SessionUsageCoordinator({
    namespace: "sig-test",
    openStore: () => new SQLiteMemoryStore(databasePath),
  });
  usage.start({ reason: "startup" }, ctx);

  const coordinator = new ConsolidationCoordinator({ openStore: () => new SQLiteMemoryStore(databasePath) });
  await coordinator.shutdown(ctx, "quit", { interrupted: true });
  usage.shutdown({ reason: "quit" }, ctx, { interrupted: true });

  const store = new SQLiteMemoryStore(databasePath);
  t.after(() => store.close());
  assert.equal(store.sourceRetentionState(sessionFile, "sig-s1").pending, false);
  assert.equal(store.sourceRetentionState(sessionFile, "sig-s1").released, false);
  assert.equal(store.listSessionUsages()[0].state, "interrupted");
});
