import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { SQLiteMemoryStore } from "../src/memory-store.ts";
import { assessSessionRetention } from "../src/session-retention.ts";

const testDir = dirname(fileURLToPath(import.meta.url));
const repoDir = dirname(testDir);
const childScript = join(testDir, "session-usage-pi-child.mjs");

function runChild(mode) {
  const result = spawnSync(process.execPath, [childScript, mode], {
    cwd: repoDir,
    encoding: "utf8",
    timeout: 20_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const report = JSON.parse(result.stdout.trim().split("\n").at(-1));
  assert.equal(report.passed, true);
  return report;
}

function openStore(root) {
  return new SQLiteMemoryStore(join(root, "agent", "distill", "memories.sqlite"));
}

function listUsages(root) {
  const store = openStore(root);
  try {
    return store.listSessionUsages();
  } finally {
    store.close();
  }
}

function assess(root, file, processToken, probeProcess) {
  const store = openStore(root);
  return assessSessionRetention([file], store, { processToken, probeProcess })
    .finally(() => store.close());
}

function launchWorker(root, sessionFile) {
  const child = spawn(process.execPath, [childScript, "worker", root, sessionFile], {
    cwd: repoDir,
    stdio: ["pipe", "pipe", "pipe"],
  });
  const messages = [];
  const waiters = [];
  let buffered = "";
  let stderr = "";

  function route(message) {
    const index = waiters.findIndex(waiter => waiter.type === message.type);
    if (index < 0) {
      messages.push(message);
      return;
    }
    const [waiter] = waiters.splice(index, 1);
    clearTimeout(waiter.timer);
    waiter.resolve(message);
  }

  child.stdout.setEncoding("utf8");
  child.stdout.on("data", chunk => {
    buffered += chunk;
    const lines = buffered.split("\n");
    buffered = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        route(JSON.parse(line));
      } catch {
        // Pi may print diagnostics during SDK startup; only structured worker messages are consumed.
      }
    }
  });
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => { stderr += chunk; });
  const exited = new Promise(resolveExit => {
    child.once("exit", (code, signal) => {
      for (const waiter of waiters.splice(0)) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error(`Pi worker exited before ${waiter.type} (code=${code}, signal=${signal}). ${stderr}`));
      }
      resolveExit({ code, signal });
    });
  });

  function next(type, timeoutMs = 15_000) {
    const index = messages.findIndex(message => message.type === type);
    if (index >= 0) return Promise.resolve(messages.splice(index, 1)[0]);
    return new Promise((resolveMessage, rejectMessage) => {
      const waiter = {
        type,
        resolve: resolveMessage,
        reject: rejectMessage,
        timer: setTimeout(() => {
          const waiterIndex = waiters.indexOf(waiter);
          if (waiterIndex >= 0) waiters.splice(waiterIndex, 1);
          rejectMessage(new Error(`Timed out waiting for worker ${type}. ${stderr}`));
        }, timeoutMs),
      };
      waiters.push(waiter);
    });
  }

  function send(command) {
    child.stdin.write(JSON.stringify({ command }) + "\n");
  }

  return { child, exited, next, send, stderr: () => stderr };
}

test("actual Pi session_start registers a not-yet-created file and assesses its live owner", () => {
  const report = runChild("initial");
  assert.equal(report.fileExistsAtStart, false);
});

test("actual Pi reload replaces its owner on the same process token", () => {
  const report = runChild("reload");
  assert.equal(report.oldOwner, "released");
  assert.equal(report.newOwner, "active");
  assert.equal(report.sameProcessToken, true);
});

test("actual Pi new, resume, and fork release only the outgoing runtime owner", () => {
  const report = runChild("switches");
  assert.equal(report.new, "released");
  assert.equal(report.resume, "released");
  assert.equal(report.fork, "released");
});

test("actual Pi tree navigation persists the current branch checkpoint", () => {
  const report = runChild("tree-checkpoint");
  assert.ok(report.currentCheckpoint);
  assert.notEqual(report.currentCheckpoint, report.previousCheckpoint);
});

test("a cancelled Pi switch preserves the same active owner", () => {
  const report = runChild("cancel");
  assert.equal(report.owner, "active");
  assert.equal(report.cancelled, true);
});

test("Pi quit marks its session usage closing before disposal", () => {
  const report = runChild("quit");
  assert.equal(report.owner, "closing");
});

test("a Pi factory failure leaves the outgoing usage closing and assessment unknown", () => {
  const report = runChild("factory-failure");
  assert.equal(report.owner, "closing");
});

test("same-process Pi runtimes have distinct owners and shared-file handoff stays closing", () => {
  const report = runChild("same-process-shared");
  assert.equal(report.ownersDiffer, true);
  assert.equal(report.sharedProcessToken, true);
  assert.equal(report.outgoingOwner, "closing");
  assert.equal(report.remainingOwner, "active");
});

test("concurrent reloads with one shared SessionManager keep both outgoing owners closing", () => {
  const report = runChild("same-manager-concurrent-reload");
  assert.deepEqual(report.outgoingOwners, ["closing", "closing"]);
  assert.deepEqual(report.replacementOwners, ["active", "active"]);
});

test("separate Pi processes release independently; a killed owner remains unknown", async t => {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-session-usage-processes-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const cwd = join(root, "project");
  const sessionsDir = join(root, "sessions");
  const manager = SessionManager.create(cwd, sessionsDir);
  manager.appendMessage({ role: "user", content: "Synthetic shared process fixture.", timestamp: 1 });
  const sessionFile = manager.getSessionFile();
  assert.ok(sessionFile);

  const first = launchWorker(root, sessionFile);
  const second = launchWorker(root, sessionFile);
  t.after(async () => {
    for (const worker of [first, second]) {
      if (worker.child.exitCode === null && worker.child.signalCode === null) {
        worker.child.kill("SIGKILL");
        await worker.exited;
      }
    }
  });

  const [firstReady, secondReady] = await Promise.all([first.next("ready"), second.next("ready")]);
  assert.equal(firstReady.sessionFile, sessionFile);
  assert.equal(secondReady.sessionFile, sessionFile);
  assert.notEqual(firstReady.pid, secondReady.pid);

  const initial = listUsages(root).filter(row => row.sessionFile === sessionFile);
  assert.equal(initial.length, 2);
  assert.ok(initial.every(row => row.state === "active"));
  assert.notEqual(initial[0].ownerId, initial[1].ownerId);
  assert.notEqual(initial[0].processToken, initial[1].processToken);
  assert.notEqual(initial[0].ownerPid, initial[1].ownerPid);

  const firstOwner = initial.find(row => row.ownerPid === firstReady.pid);
  const secondOwner = initial.find(row => row.ownerPid === secondReady.pid);
  assert.ok(firstOwner);
  assert.ok(secondOwner);
  first.send("assess");
  second.send("assess");
  const [firstAssessment, secondAssessment] = await Promise.all([first.next("assessment"), second.next("assessment")]);
  assert.equal(firstAssessment.assessmentUsage, "active");
  assert.equal(secondAssessment.assessmentUsage, "active");
  assert.equal(firstAssessment.missingLivenessUsage, "unknown");
  assert.equal(secondAssessment.missingLivenessUsage, "unknown");
  assert.equal((await assess(root, sessionFile, firstOwner.processToken, () => "alive")).sessions[0].usage, "unknown");
  assert.equal((await assess(root, sessionFile, secondOwner.processToken, () => "alive")).sessions[0].usage, "unknown");

  first.send("new");
  const replacement = await first.next("newDone");
  assert.equal(replacement.result.cancelled, false);
  const afterFirstSwitch = listUsages(root);
  assert.equal(afterFirstSwitch.find(row => row.ownerId === firstOwner.ownerId)?.state, "released");
  assert.equal(afterFirstSwitch.find(row => row.ownerId === secondOwner.ownerId)?.state, "active");
  // Another process still owns the same path, so the shared assessment remains unknown.
  assert.equal((await assess(root, sessionFile, firstOwner.processToken, () => "alive")).sessions[0].usage, "unknown");
  assert.equal((await assess(root, sessionFile, secondOwner.processToken, () => "alive")).sessions[0].usage, "unknown");

  assert.equal(second.child.kill("SIGSTOP"), true);
  await new Promise(resolve => setTimeout(resolve, 100));
  assert.equal(listUsages(root).find(row => row.ownerId === secondOwner.ownerId)?.state, "active");
  assert.equal((await assess(root, sessionFile, secondOwner.processToken, () => "alive")).sessions[0].usage, "unknown");
  assert.equal(second.child.kill("SIGCONT"), true);
  second.send("ping");
  assert.equal((await second.next("pong")).type, "pong");

  second.child.kill("SIGKILL");
  const killed = await second.exited;
  assert.equal(killed.signal, "SIGKILL", second.stderr());
  assert.equal(listUsages(root).find(row => row.ownerId === secondOwner.ownerId)?.state, "active");
  assert.equal(
    (await assess(root, sessionFile, secondOwner.processToken, pid => pid === secondOwner.ownerPid ? "dead" : "unknown")).sessions[0].usage,
    "unknown",
  );

  first.send("dispose");
  await first.next("disposed");
  const firstExit = await first.exited;
  assert.equal(firstExit.code, 0, first.stderr());
});
