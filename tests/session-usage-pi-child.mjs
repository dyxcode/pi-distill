import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";

const [mode, sharedRoot, sharedSessionFile] = process.argv.slice(2);
assert.ok(mode);
const root = sharedRoot ?? await mkdtemp(join(tmpdir(), "pi-distill-session-usage-sdk-"));
const agentDir = join(root, "agent");
const sessionsDir = join(root, "sessions");
const cwd = join(root, "project");
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_CODING_AGENT_SESSION_DIR = sessionsDir;
process.env.PI_OFFLINE = "1";
process.env.PI_SKIP_VERSION_CHECK = "1";
process.env.PI_TELEMETRY = "0";
await Promise.all([
  mkdir(agentDir, { recursive: true }),
  mkdir(join(agentDir, "distill"), { recursive: true }),
  mkdir(sessionsDir, { recursive: true }),
  mkdir(cwd, { recursive: true }),
]);

const [pi, { SQLiteMemoryStore }, { assessSessionRetention }] = await Promise.all([
  import("@earendil-works/pi-coding-agent"),
  import("../src/memory-store.ts"),
  import("../src/session-retention.ts"),
]);

const storePath = join(agentDir, "distill", "memories.sqlite");
let failNextFactory = false;
const extensionPaths = [resolve("src/index.ts")];
if (mode === "cancel") {
  const cancelExtensionPath = join(root, "cancel-session-switch.mjs");
  await writeFile(cancelExtensionPath,
    'export default function cancelSessionSwitch(pi) { pi.on("session_before_switch", () => ({ cancel: true })); }\n');
  extensionPaths.push(cancelExtensionPath);
}

function readUsages() {
  const store = new SQLiteMemoryStore(storePath);
  try {
    return store.listSessionUsages();
  } finally {
    store.close();
  }
}

function findUsage(ownerId) {
  const row = readUsages().find(usage => usage.ownerId === ownerId);
  assert.ok(row, `missing usage owner ${ownerId}`);
  return row;
}

async function assess(file, processToken, probeProcess = pid => pid === process.pid ? "alive" : "unknown") {
  const store = new SQLiteMemoryStore(storePath);
  try {
    return await assessSessionRetention([file], store, {
      processToken,
      probeProcess,
    });
  } finally {
    store.close();
  }
}

async function createRuntime(sessionManager) {
  const settingsManager = pi.SettingsManager.inMemory();
  const factory = async options => {
    if (failNextFactory) {
      failNextFactory = false;
      throw new Error("Synthetic session runtime factory failure.");
    }
    const services = await pi.createAgentSessionServices({
      cwd: options.cwd,
      agentDir,
      settingsManager,
      resourceLoaderOptions: {
        additionalExtensionPaths: extensionPaths,
        noSkills: true,
        noPromptTemplates: true,
        noThemes: true,
        noContextFiles: true,
      },
    });
    const created = await pi.createAgentSessionFromServices({
      services,
      sessionManager: options.sessionManager,
      noTools: "builtin",
      sessionStartEvent: options.sessionStartEvent,
    });
    assert.deepEqual(created.extensionsResult.errors, []);
    await created.session.bindExtensions({ mode: "print", onError: error => { throw new Error(error.error); } });
    return { ...created, services, diagnostics: services.diagnostics };
  };
  return pi.createAgentSessionRuntime(factory, { cwd, agentDir, sessionManager });
}

function activeSession(runtime) {
  const manager = runtime.session.sessionManager;
  const sessionFile = manager.getSessionFile();
  assert.ok(sessionFile);
  const usage = readUsages().find(row => row.ownerPid === process.pid && row.sessionId === manager.getSessionId() &&
    row.sessionFile === sessionFile && row.state === "active");
  assert.ok(usage, `session_start did not register ${sessionFile}`);
  return { manager, sessionFile, usage };
}

async function seedSession() {
  const manager = pi.SessionManager.create(cwd, sessionsDir);
  manager.appendMessage({ role: "user", content: "Synthetic session-usage fixture.", timestamp: 1 });
  const sessionFile = manager.getSessionFile();
  assert.ok(sessionFile);
  return { manager, sessionFile };
}

async function runWorker() {
  assert.ok(sharedSessionFile);
  const runtime = await createRuntime(pi.SessionManager.open(sharedSessionFile, sessionsDir, cwd));
  const current = activeSession(runtime);
  process.stdout.write(JSON.stringify({ type: "ready", pid: process.pid, sessionFile: current.sessionFile }) + "\n");
  const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of input) {
    let request;
    try {
      request = JSON.parse(line);
    } catch {
      continue;
    }
    if (request?.command === "assess") {
      const assessmentUsage = (await assess(current.sessionFile, current.usage.processToken)).sessions[0].usage;
      const missingLivenessUsage = (await assess(current.sessionFile, current.usage.processToken, () => "unknown")).sessions[0].usage;
      assert.equal(assessmentUsage, "active");
      assert.equal(missingLivenessUsage, "unknown");
      process.stdout.write(JSON.stringify({ type: "assessment", assessmentUsage, missingLivenessUsage }) + "\n");
    } else if (request?.command === "new") {
      const result = await runtime.newSession();
      process.stdout.write(JSON.stringify({ type: "newDone", result, sessionFile: runtime.session.sessionManager.getSessionFile() }) + "\n");
    } else if (request?.command === "ping") {
      process.stdout.write(JSON.stringify({ type: "pong" }) + "\n");
    } else if (request?.command === "dispose") {
      await runtime.dispose();
      process.stdout.write(JSON.stringify({ type: "disposed" }) + "\n");
      process.exit(0);
    }
  }
}

if (mode === "worker") {
  await runWorker();
} else {
  const sessionManager = pi.SessionManager.create(cwd, sessionsDir);
  let runtime;
  const otherRuntimes = [];
  try {
    runtime = await createRuntime(sessionManager);
    const initial = activeSession(runtime);

    if (mode === "initial") {
      assert.equal(existsSync(initial.sessionFile), false, "Pi should not create a transcript file before the first message");
      assert.equal(initial.usage.ownerPid, process.pid);
      assert.equal((await assess(initial.sessionFile, initial.usage.processToken)).sessions[0].usage, "unknown");
      console.log(JSON.stringify({ mode, registered: true, fileExistsAtStart: false, passed: true }));
    } else if (mode === "reload") {
      await runtime.session.reload();
      const rows = readUsages().filter(row => row.sessionFile === initial.sessionFile && row.processToken === initial.usage.processToken);
      assert.equal(rows.length, 2);
      assert.equal(findUsage(initial.usage.ownerId).state, "released");
      const replacement = rows.find(row => row.ownerId !== initial.usage.ownerId);
      assert.ok(replacement);
      assert.equal(replacement.state, "active");
      assert.equal(replacement.processToken, initial.usage.processToken);
      console.log(JSON.stringify({ mode, oldOwner: findUsage(initial.usage.ownerId).state, newOwner: replacement.state, sameProcessToken: true, passed: true }));
    } else if (mode === "switches") {
      initial.manager.appendMessage({ role: "user", content: "Synthetic message to enable fork.", timestamp: 2 });
      assert.equal(existsSync(initial.sessionFile), true);

      const newResult = await runtime.newSession();
      assert.equal(newResult.cancelled, false);
      const afterNew = activeSession(runtime);
      assert.equal(findUsage(initial.usage.ownerId).state, "released");
      assert.equal((await assess(initial.sessionFile, initial.usage.processToken)).sessions[0].usage, "released");
      assert.notEqual(afterNew.usage.ownerId, initial.usage.ownerId);
      assert.equal(afterNew.usage.processToken, initial.usage.processToken);

      const target = await seedSession();
      const resumeResult = await runtime.switchSession(target.sessionFile);
      assert.equal(resumeResult.cancelled, false);
      const afterResume = activeSession(runtime);
      assert.equal(findUsage(afterNew.usage.ownerId).state, "released");
      assert.equal(afterResume.sessionFile, target.sessionFile);
      assert.equal(afterResume.usage.processToken, initial.usage.processToken);

      const userEntry = afterResume.manager.getBranch().find(entry => entry.type === "message" && entry.message.role === "user");
      assert.ok(userEntry);
      const forkResult = await runtime.fork(userEntry.id, { position: "at" });
      assert.equal(forkResult.cancelled, false);
      const afterFork = activeSession(runtime);
      assert.equal(findUsage(afterResume.usage.ownerId).state, "released");
      assert.equal((await assess(target.sessionFile, initial.usage.processToken)).sessions[0].usage, "released");
      assert.notEqual(afterFork.sessionFile, target.sessionFile);
      assert.equal(afterFork.usage.processToken, initial.usage.processToken);
      console.log(JSON.stringify({ mode, new: "released", resume: "released", fork: "released", passed: true }));
    } else if (mode === "tree-checkpoint") {
      const firstMessage = initial.manager.appendMessage({ role: "user", content: "Synthetic tree root.", timestamp: 4 });
      const assistantMessage = initial.manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "Synthetic branch point." }], timestamp: 5 });
      initial.manager.appendMessage({ role: "user", content: "Synthetic later branch.", timestamp: 6 });
      const before = findUsage(initial.usage.ownerId);
      const result = await runtime.session.navigateTree(assistantMessage, { summarize: false });
      assert.equal(result.cancelled, false);
      assert.equal(initial.manager.getLeafId(), assistantMessage);
      const after = findUsage(initial.usage.ownerId);
      assert.equal(after.sessionFile, initial.sessionFile);
      assert.equal(after.leafId, assistantMessage);
      assert.notEqual(after.leafId, before.leafId);
      assert.notEqual(firstMessage, assistantMessage);
      console.log(JSON.stringify({ mode, previousCheckpoint: before.leafId, currentCheckpoint: after.leafId, passed: true }));
    } else if (mode === "cancel") {
      const result = await runtime.newSession();
      assert.equal(result.cancelled, true);
      const after = activeSession(runtime);
      assert.equal(after.usage.ownerId, initial.usage.ownerId);
      assert.equal(after.usage.state, "active");
      assert.equal(readUsages().length, 1);
      console.log(JSON.stringify({ mode, owner: after.usage.state, cancelled: true, passed: true }));
    } else if (mode === "quit") {
      await runtime.dispose();
      runtime = undefined;
      assert.equal(findUsage(initial.usage.ownerId).state, "closing");
      assert.equal((await assess(initial.sessionFile, initial.usage.processToken)).sessions[0].usage, "unknown");
      console.log(JSON.stringify({ mode, owner: "closing", passed: true }));
    } else if (mode === "factory-failure") {
      failNextFactory = true;
      await assert.rejects(runtime.newSession(), /Synthetic session runtime factory failure/u);
      assert.equal(findUsage(initial.usage.ownerId).state, "closing");
      assert.equal((await assess(initial.sessionFile, initial.usage.processToken)).sessions[0].usage, "unknown");
      console.log(JSON.stringify({ mode, owner: "closing", passed: true }));
      runtime = undefined;
    } else if (mode === "same-process-shared") {
      initial.manager.appendMessage({ role: "user", content: "Synthetic shared-file fixture.", timestamp: 3 });
      const ownersBeforeSecondRuntime = readUsages().map(row => row.ownerId);
      const secondRuntime = await createRuntime(pi.SessionManager.open(initial.sessionFile, sessionsDir, cwd));
      otherRuntimes.push(secondRuntime);
      const secondManager = secondRuntime.session.sessionManager;
      const second = readUsages().find(row => !ownersBeforeSecondRuntime.includes(row.ownerId) &&
        row.sessionId === secondManager.getSessionId() && row.sessionFile === secondManager.getSessionFile());
      assert.ok(second, JSON.stringify({ ownersBeforeSecondRuntime, rowsAfterSecondRuntime: readUsages() }));
      assert.notEqual(second.ownerId, initial.usage.ownerId);
      assert.equal(second.ownerPid, process.pid);
      assert.equal(second.processToken, initial.usage.processToken);

      await runtime.newSession();
      assert.equal(findUsage(initial.usage.ownerId).state, "closing");
      assert.equal(findUsage(second.ownerId).state, "active");
      console.log(JSON.stringify({ mode, ownersDiffer: true, sharedProcessToken: true, outgoingOwner: "closing", remainingOwner: "active", passed: true }));
    } else if (mode === "same-manager-concurrent-reload") {
      initial.manager.appendMessage({ role: "user", content: "Synthetic shared-manager fixture.", timestamp: 7 });
      const ownersBeforeSecondRuntime = readUsages().map(row => row.ownerId);
      const secondRuntime = await createRuntime(initial.manager);
      otherRuntimes.push(secondRuntime);
      assert.equal(secondRuntime.session.sessionManager, initial.manager, "both SDK runtimes must share the exact SessionManager object");
      const secondOwner = readUsages().find(row => !ownersBeforeSecondRuntime.includes(row.ownerId) &&
        row.sessionFile === initial.sessionFile && row.sessionId === initial.manager.getSessionId());
      assert.ok(secondOwner, JSON.stringify({ ownersBeforeSecondRuntime, rows: readUsages() }));
      assert.notEqual(secondOwner.ownerId, initial.usage.ownerId);
      assert.equal(secondOwner.processToken, initial.usage.processToken);

      await Promise.all([runtime.session.reload(), secondRuntime.session.reload()]);

      const rows = readUsages().filter(row => row.sessionFile === initial.sessionFile && row.processToken === initial.usage.processToken);
      const oldOwnerIds = new Set([initial.usage.ownerId, secondOwner.ownerId]);
      const outgoing = rows.filter(row => oldOwnerIds.has(row.ownerId));
      const replacements = rows.filter(row => !oldOwnerIds.has(row.ownerId));
      assert.equal(rows.length, 4, JSON.stringify(rows));
      assert.equal(outgoing.length, 2);
      assert.ok(outgoing.every(row => row.state === "closing"), JSON.stringify(outgoing));
      assert.equal(replacements.length, 2, JSON.stringify(replacements));
      assert.ok(replacements.every(row => row.state === "active"), JSON.stringify(replacements));
      assert.notEqual(replacements[0].ownerId, replacements[1].ownerId);
      console.log(JSON.stringify({ mode, outgoingOwners: outgoing.map(row => row.state), replacementOwners: replacements.map(row => row.state), passed: true }));
    } else {
      throw new Error(`Unsupported mode: ${mode}`);
    }
  } finally {
    for (const other of otherRuntimes) await other.dispose();
    if (runtime) await runtime.dispose();
    if (!sharedRoot) await rm(root, { recursive: true, force: true });
  }
}
