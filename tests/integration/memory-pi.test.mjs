import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const childPath = join(repoRoot, "tests", 'helpers', "memory-pi-child.mjs");
const extensionPath = join(repoRoot, "src", "index.ts");

async function run(mode, t) {
  const temporaryRoot = await mkdtemp(join(tmpdir(), "pi-distill-memory-pi-"));
  t.after(async () => rm(temporaryRoot, { recursive: true, force: true }));
  const agentDir = join(temporaryRoot, "personal-agent");
  const cwd = join(temporaryRoot, "unrelated-project");
  const result = spawnSync(process.execPath, [childPath, mode, temporaryRoot, agentDir, cwd, extensionPath], {
    cwd: temporaryRoot,
    env: {
      PATH: process.env.PATH,
      PI_CODING_AGENT_DIR: agentDir,
      PI_CODING_AGENT_SESSION_DIR: join(temporaryRoot, "sessions"),
      PI_OFFLINE: "1",
      PI_SKIP_VERSION_CHECK: "1",
      PI_TELEMETRY: "0",
    },
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const output = result.stdout.trim().split("\n").at(-1);
  assert.ok(output, "the isolated Pi process must return a JSON result");
  return JSON.parse(output);
}

test("Pi SDK loads the read-only memory tools and guidance without opening storage", async (t) => {
  const result = await run("load-only", t);

  assert.deepEqual(result.extensionErrors, []);
  assert.deepEqual(result.toolNames, ["memory_read", "memory_search"]);
  assert.deepEqual(result.schemas.memory_search, ["cursor", "limit", "match", "terms"]);
  assert.deepEqual(result.schemas.memory_read, ["cursor", "expectedRevision", "id"]);
  assert.match(result.prompt, /memory_search/);
  assert.match(result.prompt, /memory_read/);
  assert.match(result.prompt, /budget_exhausted/);
  assert.match(result.prompt, /unavailable/);
  assert.equal(result.storageExists, false, "registration alone must not create a personal store");
  assert.equal(result.searchRejectsUnknown, true);
  assert.equal(result.searchTermMaxLength, 128);
});

test("Pi tool dispatch rejects unknown memory search fields", async (t) => {
  const result = await run("strict-schema", t);

  assert.equal(result.toolResultIsError, true);
  assert.equal(result.storageExists, false, "Pi must reject unknown fields before the extension opens storage");
});

test("the Pi agent loop reformulates search, reads evidence, reloads, and reopens isolated storage", async (t) => {
  const result = await run("dispatch", t);

  assert.deepEqual(result.toolNames, ["memory_read", "memory_search"]);
  assert.deepEqual(result.calls, ["memory_search", "memory_search", "memory_read", "memory_search"]);
  assert.equal(result.searchStatuses[0], "ok");
  assert.equal(result.unicodeSearchStatus, "ok", "128 astral codepoints must survive Pi schema validation");
  assert.deepEqual(result.candidateIds, ["synthetic-pref"]);
  assert.equal(result.readContent, "For this synthetic demo, two short check-ins per week fit better than a daily standup.");
  assert.equal(result.reloadedToolCount, 2);
  assert.equal(result.storePathIsIsolated, true);
  assert.equal(result.providerCalls, 6, "scripted calls must travel through Pi's provider and tool loop");
});

test("agent_start resets adapter run budgets after an exhausted run", async (t) => {
  const result = await run("budget", t);

  assert.equal(result.toolCalls, 14);
  assert.equal(result.firstRunRefusal, "budget_exhausted");
  assert.equal(result.secondRunStatus, "ok");
  assert.equal(result.providerCalls, 16);
});

test('real Pi automatically records a storage failure and subsequent recovery without prompt content',async t=>{
  const result=await run('diagnostics',t);
  assert.deepEqual(result.statuses,['unavailable','ok']);
  const records=result.records.filter(event=>event.operation==='memory.search');
  assert.deepEqual(records.map(event=>event.outcome),['failed','recovered']);
  assert.equal(records[0].errorCode,'ERR_SQLITE_ERROR');
  assert.equal(records[0].piVersion,'1.3.0');assert.equal(records[0].extensionVersion,JSON.parse(await readFile(join(repoRoot,'package.json'),'utf8')).version);
  assert.ok(!JSON.stringify(result.records).includes('private synthetic words'));
});
