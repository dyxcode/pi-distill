import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(dirname(fileURLToPath(import.meta.url))));

test("the model runner reports missing credentials as an unsuccessful run without provider prompts", async (t) => {
  const agentDir = await mkdtemp(join(tmpdir(), "pi-distill-model-runner-"));
  t.after(() => rm(agentDir, { recursive: true, force: true }));
  const result = spawnSync(process.execPath, [join(root, "tests", 'model', "memory-model-eval.mjs")], {
    cwd: root,
    env: {
      PATH: process.env.PATH,
      PI_CODING_AGENT_DIR: agentDir,
      PI_OFFLINE: "1",
      PI_TELEMETRY: "0",
      PI_SKIP_VERSION_CHECK: "1",
    },
    encoding: "utf8",
    timeout: 20_000,
  });
  assert.ifError(result.error);
  const report = JSON.parse(result.stdout.trim().split("\n").at(-1));
  assert.equal(report.status, "not_run");
  assert.equal(report.providerPromptsSent, 0);
  assert.deepEqual(report.cases, []);
  assert.equal(result.status, 1, "A skipped real-model evaluation must not look successful to the caller");
});

for (const caseId of ["lifecycle", "cancellation", "cancellation-compact", "cleanup"]) {
  test(`the ${caseId} runner fails without OAuth and preserves an inspectable report without model calls`, async t => {
    const agentDir = await mkdtemp(join(tmpdir(), "pi-distill-lifecycle-preflight-"));
    t.after(() => rm(agentDir, { recursive: true, force: true }));
    const result = spawnSync(process.execPath, [join(root, "tests", 'model', "consolidation-model-eval.mjs"), "--case", caseId], {
      cwd: root,
      env: { PATH: process.env.PATH, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1", PI_TELEMETRY: "0", PI_SKIP_VERSION_CHECK: "1" },
      encoding: "utf8", timeout: 20_000,
    });
    assert.ifError(result.error);
    const report = JSON.parse(result.stdout.trim().split("\n").at(-1));
    t.after(() => rm(dirname(report.reportPath), { recursive: true, force: true }));
    assert.equal(result.status, 1);
    assert.equal(report.status, "not_run");
    assert.equal(report.case, caseId);
    assert.equal(report.providerRequests, 0);
    assert.deepEqual(report.stages, []);
  });
}
