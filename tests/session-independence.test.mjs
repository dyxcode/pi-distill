import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const childPath = join(repoRoot, 'tests', 'session-independence-child.mjs');

test('a durable memory remains searchable and correctable after its source session is deleted', async t => {
  const fixtureRoot = await mkdtemp(join(tmpdir(), 'pi-distill-session-independence-'));
  t.after(() => rm(fixtureRoot, { recursive: true, force: true }));
  const agentDir = join(fixtureRoot, 'agent');
  const sessionDir = join(fixtureRoot, 'sessions');
  const cwd = join(fixtureRoot, 'project');
  const extensionPath = join(repoRoot, 'src', 'index.ts');
  const result = spawnSync(process.execPath, [childPath, fixtureRoot, agentDir, sessionDir, cwd, extensionPath], {
    cwd: fixtureRoot,
    env: {
      PATH: process.env.PATH,
      TMPDIR: fixtureRoot,
      PI_CODING_AGENT_DIR: agentDir,
      PI_CODING_AGENT_SESSION_DIR: sessionDir,
      PI_OFFLINE: '1',
      PI_SKIP_VERSION_CHECK: '1',
      PI_TELEMETRY: '0',
    },
    encoding: 'utf8',
    timeout: 30000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const output = result.stdout.trim().split('\n').at(-1);
  assert.ok(output, 'the isolated Pi child must return a JSON result');
  const report = JSON.parse(output);
  assert.equal(report.passed, true);
});
