import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const childPath = join(repoRoot, 'tests', 'helpers', 'adaptive-distill-pi-child.mjs');

function runChild(mode, root, sourceFile) {
  const result = spawnSync(process.execPath, [childPath, mode, root, ...(sourceFile ? [sourceFile] : [])], {
    cwd: repoRoot,
    env: {
      PATH: process.env.PATH,
      TMPDIR: root,
      PI_CODING_AGENT_DIR: join(root, 'agent'),
      PI_CODING_AGENT_SESSION_DIR: join(root, 'sessions'),
      PI_OFFLINE: '1',
      PI_SKIP_VERSION_CHECK: '1',
      PI_TELEMETRY: '0',
    },
    encoding: 'utf8',
    timeout: 45_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const output = result.stdout.trim().split('\n').at(-1);
  assert.ok(output, `the ${mode} Pi child must return a JSON result`);
  const report = JSON.parse(output);
  assert.equal(report.passed, true);
  return report;
}

test('Pi SDK restarts a bounded search/read correction and recalls it after source cleanup', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-retrieval-pi-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const interrupted = runChild('retrieval-budget', root);
  const result = runChild('retrieval-finish', root, interrupted.sourceFile);
  assert.equal(interrupted.retrievalPages + result.retrievalPages, 2);
  assert.equal(result.recalled, true);
  assert.equal(result.sourceDeleted, true);
});

test('Pi SDK resumes a large prepared range through bounded small-model prefixes and cleanup', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-adaptive-pi-'));
  t.after(() => rm(root, { recursive: true, force: true }));

  const prepared = runChild('prepare', root);
  assert.equal(prepared.modelContextWindow, 128_000);
  assert.ok(prepared.sourceFile.startsWith(`${resolve(root)}/`));
  assert.equal(prepared.jobs.length, 1, 'the large model should leave one durable evidence range');
  assert.ok(prepared.evidenceBytes >= 18 * 1024 && prepared.evidenceBytes <= 23 * 1024,
    `the durable range should be about 20 KiB, got ${prepared.evidenceBytes} bytes`);
  assert.ok(prepared.sourceKeys.length > 10);
  assert.deepEqual(prepared.processedKeys, [], 'shutdown before the first commit must preserve the full pending range');
  assert.equal(prepared.providerSignalAborted, true);

  const interrupted = runChild('partial', root, prepared.sourceFile);
  assert.equal(interrupted.jobId, prepared.jobs[0].id, 'the smaller model must reclaim the same durable job');
  assert.ok(interrupted.initialRequests.length > 0, 'the smaller model must receive an initial extraction request');
  for (const request of interrupted.initialRequests) {
    assert.equal(request.contextWindow, 10_000);
    assert.ok(request.serializedBytes <= request.inputBudget,
      `initial request used ${request.serializedBytes} bytes for a ${request.inputBudget}-byte allowance`);
  }
  assert.ok(interrupted.firstCommittedKeys.length > 0);
  assert.ok(interrupted.firstCommittedKeys.length < prepared.sourceKeys.length,
    'the first small-model portion must leave durable evidence for restart');
  assert.equal(interrupted.contenderJobId, interrupted.jobId,
    'the competing Pi runtime must share the same durable job');
  assert.ok(interrupted.firstPrefixKeys.length > 0);
  assert.ok(interrupted.contenderCommittedKeys.length > 0);
  assert.equal(interrupted.duplicateRequestsWhilePrimaryHeld, 0,
    'a second actual Pi runtime must respect the active SQLite job lease');
  assert.equal(interrupted.staleReplyDelivered, true,
    'the first runtime’s provider callback must return after shutdown');
  assert.equal(interrupted.staleMemoryAbsent, true,
    'the late provider reply must not overwrite or add memory after losing its claim');
  assert.deepEqual(new Set([...interrupted.firstPrefixKeys, ...interrupted.contenderCommittedKeys]),
    new Set(interrupted.firstCommittedKeys),
    'the first runtime and its live contender must account for every committed source key');
  assert.equal(interrupted.jobStatus, 'pending');
  assert.equal(interrupted.sourceRetained, true, 'partial progress must not authorize source cleanup');
  assert.equal(interrupted.memory.content, 'For fictional Cedar, fixture policy 000: keep local sample 000 for five days.');

  const resumed = runChild('resume-and-recall', root, prepared.sourceFile);
  assert.equal(resumed.jobId, prepared.jobs[0].id);
  assert.ok(resumed.initialRequests.length > 0);
  for (const request of resumed.initialRequests) {
    assert.equal(request.contextWindow, 10_000);
    assert.ok(request.serializedBytes <= request.inputBudget,
      `resumed initial request used ${request.serializedBytes} bytes for a ${request.inputBudget}-byte allowance`);
  }
  assert.deepEqual(resumed.finalProcessedKeys, prepared.sourceKeys,
    'restart must preserve the completed prefix and process every remaining source key');
  assert.deepEqual(resumed.reprocessedKeys, [], 'restart must not send completed evidence back to the model');
  assert.deepEqual(resumed.memory, interrupted.memory, 'later portions must preserve the committed memory');
  assert.deepEqual(resumed.memoryOrigin, interrupted.memoryOrigin,
    'cleanup and garbage collection must preserve committed-prefix provenance');
  assert.equal(resumed.sourceDeletedAfterEligibleCompletion, true,
    'eligible cleanup should remove the inactive completed source');
  assert.equal(resumed.recalledContent, interrupted.memory.content,
    'a fresh Pi session must recall the memory after its source file is deleted');
  assert.deepEqual(resumed.recallTools, ['memory_search', 'memory_read']);
});
