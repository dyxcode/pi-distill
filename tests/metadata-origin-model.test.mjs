import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { CleanupCoordinator } from '../src/cleanup.ts';
import { planSources, loadSnapshot } from '../src/consolidation-source.ts';
import { consolidateWithModel } from '../src/consolidation-model.ts';
import { SQLiteMemoryStore } from '../src/memory-store.ts';

const sourceTime = '2026-10-22T10:00:00.000Z';
const correctionTime = '2026-10-23T09:00:00.000Z';
const branchAnchor = 'stable-entry-anchor';
const memoryId = 'origin-backed-memory';

function response(stopReason, content) {
  const blocks = typeof content === 'string'
    ? [{ type: 'text', text: content }]
    : Array.isArray(content) ? content : [content];
  return {
    role: 'assistant', api: 'faux', provider: 'faux', model: 'test-model', timestamp: Date.now(),
    stopReason, content: blocks,
    usage: {
      input: 1, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 11,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
}

function toolCall(name, arguments_) {
  return { type: 'toolCall', id: `call-${name}`, name, arguments: arguments_ };
}

function scriptedRegistry(responses, expectedReadDetails) {
  let index = 0;
  return {
    complete(_model, request) {
      const current = index++;
      if (current === 1 && expectedReadDetails) {
        const last = request.messages.at(-1);
        assert.equal(last.role, 'toolResult');
        const details = JSON.parse(last.content[0].text);
        for (const [key, value] of Object.entries(expectedReadDetails)) assert.equal(details[key], value);
      }
      const next = responses[current];
      if (next === undefined) throw new Error('Unexpected model call.');
      return Promise.resolve(next);
    },
  };
}

function modelContext(modelRegistry) {
  return { model: { contextWindow: 128_000, maxTokens: 8192 }, modelRegistry };
}

function attemptBatch({
  id,
  sessionId,
  cwd,
  evidenceKey,
  lineageSessionIds,
  lineageAnchors,
  sourceTime: batchTime,
}) {
  return {
    id,
    sessionId,
    sessionFile: null,
    leafId: `${id}-entry`,
    cwd,
    sourceKeys: [evidenceKey],
    evidence: [{
      key: evidenceKey,
      entryId: `${id}-entry`,
      role: 'user',
      text: 'The current synthetic evidence supports a correction.',
    }],
    context: [],
    sourceTime: batchTime,
    lineageKeys: [`${id}-lineage-key`],
    lineageSessionIds,
    ...(lineageAnchors === undefined ? {} : { lineageAnchors }),
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-metadata-origin-model-'));
  const databasePath = join(root, 'memories.sqlite');
  const sessionFile = join(root, 'source.jsonl');
  const store = new SQLiteMemoryStore(databasePath);
  let cleanup;
  t.after(async () => {
    cleanup?.close();
    store.close();
    await rm(root, { recursive: true, force: true });
  });

  const manager = SessionManager.inMemory(root);
  manager.appendMessage({ role: 'user', content: 'Synthetic source evidence for an origin lifecycle test.', timestamp: 1 });
  await writeFile(sessionFile, [manager.getHeader(), ...manager.getEntries()]
    .map(value => JSON.stringify(value)).join('\n') + '\n');
  const snapshot = await loadSnapshot(sessionFile);
  const [batch] = planSources(snapshot, new Set());
  assert.ok(batch);
  return { root, databasePath, sessionFile, manager, store, batch, setCleanup(value) { cleanup = value; } };
}

function hasJob(databasePath, jobId) {
  const database = new DatabaseSync(databasePath);
  try {
    return database.prepare('SELECT 1 AS present FROM consolidation_jobs WHERE id = ?').get(jobId) !== undefined;
  } finally {
    database.close();
  }
}

function sessionSource(f, jobId) {
  return {
    id: jobId,
    sessionId: f.batch.sessionId,
    sessionFile: f.sessionFile,
    cwd: f.root,
    leafId: f.batch.leafId,
    sourceKeys: f.batch.sourceKeys,
    lineageSessionIds: [f.batch.sessionId],
    sourceTime,
    branchAnchor,
  };
}

test('origin safety survives actual source deletion and completed-job collection', async t => {
  const f = await fixture(t);
  const job = sessionSource(f, 'origin-completed-job');
  f.store.enqueueConsolidation(job);
  assert.equal(f.store.claimConsolidation(job.id, 'origin-worker', 100, 100), true);
  assert.equal(f.store.commitConsolidation({
    jobId: job.id,
    owner: 'origin-worker',
    now: 101,
    creates: [{ id: memoryId, content: 'The current conclusion is stable.', createdAt: sourceTime }],
    updates: [],
    deletes: [],
  }), true);
  const expectedOrigin = {
    sessionId: job.sessionId,
    sourceKeys: job.sourceKeys,
    lineageSessionIds: job.lineageSessionIds,
    sourceTime: job.sourceTime,
    branchAnchor: job.branchAnchor,
  };
  assert.deepEqual(f.store.memoryOrigin(memoryId), expectedOrigin);

  const owner = {
    ownerId: 'origin-usage',
    processToken: 'origin-process',
    sessionId: job.sessionId,
    sessionFile: f.sessionFile,
    cwd: f.root,
    leafId: job.leafId,
    ownerPid: process.pid,
    state: 'active',
  };
  f.store.saveSessionUsage(owner);
  f.store.saveSessionUsage({ ...owner, state: 'closing' });
  f.store.saveSessionUsage({ ...owner, state: 'released' });
  f.store.enqueueCleanTask({
    sessionId: job.sessionId,
    sessionFile: f.sessionFile,
    cwd: f.root,
    leafId: job.leafId,
    reason: 'close',
    intentId: 'origin-cleanup',
  });

  const cleanup = new CleanupCoordinator({
    openStore: () => new SQLiteMemoryStore(f.databasePath),
    retryDelayMs: 100_000,
  });
  f.setCleanup(cleanup);
  cleanup.wake();
  await cleanup.whenIdle();
  cleanup.close();
  await assert.rejects(readFile(f.sessionFile), { code: 'ENOENT' }, 'cleanup must physically remove the processed, released source');
  assert.equal(f.store.getDeletion(f.sessionFile)?.state, 'complete');

  let collection = { hasMore: true };
  for (let pass = 0; collection.hasMore && pass < 128; pass++) {
    collection = f.store.collectMetadata(128);
  }
  assert.equal(collection.hasMore, false, 'bounded metadata passes should finish for the deleted source');
  assert.equal(hasJob(f.databasePath, job.id), false, 'completed source job metadata should be collected');
  assert.deepEqual(f.store.memoryOrigin(memoryId), expectedOrigin,
    'the origin evidence required for conflict safety survives job collection');

  const sibling = attemptBatch({
    id: 'sibling-branch',
    sessionId: 'fork-session',
    cwd: f.root,
    evidenceKey: 'sibling-evidence',
    lineageSessionIds: [job.sessionId, 'fork-session'],
    lineageAnchors: ['different-branch-anchor'],
    sourceTime: correctionTime,
  });
  for (const action of ['update', 'delete']) {
    const operation = {
      action,
      id: memoryId,
      expectedRevision: 1,
      sourceKeys: ['sibling-evidence'],
      ...(action === 'update' ? { content: 'A sibling branch must not replace this conclusion.' } : {}),
    };
    const registry = scriptedRegistry([
      response('toolUse', toolCall('memory_read', { id: memoryId, expectedRevision: 1 })),
      response('stop', JSON.stringify({ operations: [operation] })),
    ], { alternativeBranch: true });
    await assert.rejects(consolidateWithModel(sibling, modelContext(registry), f.store, new AbortController().signal));
    assert.equal(f.store.get(memoryId)?.content, 'The current conclusion is stable.');
  }

  const older = attemptBatch({
    id: 'older-source',
    sessionId: job.sessionId,
    cwd: f.root,
    evidenceKey: 'older-evidence',
    lineageSessionIds: [job.sessionId],
    lineageAnchors: [branchAnchor],
    sourceTime: '2026-10-21T10:00:00.000Z',
  });
  const olderRegistry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: memoryId, expectedRevision: 1 })),
    response('stop', JSON.stringify({ operations: [{
      action: 'update', id: memoryId, expectedRevision: 1,
      content: 'Older evidence must not restore the prior conclusion.', sourceKeys: ['older-evidence'],
    }] })),
  ], { newerEvidence: true });
  await assert.rejects(consolidateWithModel(older, modelContext(olderRegistry), f.store, new AbortController().signal));
  assert.equal(f.store.get(memoryId)?.content, 'The current conclusion is stable.');

  const corrected = attemptBatch({
    id: 'anchored-correction',
    sessionId: job.sessionId,
    cwd: f.root,
    evidenceKey: 'corr-evid-1',
    lineageSessionIds: [job.sessionId],
    lineageAnchors: [branchAnchor],
    sourceTime: correctionTime,
  });
  const correctionRegistry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: memoryId, expectedRevision: 1 })),
    response('stop', JSON.stringify({ operations: [{
      action: 'update', id: memoryId, expectedRevision: 1,
      content: 'The stable branch anchor permits this correction.', sourceKeys: ['corr-evid-1'],
    }] })),
  ]);
  const changes = await consolidateWithModel(corrected, modelContext(correctionRegistry), f.store, new AbortController().signal);
  assert.equal(changes.updates.length, 1, 'a matching stable branch anchor permits a correction despite changed source keys');
  assert.equal(changes.updates[0].content, 'The stable branch anchor permits this correction.');
  assert.equal(f.store.get(memoryId)?.content, 'The current conclusion is stable.',
    'consolidation proposes changes; the caller owns the later durable commit');
});
