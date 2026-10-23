import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { unlinkSync } from 'node:fs';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { CleanupCoordinator } from '../../src/retention/coordinator.ts';
import { loadSnapshot } from '../../src/sources/snapshot.ts';
import { planSources } from '../../src/distill/preparation.ts';

test('startup maintenance collects completed work with an empty clean queue and keeps recall intact', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-maintenance-'));
  const database = join(root, 'memory.sqlite');
  const file = join(root, 'source.jsonl');
  const store = new SQLiteMemoryStore(database);
  const workers = [];
  t.after(async () => {
    for (const worker of workers) worker.close();
    store.close();
    await rm(root, { recursive: true, force: true });
  });
  const manager = SessionManager.inMemory(root);
  manager.appendMessage({ role: 'user', content: 'Synthetic Birch requires local verification.', timestamp: 1 });
  await writeFile(file, [manager.getHeader(), ...manager.getEntries()].map(JSON.stringify).join('\n') + '\n');
  const usage = { ownerId: 'owner', processToken: 'token', ownerPid: process.pid,
    sessionId: manager.getSessionId(), sessionFile: file, cwd: root, leafId: manager.getLeafId() };
  store.saveSessionUsage({ ...usage, state: 'active' });
  const batches = planSources(await loadSnapshot(file), new Set());
  for (const { evidence, context, ...job } of batches) {
    store.enqueueConsolidation(job);
    assert.equal(store.claimConsolidation(job.id, 'worker', 100, 100), true);
    store.commitConsolidation({ jobId: job.id, owner: 'worker', now: 101,
      creates: [{ id: 'birch', content: 'Birch requires local verification.', createdAt: '2026-10-23T00:00:00.000Z' }],
      updates: [], deletes: [] });
  }
  store.saveSessionUsage({ ...usage, state: 'closing' });
  store.saveSessionUsage({ ...usage, state: 'released' });
  const original = store.get('birch');
  const origin = store.memoryOrigin('birch');
  const task = store.listCleanTasks({ limit: 10 }).items[0];
  const access = store.acquireSourceAccess();
  assert.ok(access);
  assert.equal(store.claimCleanTask(task.id, 'delete-worker', Date.now(), 60_000), true);
  assert.equal(store.prepareDeletion(task.id, 'delete-worker', access, store.retentionVersion(), '1:2:3:4:5'), true);
  assert.equal(store.finishDeletion(task.id, 'delete-worker', access, store.retentionVersion(), () => {
    unlinkSync(file); return true;
  }), true);
  store.releaseSourceAccess(access);
  await assert.rejects(readFile(file), { code: 'ENOENT' });
  assert.deepEqual(store.listCleanTasks({ limit: 10 }).items, []);

  // A new maintenance owner must work even when deletion left no clean tasks.
  const restarted = new CleanupCoordinator({ openStore: () => new SQLiteMemoryStore(database) });
  workers.push(restarted);
  for (let pass = 0; pass < 4; pass++) { restarted.wake(); await restarted.whenIdle(); }
  const raw = new DatabaseSync(database);
  try {
    assert.equal(raw.prepare('SELECT count(*) AS count FROM consolidation_jobs').get().count, 0);
  } finally { raw.close(); }
  assert.deepEqual(store.get('birch'), original);
  assert.deepEqual(store.memoryOrigin('birch'), origin);
  assert.equal(store.searchBatch({ terms: ['Birch'], match: 'any' })[0].memory.id, 'birch');
  assert.deepEqual(store.processedSources(batches.flatMap(batch => batch.sourceKeys)), batches.flatMap(batch => batch.sourceKeys));
  assert.equal(store.getDeletion(file).state, 'complete');
});
