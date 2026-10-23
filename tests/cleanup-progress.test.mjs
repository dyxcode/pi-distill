import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { CleanupCoordinator } from '../src/cleanup.ts';
import { readSessionHeader } from '../src/session-header.ts';
import { loadSnapshot, planSources } from '../src/consolidation-source.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-clean-progress-'));
  const database = join(root, 'memory.sqlite');
  const store = new SQLiteMemoryStore(database);
  const workers = [];
  t.after(async () => {
    for (const worker of workers) worker.close();
    store.close();
    await rm(root, { recursive: true, force: true });
  });
  const worker = () => {
    const cleanup = new CleanupCoordinator({
      openStore: () => new SQLiteMemoryStore(database),
      retryDelayMs: 100_000,
    });
    workers.push(cleanup);
    return cleanup;
  };
  return { root, database, store, worker };
}

function header(sessionId, cwd, parentSession) {
  return JSON.stringify({
    type: 'session', id: sessionId, timestamp: '2026-10-23T00:00:00.000Z', cwd,
    version: 1, ...(parentSession === undefined ? {} : { parentSession }),
  }) + '\n';
}

function listedSources(store) {
  const items = [];
  let afterRowId;
  for (;;) {
    const page = store.sources.listPage({ limit: 256, ...(afterRowId === undefined ? {} : { afterRowId }) });
    items.push(...page.items);
    if (page.nextCursor === null) return items;
    afterRowId = page.nextCursor;
  }
}

test('header discovery reads and validates only a bounded first JSONL record', async t => {
  const f = await fixture(t);
  const path = join(f.root, 'header-only.jsonl');
  await writeFile(path, header('header-only', f.root, '../parent.jsonl') + '{deliberately broken body\n');

  const result = readSessionHeader(path);
  assert.equal(result.state, 'known');
  assert.equal(result.sessionId, 'header-only');
  assert.equal(result.parentSession, '../parent.jsonl');
  assert.match(result.fileVersion, /^\d+:\d+:\d+:\d+:\d+$/);

  const oversized = join(f.root, 'oversized-header.jsonl');
  await writeFile(oversized, JSON.stringify({ type: 'session', id: 'x'.repeat(70_000) }) + '\n');
  assert.equal(readSessionHeader(oversized).state, 'unknown');

  const invalidFields = join(f.root, 'invalid-header-fields.jsonl');
  for (const fields of [
    { timestamp: 'not-a-timestamp' },
    { cwd: '' },
  ]) {
    await writeFile(invalidFields, JSON.stringify({
      type: 'session', id: 'invalid-fields', timestamp: '2026-10-23T00:00:00.000Z', cwd: f.root, ...fields,
    }) + '\n');
    assert.equal(readSessionHeader(invalidFields).state, 'unknown');
  }
});

test('header discovery advances a durable 128-row cursor across restarts beyond 1024 registered sessions', async t => {
  const f = await fixture(t);
  const count = 1_025;
  for (let index = 0; index < count; index++) {
    const sessionId = `catalog-${index}`;
    const path = join(f.root, `${sessionId}.jsonl`);
    await writeFile(path, header(sessionId, f.root));
    f.store.sources.register({ sessionId, sessionFile: path, cwd: f.root });
  }

  let known = 0;
  const expectedPages = Math.ceil(count / 128);
  for (let pageIndex = 0; pageIndex < expectedPages; pageIndex++) {
    const cleanup = f.worker();
    cleanup.wake();
    await cleanup.whenIdle();
    cleanup.close();

    known = listedSources(f.store).filter(source => source.headerState === 'known').length;
    assert.equal(known, Math.min(count, (pageIndex + 1) * 128));
  }
  assert.equal(known, count);
  assert.equal(f.store.sources.isTopologyComplete(), true);
});

test('unknown headers beyond 1024 rows retain a clean target until restartable discovery finishes', async t => {
  const f = await fixture(t);
  const manager = SessionManager.inMemory(f.root);
  manager.appendMessage({ role: 'user', content: 'Synthetic progress target.', timestamp: 1 });
  const target = join(f.root, 'progress-target.jsonl');
  await writeFile(target, [manager.getHeader(), ...manager.getEntries()].map(JSON.stringify).join('\n') + '\n');

  const usage = {
    ownerId: 'progress-target-owner', processToken: 'progress-target-owner', ownerPid: process.pid,
    sessionId: manager.getSessionId(), sessionFile: target, cwd: f.root, leafId: manager.getLeafId(),
  };
  f.store.saveSessionUsage({ ...usage, state: 'active' });
  for (const { evidence, context, ...job } of planSources(await loadSnapshot(target), new Set())) {
    f.store.enqueueConsolidation(job);
    assert.equal(f.store.claimConsolidation(job.id, 'progress-test', Date.now(), 60_000), true);
    f.store.commitConsolidation({ jobId: job.id, owner: 'progress-test', now: Date.now(), creates: [], updates: [], deletes: [] });
    f.store.releaseConsolidation(job.id, 'progress-test');
  }
  f.store.saveSessionUsage({ ...usage, state: 'closing' });
  f.store.saveSessionUsage({ ...usage, state: 'released' });
  f.store.enqueueCleanTask({
    sessionId: usage.sessionId, sessionFile: target, leafId: usage.leafId,
    cwd: f.root, reason: 'close', intentId: 'progress-target-close',
  });

  const unrelatedCount = 1_025;
  for (let index = 0; index < unrelatedCount; index++) {
    const sessionId = `unknown-catalog-${index}`;
    const path = join(f.root, `${sessionId}.jsonl`);
    await writeFile(path, header(sessionId, f.root));
    f.store.sources.register({ sessionId, sessionFile: path, cwd: f.root });
  }

  const totalRows = unrelatedCount + 1;
  const expectedPasses = Math.ceil(totalRows / 128);
  for (let pass = 0; pass < expectedPasses; pass++) {
    const cleanup = f.worker();
    cleanup.wake();
    await cleanup.whenIdle();
    cleanup.close();

    if (pass < expectedPasses - 1) {
      assert.ok(await readFile(target));
      assert.equal(f.store.sources.isTopologyComplete(), false);
      continue;
    }
    await assert.rejects(readFile(target), { code: 'ENOENT' });
  }

  assert.equal(f.store.sources.isTopologyComplete(), true);
  assert.ok(listedSources(f.store).filter(source => source.sessionId.startsWith('unknown-catalog-') &&
    source.headerState === 'known').length === unrelatedCount);
});

test('clean-task pagination resumes fairly after a coordinator restart', async t => {
  const f = await fixture(t);
  const count = 11;
  for (let index = 0; index < count; index++) {
    f.store.enqueueCleanTask({
      sessionId: `missing-${index}`,
      sessionFile: join(f.root, `missing-${index}.jsonl`),
      leafId: null,
      cwd: f.root,
      reason: 'close',
      intentId: `missing-${index}`,
    });
  }

  for (let pass = 0; pass < 3; pass++) {
    const cleanup = f.worker();
    cleanup.wake();
    await cleanup.whenIdle();
    cleanup.close();
  }

  const tasks = f.store.listCleanTasks({ limit: 20 }).items;
  assert.equal(tasks.length, count);
  assert.ok(tasks.every(task => task.status === 'waiting'));
});

test('invalid parent path leaves one header unknown without pinning discovery progress', async t => {
  const f = await fixture(t);
  for (const [sessionId, parent] of [['bad-nul', 'parent\0.jsonl'], ['bad-surrogate', '\ud800'], ['valid', undefined]]) {
    const path = join(f.root, `${sessionId}.jsonl`);
    await writeFile(path, header(sessionId, f.root, parent));
    f.store.sources.register({ sessionId, sessionFile: path, cwd: f.root });
  }
  const cleanup = f.worker();
  cleanup.wake();
  await cleanup.whenIdle();
  assert.equal(f.store.sources.get(join(f.root, 'bad-nul.jsonl')).headerState, 'unknown');
  assert.equal(f.store.sources.get(join(f.root, 'bad-surrogate.jsonl')).headerState, 'unknown');
  assert.equal(f.store.sources.get(join(f.root, 'valid.jsonl')).headerState, 'known');
  assert.equal(f.store.sources.isTopologyComplete(), false);
});
