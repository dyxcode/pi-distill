import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { ConsolidationCoordinator } from '../src/consolidation.ts';
import { planSources } from '../src/consolidation-source.ts';
import { SourceWorkPending } from '../src/source-view.ts';
const at = '2026-10-23T00:00:00.000Z', empty = { creates: [], updates: [], deletes: [] };
async function fixture(t, run, options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-scheduling-')), db = join(root, 'memory.sqlite'), sources = new Map(), workers = [];
  const store = new SQLiteMemoryStore(db);
  const ctx = { model: { id: 'synthetic', contextWindow: 10000, maxTokens: 4096 }, modelRegistry: {}, sessionManager: { getSessionId: () => 'live', getSessionFile: () => undefined, getCwd: () => root, getLeafId: () => null } };
  const source = async (id, long = false) => {
    const file = join(root, `${id}.jsonl`);
    await writeFile(file, 'synthetic source');
    const entries = Array.from({ length: long ? 18 : 1 }, (_, i) => ({ type: 'message', id: `u${i}`, parentId: i ? `u${i - 1}` : null, timestamp: at,
      message: { role: 'user', content: long ? `Decision ${i}: ${'A conditional synthetic fact. '.repeat(32)}` : 'One synthetic fact.', timestamp: i + 1 } }));
    const s = { sessionId: id, sessionFile: file, cwd: root, leafId: entries.at(-1).id, entries };
    sources.set(file, s);
    return s;
  };
  const trigger = s => store.enqueueDistillTrigger({ id: `trigger-${s.sessionId}`, sessionId: s.sessionId, sessionFile: s.sessionFile, leafId: s.leafId, cwd: s.cwd, reason: 'close' });
  const jobs = s => { const result = planSources(s, new Set()).map(({ evidence, context, ...job }) => job); for (const job of result)
    store.enqueueConsolidation(job); return result; };
  const worker = (extra = {}) => { const c = new ConsolidationCoordinator({ openStore: () => new SQLiteMemoryStore(db), loadSnapshot: async (path) => sources.get(path), runModel: run, retryDelayMs: 100000, ...options, ...extra }); workers.push(c); return c; };
  const wake = async (c) => { await c.start(ctx); await c.whenIdle(); };
  t.after(async () => { for (const c of workers)
    await c.shutdown(ctx, 'reload'); store.close(); await rm(root, { recursive: true, force: true }); });
  return { root, db, store, ctx, source, trigger, jobs, worker, wake, sources };
}
test('a short session gets a turn while an earlier source still has projection pages, including after worker restart', async (t) => {
  const seen = [];
  const f = await fixture(t, async (batch) => { seen.push(batch.sessionId); return empty; });
  const a = await f.source('A'), b = await f.source('B');
  f.trigger(a);
  f.trigger(b);
  let pages = 0;
  const loader = async (path) => { if (path === a.sessionFile) {
    pages++;
    throw new SourceWorkPending();
  } return f.sources.get(path); };
  let c = f.worker({ loadSnapshot: loader });
  await f.wake(c);
  await c.shutdown(f.ctx, 'reload');
  c = f.worker({ loadSnapshot: loader });
  for (let i = 0; i < 3 && !seen.includes('B'); i++)
    await f.wake(c);
  assert.ok(seen.includes('B'));
  assert.ok(pages > 0);
  assert.equal(f.store.readDistillTrigger('trigger-A').status, 'pending');
});
test('partial model jobs rotate across sessions instead of repeatedly serving the first pair', async (t) => {
  const seen = [];
  const f = await fixture(t, async (batch) => { seen.push(batch.sessionId); return empty; });
  for (const id of ['A', 'B', 'C'])
    f.jobs(await f.source(id, true));
  const c = f.worker();
  for (let i = 0; i < 3; i++)
    await f.wake(c);
  assert.ok(seen.slice(0, 3).includes('C'), `initial turns: ${seen}`);
  assert.ok(f.store.listConsolidationJobs().some(job => job.sessionId === 'A'), 'A should still have work when C gets a turn');
});
test('a prepared job on cooldown blocks its preparation without rereading the source', async (t) => {
  const reads = [];
  const f = await fixture(t, async () => empty);
  const a = await f.source('A'), b = await f.source('B');
  f.trigger(a);
  const job = f.jobs(a)[0];
  const claim = f.store.claimDistillTrigger({ id: 'trigger-A', workerId: 'setup', now: 1, leaseMs: 100 });
  f.store.prepareDistillPage({ id: 'trigger-A', claimToken: claim.claimToken, now: 2, viewId: 'a'.repeat(64), after: 0, cursor: 1, complete: false, jobs: [job] });
  const work = f.store.claimDistillJob({ id: job.id, workerId: 'setup', now: 3, leaseMs: 100 });
  f.store.releaseDistillClaim({ claimToken: work.claimToken, now: 4, retry: { category: 'transient', retryAt: Date.now() + 100000 } });
  f.trigger(b);
  const c = f.worker({ loadSnapshot: async (path) => { reads.push(path); return f.sources.get(path); } });
  await f.wake(c);
  await f.wake(c);
  assert.ok(!reads.includes(a.sessionFile));
  assert.equal(f.store.readDistillTrigger('trigger-B').status, 'complete');
});
test('a successful partial commit continues after yielding without a new host event', async (t) => {
  let calls = 0;
  const f = await fixture(t, async () => { calls++; return empty; });
  f.jobs(await f.source('A', true));
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const c = f.worker({ retryDelayMs: undefined });
  await f.wake(c);
  const initial = calls;
  assert.ok(initial > 0);
  t.mock.timers.tick(100);
  await c.whenIdle();
  assert.ok(calls > initial, 'ready partial work should continue on the short scheduling wake');
});
test('shared scheduling rejects stale selections and gives a session one place despite multiple triggers', async (t) => {
  const f = await fixture(t, async () => empty);
  const a = await f.source('A'), b = await f.source('B');
  f.trigger(a);
  f.trigger(b);
  for (let i = 0; i < 5; i++)
    f.store.enqueueDistillTrigger({ id: `extra-${i}`, sessionId: a.sessionId, sessionFile: a.sessionFile, leafId: a.leafId, cwd: a.cwd, reason: 'close' });
  const page = f.store.listScheduledDistill(10, true);
  assert.deepEqual(page.items.map(x => x.sessionId), ['A', 'B']);
  const request = { workerId: 'one', now: 10, leaseMs: 10 };
  const first = f.store.claimScheduledDistill(page.items[0], request);
  assert.equal(first.kind, 'trigger');
  const other = new SQLiteMemoryStore(f.db);
  try {
    assert.equal(other.claimScheduledDistill(page.items[1], { ...request, workerId: 'two' }).kind, 'busy');
    assert.equal(other.claimScheduledDistill(page.items[0], { ...request, now: 21 }).kind, 'changed');
    assert.equal(other.listScheduledDistill(21, true).items[0].sessionId, 'B');
    const next = other.claimScheduledDistill(page.items[1], { ...request, workerId: 'two', now: 21 });
    assert.equal(next.kind, 'trigger');
    assert.throws(() => f.store.prepareDistillTrigger({ id: 'trigger-A', claimToken: first.claim.claimToken, now: 22, jobs: [] }));
  }
  finally {
    other.close();
  }
});
test('newly eligible sessions join behind ready work without bypassing their cooldown', async (t) => {
  const f = await fixture(t, async () => empty);
  f.jobs(await f.source('A'));
  f.jobs(await f.source('B'));
  const [a, b] = f.store.listScheduledDistill(1, true).items;
  const claim = f.store.claimScheduledDistill(a, { workerId: 'one', now: 1, leaseMs: 100 });
  assert.equal(claim.kind, 'job');
  f.store.releaseDistillClaim({ claimToken: claim.claim.claimToken, now: 2, retry: { category: 'transient', retryAt: 1000 } });
  f.store.settleScheduledDistill(a.sessionId, claim.position, undefined, 2);
  assert.deepEqual(f.store.listScheduledDistill(3).items.map(x => x.sessionId), ['B']);
  const waiting = f.store.listScheduledDistill(1000).items.find(x => x.sessionId === 'A');
  assert.equal(f.store.claimScheduledDistill(waiting, { workerId: 'one', now: 1000, leaseMs: 100 }).kind, 'ready');
  assert.equal(f.store.listScheduledDistill(1000).items[0].sessionId, b.sessionId);
});
test('bounded scans pass held sessions beyond one page despite continual arrivals and restarts', async (t) => {
  const seen = [];
  const f = await fixture(t, async (batch) => { seen.push(batch.sessionId); return empty; });
  for (let i = 0; i < 140; i++) {
    const id = `held-${i}`;
    const s = { sessionId: id, sessionFile: join(f.root, `${id}.jsonl`), cwd: f.root, leafId: 'leaf' };
    f.trigger(s);
    const claim = f.store.claimDistillTrigger({ id: `trigger-${id}`, workerId: 'setup', now: 1, leaseMs: 10 });
    f.store.releaseDistillClaim({ claimToken: claim.claimToken, now: 2, retry: { category: 'transient', retryAt: Date.now() + 100000 } });
  }
  f.trigger(await f.source('ready'));
  for (let i = 0; i < 6 && !seen.includes('ready'); i++) {
    f.trigger(await f.source(`arrival-${i}`));
    const c = f.worker();
    await f.wake(c);
    await c.shutdown(f.ctx, 'reload');
  }
  assert.ok(seen.includes('ready'), 'old ready work cannot be hidden forever by the first held page');
});
test('all source prefixes eventually commit once while a short session finishes before the long session', async (t) => {
  const seen = [];
  const f = await fixture(t, async (batch) => { seen.push({ session: batch.sessionId, keys: batch.sourceKeys }); return empty; });
  const a = await f.source('long', true), b = await f.source('short');
  f.trigger(a);
  f.trigger(b);
  let c = f.worker();
  for (let i = 0; i < 40; i++) {
    await f.wake(c);
    if (i === 2) {
      await c.shutdown(f.ctx, 'reload');
      c = f.worker();
    }
    if (f.store.readDistillTrigger('trigger-long').status === 'complete' && f.store.readDistillTrigger('trigger-short').status === 'complete')
      break;
  }
  const aKeys = planSources(a, new Set()).flatMap(x => x.sourceKeys), bKeys = planSources(b, new Set()).flatMap(x => x.sourceKeys);
  assert.deepEqual(seen.filter(x => x.session === 'long').flatMap(x => x.keys), aKeys);
  assert.deepEqual(seen.filter(x => x.session === 'short').flatMap(x => x.keys), bKeys);
  assert.ok(seen.findIndex(x => x.session === 'short') < seen.findLastIndex(x => x.session === 'long'));
  assert.equal(f.store.readDistillTrigger('trigger-long').status, 'complete');
});
test('schema 11 upgrade preserves memories, progress and pending work', async (t) => {
  const f = await fixture(t, async () => empty);
  const a = await f.source('migration', true);
  const jobs = f.jobs(a), job = jobs[0];
  const claim = f.store.claimDistillJob({ id: job.id, workerId: 'old', now: 1, leaseMs: 10 });
  const memory = { id: 'retained', content: 'Synthetic retained policy.', createdAt: at };
  f.store.commitConsolidation({ jobId: job.id, owner: claim.claimToken, now: 2, sourceKeys: [job.sourceKeys[0]], evidence: { sourceTime: at, branchAnchor: `${a.sessionId}:u0` }, creates: [memory], updates: [], deletes: [] });
  const origin = f.store.memoryOrigin(memory.id);
  f.store.close();
  const { DatabaseSync } = await import('node:sqlite');
  const { DISTILL_SCHEDULE_SCHEMAS } = await import('../src/distill-schedule.ts');
  const raw = new DatabaseSync(f.db);
  for (const schema of [...DISTILL_SCHEDULE_SCHEMAS].reverse())
    raw.exec(`DROP ${schema.sql.split(' ')[1]} ${schema.name}`);
  raw.exec('PRAGMA user_version=11');
  raw.close();
  const current = new SQLiteMemoryStore(f.db);
  try {
    assert.equal(current.get(memory.id).content, memory.content);
    assert.deepEqual(current.memoryOrigin(memory.id), origin);
    assert.deepEqual(current.processedSources(job.sourceKeys), [job.sourceKeys[0]]);
    const page = current.listScheduledDistill(10);
    assert.equal(page.items[0].id, job.id);
    const resumed = current.claimScheduledDistill(page.items[0], { workerId: 'new', now: 10, leaseMs: 100 });
    assert.equal(resumed.kind, 'job');
    assert.deepEqual(resumed.claim.job.sourceKeys, job.sourceKeys);
  }
  finally {
    current.close();
  }
});

test('rotation preserves embedded-NUL identities and idle groups are reclaimed before new work rejoins', async t => {
  const f = await fixture(t, async () => empty);
  const sessionId = 'session\0suffix';
  const source = { sessionId, sessionFile: join(f.root, 'nul.jsonl'), cwd: f.root, leafId: 'leaf', reason: 'close' };
  f.store.enqueueDistillTrigger({ ...source, id: 'trigger\0one' });
  const before = f.store.retentionVersion([source.sessionFile]);
  const [candidate] = f.store.listScheduledDistill(1).items;
  assert.equal(candidate.sessionId, sessionId);
  assert.equal(candidate.id, 'trigger\0one');
  assert.equal(f.store.retentionVersion([source.sessionFile]), before, 'rotation bookkeeping is not new source activity');
  const claim = f.store.claimScheduledDistill(candidate, { workerId: 'one', now: 1, leaseMs: 100 });
  assert.equal(claim.kind, 'trigger');
  f.store.prepareDistillTrigger({ id: candidate.id, claimToken: claim.claim.claimToken, now: 2, jobs: [] });
  assert.deepEqual(f.store.listScheduledDistill(3).items, []);
  f.trigger(await f.source('already-ready'));
  f.store.enqueueDistillTrigger({ ...source, id: 'trigger\0two' });
  assert.deepEqual(f.store.listScheduledDistill(4).items.map(x => x.sessionId), ['already-ready', sessionId]);
});

test('a cooled linked job does not schedule rapid no-progress source or model retries', async t => {
  let calls = 0, reads = 0;
  let now = Date.now();
  const f = await fixture(t, async () => { calls++; throw new Error('synthetic transient outage'); });
  const a = await f.source('cooldown'); f.trigger(a);
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const c = f.worker({ retryDelayMs: undefined, now: () => now, loadSnapshot: async path => { reads++; return f.sources.get(path); } });
  await f.wake(c); assert.equal(calls, 1);
  const initialReads = reads;
  for (let i = 0; i < 20; i++) { now += 100; t.mock.timers.tick(100); await c.whenIdle(); }
  assert.equal(calls, 1); assert.equal(reads, initialReads);
  now += 28000; t.mock.timers.tick(28000); await c.whenIdle();
  assert.equal(calls, 2, 'the existing transient retry deadline still permits the next attempt');
});

test('migration backfill resumes after reopen and eventually exposes work beyond its conversion budget', async t => {
  const f = await fixture(t, async () => empty);
  for (let i = 0; i < 260; i++) f.trigger({ sessionId: `old-${i}`, sessionFile: join(f.root, `${i}.jsonl`), cwd: f.root, leafId: 'leaf' });
  f.store.close();
  const { DatabaseSync } = await import('node:sqlite');
  const { DISTILL_SCHEDULE_SCHEMAS } = await import('../src/distill-schedule.ts');
  const raw = new DatabaseSync(f.db);
  for (const schema of [...DISTILL_SCHEDULE_SCHEMAS].reverse()) raw.exec(`DROP ${schema.sql.split(' ')[1]} ${schema.name}`);
  raw.exec('PRAGMA user_version=11'); raw.close();
  let store = new SQLiteMemoryStore(f.db);
  try {
    const first = store.listScheduledDistill(1);
    assert.equal(first.backfilled, false, 'one wake must not convert all old work');
    assert.ok(first.items.length <= 32);
    store.close(); store = new SQLiteMemoryStore(f.db);
    assert.equal(store.listScheduledDistill(1).backfilled, false);
    assert.equal(store.listScheduledDistill(1).backfilled, true);
    const seen = new Set();
    for (let turn = 0; turn < 10; turn++) {
      for (const candidate of store.listScheduledDistill(10).items) {
        assert.ok(!seen.has(candidate.id)); seen.add(candidate.id);
        const claim = store.claimScheduledDistill(candidate, { workerId: 'new', now: 10, leaseMs: 100 });
        assert.equal(claim.kind, 'trigger');
        store.prepareDistillTrigger({ id: candidate.id, claimToken: claim.claim.claimToken, now: 11, jobs: [] });
      }
    }
    assert.equal(seen.size, 260);
    assert.deepEqual(store.listScheduledDistill(12).items, []);
  } finally { store.close(); }
});
