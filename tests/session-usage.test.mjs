import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { SessionUsageCoordinator, createUsageProcessState } from '../src/session-usage.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-usage-'));
  const stores = [];
  t.after(async () => { for (const store of stores) store.close(); await rm(root, { recursive: true, force: true }); });
  const path = join(root, 'memory.sqlite');
  const state = createUsageProcessState();
  let calls = 0;
  const openStore = () => { const store = new SQLiteMemoryStore(path); stores.push(store); return store; };
  const rows = () => { const db = openStore(); try { return db.listSessionUsages(); } finally { db.close(); } };
  const assess = async files => { calls++; assert.ok(rows().some(r => r.state === 'active')); return { scope: 'provided-session-files', dependenciesComplete: true, sessions: [], files }; };
  const coordinator = (extra = {}) => new SessionUsageCoordinator({ openStore, namespace: path, processState: state, assess, ...extra });
  const context = (name) => {
    let leaf = null;
    const manager = { getSessionFile: () => join(root, name + '.jsonl'), getSessionId: () => name, getLeafId: () => leaf };
    return { cwd: root, sessionManager: manager, leaf(id) { leaf = id; } };
  };
  return { root, openStore, rows, coordinator, context, get calls() { return calls; } };
}

test('register before one background assessment; reload, switch and extra runtime starts do not repeat it', async t => {
  const f = await fixture(t); const a = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, a);
  assert.equal(f.calls, 0, 'assessment must be background work');
  assert.equal(f.rows()[0].state, 'active');
  await c.settled(); assert.equal(f.calls, 1);
  c.shutdown({ reason: 'reload' }, a);
  assert.equal(f.rows()[0].state, 'closing');
  const reload = f.coordinator(); reload.start({ reason: 'reload' }, a); await reload.settled();
  assert.deepEqual(f.rows().map(r => r.state).sort(), ['active', 'released']);
  reload.shutdown({ reason: 'new' }, a);
  const b = f.context('b'); const next = f.coordinator();
  next.start({ reason: 'new', previousSessionFile: a.sessionManager.getSessionFile() }, b);
  await next.settled(); assert.equal(f.calls, 1);
  assert.equal(f.rows().filter(r => r.state === 'released').length, 2);
  const other = f.coordinator(); other.start({ reason: 'startup' }, f.context('c')); await other.settled();
  assert.equal(f.calls, 1);
});

test('distinct owners on the same session cannot release each other; ambiguous same-process handoff retains protection', async t => {
  const f = await fixture(t); const a = f.context('shared'); const b = f.context('shared');
  const one = f.coordinator(); const two = f.coordinator();
  one.start({ reason: 'startup' }, a); two.start({ reason: 'startup' }, b);
  await one.settled();
  assert.equal(new Set(f.rows().map(r => r.ownerId)).size, 2);
  one.shutdown({ reason: 'new' }, a);
  const next = f.coordinator(); next.start({ reason: 'new', previousSessionFile: a.sessionManager.getSessionFile() }, f.context('new'));
  assert.deepEqual(f.rows().filter(r => r.sessionId === 'shared').map(r => r.state).sort(), ['active', 'closing']);
  two.observe(b); assert.equal(f.rows().filter(r => r.sessionId === 'shared' && r.state === 'active').length, 1);
});

test('quit and unsuccessful replacement leave closing claims, checkpoints persist and replay cannot revive an owner', async t => {
  const f = await fixture(t); const ctx = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, ctx); await c.settled();
  ctx.leaf('branch-checkpoint'); c.observe(ctx); c.shutdown({ reason: 'quit' }, ctx);
  c.observe(ctx); c.shutdown({ reason: 'quit' }, ctx);
  assert.equal(f.rows().length, 1); assert.equal(f.rows()[0].leafId, 'branch-checkpoint'); assert.equal(f.rows()[0].state, 'closing');
});

test('startup resume registers protection; storage and assessment failures do not escape lifecycle handlers', async t => {
  const f = await fixture(t); const c = f.coordinator({ assess: async () => { throw new Error('synthetic failure'); } });
  c.start({ reason: 'startup' }, f.context('resumed')); assert.equal(await c.settled(), undefined);
  assert.equal(f.rows()[0].state, 'active');
  const broken = f.coordinator({ openStore: () => { throw new Error('unavailable'); } });
  assert.doesNotThrow(() => broken.start({ reason: 'reload' }, f.context('other')));
  assert.doesNotThrow(() => broken.observe(f.context('other')));
  assert.doesNotThrow(() => broken.shutdown({ reason: 'quit' }, f.context('other')));
});

test('an unrelated startup cannot confirm an old quit or switch; duplicate start delivery is idempotent', async t => {
  const f = await fixture(t); const ctx = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, ctx); c.start({ reason: 'startup' }, ctx); await c.settled();
  assert.equal(f.rows().length, 1);
  c.shutdown({ reason: 'new' }, ctx);
  f.coordinator().start({ reason: 'startup' }, f.context('b'));
  assert.equal(f.rows().find(r => r.sessionId === 'a').state, 'closing');
});

test('enabling an extension on reload or replacement does not masquerade as process startup', async t => {
  const f = await fixture(t);
  for (const reason of ['reload', 'new', 'resume', 'fork']) {
    const c = f.coordinator(); c.start({ reason }, f.context(reason)); await c.settled();
  }
  assert.equal(f.calls, 0);
});

test('a pending startup pass does not block checkpoint recording or lifecycle handoff', async t => {
  const f = await fixture(t); let finish;
  const wait = new Promise(resolve => { finish = resolve; });
  const c = f.coordinator({ assess: async () => wait }); const ctx = f.context('a');
  c.start({ reason: 'startup' }, ctx);
  await new Promise(resolve => setImmediate(resolve));
  ctx.leaf('updated'); c.observe(ctx);
  assert.equal(f.rows()[0].leafId, 'updated');
  c.shutdown({ reason: 'new' }, ctx);
  const next = f.coordinator(); next.start({ reason: 'new', previousSessionFile: ctx.sessionManager.getSessionFile() }, f.context('b'));
  assert.equal(f.rows().find(row => row.sessionId === 'a').state, 'released');
  finish({ scope: 'provided-session-files', dependenciesComplete: false, sessions: [] });
  assert.equal((await c.settled()).dependenciesComplete, false);
});

test('startup labels a saturated candidate batch as incomplete known-file discovery', async t => {
  const f = await fixture(t); const db = f.openStore();
  try {
    for (let i = 0; i < 1024; i++) db.saveSessionUsage({ ownerId: 'owner-' + i, processToken: 'prior', state: 'active', sessionId: 'session-' + i, sessionFile: join(f.root, i + '.jsonl'), cwd: f.root, leafId: null, ownerPid: process.pid });
  } finally { db.close(); }
  const c = f.coordinator(); c.start({ reason: 'startup' }, f.context('current'));
  const report = await c.settled();
  assert.equal(report.discoveryScope, 'known-session-files');
  assert.equal(report.candidateLimitReached, true);
  assert.equal(report.files.length, 1024);
});

test('runtimes sharing the same manager cannot attribute overlapping reloads to each other', async t => {
  const f = await fixture(t); const ctx = f.context('shared-manager');
  const a = f.coordinator(); const b = f.coordinator();
  a.start({ reason: 'startup' }, ctx); b.start({ reason: 'startup' }, ctx); await a.settled();
  const old = f.rows().map(row => row.ownerId);
  a.shutdown({ reason: 'reload' }, ctx); b.shutdown({ reason: 'reload' }, ctx);
  f.coordinator().start({ reason: 'reload' }, ctx);
  f.coordinator().start({ reason: 'reload' }, ctx);
  assert.ok(f.rows().filter(row => old.includes(row.ownerId)).every(row => row.state === 'closing'));
  assert.equal(f.rows().filter(row => row.state === 'active').length, 2);
});
