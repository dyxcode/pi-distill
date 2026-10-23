import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { SessionUsageCoordinator, createUsageProcessState } from '../../src/retention/usage.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-usage-'));
  const stores = [];
  t.after(async () => { for (const store of stores) store.close(); await rm(root, { recursive: true, force: true }); });
  const path = join(root, 'memory.sqlite');
  const state = createUsageProcessState();
  let scans = 0;
  const openStore = () => {
    const store = new SQLiteMemoryStore(path);
    const listKnownSessionFiles = store.listKnownSessionFiles.bind(store);
    store.listKnownSessionFiles = (...args) => { scans++; return listKnownSessionFiles(...args); };
    stores.push(store);
    return store;
  };
  const rows = () => { const db = openStore(); try { return db.listSessionUsages(); } finally { db.close(); } };
  const coordinator = (extra = {}) => new SessionUsageCoordinator({ openStore, namespace: path, processState: state, ...extra });
  const context = (name, parentSession) => {
    let leaf = null;
    const manager = {
      getSessionFile: () => join(root, name + '.jsonl'),
      getSessionId: () => name,
      getLeafId: () => leaf,
      getHeader: () => ({ type: 'session', id: name, timestamp: '2026-10-23T00:00:00.000Z', cwd: root,
        ...(parentSession === undefined ? {} : { parentSession }) }),
    };
    return { cwd: root, sessionManager: manager, leaf(id) { leaf = id; } };
  };
  return { root, openStore, rows, coordinator, context, get scans() { return scans; } };
}

test('startup tracks use without scanning known sessions; reload and switch preserve owner handoff', async t => {
  const f = await fixture(t); const a = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, a);
  assert.equal(f.rows()[0].state, 'active');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.scans, 0);
  assert.equal(c.settled, undefined);
  c.shutdown({ reason: 'reload' }, a);
  assert.equal(f.rows()[0].state, 'closing');
  const reload = f.coordinator(); reload.start({ reason: 'reload' }, a);
  assert.deepEqual(f.rows().map(r => r.state).sort(), ['active', 'released']);
  reload.shutdown({ reason: 'new' }, a);
  const b = f.context('b'); const next = f.coordinator();
  next.start({ reason: 'new', previousSessionFile: a.sessionManager.getSessionFile() }, b);
  assert.equal(f.scans, 0);
  assert.equal(f.rows().filter(r => r.state === 'released').length, 2);
  const other = f.coordinator(); other.start({ reason: 'startup' }, f.context('c'));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.scans, 0);
});

test('distinct owners on the same session cannot release each other; ambiguous same-process handoff retains protection', async t => {
  const f = await fixture(t); const a = f.context('shared'); const b = f.context('shared');
  const one = f.coordinator(); const two = f.coordinator();
  one.start({ reason: 'startup' }, a); two.start({ reason: 'startup' }, b);
  assert.equal(new Set(f.rows().map(r => r.ownerId)).size, 2);
  one.shutdown({ reason: 'new' }, a);
  const next = f.coordinator(); next.start({ reason: 'new', previousSessionFile: a.sessionManager.getSessionFile() }, f.context('new'));
  assert.deepEqual(f.rows().filter(r => r.sessionId === 'shared').map(r => r.state).sort(), ['active', 'closing']);
  two.observe(b); assert.equal(f.rows().filter(r => r.sessionId === 'shared' && r.state === 'active').length, 1);
});

test('quit and unsuccessful replacement leave closing claims, checkpoints persist and replay cannot revive an owner', async t => {
  const f = await fixture(t); const ctx = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, ctx);
  ctx.leaf('branch-checkpoint'); c.observe(ctx); c.shutdown({ reason: 'quit' }, ctx);
  c.observe(ctx); c.shutdown({ reason: 'quit' }, ctx);
  assert.equal(f.rows().length, 1); assert.equal(f.rows()[0].leafId, 'branch-checkpoint'); assert.equal(f.rows()[0].state, 'closing');
});

test('storage failures do not escape lifecycle handlers or allow uncertain owner handoff', async t => {
  const f = await fixture(t); const c = f.coordinator();
  const resumed = f.context('resumed');
  c.start({ reason: 'startup' }, resumed);
  assert.equal(f.rows()[0].state, 'active');
  c.shutdown({ reason: 'new' }, resumed);
  assert.equal(f.rows()[0].state, 'closing');
  const broken = f.coordinator({ openStore: () => { throw new Error('unavailable'); } });
  assert.doesNotThrow(() => broken.start({ reason: 'reload' }, f.context('other')));
  assert.doesNotThrow(() => broken.observe(f.context('other')));
  assert.doesNotThrow(() => broken.shutdown({ reason: 'quit' }, f.context('other')));
  f.coordinator().start({ reason: 'new', previousSessionFile: resumed.sessionManager.getSessionFile() }, f.context('replacement'));
  assert.equal(f.rows().find(row => row.sessionId === 'resumed').state, 'closing');
});

test('a failed reservation after usage save keeps the binding and does not create duplicate owners', async t => {
  const f = await fixture(t);
  const openStore = () => {
    const store = f.openStore();
    store.reserveSessionSource = () => { throw new Error('synthetic reservation failure'); };
    return store;
  };
  const c = f.coordinator({ openStore });
  const ctx = f.context('reservation-failure');
  c.start({ reason: 'startup' }, ctx);
  c.start({ reason: 'startup' }, ctx);
  ctx.leaf('after-reservation-failure');
  c.observe(ctx);
  assert.equal(f.rows().length, 1);
  assert.equal(f.rows()[0].state, 'active');
  assert.equal(f.rows()[0].leafId, 'after-reservation-failure');
});

test('an unsaved fork reservation records its parent dependency', async t => {
  const f = await fixture(t);
  const parent = f.context('parent');
  const fork = f.context('fork', 'parent.jsonl');
  f.coordinator().start({ reason: 'startup' }, parent);
  f.coordinator().start({ reason: 'fork', previousSessionFile: parent.sessionManager.getSessionFile() }, fork);
  const store = f.openStore();
  try {
    const childSource = store.sources.get(fork.sessionManager.getSessionFile(), 'fork');
    assert.ok(childSource?.reserved);
    assert.equal(childSource.parentPath, parent.sessionManager.getSessionFile());
    assert.ok(store.sources.reverseDependencies(parent.sessionManager.getSessionFile()).includes(fork.sessionManager.getSessionFile()));
  } finally { store.close(); }
});

test('an unrelated startup cannot confirm an old quit or switch; duplicate start delivery is idempotent', async t => {
  const f = await fixture(t); const ctx = f.context('a'); const c = f.coordinator();
  c.start({ reason: 'startup' }, ctx); c.start({ reason: 'startup' }, ctx);
  assert.equal(f.rows().length, 1);
  c.shutdown({ reason: 'new' }, ctx);
  f.coordinator().start({ reason: 'startup' }, f.context('b'));
  assert.equal(f.rows().find(r => r.sessionId === 'a').state, 'closing');
});

test('enabling an extension on reload or replacement does not masquerade as process startup', async t => {
  const f = await fixture(t);
  for (const reason of ['reload', 'new', 'resume', 'fork']) {
    const c = f.coordinator(); c.start({ reason }, f.context(reason));
  }
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.scans, 0);
});

test('checkpoint recording and lifecycle handoff do not start a retention scan', async t => {
  const f = await fixture(t); const c = f.coordinator(); const ctx = f.context('a');
  c.start({ reason: 'startup' }, ctx);
  ctx.leaf('updated'); c.observe(ctx);
  assert.equal(f.rows()[0].leafId, 'updated');
  c.shutdown({ reason: 'new' }, ctx);
  const next = f.coordinator(); next.start({ reason: 'new', previousSessionFile: ctx.sessionManager.getSessionFile() }, f.context('b'));
  assert.equal(f.rows().find(row => row.sessionId === 'a').state, 'released');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.scans, 0);
});

test('runtimes sharing the same manager cannot attribute overlapping reloads to each other', async t => {
  const f = await fixture(t); const ctx = f.context('shared-manager');
  const a = f.coordinator(); const b = f.coordinator();
  a.start({ reason: 'startup' }, ctx); b.start({ reason: 'startup' }, ctx);
  const old = f.rows().map(row => row.ownerId);
  a.shutdown({ reason: 'reload' }, ctx); b.shutdown({ reason: 'reload' }, ctx);
  f.coordinator().start({ reason: 'reload' }, ctx);
  f.coordinator().start({ reason: 'reload' }, ctx);
  assert.ok(f.rows().filter(row => old.includes(row.ownerId)).every(row => row.state === 'closing'));
  assert.equal(f.rows().filter(row => row.state === 'active').length, 2);
});
