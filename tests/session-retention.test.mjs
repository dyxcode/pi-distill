import assert from 'node:assert/strict';
import { appendFileSync } from 'node:fs';
import { mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { loadSnapshot, planSources } from '../src/consolidation-source.ts';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { assessSessionRetention } from '../src/session-retention.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-retention-'));
  const db = join(root, 'memories.sqlite');
  let store = new SQLiteMemoryStore(db);
  t.after(async () => { store.close(); await rm(root, { recursive: true, force: true }); });
  const manager = SessionManager.inMemory(root);
  const file = join(root, 'session.jsonl');
  async function save() {
    await writeFile(file, [manager.getHeader(), ...manager.getEntries()].map(JSON.stringify).join('\n') + '\n');
  }
  function message(text = 'Use synthetic Cedar settings.') {
    return manager.appendMessage({ role: 'user', content: text, timestamp: 1 });
  }
  async function complete(path = file, leaf) {
    const batches = planSources(await loadSnapshot(path, leaf), new Set());
    for (const batch of batches) {
      const { evidence, context, ...job } = batch;
      store.enqueueConsolidation(job);
      assert.equal(store.claimConsolidation(job.id, 'worker', 10, 100), true);
      assert.equal(store.commitConsolidation({ jobId: job.id, owner: 'worker', now: 11, creates: [], updates: [], deletes: [] }), true);
    }
  }
  return { root, manager, file, save, message, complete, get store() { return store; },
    reopen() { store.close(); store = new SQLiteMemoryStore(db); } };
}

async function single(f, store = f.store) {
  const report = await assessSessionRetention([f.file], store);
  assert.equal(report.scope, 'provided-session-files');
  assert.equal(report.sessions.length, 1);
  return report.sessions[0];
}

test('completed processing without new memories survives reopening; assessment leaves the source and queue unchanged', async t => {
  const f = await fixture(t);
  f.message(); await f.save();
  assert.equal((await single(f)).status, 'pending');
  await f.complete();
  const before = await readFile(f.file, 'utf8');
  const result = await single(f);
  assert.equal(result.status, 'processed');
  assert.equal(result.unprocessedSources, 0);
  assert.equal(result.pendingJobs, 0);
  assert.equal('deletable' in result, false);
  f.reopen();
  assert.deepEqual(await single(f), result);
  assert.equal(await readFile(f.file, 'utf8'), before);
  assert.deepEqual(f.store.listConsolidationJobs(), []);
});

test('capture acknowledgement does not establish processing, and queued work remains pending', async t => {
  const f = await fixture(t);
  const leafId = f.message(); await f.save();
  f.store.observeSession({ sessionId: f.manager.getSessionId(), sessionFile: f.file, cwd: f.root, leafId, ownerPid: process.pid });
  f.store.acknowledgeObservation(f.manager.getSessionId(), leafId);
  assert.equal((await single(f)).status, 'pending');
  await f.complete();
  const sourceKeys = planSources(await loadSnapshot(f.file), new Set()).flatMap(x => x.sourceKeys);
  f.store.enqueueConsolidation({ id: 'retry', sessionId: f.manager.getSessionId(), sessionFile: f.file, cwd: f.root, leafId, sourceKeys });
  const result = await single(f);
  assert.equal(result.status, 'pending');
  assert.equal(result.unprocessedSources, 0);
  assert.equal(result.pendingJobs, 1);
});

test('new messages after a processed compact checkpoint require processing', async t => {
  const f = await fixture(t);
  const first = f.message();
  f.manager.appendCompaction('Synthetic compact summary.', first, 100);
  await f.save(); await f.complete();
  assert.equal((await single(f)).status, 'processed');
  f.message('A new decision after compact.'); await f.save();
  assert.equal((await single(f)).status, 'pending');
});

test('all sibling branches and resetLeaf roots require their own processing', async t => {
  const f = await fixture(t);
  const common = f.message('Shared context.');
  const left = f.message('Blue branch.');
  f.manager.branch(common);
  const right = f.message('Green branch.');
  f.manager.resetLeaf();
  const otherRoot = f.message('Independent root.');
  await f.save(); await f.complete(f.file, otherRoot);
  assert.equal((await single(f)).status, 'pending');
  await f.complete(f.file, right);
  assert.equal((await single(f)).status, 'pending');
  await f.complete(f.file, left);
  assert.equal((await single(f)).status, 'processed');
});

test('a context edit requires the effective source version, not superseded source keys', async t => {
  const f = await fixture(t);
  const first = f.message('Original decision.'); await f.save(); await f.complete();
  f.manager.appendContextEdit(first, { content: [{ type: 'text', text: 'Corrected decision.' }] });
  await f.save();
  assert.equal((await single(f)).status, 'pending');
  await f.complete();
  assert.equal((await single(f)).status, 'processed');
});

test('actual Pi fork files retain ancestor dependencies even after every session is processed', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const child = SessionManager.forkFrom(f.file, f.root, join(f.root, 'forks'));
  const childFile = child.getSessionFile();
  assert.ok(childFile);
  child.appendMessage({ role: 'user', content: 'A new fork decision.', timestamp: 2 });
  child.appendMessage({ role: 'assistant', content: [{ type: 'text', text: 'Confirmed fork decision.' }], timestamp: 3 });
  let report = await assessSessionRetention([f.file, childFile], f.store);
  assert.equal(report.dependenciesComplete, true);
  assert.equal(report.sessions[0].status, 'processed');
  assert.deepEqual(report.sessions[0].requiredBy, [childFile]);
  assert.deepEqual(report.sessions[1].dependsOn, [f.file]);
  assert.equal(report.sessions[1].status, 'pending');
  await f.complete(childFile);
  report = await assessSessionRetention([f.file, childFile], f.store);
  assert.equal(report.sessions[1].status, 'processed');
  assert.deepEqual(report.sessions[0].requiredBy, [childFile]);
  const partial = await assessSessionRetention([f.file], f.store);
  assert.equal(partial.scope, 'provided-session-files');
  assert.deepEqual(partial.sessions[0].requiredBy, []);
});

test('missing ancestors, malformed records, partial writes and cycles produce unknown', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const original = await readFile(f.file, 'utf8');
  for (const variant of [
    original + '{',
    original + '{bad}\n',
    original.replace('"type":"session"', '"parentSession":"missing.jsonl","type":"session"'),
    original.replace('"type":"session"', '"parentSession":"session.jsonl","type":"session"'),
  ]) {
    await writeFile(f.file, variant);
    const report = await assessSessionRetention([f.file], f.store);
    assert.equal(report.sessions[0].status, 'unknown');
    assert.equal(report.dependenciesComplete, false);
  }
});

test('a changed file invalidates the assessment and a fresh assessment sees the added work', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const parentId = f.manager.getLeafId();
  const entry = { type: 'message', id: 'added', parentId, timestamp: new Date(2).toISOString(), message: { role: 'user', content: 'New work.', timestamp: 2 } };
  const store = { readConsolidationStatus(keys) {
    const result = f.store.readConsolidationStatus(keys);
    if (keys.length > 0) appendFileSync(f.file, JSON.stringify(entry) + '\n');
    return result;
  } };
  assert.equal((await single(f, store)).status, 'unknown');
  assert.equal((await single(f)).status, 'pending');
});

test('empty sessions have no processing work; unavailable progress is unknown rather than complete', async t => {
  const f = await fixture(t); await f.save();
  assert.equal((await single(f)).status, 'processed');
  const unavailable = { readConsolidationStatus() { throw new Error('Synthetic database failure'); } };
  assert.equal((await single(f, unavailable)).status, 'unknown');
});

test('invalid file inputs are rejected rather than silently producing an empty assessment', async t => {
  const f = await fixture(t);
  for (const input of [null, 'session.jsonl', [''], [42], Array(1)]) {
    await assert.rejects(assessSessionRetention(input, f.store), TypeError);
  }
});

test('transitive fork dependencies include ancestors outside the supplied file list', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const child = SessionManager.forkFrom(f.file, f.root, join(f.root, 'forks'));
  const grandchild = SessionManager.forkFrom(child.getSessionFile(), f.root, join(f.root, 'forks'));
  const childFile = child.getSessionFile();
  const grandchildFile = grandchild.getSessionFile();
  const report = await assessSessionRetention([f.file, grandchildFile], f.store);
  assert.equal(report.dependenciesComplete, true);
  assert.deepEqual(report.sessions[0].requiredBy, [grandchildFile]);
  assert.deepEqual(report.sessions[1].dependsOn, [f.file, childFile].sort());
  const progress = { readConsolidationStatus(keys) {
    const result = f.store.readConsolidationStatus(keys);
    if (keys.length > 0) appendFileSync(f.file, '\n');
    return result;
  } };
  const changed = await assessSessionRetention([f.file, childFile, grandchildFile], progress);
  assert.ok(changed.sessions.every(session => session.status === 'unknown'));
  assert.equal(changed.dependenciesComplete, false);
});

test('duplicate session identities are ambiguous; repeated input paths are evaluated once', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const copied = join(f.root, 'copy.jsonl');
  await writeFile(copied, await readFile(f.file));
  const report = await assessSessionRetention([f.file, copied, f.file], f.store);
  assert.equal(report.sessions.length, 2);
  assert.ok(report.sessions.every(session => session.status === 'unknown'));
  assert.equal(report.dependenciesComplete, false);
});

test('an invalid context edit in an inactive branch is unknown even when the current branch is processed', async t => {
  const f = await fixture(t);
  const root = f.message();
  f.manager.appendContextEdit(root, { content: [{ type: 'text', text: 'Invalid edit.' }] });
  f.manager.branch(root);
  f.message('Valid current branch.');
  await f.save(); await f.complete();
  await writeFile(f.file, (await readFile(f.file, 'utf8')).replace('"targetId":"' + root + '"', '"targetId":"missing-entry"'));
  assert.equal((await single(f)).status, 'unknown');
});

test('branch expansion is bounded and returns unknown rather than a partial completion claim', async t => {
  const f = await fixture(t);
  let branchPoint;
  for (let i = 0; i < 210; i++) branchPoint = f.message('Shared prefix ' + i);
  for (let i = 0; i < 100; i++) {
    f.manager.branch(branchPoint);
    f.message('Branch ' + i);
  }
  await f.save();
  assert.equal((await single(f)).status, 'unknown');
});

test('a parent reached through a directory alias still reports its dependent fork', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const alias = join(f.root, 'alias');
  await symlink(f.root, alias, 'dir');
  const child = SessionManager.forkFrom(join(alias, 'session.jsonl'), f.root, join(f.root, 'forks'));
  const childFile = child.getSessionFile();
  const report = await assessSessionRetention([f.file, childFile], f.store);
  assert.equal(report.dependenciesComplete, true);
  assert.deepEqual(report.sessions[0].requiredBy, [childFile]);
});

test('an observed internal checkpoint needs its own source progress even if the terminal edit was processed', async t => {
  const f = await fixture(t);
  const root = f.message('Original unprocessed wording.');
  f.manager.appendContextEdit(root, { content: [{ type: 'text', text: 'Corrected wording.' }] });
  await f.save(); await f.complete();
  f.store.observeSession({ sessionId: f.manager.getSessionId(), sessionFile: f.file, cwd: f.root, leafId: root, ownerPid: process.pid });
  // Capture alone is not durable processing of this checkpoint.
  f.store.acknowledgeObservation(f.manager.getSessionId(), root);
  assert.equal((await single(f)).status, 'pending');
  await f.complete(f.file, root);
  assert.equal((await single(f)).status, 'processed');
});

test('a changed or missing observed checkpoint prevents a completion claim', async t => {
  const f = await fixture(t);
  f.message(); await f.save(); await f.complete();
  const observation = { sessionId: f.manager.getSessionId(), sessionFile: f.file, cwd: f.root, leafId: 'missing-checkpoint', ownerPid: process.pid };
  const reader = { readConsolidationStatus(keys) {
    if (keys.length > 0) f.store.observeSession(observation);
    return f.store.readConsolidationStatus(keys);
  } };
  assert.equal((await single(f, reader)).status, 'unknown');
  assert.equal((await single(f)).status, 'unknown');
});

test('one aggregate work budget covers the supplied file set', async t => {
  const f = await fixture(t);
  for (let i = 0; i < 6000; i++) f.manager.appendModelChange('synthetic', 'synthetic-model');
  await f.save();
  const secondFile = join(f.root, 'second.jsonl');
  const rows = (await readFile(f.file, 'utf8')).trimEnd().split('\n');
  const header = JSON.parse(rows[0]);
  rows[0] = JSON.stringify({ ...header, id: 'second-session' });
  await writeFile(secondFile, rows.join('\n') + '\n');
  const alone = await assessSessionRetention([secondFile], f.store);
  assert.equal(alone.sessions[0].status, 'processed');
  const together = await assessSessionRetention([f.file, secondFile], f.store);
  assert.equal(together.sessions[0].status, 'processed');
  assert.equal(together.sessions[1].status, 'unknown');
  assert.equal(together.dependenciesComplete, false);
});

test('an oversized file consumes the shared read budget instead of resetting it for the next file', async t => {
  const f = await fixture(t); await f.save();
  const large = join(f.root, 'oversized.jsonl');
  await writeFile(large, ' '.repeat(16 * 1024 * 1024 + 1));
  const report = await assessSessionRetention([large, f.file], f.store);
  assert.ok(report.sessions.every(session => session.status === 'unknown'));
});
