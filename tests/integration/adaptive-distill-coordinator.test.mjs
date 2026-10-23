import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildSessionProjection } from '@earendil-works/pi-coding-agent';
import { fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { ConsolidationCoordinator } from '../../src/distill/coordinator.ts';
import { consolidateWithModel, fitsConsolidationRequest } from '../../src/distill/model.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-adaptive-coordinator-'));
  const path = join(root, 'memory.sqlite');
  const entries = Array.from({ length: 18 }, (_, i) => ({ type: 'message', id: `entry-${i}`,
    parentId: i === 0 ? null : `entry-${i - 1}`, timestamp: new Date(Date.UTC(2026, 9, 23, 0, i)).toISOString(),
    message: { role: 'user', content: `Cedar decision ${i}: ${'retain useful conditions '.repeat(32)}`, timestamp: i + 1 } }));
  const snapshot = { sessionId: 'adaptive', sessionFile: join(root, 'source.jsonl'), cwd: root,
    leafId: entries.at(-1).id, entries };
  await writeFile(snapshot.sessionFile, [JSON.stringify({ type: 'session', id: snapshot.sessionId, version: 3,
    cwd: root, timestamp: entries[0].timestamp }), ...entries.map(e => JSON.stringify(e))].join('\n') + '\n');
  const context = contextWindow => ({ model: { id: `synthetic-${contextWindow}`, provider: 'synthetic',
    api: 'synthetic', contextWindow, maxTokens: 4096 }, modelRegistry: {}, sessionManager: {
      getSessionId: () => snapshot.sessionId, getSessionFile: () => snapshot.sessionFile,
      getCwd: () => root, getLeafId: () => snapshot.leafId, getEntries: () => entries,
      getBranch: () => entries, buildSessionProjection: () => buildSessionProjection(entries, snapshot.leafId),
      getHeader: () => ({ type: 'session', id: snapshot.sessionId, cwd: root, timestamp: entries[0].timestamp }),
    } });
  const workers = [];
  const worker = options => {
    const c = new ConsolidationCoordinator({ openStore: () => new SQLiteMemoryStore(path),
      loadSnapshot: async () => snapshot, retryDelayMs: 100_000, ...options });
    workers.push(c); return c;
  };
  const inspect = fn => { const store = new SQLiteMemoryStore(path); try { return fn(store); } finally { store.close(); } };
  t.after(async () => { for (const c of workers) await c.shutdown(context(128000), 'reload'); await rm(root, { recursive: true, force: true }); });
  return { root, path, snapshot, context, worker, inspect };
}

async function prepare(f, contextWindow = 128000) {
  let started;
  const ready = new Promise(resolve => { started = resolve; });
  const ctx = f.context(contextWindow);
  const c = f.worker({ runModel: async (_b, _c, _s, signal) => new Promise((_, reject) => {
    started(); signal.addEventListener('abort', () => reject(new Error('stopped')), { once: true });
  }) });
  c.beforeCompact(ctx); c.afterCompact(ctx, 'compact-record');
  await ready;
  const jobs = f.inspect(store => store.listConsolidationJobs());
  await c.shutdown(ctx, 'reload');
  return jobs;
}

test('smaller-model takeover preserves committed prefixes across failure and restart and completes every source once', { timeout: 15000 }, async t => {
  const f = await fixture(t);
  const jobs = await prepare(f);
  assert.equal(jobs.length, 1, 'a large model prepares one durable evidence range');
  const originalKeys = jobs[0].sourceKeys;
  const committed = [];
  let clock = Date.now();
  let fail = false;
  const ctx = f.context(10000);
  let calls = 0;
  ctx.modelRegistry.complete = async (_model, request) => {
    calls++;
    assert.ok(Buffer.byteLength(JSON.stringify(request)) <= 5904);
    if (fail) throw new Error('synthetic temporary provider failure');
    const evidence = JSON.parse(request.messages[0].content).newEvidence;
    return fauxAssistantMessage(JSON.stringify({ operations: [{ action: 'create',
      content: `Cedar prefix ${committed.length}`, sourceKeys: [evidence[0].key] }] }));
  };
  const runModel = async (batch, context, store, signal, options) => {
    assert.ok(fitsConsolidationRequest(batch,context.model,{reserveRetrieval:true}) ||
      (batch.evidence.length===1 && batch.context.length===0 && fitsConsolidationRequest(batch,context.model)));
    const changes = await consolidateWithModel(batch, context, store, signal, options);
    committed.push({ keys: [...batch.sourceKeys], sourceTime: batch.sourceTime,
      branchAnchor: batch.branchAnchor, id: changes.creates[0].id });
    return changes;
  };
  let c = f.worker({ runModel, now: () => clock });
  await c.start(ctx); await c.whenIdle();
  assert.equal(committed.length, 1, 'the smaller model must make durable progress on the existing range');
  const first = committed[0];
  assert.ok(first.keys.length > 0 && first.keys.length < originalKeys.length);
  assert.deepEqual(first.keys, originalKeys.slice(0, first.keys.length));
  assert.equal(first.sourceTime, f.snapshot.entries[first.keys.length - 1].timestamp);
  assert.notEqual(first.branchAnchor, jobs[0].branchAnchor, 'prefix provenance must not use the unprocessed tail');
  f.inspect(store => {
    assert.deepEqual(store.processedSources(originalKeys), first.keys);
    assert.equal(store.memoryOrigin(first.id).sourceTime, first.sourceTime);
    assert.deepEqual(store.memoryOrigin(first.id).sourceKeys, first.keys);
    assert.deepEqual(store.listConsolidationJobs()[0].sourceKeys, originalKeys, 'durable range stays immutable');
    assert.equal(store.listDistillTriggers({ limit: 20 }).items[0].status, 'prepared');
  });
  fail = true;
  await c.start(ctx); await c.whenIdle();
  assert.equal(committed.length, 1);
  assert.equal(calls, 2);
  await c.shutdown(ctx, 'reload');
  c = f.worker({ runModel, now: () => clock });
  await c.start(ctx); await c.whenIdle();
  assert.equal(calls, 2, 'restart must preserve the failure cooldown on the remaining work');
  f.inspect(store => { assert.deepEqual(store.processedSources(originalKeys), first.keys); assert.ok(store.get(first.id)); });
  fail = false; clock += 30001;
  for (let i = 0; i < 30 && f.inspect(store => store.listConsolidationJobs().length); i++) {
    const before = calls; await c.start(ctx); await c.whenIdle();
    assert.ok(calls - before <= 2, 'one worker pass has bounded model work');
  }
  assert.deepEqual(committed.flatMap(group => group.keys), originalKeys, 'no successful source prefix is replayed or omitted');
  f.inspect(store => {
    assert.equal(store.listConsolidationJobs().length, 0);
    assert.equal(store.listDistillTriggers({ limit: 20 }).items[0].status, 'complete');
    for (const group of committed) assert.deepEqual(store.memoryOrigin(group.id).sourceKeys, group.keys);
  });
});

test('soft headroom falls back to one hard-fitting source part without optional context', async t => {
 const f=await fixture(t);const [job]=await prepare(f);
 const batches=[];
 const ctx=f.context(9600);
 const worker=f.worker({runModel:async batch=>{
  batches.push(batch);return {creates:[],updates:[],deletes:[]};
 }});
 await worker.start(ctx);await worker.whenIdle();
 assert.equal(batches.length,1);
 assert.equal(batches[0].evidence.length,1);assert.equal(batches[0].context.length,0);
 assert.equal(fitsConsolidationRequest(batches[0],ctx.model,{reserveRetrieval:true}),false);
 assert.equal(fitsConsolidationRequest(batches[0],ctx.model),true);
 assert.deepEqual(f.inspect(store=>store.processedSources(job.sourceKeys)),job.sourceKeys.slice(0,1));
});

test('new durable job grouping is independent of the preparing model capacity', { timeout: 15000 }, async t => {
  const f = await fixture(t);
  const large = await prepare(f);
  // A separate store prepares the identical frozen source under the smaller model.
  const otherPath = join(f.root, 'small.sqlite');
  let started;
  const ready = new Promise(resolve => { started = resolve; });
  const ctx = f.context(10000);
  const c = f.worker({ openStore: () => new SQLiteMemoryStore(otherPath), runModel: async (_b, _c, _s, signal) => new Promise((_, reject) => {
    started(); signal.addEventListener('abort', () => reject(new Error('stopped')), { once: true });
  }) });
  c.beforeCompact(ctx); c.afterCompact(ctx, 'compact-record'); await ready;
  const store = new SQLiteMemoryStore(otherPath);
  try { assert.deepEqual(store.listConsolidationJobs(), large); } finally { store.close(); }
  await c.shutdown(ctx, 'reload');
});

test('a model too small for one source part makes no provider request and preserves pending work', async t => {
  const f = await fixture(t);
  const jobs = await prepare(f);
  let calls = 0;
  const ctx = f.context(4500);
  ctx.modelRegistry.complete = async () => { calls++; return fauxAssistantMessage('{"operations":[]}'); };
  const c = f.worker({});
  await c.start(ctx); await c.whenIdle();
  assert.equal(calls, 0);
  f.inspect(store => {
    assert.deepEqual(store.listConsolidationJobs(), jobs);
    assert.deepEqual(store.processedSources(jobs[0].sourceKeys), []);
    assert.equal(store.readDistillFailure('job', jobs[0].id)?.reason, 'model_unavailable');
  });
});

test('a later tool-result overflow preserves earlier prefixes and applies the model-budget retry policy', async t => {
  const f = await fixture(t);
  const [job] = await prepare(f);
  f.inspect(store => store.save({ id: 'large-retrieval', content: 'Cedar '.repeat(900), createdAt: '2026-10-23T00:00:00.000Z' }));
  const ctx = f.context(10000);
  let calls = 0;
  let first = true;
  ctx.modelRegistry.complete = async (_model, request) => {
    calls++;
    assert.ok(Buffer.byteLength(JSON.stringify(request)) <= 5904);
    if (first) {
      first = false;
      return fauxAssistantMessage('{"operations":[]}');
    }
    const last=request.messages.at(-1);
    const page=last.role==='toolResult'?JSON.parse(last.content[0].text):{};
    return fauxAssistantMessage(fauxToolCall('memory_read', { id: 'large-retrieval', expectedRevision: 1,
      ...(page.nextCursor?{cursor:page.nextCursor}:{}) }), { stopReason: 'toolUse' });
  };
  const c = f.worker({});
  await c.start(ctx); await c.whenIdle();
  const saved = f.inspect(store => store.processedSources(job.sourceKeys));
  assert.ok(saved.length > 0 && saved.length < job.sourceKeys.length);
  await c.start(ctx); await c.whenIdle();
  assert.ok(calls >= 3, 'a bounded partial read fits, but the whole memory cannot fit the retained history');
  const callsBeforeRestart = calls;
  f.inspect(store => {
    assert.deepEqual(store.processedSources(job.sourceKeys), saved);
    assert.equal(store.readDistillFailure('job', job.id)?.reason, 'model_budget');
    assert.equal(store.listDistillTriggers({ limit: 20 }).items[0].status, 'prepared');
  });
  await c.shutdown(ctx, 'reload');
  const resumed = f.worker({});
  await resumed.start(ctx); await resumed.whenIdle();
  assert.equal(calls, callsBeforeRestart, 'a new worker cannot evade cooldown by subdividing remaining evidence again');
});

test('each prefix uses its supporting context edits without inheriting another prefix source time', async t => {
  const f = await fixture(t);
  const editedAt = '2026-10-23T00:30:00.000Z';
  f.snapshot.entries.push({ type: 'context_edit', id: 'edit-first', parentId: f.snapshot.leafId,
    timestamp: editedAt, targetId: 'entry-0', replacement: { content: [{ type: 'text',
      text: `Corrected Cedar decision: ${'retain edited conditions '.repeat(32)}` }] } });
  f.snapshot.leafId = 'edit-first';
  const [job] = await prepare(f);
  assert.equal(job.sourceTime, editedAt);
  const batches = [];
  const c = f.worker({ runModel: async batch => {
    const id = `edited-prefix-${batches.length}`;
    batches.push({ ...batch, memoryId: id });
    return { creates: [{ id, content: 'Cedar retains its conditions.', createdAt: editedAt }], updates: [], deletes: [] };
  } });
  const ctx = f.context(10000);
  await c.start(ctx); await c.whenIdle();
  await c.start(ctx); await c.whenIdle();
  assert.equal(batches.length, 2);
  assert.ok(batches[0].evidence[0].text.startsWith('Corrected Cedar decision:'));
  assert.equal(batches[0].sourceTime, editedAt);
  assert.ok(batches[1].sourceTime < editedAt, 'later source keys do not inherit the first entry correction time');
  assert.notEqual(batches[1].branchAnchor, batches[0].branchAnchor);
  f.inspect(store => {
    for (const batch of batches) {
      const origin = store.memoryOrigin(batch.memoryId);
      assert.deepEqual(origin.sourceKeys, batch.sourceKeys);
      assert.equal(origin.sourceTime, batch.sourceTime);
      assert.equal(origin.branchAnchor, batch.branchAnchor);
    }
  });
});
