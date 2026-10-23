import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fauxAssistantMessage } from '@earendil-works/pi-ai';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { ConsolidationCoordinator } from '../src/consolidation.ts';
import { consolidateWithModel } from '../src/consolidation-model.ts';

const stamp = '2026-10-22T00:00:00.000Z';

function makeSnapshot(root) {
  return {
    sessionId: 'runtime-session',
    sessionFile: join(root, 'runtime-session.jsonl'),
    cwd: root,
    leafId: 'assistant-1',
    entries: [
      { type: 'message', id: 'user-1', parentId: null, timestamp: stamp,
        message: { role: 'user', content: 'For synthetic Cedar, use local tests.', timestamp: 1 } },
      { type: 'message', id: 'assistant-1', parentId: 'user-1', timestamp: stamp,
        message: { role: 'assistant', content: [{ type: 'text', text: 'Cedar uses local tests.' }], timestamp: 2 } },
    ],
  };
}

function makeContext(snapshot, { getEntries = () => snapshot.entries, idle = () => true } = {}) {
  return {
    model: { id: 'synthetic', contextWindow: 32_768, maxTokens: 4_096 },
    modelRegistry: {},
    isIdle: idle,
    hasPendingMessages: () => false,
    sessionManager: {
      getSessionId: () => snapshot.sessionId,
      getSessionFile: () => snapshot.sessionFile,
      getCwd: () => snapshot.cwd,
      getLeafId: () => snapshot.leafId,
      getEntries,
      getHeader: () => ({ type: 'session', id: snapshot.sessionId, cwd: snapshot.cwd, timestamp: stamp }),
      getBranch: () => snapshot.entries,
      buildSessionProjection: () => ({ messages: [] }),
    },
  };
}

async function fixture(t, options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-task-runtime-'));
  const path = join(root, 'memory.sqlite');
  const source = makeSnapshot(root);
  await writeFile(source.sessionFile, 'synthetic persisted session');
  const ctx = makeContext(source);
  const coordinator = new ConsolidationCoordinator({
    openStore: () => new SQLiteMemoryStore(path),
    loadSnapshot: async (_path, leaf) => {
      assert.equal(leaf, source.leafId);
      return source;
    },
    retryDelayMs: 100_000,
    ...options,
  });
  t.after(async () => {
    await coordinator.shutdown(ctx, 'reload');
    await rm(root, { recursive: true, force: true });
  });
  return { root, path, source, ctx, coordinator };
}

test('lifecycle hooks persist only a compact boundary and defer source reads and model calls while Pi is busy', async t => {
  let calls = 0;
  let sourceReads = 0;
  const f = await fixture(t, {
    loadSnapshot: async (_path, leaf) => {
      sourceReads++;
      assert.equal(leaf, f.source.leafId);
      return f.source;
    },
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  let historyReads = 0;
  f.ctx.sessionManager.getEntries = () => { historyReads++; throw new Error('lifecycle hook read full history'); };

  f.coordinator.setForegroundBusy(true);
  f.coordinator.beforeCompact(f.ctx);
  f.coordinator.afterCompact(f.ctx, 'compact-record-1');
  await new Promise(resolve => setTimeout(resolve, 20));

  assert.equal(historyReads, 0);
  assert.equal(sourceReads, 0);
  assert.equal(calls, 0);
  const queued = new SQLiteMemoryStore(f.path);
  const triggers = queued.listDistillTriggers({ limit: 20 }).items;
  assert.equal(triggers.length, 1);
  assert.equal(triggers[0].reason, 'compact');
  assert.equal(triggers[0].status, 'pending');
  assert.equal(triggers[0].compactionRecordId, 'compact-record-1');
  assert.equal(triggers[0].leafId, 'assistant-1');
  assert.equal(Object.hasOwn(triggers[0], 'transcript'), false);
  queued.close();

  f.coordinator.setForegroundBusy(false);
  await f.coordinator.whenIdle();
  assert.ok(sourceReads > 0);
  assert.equal(calls, 1);
});

test('session close records the frozen source reference and returns without waiting for a model request', async t => {
  let calls = 0;
  const f = await fixture(t, {
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  let historyReads = 0;
  f.ctx.sessionManager.getEntries = () => { historyReads++; throw new Error('shutdown read full history'); };
  f.coordinator.setForegroundBusy(true);

  const started = Date.now();
  await f.coordinator.shutdown(f.ctx, 'quit');

  assert.ok(Date.now() - started < 500);
  assert.equal(historyReads, 0);
  assert.equal(calls, 0);
  const store = new SQLiteMemoryStore(f.path);
  const triggers = store.listDistillTriggers({ limit: 20 }).items;
  assert.equal(triggers.length, 1);
  assert.equal(triggers[0].reason, 'close');
  assert.equal(triggers[0].status, 'pending');
  assert.equal(triggers[0].leafId, 'assistant-1');
  store.close();
});

test('a pending close trigger runs after reload even when its source session is now current', async t => {
  const f = await fixture(t, {
    runModel: async () => ({ creates: [], updates: [], deletes: [] }),
  });
  f.coordinator.setForegroundBusy(true);
  await f.coordinator.shutdown(f.ctx, 'quit');

  let calls = 0;
  const recovered = new ConsolidationCoordinator({
    openStore: () => new SQLiteMemoryStore(f.path),
    loadSnapshot: async () => f.source,
    runModel: async () => {
      calls++;
      return { creates: [{ id: 'recovered', content: 'Cedar uses local tests.', createdAt: stamp }], updates: [], deletes: [] };
    },
    retryDelayMs: 100_000,
  });
  await recovered.start(f.ctx);
  await recovered.whenIdle();
  await recovered.shutdown(f.ctx, 'reload');

  const store = new SQLiteMemoryStore(f.path);
  assert.equal(calls, 1);
  assert.equal(store.get('recovered')?.content, 'Cedar uses local tests.');
  assert.equal(store.listConsolidationJobs().length, 0);
  store.close();
});

test('a later pending trigger is found even when the first trigger is already complete', async t => {
  let calls = 0;
  let f;
  let emptySnapshot;
  const fTemp = await fixture(t, {
    loadSnapshot: async (path, leaf) => path === emptySnapshot.sessionFile ? emptySnapshot : f.source,
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
    retryDelayMs: 10,
  });
  f = fTemp;
  const emptyFile = join(f.root, 'empty-session.jsonl');
  await writeFile(emptyFile, 'synthetic empty session');
  emptySnapshot = {
    sessionId: 'empty-session', sessionFile: emptyFile, cwd: f.root, leafId: null, entries: [],
  };
  const store = new SQLiteMemoryStore(f.path);
  store.enqueueDistillTrigger({
    id: 'complete-first', sessionId: emptySnapshot.sessionId, sessionFile: emptyFile,
    leafId: null, cwd: f.root, reason: 'close',
  });
  store.enqueueDistillTrigger({
    id: 'pending-second', sessionId: f.source.sessionId, sessionFile: f.source.sessionFile,
    leafId: f.source.leafId, cwd: f.source.cwd, reason: 'close',
  });
  store.close();

  await f.coordinator.start(f.ctx);
  for (let attempt = 0; attempt < 50 && calls === 0; attempt++) await new Promise(resolve => setTimeout(resolve, 10));
  await f.coordinator.whenIdle();

  const after = new SQLiteMemoryStore(f.path);
  const triggers = after.listDistillTriggers({ limit: 20 }).items;
  assert.equal(calls, 1);
  assert.equal(triggers.find(item => item.id === 'complete-first')?.status, 'complete');
  assert.equal(triggers.find(item => item.id === 'pending-second')?.status, 'complete');
  after.close();
});

test('an invalid or over-budget source snapshot stays pending and never completes an empty range', async t => {
  let calls = 0;
  let f;
  const fTemp = await fixture(t, {
    loadSnapshot: async () => ({ ...f.source, leafId: 'missing-leaf', entries: [] }),
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  f = fTemp;
  const store = new SQLiteMemoryStore(f.path);
  store.enqueueDistillTrigger({
    id: 'invalid-boundary', sessionId: f.source.sessionId, sessionFile: f.source.sessionFile,
    leafId: 'missing-leaf', cwd: f.source.cwd, reason: 'close',
  });
  store.close();

  await f.coordinator.start(f.ctx);
  await f.coordinator.whenIdle();

  const after = new SQLiteMemoryStore(f.path);
  assert.equal(after.listDistillTriggers({ limit: 20 }).items[0]?.status, 'pending');
  assert.deepEqual(after.listConsolidationJobs(), []);
  assert.deepEqual(after.listCleanTasks({ limit: 20 }).items, []);
  assert.equal(calls, 0);
  after.close();
});

test('startup reconciles a persisted compaction even when the post-compact hook was missed', async t => {
  let f;
  let calls = 0;
  const fTemp = await fixture(t, {
    loadSnapshot: async () => f.source,
    loadCompactionRecord: async (path, sessionId, cwd, leafId) => {
      assert.equal(path, f.source.sessionFile);
      assert.equal(sessionId, f.source.sessionId);
      assert.equal(cwd, f.source.cwd);
      assert.equal(leafId, f.source.leafId);
      return 'saved-compaction-record';
    },
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  f = fTemp;
  const store = new SQLiteMemoryStore(f.path);
  store.enqueueDistillTrigger({
    id: 'missed-after-compact', sessionId: f.source.sessionId, sessionFile: f.source.sessionFile,
    leafId: f.source.leafId, cwd: f.source.cwd, reason: 'compact',
  });
  store.close();

  await f.coordinator.start(f.ctx);
  await f.coordinator.whenIdle();

  const after = new SQLiteMemoryStore(f.path);
  const trigger = after.listDistillTriggers({ limit: 20 }).items[0];
  assert.equal(trigger?.status, 'complete');
  assert.equal(trigger?.compactionRecordId, 'saved-compaction-record');
  assert.equal(calls, 1);
  after.close();
});

test('dead-source recovery gets its own bounded page even behind many completed triggers', async t => {
  let f;
  let releaseSource;
  let resolveStarted;
  const sourceStarted = new Promise(resolve => { resolveStarted = resolve; });
  const sourceGate = new Promise(resolve => { releaseSource = resolve; });
  let calls = 0;
  const fTemp = await fixture(t, {
    loadSnapshot: async path => {
      if (path === join(f.root, 'closed-source.jsonl')) {
        resolveStarted();
        await sourceGate;
        return { ...f.source, sessionId: 'closed-source', sessionFile: path };
      }
      return f.source;
    },
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  f = fTemp;
  const closedPath = join(f.root, 'closed-source.jsonl');
  await writeFile(closedPath, 'synthetic dead session');
  const store = new SQLiteMemoryStore(f.path);
  for (let index = 0; index < 20; index++) {
    store.enqueueDistillTrigger({
      id: `complete-${index}`, sessionId: `complete-session-${index}`,
      sessionFile: f.source.sessionFile, leafId: null, cwd: f.root, reason: 'close',
    });
    const claim = store.claimDistillTrigger({ workerId: 'seed', now: Date.now(), leaseMs: 10_000 });
    assert.ok(claim);
    store.prepareDistillTrigger({
      id: claim.trigger.id, claimToken: claim.claimToken, now: Date.now(), jobs: [],
    });
  }
  store.observeSession({
    sessionId: 'closed-source', sessionFile: closedPath, cwd: f.root,
    leafId: f.source.leafId, ownerPid: 2_147_483_647,
  });
  store.close();

  await f.coordinator.start(f.ctx);
  await sourceStarted;
  const queued = new SQLiteMemoryStore(f.path);
  assert.ok(queued.listDistillTriggers({ limit: 40 }).items.some(item =>
    item.reason === 'close' && item.sessionId === 'closed-source',
  ));
  queued.close();
  releaseSource();
  await f.coordinator.whenIdle();

  const after = new SQLiteMemoryStore(f.path);
  assert.equal(calls, 1);
  assert.ok(after.listDistillTriggers({ limit: 40 }).items.some(item =>
    item.reason === 'close' && item.sessionId === 'closed-source' && item.status === 'complete',
  ));
  after.close();
});

test('a new source range waits for its already prepared work and does not replay committed evidence', async t => {
  let started;
  const requestStarted = new Promise(resolve => { started = resolve; });
  let calls = 0;
  const f = await fixture(t, {
    runModel: async (_batch, _ctx, _store, signal) => {
      calls++;
      started();
      return new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('fenced')), { once: true }));
    },
  });
  f.coordinator.beforeCompact(f.ctx);
  f.coordinator.afterCompact(f.ctx, 'first-compaction');
  await requestStarted;
  await f.coordinator.shutdown(f.ctx, 'quit');
  await f.coordinator.whenIdle();

  const recovered = new ConsolidationCoordinator({
    openStore: () => new SQLiteMemoryStore(f.path),
    loadSnapshot: async () => f.source,
    runModel: async (batch, _ctx, store) => {
      calls++;
      const close = store.listDistillTriggers({ limit: 20 }).items.find(item => item.reason === 'close');
      assert.equal(close?.status, 'pending');
      assert.deepEqual(store.processedSources(batch.sourceKeys), []);
      assert.ok(store.readConsolidationStatus([]).pendingTriggers.some(item => item.id === close.id));
      return { creates: [], updates: [], deletes: [] };
    },
    retryDelayMs: 100_000,
  });
  await recovered.start(f.ctx);
  await recovered.whenIdle();
  await recovered.shutdown(f.ctx, 'reload');

  const after = new SQLiteMemoryStore(f.path);
  const triggers = after.listDistillTriggers({ limit: 20 }).items;
  assert.equal(calls, 2);
  assert.ok(triggers.every(item => item.status === 'complete'));
  after.close();
});

test('reload creates no close trigger and failed compaction cancels only its candidate', async t => {
  let calls = 0;
  const f = await fixture(t, {
    runModel: async () => { calls++; return { creates: [], updates: [], deletes: [] }; },
  });
  f.coordinator.beforeCompact(f.ctx);
  f.coordinator.compactFailed();
  await f.coordinator.shutdown(f.ctx, 'reload');

  const store = new SQLiteMemoryStore(f.path);
  assert.deepEqual(store.listDistillTriggers({ limit: 20 }).items, []);
  assert.equal(calls, 0);
  store.close();
});

test('foreground activity does not abort an issued model request', async t => {
  let resolveRequest;
  let started;
  const requestStarted = new Promise(resolve => { started = resolve; });
  const request = new Promise(resolve => { resolveRequest = resolve; });
  let signal;
  const f = await fixture(t, {
    runModel: async (_batch, _ctx, _store, modelSignal) => {
      signal = modelSignal;
      started();
      return request;
    },
  });

  f.coordinator.beforeCompact(f.ctx);
  f.coordinator.afterCompact(f.ctx, 'compact-record-1');
  await requestStarted;
  f.coordinator.setForegroundBusy(true);
  assert.equal(signal.aborted, false);
  resolveRequest({ creates: [], updates: [], deletes: [] });
  await f.coordinator.whenIdle();
  assert.equal(signal.aborted, false);
});

test('each consolidation model round waits for foreground idle before issuing another request', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-model-gate-'));
  const store = new SQLiteMemoryStore(join(root, 'memory.sqlite'));
  t.after(() => { store.close(); return rm(root, { recursive: true, force: true }); });
  let busy = false;
  let wake;
  const waitUntilIdle = async () => {
    while (busy) await new Promise(resolve => { wake = resolve; });
  };
  let calls = 0;
  const registry = {
    complete: async () => {
      calls++;
      if (calls === 1) {
        busy = true;
        return {
          role: 'assistant', api: 'faux', provider: 'faux', model: 'synthetic', timestamp: 1,
          stopReason: 'toolUse',
          content: [{ type: 'toolCall', id: 'search-1', name: 'memory_search', arguments: { terms: ['Cedar'] } }],
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        };
      }
      return fauxAssistantMessage('{"operations":[]}');
    },
  };
  const batch = {
    id: 'job-1', sessionId: 'runtime-session', sessionFile: null, leafId: 'entry-1', cwd: root,
    sourceKeys: ['source-1'],
    evidence: [{ key: 'source-1', entryId: 'entry-1', role: 'user', text: 'Cedar uses local tests.' }],
    context: [],
  };
  const controller = new AbortController();
  const work = consolidateWithModel(
    batch,
    { model: { contextWindow: 32_768, maxTokens: 4_096 }, modelRegistry: registry },
    store,
    controller.signal,
    { waitUntilIdle },
  );

  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(calls, 1);
  assert.equal(controller.signal.aborted, false);
  busy = false;
  wake?.();
  assert.deepEqual(await work, { creates: [], updates: [], deletes: [] });
  assert.equal(calls, 2);
});

test('crash recovery captures durable messages written after the last recorded boundary', async t => {
  const { SessionManager } = await import('@earendil-works/pi-coding-agent');
  const { loadSnapshot } = await import('../src/consolidation-source.ts');
  const captured = [];
  const f = await fixture(t, { loadSnapshot, runModel: async batch => {
    captured.push(JSON.stringify(batch.evidence));return {creates:[],updates:[],deletes:[]};
  }});
  const old = SessionManager.create(f.root,join(f.root,'sessions'));
  old.appendMessage({role:'user',content:'The earlier conclusion.',timestamp:1});
  const recordedLeaf=old.getLeafId();
  old.appendMessage({role:'user',content:'The newer conclusion survived a crash.',timestamp:2});
  const store = new SQLiteMemoryStore(f.path);
  store.observeSession({sessionId:old.getSessionId(),sessionFile:old.getSessionFile(),cwd:f.root,leafId:recordedLeaf,ownerPid:2_147_483_647});
  store.close();
  await f.coordinator.start(f.ctx);await f.coordinator.whenIdle();
  for (let pass=0;pass<3&&!captured.some(text=>text.includes('The newer conclusion survived a crash.'));pass++) {
    f.coordinator.setForegroundBusy(false);await f.coordinator.whenIdle();
  }
  assert.ok(captured.some(text=>text.includes('The newer conclusion survived a crash.')));
});

test('crash recovery finds a persisted first message even when no observation hook ran', async t => {
 const {SessionManager}=await import('@earendil-works/pi-coding-agent');
 const {loadSnapshot}=await import('../src/consolidation-source.ts');
 const captured=[];const f=await fixture(t,{loadSnapshot,runModel:async batch=>{captured.push(JSON.stringify(batch.evidence));return {creates:[],updates:[],deletes:[]};}});
 const old=SessionManager.create(f.root,join(f.root,'sessions'));old.appendMessage({role:'user',content:'Persisted before the first observation hook.',timestamp:1});
 const store=new SQLiteMemoryStore(f.path);
 store.saveSessionUsage({sessionId:old.getSessionId(),sessionFile:old.getSessionFile(),cwd:f.root,leafId:null,ownerId:'dead-first-message',processToken:'dead',ownerPid:2_147_483_647,state:'active'});store.close();
 await f.coordinator.start(f.ctx);await f.coordinator.whenIdle();
 assert.ok(captured.some(text=>text.includes('Persisted before the first observation hook.')));
});
