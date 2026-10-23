import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { MemoryTools } from '../../src/memory/retrieval.ts';
import { consolidateWithModel } from '../../src/distill/model.ts';

const time = '2026-10-21T08:00:00.000Z';

function sourceBatch(evidence = [{ key: 'source-1', entryId: 'entry-1', role: 'user', text: 'I prefer concise explanations.' }], context = []) {
  return {
    id: 'job-1', sessionId: 'session-1', sessionFile: null, leafId: 'entry-1',
    cwd: '/synthetic/project', sourceKeys: evidence.map((part) => part.key), evidence, context,
  };
}

function response(stopReason, content, output = 10) {
  return {
    role: 'assistant', api: 'faux', provider: 'faux', model: 'test-model', timestamp: Date.now(),
    stopReason,
    content: typeof content === 'string'
      ? [{ type: 'text', text: content }]
      : Array.isArray(content) ? content : [content],
    usage: {
      input: 1, output, cacheRead: 0, cacheWrite: 0, totalTokens: output + 1,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
}

function textResponse(json, output = 10) { return response('stop', json, output); }
function toolCall(name, arguments_, id = `call-${name}`) {
  return { type: 'toolCall', id, name, arguments: arguments_ };
}

function scriptedRegistry(responses, onCall = () => {}) {
  let index = 0;
  return {
    get calls() { return index; },
    complete(model, request, options) {
      const current = index++;
      onCall({ model, request, options, current });
      const next = responses[current];
      if (next === undefined) throw new Error('Unexpected model call.');
      return Promise.resolve(next);
    },
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-consolidation-model-'));
  const store = new SQLiteMemoryStore(join(root, 'memories.sqlite'));
  t.after(() => { store.close(); return rm(root, { recursive: true, force: true }); });
  return store;
}

function modelContext(registry, contextWindow = 128_000) {
  return { model: { contextWindow, maxTokens: 8192 }, modelRegistry: registry };
}

test('creates a self-contained memory from ordinary conversation evidence', async () => {
  const registry = scriptedRegistry([textResponse(JSON.stringify({
    operations: [{ action: 'create', content: 'The user prefers concise explanations for technical topics.', sourceKeys: ['source-1'] }],
  }))]);
  const changes = await consolidateWithModel(sourceBatch(), modelContext(registry), {}, new AbortController().signal);
  assert.equal(changes.creates.length, 1);
  assert.match(changes.creates[0].id, /^[0-9a-f-]{36}$/i);
  assert.equal(changes.creates[0].content, 'The user prefers concise explanations for technical topics.');
  assert.equal(changes.creates[0].createdAt, new Date(changes.creates[0].createdAt).toISOString());
  assert.deepEqual(changes.updates, []);
  assert.deepEqual(changes.deletes, []);
  assert.equal(registry.calls, 1);
});

test('allows a valid no-change result without issuing user-visible memory commands', async () => {
  const registry = scriptedRegistry([textResponse('{"operations":[]}')]);
  assert.deepEqual(
    await consolidateWithModel(sourceBatch(), modelContext(registry), {}, new AbortController().signal),
    { creates: [], updates: [], deletes: [] },
  );
});

test('long evidence identities use short unambiguous model references without changing durable source keys', async () => {
  const key = 'a'.repeat(64);
  const batch = sourceBatch([
    { key, entryId: 'entry-long', role: 'user', text: 'Maple keeps logs for 10 days by default.' },
    { key: 'e1', entryId: 'entry-short', role: 'user', text: 'During investigation, retain logs for 21 days.' },
  ], [{ key: 'e2', entryId: 'entry-context', role: 'assistant', text: 'Older interpretation context.' }]);
  const original = structuredClone(batch);
  let reference;
  const registry = { complete: async (_model, request) => {
    const source = JSON.parse(request.messages[0].content);
    reference = source.newEvidence[0].key;
    assert.ok(reference.length <= 16, 'Model citations should not require copying a long source digest');
    assert.notEqual(reference, 'e1');assert.notEqual(reference, 'e2');
    assert.equal(source.newEvidence[0].text, batch.evidence[0].text);
    return textResponse(JSON.stringify({operations:[{action:'create',content:'Maple retains logs for 10 days by default.',sourceKeys:[reference]}]}));
  }};
  const changes = await consolidateWithModel(batch, modelContext(registry), {}, new AbortController().signal);
  assert.equal(changes.creates.length, 1);
  assert.deepEqual(batch, original);
});

test('a mistyped long evidence citation is rejected rather than matched approximately', async () => {
  const key = 'a'.repeat(64);
  const registry = scriptedRegistry([textResponse(JSON.stringify({operations:[{action:'create',content:'A proposed policy.',sourceKeys:[key.slice(0,-1)]}]}))]);
  await assert.rejects(consolidateWithModel(sourceBatch([{key,entryId:'entry',role:'user',text:'A confirmed policy.'}]),modelContext(registry),{},new AbortController().signal));
});

test('consolidation exposes literal-miss feedback and accepts a correction after broader search and full read', async t => {
  const store = await fixture(t);
  store.save({ id: 'policy', content: 'Cedar-72 retains logs for 14 days by default; 30 days during an incident investigation.', createdAt: time });
  const corrected = 'Cedar-72 retains logs for 7 days by default; 30 days during an incident investigation.';
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_search', { terms: ['Cedar-72 log retention incident investigation'] }, 'narrow')),
    response('toolUse', toolCall('memory_search', { terms: ['Cedar-72'] }, 'broad')),
    response('toolUse', toolCall('memory_read', { id: 'policy', expectedRevision: 1 }, 'read')),
    textResponse(JSON.stringify({ operations: [{ action: 'update', id: 'policy', expectedRevision: 1, content: corrected, sourceKeys: ['correction'] }] })),
  ], ({ request, current }) => {
    const last = request.messages.at(-1);
    if (current === 1) {
      assert.equal(last.role, 'toolResult');
      const result = JSON.parse(last.content[0].text);
      assert.deepEqual(result.candidates, []);
      assert.match(result.hint, /short|broad/i);
    }
    if (current === 2) assert.equal(JSON.parse(last.content[0].text).candidates[0].id, 'policy');
    if (current === 3) assert.equal(JSON.parse(last.content[0].text).hasMore, false);
  });
  const batch = sourceBatch([{ key: 'correction', entryId: 'correction', role: 'user', text: 'For Cedar-72, change the default to 7 days; keep the incident exception.' }]);
  const changes = await consolidateWithModel(batch, modelContext(registry), store, new AbortController().signal);
  assert.deepEqual(changes.creates, []);
  assert.deepEqual(changes.deletes, []);
  assert.equal(changes.updates.length, 1);
  assert.equal(changes.updates[0].id, 'policy');
  assert.equal(changes.updates[0].expectedRevision, 1);
  assert.equal(changes.updates[0].content, corrected);
});

test('updates and deletes only fully read records and uses extension-owned timestamps', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'update-me', content: 'The user was considering option A.', createdAt: time });
  store.save({ id: 'delete-me', content: 'Option B was a temporary alternative.', createdAt: time });
  const registry = scriptedRegistry([
    response('toolUse', [toolCall('memory_read', { id: 'update-me', expectedRevision: 1 }), toolCall('memory_read', { id: 'delete-me', expectedRevision: 1 })]),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'update-me', expectedRevision: 1, content: 'The user chose option A for the current task.', sourceKeys: ['source-1'] },
      { action: 'delete', id: 'delete-me', expectedRevision: 1, sourceKeys: ['source-1'] },
    ] })),
  ]);
  const changes = await consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal);
  assert.deepEqual(changes.creates, []);
  assert.deepEqual(changes.updates.map(({ id, expectedRevision, content }) => ({ id, expectedRevision, content })), [
    { id: 'update-me', expectedRevision: 1, content: 'The user chose option A for the current task.' },
  ]);
  assert.equal(changes.updates[0].updatedAt, new Date(changes.updates[0].updatedAt).toISOString());
  assert.deepEqual(changes.deletes, [{ id: 'delete-me', expectedRevision: 1 }]);
  assert.equal(store.get('update-me').content, 'The user was considering option A.');
  assert.ok(store.get('delete-me'));
});

test('rejects hallucinated or unread IDs even when cited by a source entry', async (t) => {
  const store = await fixture(t);
  const registry = scriptedRegistry([textResponse(JSON.stringify({ operations: [
    { action: 'update', id: 'invented-id', expectedRevision: 1, content: 'Changed conclusion.', sourceKeys: ['source-1'] },
  ] }))]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal));
});

test('rejects a stale revision after memory_read reports the current version differs', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'memory-1', content: 'Old conclusion.', createdAt: time });
  store.update({ id: 'memory-1', expectedRevision: 1, content: 'Current conclusion.', updatedAt: '2026-10-22T08:00:00.000Z' });
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'memory-1', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'memory-1', expectedRevision: 1, content: 'Unverified stale update.', sourceKeys: ['source-1'] },
    ] })),
  ]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal));
});

test('rejects writes based on a read that stopped at a partial memory chunk', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'long-memory', content: 'important '.repeat(4_000), createdAt: time });
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'long-memory', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'long-memory', expectedRevision: 1, content: 'Rewrite without reading all conditions.', sourceKeys: ['source-1'] },
    ] })),
  ]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal));
});

test('rejects mutation output after any retrieval failure, including an empty result', async (t) => {
  const store = await fixture(t);
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'missing', expectedRevision: 1 })),
    textResponse('{"operations":[]}'),
  ]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal));
});

test('a partial search may be enough when the selected record is read completely', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'selected', content: 'Existing related memory.', createdAt: time });
  store.save({ id: 'later-candidate', content: 'Existing related memory with another condition.', createdAt: time });
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_search', { terms: ['related'], limit: 1 })),
    response('toolUse', toolCall('memory_read', { id: 'selected', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'selected', expectedRevision: 1, content: 'Updated condition from the new evidence.', sourceKeys: ['source-1'] },
    ] })),
  ]);
  const changes = await consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal);
  assert.equal(changes.updates[0].id, 'selected');
  assert.equal(registry.calls, 3);
});

test('same-session sibling-branch memories are explained as alternatives and cannot be rewritten', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'sibling-memory', content: 'In the other branch, option B was chosen.', createdAt: time });
  store.memoryOrigin = (id) => id === 'sibling-memory'
    ? { sessionId: 'session-1', sourceKeys: ['sibling-source'] }
    : undefined;
  let alternativeBranch;
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'sibling-memory', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'sibling-memory', expectedRevision: 1, content: 'Option A replaces option B.', sourceKeys: ['source-1'] },
    ] })),
  ], ({ request, current }) => {
    if (current === 1) {
      const toolResult = request.messages.find((message) => message.role === 'toolResult');
      alternativeBranch = JSON.parse(toolResult.content[0].text).alternativeBranch;
    }
  });
  const batch = { ...sourceBatch(), lineageKeys: ['source-1'], lineageSessionIds: ['session-1'] };
  await assert.rejects(consolidateWithModel(batch, modelContext(registry), store, new AbortController().signal));
  assert.equal(alternativeBranch, true);
});

test('memories grounded in verified ancestor evidence remain eligible for updates', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'ancestor-memory', content: 'A decision from an earlier branch ancestor.', createdAt: time });
  store.memoryOrigin = (id) => id === 'ancestor-memory'
    ? { sessionId: 'ancestor-session', sourceKeys: ['ancestor-key'] }
    : undefined;
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'ancestor-memory', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'ancestor-memory', expectedRevision: 1, content: 'The current evidence updates the ancestor decision.', sourceKeys: ['source-1'] },
    ] })),
  ]);
  const batch = {
    ...sourceBatch(),
    lineageKeys: ['source-1', 'ancestor-key'],
    lineageSessionIds: ['session-1', 'ancestor-session'],
  };
  const changes = await consolidateWithModel(batch, modelContext(registry), store, new AbortController().signal);
  assert.equal(changes.updates[0].id, 'ancestor-memory');
});

test('requires model output to cite only nonempty new evidence keys', async () => {
  const registry = scriptedRegistry([textResponse(JSON.stringify({ operations: [
    { action: 'create', content: 'Unsupported context-only inference.', sourceKeys: ['old-context'] },
  ] }))]);
  const batch = sourceBatch(undefined, [{ key: 'old-context', entryId: 'older', role: 'user', text: 'Earlier context.' }]);
  await assert.rejects(consolidateWithModel(batch, modelContext(registry), {}, new AbortController().signal));
});

test('rejects malformed, oversized, duplicate-target, and unsupported model JSON', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'same', content: 'Current memory.', createdAt: time });
  const outputs = [
    'not JSON',
    JSON.stringify({ operations: [{ action: 'create', content: 'x'.repeat(8_193), sourceKeys: ['source-1'] }] }),
    JSON.stringify({ operations: [{ action: 'create', content: '\ud800', sourceKeys: ['source-1'] }] }),
    JSON.stringify({ operations: [{ action: 'create', content: 'x', sourceKeys: ['source-1'], extra: true }] }),
  ];
  for (const output of outputs) {
    const registry = scriptedRegistry([textResponse(output)]);
    await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal));
  }
  const duplicateRegistry = scriptedRegistry([
    response('toolUse', toolCall('memory_read', { id: 'same', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'same', expectedRevision: 1, content: 'First update.', sourceKeys: ['source-1'] },
      { action: 'delete', id: 'same', expectedRevision: 1, sourceKeys: ['source-1'] },
    ] })),
  ]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(duplicateRegistry), store, new AbortController().signal));
});

test('accepts only stop or toolUse turns and rejects truncated or provider-error output', async () => {
  for (const stopReason of ['length', 'error', 'aborted', 'deferred']) {
    const registry = scriptedRegistry([response(stopReason, '{"operations":[]}')]);
    await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(registry), {}, new AbortController().signal));
  }
});

test('enforces the input allowance and model context window without truncating source', async () => {
  for (const [contextWindow, text] of [[128_000, 'x'.repeat(70_000)], [5_000, 'ordinary source '.repeat(100)]]) {
    const registry = scriptedRegistry([textResponse('{"operations":[]}')]);
    await assert.rejects(consolidateWithModel(sourceBatch([
      { key: 'source-1', entryId: 'entry-1', role: 'user', text },
    ]), modelContext(registry, contextWindow), {}, new AbortController().signal));
    assert.equal(registry.calls, 0);
  }
});

test('completes a multi-call update for a large but per-request-bounded source batch', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'related', content: 'Existing related memory.', createdAt: time });
  let cumulativeBytes = 0;
  let largestRequestBytes = 0;
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_search', { terms: ['related'] })),
    response('toolUse', toolCall('memory_read', { id: 'related', expectedRevision: 1 })),
    textResponse(JSON.stringify({ operations: [
      { action: 'update', id: 'related', expectedRevision: 1, content: 'The updated conclusion retains its original conditions.', sourceKeys: ['source-1'] },
    ] })),
  ], ({ request }) => {
    const requestBytes = Buffer.byteLength(JSON.stringify(request));
    cumulativeBytes += requestBytes;
    largestRequestBytes = Math.max(largestRequestBytes, requestBytes);
  });
  const evidenceText = 'The current decision and its conditions. '.repeat(600);
  const contextText = 'Earlier context for interpreting that decision. '.repeat(175);
  const batch = sourceBatch(
    [{ key: 'source-1', entryId: 'entry-1', role: 'user', text: evidenceText }],
    [{ key: 'context-1', entryId: 'older-entry', role: 'user', text: contextText }],
  );
  const changes = await consolidateWithModel(batch, modelContext(registry), store, new AbortController().signal);
  assert.equal(changes.updates[0].id, 'related');
  assert.ok(largestRequestBytes <= 64 * 1024);
  assert.ok(cumulativeBytes > 64 * 1024);
  assert.ok(cumulativeBytes <= 256 * 1024);
});

test('bounds tool-loop calls and cumulative model output tokens', async (t) => {
  const store = await fixture(t);
  const overLimitCalls = Array.from({ length: 13 }, (_, index) => toolCall('memory_search', { terms: [`term-${index}`] }, `call-${index}`));
  const loopResponses = [response('toolUse', overLimitCalls)];
  const loopRegistry = scriptedRegistry(loopResponses);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(loopRegistry), store, new AbortController().signal));
  assert.equal(loopRegistry.calls, 1);

  const outputRegistry = scriptedRegistry([
    response('toolUse', toolCall('memory_search', { terms: ['term'] }), 3_000),
    textResponse('{"operations":[]}', 1_200),
  ]);
  await assert.rejects(consolidateWithModel(sourceBatch(), modelContext(outputRegistry), store, new AbortController().signal));
  assert.equal(outputRegistry.calls, 2);
});

test('aborts an in-flight provider request when the job signal is cancelled', async () => {
  const controller = new AbortController();
  let requestedSignal;
  const registry = {
    complete(_model, _request, options) {
      requestedSignal = options.signal;
      return new Promise(() => {});
    },
  };
  const pending = consolidateWithModel(sourceBatch(), modelContext(registry), {}, controller.signal);
  setTimeout(() => controller.abort(), 10);
  await assert.rejects(pending);
  assert.equal(requestedSignal.aborted, true);
});

test('internal search budgets do not consume the foreground MemoryTools budget', async (t) => {
  const store = await fixture(t);
  store.save({ id: 'memory-1', content: 'matching memory', createdAt: time });
  const foreground = new MemoryTools(() => store);
  t.after(() => foreground.close());
  for (let index = 0; index < 11; index += 1) {
    assert.equal((await foreground.search({ terms: ['matching'] })).details.status, 'ok');
  }
  const registry = scriptedRegistry([
    response('toolUse', toolCall('memory_search', { terms: ['matching'] })),
    textResponse('{"operations":[]}'),
  ]);
  await consolidateWithModel(sourceBatch(), modelContext(registry), store, new AbortController().signal);
  assert.equal((await foreground.search({ terms: ['matching'] })).details.status, 'ok');
});
test('separate fork sessions sharing an ancestor cannot overwrite sibling memories',async t=>{
 const store=await fixture(t);
 store.save({id:'fork-b-memory',content:'In fork B, the tentative choice is blue.',createdAt:time});
 store.memoryOrigin=()=>({sessionId:'fork-b',lineageSessionIds:['root-session','fork-b'],sourceKeys:['branch-b-key']});
 const batch={...sourceBatch(),lineageSessionIds:['root-session','fork-c'],lineageKeys:['shared-key','source-1']};
 for(const action of ['update','delete']){
  const operation={action,id:'fork-b-memory',expectedRevision:1,sourceKeys:['source-1'],...(action==='update'?{content:'Fork C chooses green.'}:{})};
  const registry=scriptedRegistry([response('toolUse',toolCall('memory_read',{id:'fork-b-memory',expectedRevision:1})),textResponse(JSON.stringify({operations:[operation]}))]);
  await assert.rejects(consolidateWithModel(batch,modelContext(registry),store,new AbortController().signal));
 }
});
test('whitespace-only source text can complete as no change without blocking later work',async()=>{
 const registry=scriptedRegistry([textResponse('{"operations":[]}')]);
 const batch=sourceBatch([{key:'source-1',entryId:'entry-1',role:'user',text:'  \n '}]);
 const changes=await consolidateWithModel(batch,modelContext(registry),{},new AbortController().signal);
 assert.deepEqual(changes,{creates:[],updates:[],deletes:[]});assert.equal(registry.calls,1);
});
test('late recovery of older evidence cannot overwrite a memory grounded in newer evidence',async t=>{
 const store=await fixture(t);store.save({id:'current',content:'Cedar now uses local tests.',createdAt:time});
 store.memoryOrigin=()=>({sessionId:'other-session',sourceKeys:['newer'],sourceTime:'2026-10-21T10:00:00.000Z'});
 const batch={...sourceBatch(),sourceTime:'2026-10-20T10:00:00.000Z'};
 const registry=scriptedRegistry([response('toolUse',toolCall('memory_read',{id:'current',expectedRevision:1})),textResponse(JSON.stringify({operations:[{action:'update',id:'current',expectedRevision:1,content:'Cedar uses the old remote setup.',sourceKeys:['source-1']}]}))]);
 await assert.rejects(consolidateWithModel(batch,modelContext(registry),store,new AbortController().signal));
});
test('an edited ancestor entry remains mutable through its stable branch anchor',async t=>{
 const store=await fixture(t);store.save({id:'edited',content:'The prior condition.',createdAt:time});
 store.memoryOrigin=()=>({sessionId:'session-1',sourceKeys:['old-content-key'],branchAnchor:'stable-entry-anchor'});
 const batch={...sourceBatch(),lineageSessionIds:['session-1'],lineageKeys:['source-1'],lineageAnchors:['stable-entry-anchor']};
 const registry=scriptedRegistry([response('toolUse',toolCall('memory_read',{id:'edited',expectedRevision:1})),textResponse(JSON.stringify({operations:[{action:'update',id:'edited',expectedRevision:1,content:'The corrected condition.',sourceKeys:['source-1']}]}))]);
 const changes=await consolidateWithModel(batch,modelContext(registry),store,new AbortController().signal);assert.equal(changes.updates.length,1);
});
