import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { consolidateWithModel, fitsConsolidationRequest } from '../src/consolidation-model.ts';

function sourceBatch(evidence, context = [], sourceTime = '2026-10-23T00:00:00.000Z') {
  return {
    id: 'job-adaptive', sessionId: 'session-adaptive', sessionFile: null, leafId: 'entry-last',
    cwd: '/synthetic/adaptive', sourceKeys: evidence.map(part => part.key), evidence, context, sourceTime,
  };
}

function model(contextWindow = 128_000) {
  return { contextWindow, maxTokens: 8_192 };
}

function response(stopReason, content) {
  return {
    role: 'assistant', api: 'synthetic', provider: 'synthetic', model: 'adaptive-test', timestamp: 1,
    stopReason,
    content: Array.isArray(content) ? content : [{ type: 'text', text: content }],
    usage: {
      input: 1, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 11,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
}

function toolCall(name, arguments_, id) {
  return { type: 'toolCall', id, name, arguments: arguments_ };
}

function failureReason(reason) {
  return error => {
    assert.equal(error?.name, 'DistillFailure');
    assert.equal(error?.reason, reason);
    return true;
  };
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-adaptive-model-'));
  const store = new SQLiteMemoryStore(join(root, 'memories.sqlite'));
  t.after(() => { store.close(); return rm(root, { recursive: true, force: true }); });
  return store;
}

test('fit check matches the executed serialized request at the byte boundary', async () => {
  const batch = sourceBatch([
    { key: 'a'.repeat(64), entryId: 'entry-long', role: 'user', text: 'The grove uses “cedar” 🌲.\nThe path is C:\\logs\\daily.' },
    { key: 'e1', entryId: 'entry-e1', role: 'user', text: 'Keep the original condition.' },
    { key: 'e2', entryId: 'entry-e2', role: 'user', text: 'Record the exception.' },
    { key: 'e3', entryId: 'entry-e3', role: 'assistant', text: 'A separate branch remains unresolved.' },
  ], [{ key: 'older-context', entryId: 'entry-old', role: 'user', text: 'Earlier context with a quote: "retain".' }]);
  const requests = [];
  const registry = {
    async complete(_selectedModel, request) {
      requests.push(structuredClone(request));
      const source = JSON.parse(request.messages[0].content);
      assert.deepEqual(source.newEvidence.map(part => part.key), ['e4', 'e1', 'e2', 'e3']);
      assert.equal(source.newEvidence[0].text, batch.evidence[0].text);
      assert.equal(source.interpretationContext[0].text, batch.context[0].text);
      return response('stop', JSON.stringify({ operations: [
        { action: 'create', content: 'The grove keeps the original condition and exception.', sourceKeys: ['e4'] },
      ] }));
    },
  };

  const looseContext = { model: model(), modelRegistry: registry };
  await consolidateWithModel(batch, looseContext, {}, new AbortController().signal);
  assert.equal(requests.length, 1);
  const serializedBytes = Buffer.byteLength(JSON.stringify(requests[0]), 'utf8');
  assert.equal(requests[0].messages[0].timestamp, Date.parse(batch.sourceTime));

  const exactModel = model(serializedBytes + 4_096);
  assert.equal(fitsConsolidationRequest(batch, exactModel), true);
  assert.equal(fitsConsolidationRequest(batch, model(serializedBytes + 4_095)), false);

  requests.length = 0;
  await consolidateWithModel(batch, { model: exactModel, modelRegistry: registry }, {}, new AbortController().signal);
  assert.equal(requests.length, 1);
  assert.equal(Buffer.byteLength(JSON.stringify(requests[0]), 'utf8'), serializedBytes);

  let oversizedCalls = 0;
  await assert.rejects(
    consolidateWithModel(batch, {
      model: model(serializedBytes + 4_095),
      modelRegistry: { complete: async () => { oversizedCalls += 1; return response('stop', '{"operations":[]}'); } },
    }, {}, new AbortController().signal),
    failureReason('model_unavailable'),
  );
  assert.equal(oversizedCalls, 0, 'an oversized initial request must be rejected before contacting the provider');
});

test('fit check rejects invalid model and source data instead of treating them as oversized', () => {
  const batch = sourceBatch([{ key: 'source-1', entryId: 'entry-1', role: 'user', text: 'A confirmed preference.' }]);
  assert.throws(() => fitsConsolidationRequest(batch, null), failureReason('model_unavailable'));
  assert.throws(() => fitsConsolidationRequest(batch, { contextWindow: 4_096, maxTokens: 8_192 }), failureReason('model_unavailable'));
  assert.throws(() => fitsConsolidationRequest(sourceBatch([{ key: 'source-1', entryId: 'entry-1', role: 'user', text: '\ud800' }]), model()), failureReason('source_invalid'));
});

test('a fitting search/read loop still rejects an oversized later turn under the same request budgets', async t => {
  const store = await fixture(t);
  store.save({
    id: 'long-memory', content: `needle ${'memory detail '.repeat(700)}`,
    createdAt: '2026-10-23T00:00:00.000Z',
  });
  const batch = sourceBatch([{
    key: 'source-1', entryId: 'entry-1', role: 'user', text: 'x'.repeat(55_000),
  }]);
  const selectedModel = model();
  assert.equal(fitsConsolidationRequest(batch, selectedModel), true);
  const requests = [];
  const registry = {
    async complete(_selectedModel, request) {
      requests.push(structuredClone(request));
      const current = requests.length;
      return current === 1
        ? response('toolUse', [toolCall('memory_search', { terms: ['needle'] }, 'search-1')])
        : response('toolUse', [toolCall('memory_read', { id: 'long-memory', expectedRevision: 1 }, 'read-1')]);
    },
  };

  await assert.rejects(
    consolidateWithModel(batch, { model: selectedModel, modelRegistry: registry }, store, new AbortController().signal),
    failureReason('model_budget'),
  );
  assert.equal(requests.length, 2, 'the initial request and the request after memory_search fit; the memory_read result exceeds the next turn budget');
  const serializedSizes = requests.map(request => Buffer.byteLength(JSON.stringify(request), 'utf8'));
  assert.ok(serializedSizes.every(size => size <= 64 * 1024));
  assert.ok(requests[1].messages.some(message => message.role === 'toolResult' && message.toolName === 'memory_search'));
});
