import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { loadSnapshot } from '../../src/sources/snapshot.ts';
import { consolidateWithModel } from '../../src/distill/model.ts';

function sourceBatch(evidence = [{ key: 'source-1', entryId: 'entry-1', role: 'user', text: 'A confirmed preference.' }]) {
  return {
    id: 'job-1', sessionId: 'session-1', sessionFile: null, leafId: 'entry-1',
    cwd: '/synthetic/project', sourceKeys: evidence.map(part => part.key), evidence, context: [],
  };
}

function modelContext(registry, model = { contextWindow: 128_000, maxTokens: 8_192 }) {
  return { ...(model === undefined ? {} : { model }), modelRegistry: registry };
}

function response(stopReason, text, output = 10) {
  return {
    role: 'assistant', api: 'synthetic', provider: 'synthetic', model: 'test-model', timestamp: Date.now(),
    stopReason,
    content: [{ type: 'text', text }],
    usage: {
      input: 1, output, cacheRead: 0, cacheWrite: 0, totalTokens: output + 1,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  };
}

function failureReason(reason) {
  return error => {
    assert.equal(error?.name, 'DistillFailure');
    assert.equal(error?.reason, reason);
    return true;
  };
}

async function tempDir(t) {
  const path = await mkdtemp(join(tmpdir(), 'pi-distill-failure-'));
  t.after(() => rm(path, { recursive: true, force: true }));
  return path;
}

test('DistillFailure preserves its reason, cause, and only a validated retry delay', async () => {
  const { DistillFailure } = await import('../../src/distill/failure.ts');
  const cause = new Error('synthetic cause');
  const failure = new DistillFailure('transient', 'provider unavailable', { cause, retryAfterMs: 1_500 });
  assert.ok(failure instanceof Error);
  assert.equal(failure.name, 'DistillFailure');
  assert.equal(failure.reason, 'transient');
  assert.equal(failure.cause, cause);
  assert.equal(failure.retryAfterMs, 1_500);
  assert.throws(() => new DistillFailure('transient', 'invalid delay', { retryAfterMs: -1 }), RangeError);
  assert.throws(() => new DistillFailure('transient', 'invalid delay', { retryAfterMs: Number.POSITIVE_INFINITY }), RangeError);
});

test('persisted source parser failures have source_invalid while filesystem errors stay unclassified', async t => {
  const dir = await tempDir(t);
  const malformedPath = join(dir, 'malformed.jsonl');
  await writeFile(malformedPath, '{not-json}\n', 'utf8');
  await assert.rejects(loadSnapshot(malformedPath), failureReason('source_invalid'));

  const oversizedPath = join(dir, 'oversized.jsonl');
  await writeFile(oversizedPath, `${JSON.stringify({ type: 'session', id: 'large', timestamp: '2026-10-23T00:00:00.000Z', cwd: '/synthetic' })}\n${'x'.repeat(16 * 1024 * 1024)}\n`, 'utf8');
  await assert.rejects(loadSnapshot(oversizedPath), failureReason('source_invalid'));

  await assert.rejects(loadSnapshot(join(dir, 'missing.jsonl')), error => {
    assert.equal(error?.name, 'Error');
    assert.notEqual(error?.name, 'DistillFailure');
    assert.equal(error?.code, 'ENOENT');
    return true;
  });
});

test('invalid source batches are classified before any model request', async () => {
  let calls = 0;
  const registry = { complete: async () => { calls += 1; return response('stop', '{"operations":[]}'); } };
  const duplicate = { key: 'same', entryId: 'a', role: 'user', text: 'first' };
  await assert.rejects(
    consolidateWithModel(sourceBatch([duplicate, { ...duplicate, entryId: 'b' }]), modelContext(registry), {}, new AbortController().signal),
    failureReason('source_invalid'),
  );
  assert.equal(calls, 0);
});

test('missing or undersized selected models are model_unavailable before a request', async () => {
  let calls = 0;
  const registry = { complete: async () => { calls += 1; return response('stop', '{"operations":[]}'); } };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), { modelRegistry: registry }, {}, new AbortController().signal),
    failureReason('model_unavailable'),
  );
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(registry, { contextWindow: 5_000, maxTokens: 8_192 }), {}, new AbortController().signal),
    failureReason('model_unavailable'),
  );
  assert.equal(calls, 0);
});

test('malformed structured model output is model_invalid', async () => {
  const registry = { complete: async () => response('stop', '{not-json}') };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(registry), {}, new AbortController().signal),
    failureReason('model_invalid'),
  );

  const invalidOperationRegistry = {
    complete: async () => response('stop', JSON.stringify({ operations: [
      { action: 'create', content: 'A proposed memory.', sourceKeys: ['unknown-source'] },
    ] })),
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(invalidOperationRegistry), {}, new AbortController().signal),
    failureReason('model_invalid'),
  );
});

test('model output and tool-call caps are model_budget', async () => {
  const outputRegistry = { complete: async () => response('stop', '{"operations":[]}', 5_000) };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(outputRegistry), {}, new AbortController().signal),
    failureReason('model_budget'),
  );

  const calls = Array.from({ length: 13 }, (_, index) => ({
    type: 'toolCall', id: `call-${index}`, name: 'memory_search', arguments: { terms: [`term-${index}`] },
  }));
  const toolRegistry = { complete: async () => ({
    ...response('toolUse', ''), content: calls,
  }) };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(toolRegistry), {}, new AbortController().signal),
    failureReason('model_budget'),
  );

  const lengthRegistry = { complete: async () => response('length', '{"operations":[]}') };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(lengthRegistry), {}, new AbortController().signal),
    failureReason('model_budget'),
  );

  let roundCalls = 0;
  const roundRegistry = {
    async complete() {
      roundCalls += 1;
      return {
        ...response('toolUse', ''),
        content: [{ type: 'toolCall', id: `round-${roundCalls}`, name: 'memory_search', arguments: { terms: ['term'] } }],
      };
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(roundRegistry), { searchBatch: () => [] }, new AbortController().signal),
    failureReason('model_budget'),
  );
  assert.equal(roundCalls, 12);
});

test('input that grows beyond the request allowance after a retrieval call is model_budget', async () => {
  const store = {
    get: id => ({ id, revision: 1, content: 'm'.repeat(8_192), createdAt: '2026-10-23T00:00:00.000Z', updatedAt: '2026-10-23T00:00:00.000Z' }),
    memoryOrigin: () => undefined,
  };
  let calls = 0;
  const registry = {
    async complete(_model, request) {
      calls += 1;
      const last=request.messages.at(-1);
      const page=last.role==='toolResult'?JSON.parse(last.content[0].text):{};
      assert.notEqual(page.hasMore,false,'the whole memory cannot fit this retained request');
      return {
        ...response('toolUse', ''),
        content: [{ type: 'toolCall', id: `read-large-${calls}`, name: 'memory_read',
          arguments: { id: 'large', expectedRevision: 1,...(page.nextCursor?{cursor:page.nextCursor}:{}) } }],
      };
    },
  };
  const batch = sourceBatch([{ key: 'source-1', entryId: 'entry-1', role: 'user', text: 'x'.repeat(57_000) }]);
  await assert.rejects(
    consolidateWithModel(batch, modelContext(registry), store, new AbortController().signal),
    failureReason('model_budget'),
  );
  assert.ok(calls>=2,'bounded partial reads fit before the retained request exhausts capacity');
});

test('provider stops and retrieval failures retain transient, conflict, or invalid-input causes', async () => {
  for (const stopReason of ['error', 'aborted']) {
    const registry = { complete: async () => response(stopReason, '') };
    await assert.rejects(
      consolidateWithModel(sourceBatch(), modelContext(registry), {}, new AbortController().signal),
      failureReason('transient'),
    );
  }

  const retrievalRegistry = {
    calls: 0,
    async complete() {
      this.calls += 1;
      if (this.calls === 1) {
        return {
          ...response('toolUse', ''),
          content: [{ type: 'toolCall', id: 'read-missing', name: 'memory_read', arguments: { id: 'missing', expectedRevision: 1 } }],
        };
      }
      return response('stop', '{"operations":[]}');
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(retrievalRegistry), { get: () => undefined }, new AbortController().signal),
    failureReason('conflict'),
  );

  const storageRegistry = {
    calls: 0,
    async complete() {
      this.calls += 1;
      if (this.calls === 1) {
        return {
          ...response('toolUse', ''),
          content: [{ type: 'toolCall', id: 'read-storage-error', name: 'memory_read', arguments: { id: 'memory', expectedRevision: 1 } }],
        };
      }
      return response('stop', '{"operations":[]}');
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(storageRegistry), { get: () => { throw new Error('synthetic storage failure'); } }, new AbortController().signal),
    failureReason('transient'),
  );

  const invalidToolRegistry = {
    calls: 0,
    async complete() {
      this.calls += 1;
      if (this.calls === 1) {
        return {
          ...response('toolUse', ''),
          content: [{ type: 'toolCall', id: 'invalid-search', name: 'memory_search', arguments: { terms: [] } }],
        };
      }
      return response('stop', '{"operations":[]}');
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(invalidToolRegistry), {}, new AbortController().signal),
    failureReason('model_invalid'),
  );
});

test('unknown provider errors are preserved, while structured retry-after is validated and carried', async () => {
  const textOnlyError = new Error('malformed JSON and retry after 10 seconds');
  const unstructuredRegistry = { complete: async () => { throw textOnlyError; } };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(unstructuredRegistry), {}, new AbortController().signal),
    error => error === textOnlyError,
  );

  const providerError = Object.assign(new Error('synthetic provider response'), { headers: new Headers({ 'retry-after-ms': '1250' }) });
  const structuredRegistry = { complete: async () => { throw providerError; } };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(structuredRegistry), {}, new AbortController().signal),
    error => {
      assert.equal(error?.name, 'DistillFailure');
      assert.equal(error?.reason, 'transient');
      assert.equal(error?.cause, providerError);
      assert.equal(error?.retryAfterMs, 1_250);
      return true;
    },
  );
});

test('validated provider response retry-after reaches transient stop errors and exceptions, but not success', async () => {
  let stopObservedResponse = false;
  const stopRegistry = {
    async complete(model, _request, options) {
      if (typeof options?.onResponse === 'function') {
        stopObservedResponse = true;
        await options.onResponse({ status: 429, headers: { 'retry-after-ms': '1250' } }, model);
      }
      return response('error', '');
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(stopRegistry), {}, new AbortController().signal),
    error => {
      assert.equal(error?.name, 'DistillFailure');
      assert.equal(error?.reason, 'transient');
      assert.equal(error?.retryAfterMs, 1_250);
      return true;
    },
  );
  assert.equal(stopObservedResponse, true, 'the provider response callback should be wired through the model registry');

  const providerError = new Error('synthetic provider failure without retry metadata');
  let exceptionObservedResponse = false;
  const exceptionRegistry = {
    async complete(model, _request, options) {
      if (typeof options?.onResponse === 'function') {
        exceptionObservedResponse = true;
        await options.onResponse({ status: 503, headers: { 'retry-after': '2' } }, model);
      }
      throw providerError;
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(exceptionRegistry), {}, new AbortController().signal),
    error => {
      assert.equal(error?.name, 'DistillFailure');
      assert.equal(error?.reason, 'transient');
      assert.equal(error?.cause, providerError);
      assert.equal(error?.retryAfterMs, 2_000);
      return true;
    },
  );
  assert.equal(exceptionObservedResponse, true, 'the provider response callback should also cover thrown requests');

  let successObservedResponse = false;
  const successRegistry = {
    async complete(model, _request, options) {
      if (typeof options?.onResponse === 'function') {
        successObservedResponse = true;
        await options.onResponse({ status: 200, headers: { 'retry-after-ms': '9000' } }, model);
      }
      return response('stop', '{"operations":[]}');
    },
  };
  assert.deepEqual(
    await consolidateWithModel(sourceBatch(), modelContext(successRegistry), {}, new AbortController().signal),
    { creates: [], updates: [], deletes: [] },
    'a valid successful model response remains successful even when the provider sends a retry hint',
  );
  assert.equal(successObservedResponse, true);
});

test('a caller-owned abort remains cancellation and is not relabeled as a model timeout', async () => {
  const controller = new AbortController();
  const registry = {
    complete(_model, _request, options) {
      setTimeout(() => controller.abort(new Error('owner shutdown')), 5);
      return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true }));
    },
  };
  await assert.rejects(
    consolidateWithModel(sourceBatch(), modelContext(registry), {}, controller.signal),
    error => error?.message === 'owner shutdown' && error?.name !== 'DistillFailure',
  );
});
