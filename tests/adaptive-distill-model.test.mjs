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
      const last = request.messages.at(-1);
      const page = last.role === 'toolResult' ? JSON.parse(last.content[0].text) : {};
      return current === 1
        ? response('toolUse', [toolCall('memory_search', { terms: ['needle'] }, 'search-1')])
        : response('toolUse', [toolCall('memory_read', { id: 'long-memory', expectedRevision: 1,
          ...(page.nextCursor && last.toolName === 'memory_read' ? { cursor: page.nextCursor } : {}) }, `read-${current}`)]);
    },
  };

  await assert.rejects(
    consolidateWithModel(batch, { model: selectedModel, modelRegistry: registry }, store, new AbortController().signal),
    failureReason('model_budget'),
  );
  assert.ok(requests.length >= 3, 'a partial read now fits, but retaining the entire memory still exceeds the total context');
  const reads = requests.flatMap(request => request.messages.filter(message => message.role === 'toolResult' && message.toolName === 'memory_read'));
  assert.ok(reads.length > 0);
  assert.ok(reads.every(message => JSON.parse(message.content[0].text).hasMore !== false));
  const serializedSizes = requests.map(request => Buffer.byteLength(JSON.stringify(request), 'utf8'));
  assert.ok(serializedSizes.every(size => size <= 64 * 1024));
  assert.ok(requests[1].messages.some(message => message.role === 'toolResult' && message.toolName === 'memory_search'));
});

test('bounded candidate pages leave room to read and correct a short memory on a small model', async t => {
 const store=await fixture(t);
 for(let i=1;i<30;i++)store.save({id:String(i).padStart(2,'0'),content:`needle ${'background '.repeat(80)}`,createdAt:'2026-10-23T00:00:00.000Z'});
 store.save({id:'00',content:'needle: retain 14 days; 30 during incidents.',createdAt:'2026-10-23T00:00:00.000Z'});
 const target=store.get('00');
 const batch=sourceBatch([{key:'source-1',entryId:'entry-1',role:'user',text:'Change needle to 21 days; preserve the incident exception.'}]);
 const requests=[];
 const changes=await consolidateWithModel(batch,{model:model(12_000),modelRegistry:{async complete(_model,request){
  requests.push(structuredClone(request));
  assert.ok(Buffer.byteLength(JSON.stringify(request))<=12_000-4096);
  if(requests.length===1)return response('toolUse',[toolCall('memory_search',{terms:['needle']},'search-"\\🧠')]);
  if(requests.length===2){
   const page=JSON.parse(request.messages.at(-1).content[0].text);
   assert.equal(page.status,'ok');assert.equal(page.hasMore,true);assert.ok(page.nextCursor);
   assert.equal(page.candidates[0].id,'00');
   return response('toolUse',[toolCall('memory_read',{id:'00',expectedRevision:target.revision},'read-1')]);
  }
  const read=JSON.parse(request.messages.at(-1).content[0].text);
  assert.equal(read.content,target.content);assert.equal(read.hasMore,false);
  return response('stop',JSON.stringify({operations:[{action:'update',id:'00',expectedRevision:target.revision,content:'needle: retain 21 days; 30 during incidents.',sourceKeys:['source-1']}]}));
 }}},store,new AbortController().signal);
 assert.equal(requests.length,3);assert.equal(changes.updates[0].content,'needle: retain 21 days; 30 during incidents.');
});

test('soft packing leaves useful retrieval room across model sizes without changing hard fit', async () => {
 for(const window of [12_000,32_000,128_000]){
  const selected=model(window);
  const batch=n=>sourceBatch([{key:'source-1',entryId:'entry-1',role:'user',text:'x'.repeat(n)}]);
  const maximum=options=>{
   let low=1,high=64*1024;
   while(low<high){const mid=Math.ceil((low+high)/2);if(fitsConsolidationRequest(batch(mid),selected,options))low=mid;else high=mid-1;}
   return low;
  };
  const hard=maximum({}),soft=maximum({reserveRetrieval:true});
  assert.ok(soft>0 && soft<hard);assert.ok(hard-soft<=8192);
  assert.equal(fitsConsolidationRequest(batch(hard),selected),true);
  assert.equal(fitsConsolidationRequest(batch(hard),selected,{reserveRetrieval:true}),false);
 }
});

test('several calls share the remaining turn capacity including escaped IDs and read provenance flags', async t => {
 const store=await fixture(t);
 store.save({id:'current',content:'needle 条件🧠\u0000\\\n" incident exception.',createdAt:'2026-10-23T00:00:00.000Z'});
 for(let i=0;i<20;i++)store.save({id:`z${i}`,content:'needle '.repeat(100),createdAt:'2026-10-23T00:00:00.000Z'});
 store.memoryOrigin=()=>({sessionId:'other',sourceKeys:['newer'],sourceTime:'2026-10-24T00:00:00.000Z'});
 let calls=0;
 const batch=sourceBatch([{key:'source-1',entryId:'entry-1',role:'user',text:'Old needle preference.'}]);
 const changes=await consolidateWithModel(batch,{model:model(14_000),modelRegistry:{async complete(_model,request){
  calls++;assert.ok(Buffer.byteLength(JSON.stringify(request))<=14_000-4096);
  if(calls===1)return response('toolUse',[
   toolCall('memory_search',{terms:['needle']},'search-"\\🧠'.repeat(10)),
   toolCall('memory_read',{id:'current',expectedRevision:1},'read-"\\🧠'.repeat(10)),
  ]);
  const results=request.messages.filter(message=>message.role==='toolResult');
  assert.equal(results.length,2);
  const search=JSON.parse(results[0].content[0].text),read=JSON.parse(results[1].content[0].text);
  assert.equal(search.status,'ok');assert.equal(search.hasMore,true);
  assert.equal(read.content,store.get('current').content);assert.equal(read.hasMore,false);
  assert.equal(read.newerEvidence,true);assert.equal(read.alternativeBranch,false);
  return response('stop','{"operations":[]}');
 }}},store,new AbortController().signal);
 assert.equal(calls,2);assert.deepEqual(changes,{creates:[],updates:[],deletes:[]});
});

test('oversized assistant tool arguments cannot dispatch another request or authorize changes', async t => {
 const store=await fixture(t);
 const batch=sourceBatch([{key:'source-1',entryId:'entry-1',role:'user',text:'A short source.'}]);
 let calls=0;
 await assert.rejects(consolidateWithModel(batch,{model:model(12_000),modelRegistry:{async complete(){
  calls++;
  return response('toolUse',[toolCall('memory_read',{id:'x'.repeat(10_000),expectedRevision:1},'oversized')]);
 }}},store,new AbortController().signal),failureReason('model_budget'));
 assert.equal(calls,1);
});
