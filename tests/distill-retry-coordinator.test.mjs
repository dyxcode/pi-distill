import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { ConsolidationCoordinator } from '../src/consolidation.ts';
import { DistillFailure } from '../src/distill-failure.ts';

const stamp = '2026-10-21T00:00:00.000Z';
const empty = {creates:[],updates:[],deletes:[]};
async function fixture(t, runModel, load) {
  const root=await mkdtemp(join(tmpdir(),'pi-distill-retry-'));
  const file=join(root,'session.jsonl'), db=join(root,'memory.sqlite');
  const snapshot={sessionId:'synthetic',sessionFile:file,cwd:root,leafId:'a',entries:[
    {type:'message',id:'u',parentId:null,timestamp:stamp,message:{role:'user',content:'Cedar uses local tests.',timestamp:1}},
    {type:'message',id:'a',parentId:'u',timestamp:stamp,message:{role:'assistant',content:[{type:'text',text:'Understood.'}],timestamp:2}},
  ]};
  await writeFile(file,'synthetic source');
  let now=Date.now(); const workers=[];
  const ctx={model:{id:'synthetic',provider:'test',contextWindow:128000,maxTokens:4096},modelRegistry:{},sessionManager:{
    getSessionId:()=>snapshot.sessionId,getSessionFile:()=>file,getCwd:()=>root,getLeafId:()=>snapshot.leafId,
  }};
  function worker() {const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(db),loadSnapshot:async(...args)=>load?load(snapshot,...args):snapshot,runModel,retryDelayMs:100000,now:()=>now});workers.push(c);return c;}
  function read(fn){const s=new SQLiteMemoryStore(db);try{return fn(s);}finally{s.close();}}
  const c=worker();
  t.after(async()=>{for(const w of workers)await w.shutdown(ctx,'reload');await rm(root,{recursive:true,force:true});});
  return {c,ctx,file,db,snapshot,worker,read,advance:ms=>now+=ms,wake:async(w=c)=>{await w.start(ctx);await w.whenIdle();},enqueue:async()=>{c.beforeCompact(ctx);c.afterCompact(ctx,'compaction');await c.whenIdle();}};
}
test('model failure cooldown survives wake and a new worker, then automatically probes',async t=>{
  let calls=0, fail=true;
  const f=await fixture(t,async()=>{calls++;if(fail)throw new DistillFailure('model_invalid','synthetic');return empty;});
  await f.enqueue(); assert.equal(calls,1);
  await f.wake(); assert.equal(calls,1);
  f.advance(30000);await f.wake();assert.equal(calls,2);
  await writeFile(f.file,'new ordinary message after frozen boundary');await f.wake();assert.equal(calls,2);
  await f.c.shutdown(f.ctx,'reload');const other=f.worker();
  f.advance(30000);await f.wake(other);assert.equal(calls,2);
  assert.equal(f.read(s=>s.listConsolidationJobs().length),1);
  f.advance(6*60*60*1000);fail=false;await f.wake(other);assert.equal(calls,3);
  assert.equal(f.read(s=>s.listConsolidationJobs().length),0);
});
test('unchanged source validation failure skips parsing across restart, repaired file resumes',async t=>{
  let reads=0, broken=true,calls=0;
  const f=await fixture(t,async()=>{calls++;return empty;},s=>{reads++;if(broken)throw new DistillFailure('source_invalid','synthetic');return s;});
  await f.enqueue();assert.equal(reads,1);
  f.advance(86400000);await f.wake();assert.equal(reads,1);
  await f.c.shutdown(f.ctx,'reload');const other=f.worker();await f.wake(other);assert.equal(reads,1);
  broken=false;await writeFile(f.file,'repaired synthetic source');await f.wake(other);
  assert.equal(calls,1);assert.equal(f.read(s=>s.listConsolidationJobs().length),0);
});
test('model capability change reconsiders an unavailable model without waiting for restart',async t=>{
  let calls=0;
  const f=await fixture(t,async(_b,ctx)=>{calls++;if(ctx.model.id==='synthetic')throw new DistillFailure('model_unavailable','synthetic');return empty;});
  await f.enqueue();await f.wake();assert.equal(calls,1);
  f.ctx.model={...f.ctx.model,id:'capable'};await f.wake();assert.equal(calls,2);
  assert.equal(f.read(s=>s.listConsolidationJobs().length),0);
});
test('transient retry delays persist without marking evidence processed',async t=>{
  let calls=0;
  const f=await fixture(t,async()=>{calls++;throw new Error('synthetic transient');});
  await f.enqueue();f.advance(30000);await f.wake();assert.equal(calls,2);
  f.advance(30000);await f.wake();assert.equal(calls,2);
  f.advance(90000);await f.wake();assert.equal(calls,3);
  const job=f.read(s=>s.listConsolidationJobs()[0]);
  assert.equal(f.read(s=>s.processedSources(job.sourceKeys).length),0);
});
test('crash recovery of an invalid observed source does not bypass the source hold',async t=>{
  let reads=0,broken=true,calls=0;
  const f=await fixture(t,async()=>{calls++;return empty;},s=>{reads++;if(broken)throw new DistillFailure('source_invalid','synthetic');return s;});
  f.read(s=>s.observeSession({sessionId:f.snapshot.sessionId,sessionFile:f.file,cwd:f.snapshot.cwd,leafId:'a',ownerPid:2147483647}));
  await f.wake();const first=reads;assert.ok(first>0);
  await f.wake();assert.equal(reads,first);
  await f.c.shutdown(f.ctx,'reload');const other=f.worker();await f.wake(other);assert.equal(reads,first);
  broken=false;await writeFile(f.file,'repaired observed source');await f.wake(other);assert.equal(calls,1);
});
test('unconfirmed compaction recovery waits for source change after invalid source',async t=>{
  let reads=0;
  const f=await fixture(t,async()=>empty);
  f.read(s=>s.enqueueDistillTrigger({id:'candidate',sessionId:f.snapshot.sessionId,sessionFile:f.file,cwd:f.snapshot.cwd,leafId:'a',reason:'compact'}));
  const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.db),loadCompactionRecord:async()=>{reads++;throw new DistillFailure('source_invalid','synthetic');},retryDelayMs:100000});
  t.after(()=>c.shutdown(f.ctx,'reload'));
  await c.start(f.ctx);await c.whenIdle();assert.equal(reads,1);
  await c.start(f.ctx);await c.whenIdle();assert.equal(reads,1);
  await writeFile(f.file,'changed candidate source');await c.start(f.ctx);await c.whenIdle();assert.equal(reads,2);
});
test('a held source does not prevent another source from completing',async t=>{
  const seen=[];let other;
  const f=await fixture(t,async batch=>{seen.push(batch.sessionId);if(batch.sessionId==='synthetic')throw new DistillFailure('model_invalid','synthetic');return empty;},(s,path)=>path===other?.sessionFile?other:s);
  await f.enqueue();
  other={...f.snapshot,sessionId:'other',sessionFile:join(f.snapshot.cwd,'other.jsonl')};
  await writeFile(other.sessionFile,'other source');
  f.read(s=>s.enqueueDistillTrigger({id:'other',sessionId:other.sessionId,sessionFile:other.sessionFile,cwd:other.cwd,leafId:other.leafId,reason:'close'}));
  await f.wake();
  assert.deepEqual(seen,['synthetic','other']);
  assert.deepEqual(f.read(s=>s.listConsolidationJobs().map(job=>job.sessionId)),['synthetic']);
});
test('shutdown cancellation releases ownership without recording a failed model attempt',async t=>{
  let started;const ready=new Promise(resolve=>started=resolve);
  const f=await fixture(t,async(_b,_c,_s,signal)=>{started();return new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}));});
  f.c.beforeCompact(f.ctx);f.c.afterCompact(f.ctx,'compaction');await ready;
  await f.c.shutdown(f.ctx,'reload');await f.c.whenIdle();
  const job=f.read(s=>s.listConsolidationJobs()[0]);assert.ok(job);
  assert.equal(f.read(s=>s.readDistillFailure('job',job.id)),undefined);
  assert.deepEqual(f.read(s=>s.processedSources(job.sourceKeys)),[]);
});
