import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildSessionProjection, SessionManager } from '@earendil-works/pi-coding-agent';
import { fauxAssistantMessage, fauxToolCall } from '@earendil-works/pi-ai';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { ConsolidationCoordinator } from '../../src/distill/coordinator.ts';
import { fitsConsolidationRequest } from '../../src/distill/model.ts';

const stamp = '2026-10-21T00:00:00.000Z';
function snapshot(root, suffix = '') {
  return { sessionId: 'synthetic-session', sessionFile: join(root, 'session.jsonl'), cwd: root, leafId: `a${suffix}`, entries: [
    { type: 'message', id: `u${suffix}`, parentId: null, timestamp: stamp, message: { role: 'user', content: 'For synthetic Cedar, use local tests.', timestamp: 1 } },
    { type: 'message', id: `a${suffix}`, parentId: `u${suffix}`, timestamp: stamp, message: { role: 'assistant', content: [{type:'text',text:'The Cedar convention is established.'}], timestamp: 2 } },
  ] };
}
function context(s) {
  return { model: { id: 'synthetic', contextWindow: 128_000, maxTokens: 4_096 }, modelRegistry: {}, sessionManager: {
    getSessionId: () => s.sessionId, getSessionFile: () => s.sessionFile, getCwd: () => s.cwd,
    getLeafId: () => s.leafId, getEntries: () => s.entries, getHeader: () => ({type:'session',id:s.sessionId,cwd:s.cwd,timestamp:stamp}),
    getBranch: () => s.entries, buildSessionProjection: () => buildSessionProjection(s.entries,s.leafId),
  }};
}
async function fixture(t, runModel, extra = {}) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-consolidator-'));
  const path = join(root, 'memory.sqlite');
  const s = snapshot(root);
  await writeFile(s.sessionFile, 'synthetic persisted session');
  const ctx = context(s);
  const c = new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(path), loadSnapshot:async()=>s, runModel, retryDelayMs:100_000, ...extra});
  t.after(async()=>{ await c.shutdown(ctx, 'reload'); await rm(root,{recursive:true,force:true}); });
  return {root,path,s,ctx,c};
}
test('successful compaction automatically saves ordinary conversation without a memory command', async t=>{
  let calls=0;
  const f=await fixture(t,async()=>{calls++;return {creates:[{id:'cedar',content:'Cedar uses local tests.',createdAt:stamp}],updates:[],deletes:[]};});
  f.c.beforeCompact(f.ctx);
  assert.equal(calls,0);
  f.c.afterCompact(f.ctx,'compact-record-1');
  await f.c.whenIdle();
  const store=new SQLiteMemoryStore(f.path);
  assert.equal(store.get('cedar')?.content,'Cedar uses local tests.');
  assert.equal(store.listConsolidationJobs().length,0);
  store.close();
  assert.equal(calls,1);
  f.c.beforeCompact(f.ctx); f.c.afterCompact(f.ctx,'compact-record-2'); await f.c.whenIdle();
  assert.equal(calls,1);
});
test('failed compaction does not trigger extraction and reload does not create new work',async t=>{
  let calls=0;
  const f=await fixture(t,async()=>{calls++;return {creates:[],updates:[],deletes:[]};});
  f.c.beforeCompact(f.ctx); f.c.compactFailed();
  await f.c.shutdown(f.ctx,'reload');
  assert.equal(calls,0);
});
test('session closure queues a frozen range; a later live Pi runtime consolidates it',async t=>{
  const f=await fixture(t,async()=>({creates:[],updates:[],deletes:[{id:'old',expectedRevision:1}]}));
  f.c.setForegroundBusy(true);
  const db=new SQLiteMemoryStore(f.path); db.save({id:'old',content:'Cedar uses remote tests.',createdAt:stamp}); db.close();
  await f.c.shutdown(f.ctx,'quit');
  const queued=new SQLiteMemoryStore(f.path);
  assert.equal(queued.get('old')?.content,'Cedar uses remote tests.');
  assert.equal(queued.listDistillTriggers({limit:20}).items[0]?.status,'pending');
  queued.close();
  let calls=0;
  const recovered=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async()=>f.s,runModel:async()=>{calls++;return {creates:[],updates:[],deletes:[{id:'old',expectedRevision:1}]};},retryDelayMs:100_000});
  await recovered.start(f.ctx); await recovered.whenIdle(); await recovered.shutdown(f.ctx,'reload');
  const read=new SQLiteMemoryStore(f.path); assert.equal(calls,1); assert.equal(read.get('old'),undefined); assert.equal(read.listConsolidationJobs().length,0); read.close();
});
test('shutdown fences an active model request immediately and leaves its range recoverable',async t=>{
  let started;
  const requestStarted=new Promise(resolve=>{started=resolve;});
  const f=await fixture(t,async(_b,_c,_s,signal)=>new Promise((_,reject)=>{
    started(); signal.addEventListener('abort',()=>reject(new Error('cancelled')),{once:true});
  }));
  f.c.beforeCompact(f.ctx); f.c.afterCompact(f.ctx,'compact-record-1');
  await requestStarted;
  const start=Date.now(); await f.c.shutdown(f.ctx,'quit'); assert.ok(Date.now()-start<500);
  const db=new SQLiteMemoryStore(f.path); assert.equal(db.listConsolidationJobs().length,1); db.close();
  const recovered=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async()=>f.s,runModel:async()=>({creates:[{id:'recovered',content:'Cedar uses local tests.',createdAt:stamp}],updates:[],deletes:[]}),retryDelayMs:100_000});
  await recovered.start(f.ctx); await recovered.whenIdle();
  await recovered.shutdown(f.ctx,'reload');
  const after=new SQLiteMemoryStore(f.path); assert.ok(after.get('recovered')); assert.equal(after.listConsolidationJobs().length,0); after.close();
});
test('invalid model output leaves all source work pending for retry',async t=>{
  const f=await fixture(t,async()=>{throw new Error('invalid model result');});
  f.c.beforeCompact(f.ctx); f.c.afterCompact(f.ctx,'compact-record-1'); await f.c.whenIdle();
  const db=new SQLiteMemoryStore(f.path); assert.equal(db.listConsolidationJobs().length,1); db.close();
});

test('startup recovers missed closure only for an inactive observed process',async t=>{
  let calls=0;
  const f=await fixture(t,async()=>{calls++;return {creates:[],updates:[],deletes:[]};});
  const db=new SQLiteMemoryStore(f.path);
  db.observeSession({sessionId:'closed',sessionFile:join(f.root,'closed.jsonl'),cwd:f.root,leafId:'a',ownerPid:2147483647});
  db.observeSession({sessionId:'active',sessionFile:join(f.root,'active.jsonl'),cwd:f.root,leafId:'a',ownerPid:process.pid});
  db.close();
  const closed={...f.s,sessionId:'closed',sessionFile:join(f.root,'closed.jsonl')};
  const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async path=>{
    assert.equal(path,closed.sessionFile);return closed;
  },runModel:async()=>{calls++;return {creates:[],updates:[],deletes:[]};},retryDelayMs:100000});
  await c.start(f.ctx);await c.whenIdle();await c.shutdown(f.ctx,'reload');assert.equal(calls,1);
});
test('already committed sources are not re-applied after the resulting memory is removed',async t=>{
 let calls=0;
 const f=await fixture(t,async()=>{calls++;return {creates:[{id:'temporary',content:'Cedar local convention.',createdAt:stamp}],updates:[],deletes:[]};});
 f.c.beforeCompact(f.ctx);f.c.afterCompact(f.ctx,'compact-record-1');await f.c.whenIdle();
 const db=new SQLiteMemoryStore(f.path);db.forget({id:'temporary',expectedRevision:1});db.close();
 await f.c.shutdown(f.ctx,'quit');
 const after=new SQLiteMemoryStore(f.path);assert.equal(after.get('temporary'),undefined);after.close();assert.equal(calls,1);
});
test('a provider that ignores cancellation cannot write after immediate shutdown fencing',async t=>{
 let resolve;
 const pending=new Promise(r=>{resolve=r;});
 const f=await fixture(t,()=>pending);
 f.c.beforeCompact(f.ctx);f.c.afterCompact(f.ctx,'compact-record-1');
 await new Promise(r=>setTimeout(r,20));
 await f.c.shutdown(f.ctx,'quit');
 resolve({creates:[{id:'late',content:'Must not be written.',createdAt:stamp}],updates:[],deletes:[]});
 await new Promise(r=>setTimeout(r,10));
 const db=new SQLiteMemoryStore(f.path);assert.equal(db.get('late'),undefined);assert.equal(db.listConsolidationJobs().length,1);db.close();
});
test('large conversation content is processed in bounded pieces without marking omitted text complete',async t=>{
 const seen=new Map();
 const f=await fixture(t,async (batch,ctx)=>{
   assert.equal(fitsConsolidationRequest(batch,ctx.model),true);
   assert.ok(Buffer.byteLength(JSON.stringify(batch.evidence))<=24*1024);
   assert.ok(Buffer.byteLength(JSON.stringify(batch.context))<=8*1024);
   for(const part of batch.evidence)seen.set(part.key,part.text);
   return {creates:[],updates:[],deletes:[]};
 });
 f.ctx.model.contextWindow=32768;
 const original='条件: fictional Cedar only. 🧠 '.repeat(2500);
 f.s.entries[0].message.content=original;
 for(let round=0;round<10;round++){
   f.c.beforeCompact(f.ctx);f.c.afterCompact(f.ctx,`compact-record-${round}`);await f.c.whenIdle();
   const db=new SQLiteMemoryStore(f.path);const pending=db.listConsolidationJobs().length;db.close();
   if(pending===0)break;
 }
 const reconstructed=[...seen.values()].filter(text=>text.includes('条件:')||text.includes('Cedar only')).join('');
 assert.equal(reconstructed,original);
 const db=new SQLiteMemoryStore(f.path);assert.equal(db.listConsolidationJobs().length,0);db.close();
});
test('a transient source-read failure during startup recovery retries without restarting Pi',async t=>{
 const f=await fixture(t,async()=>({creates:[],updates:[],deletes:[]}));
 const db=new SQLiteMemoryStore(f.path);db.observeSession({sessionId:'closed',sessionFile:join(f.root,'closed.jsonl'),cwd:f.root,leafId:'a',ownerPid:2147483647});db.close();
 const closed={...f.s,sessionId:'closed',sessionFile:join(f.root,'closed.jsonl')};
 let loads=0,calls=0,now=Date.now();
 const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async()=>{loads++;if(loads===1)throw new Error('transient read');return closed;},runModel:async()=>{calls++;return {creates:[],updates:[],deletes:[]};},retryDelayMs:10,now:()=>now});
 await c.start(f.ctx);await c.whenIdle();assert.equal(calls,0);now+=30000;
 for(let i=0;i<30&&calls===0;i++)await new Promise(resolve=>setTimeout(resolve,10));
 await c.shutdown(f.ctx,'reload');assert.equal(calls,1);assert.ok(loads>=2);
});
test('a 10k-context selected model processes 5KiB of source without permanent oversized jobs',async t=>{
 const {consolidateWithModel}=await import('../../src/distill/model.ts');
 const {fauxAssistantMessage}=await import('@earendil-works/pi-ai');
 let calls=0;
 const f=await fixture(t,consolidateWithModel);
 f.ctx.model.contextWindow=10000;f.ctx.model.maxTokens=4096;
 f.ctx.modelRegistry.complete=async(_model,request)=>{calls++;assert.ok(Buffer.byteLength(JSON.stringify(request))<=5904);return fauxAssistantMessage('{"operations":[]}');};
 f.s.entries[0].message.content='A'.repeat(5120);
 for(let round=0;round<10;round++){
  f.c.beforeCompact(f.ctx);f.c.afterCompact(f.ctx,`compact-record-${round}`);await f.c.whenIdle();
  const db=new SQLiteMemoryStore(f.path);const pending=db.listConsolidationJobs().length;db.close();
  if(pending===0)break;
 }
 assert.ok(calls>0);
 const db=new SQLiteMemoryStore(f.path);assert.equal(db.listConsolidationJobs().length,0);db.close();
});
test('recovery uses the observed branch checkpoint instead of the last appended sibling',async t=>{
 const f=await fixture(t,async()=>({creates:[],updates:[],deletes:[]}));
 const db=new SQLiteMemoryStore(f.path);db.observeSession({sessionId:'closed',sessionFile:join(f.root,'closed.jsonl'),cwd:f.root,leafId:'a',ownerPid:2147483647});db.close();
 const closed={...f.s,sessionId:'closed',sessionFile:join(f.root,'closed.jsonl')};let calls=0;
 const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async(_path,leaf)=>{if(leaf!==undefined)assert.equal(leaf,'a');return closed;},runModel:async()=>{calls++;return {creates:[],updates:[],deletes:[]};},retryDelayMs:100000});
 await c.start(f.ctx);await c.whenIdle();await c.shutdown(f.ctx,'reload');assert.equal(calls,1);
});
test('startup source recovery bounds each background pass and continues fairly',async t=>{
 const f=await fixture(t,async()=>({creates:[],updates:[],deletes:[]}));
 const db=new SQLiteMemoryStore(f.path);
 for(let i=0;i<10;i++)db.observeSession({sessionId:`closed-${i}`,sessionFile:join(f.root,`closed-${i}.jsonl`),cwd:f.root,leafId:'a',ownerPid:2147483647});db.close();
 let loads=0;
 const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(f.path),loadSnapshot:async path=>{loads++;const id=path.split('/').at(-1).replace('.jsonl','');return {...f.s,sessionId:id,sessionFile:path};},runModel:async()=>({creates:[],updates:[],deletes:[]}),retryDelayMs:100000});
 await c.start(f.ctx);await c.whenIdle();await c.shutdown(f.ctx,'reload');
 // Four recovery reads plus at most two model-job rehydration reads.
 assert.ok(loads<=6,`one pass performed ${loads} source reads`);
 const after=new SQLiteMemoryStore(f.path);assert.ok(after.observedSessions().length>=6);after.close();
});

test('context edits update their own branch while an unedited sibling cannot rewrite that memory', async t => {
 const root=await mkdtemp(join(tmpdir(),'pi-distill-edit-branch-'));
 const path=join(root,'memory.sqlite');
 const manager=SessionManager.create(root,join(root,'sessions'));
 const shared=manager.appendMessage({role:'user',content:'Original Cedar condition.',timestamp:1});
 const forkPoint=manager.appendMessage({role:'assistant',content:[{type:'text',text:'Shared Cedar context.'}],timestamp:2});
 manager.appendContextEdit(shared,{content:[{type:'text',text:'Corrected Cedar condition.'}]});
 let phase='create',id;
 const flags=[];
 const ctx={sessionManager:manager,model:{contextWindow:32768,maxTokens:4096},modelRegistry:{complete:async(_model,request)=>{
  const evidence=JSON.parse(request.messages[0].content).newEvidence;
  const sourceKeys=[evidence[0].key];
  if(phase==='create')return fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'Corrected Cedar condition.',sourceKeys}]}));
  const toolResult=request.messages.find(message=>message.role==='toolResult');
  const revision=phase==='update'?1:2;
  if(!toolResult)return fauxAssistantMessage(fauxToolCall('memory_read',{id,expectedRevision:revision}),{stopReason:'toolUse'});
  flags.push(JSON.parse(toolResult.content[0].text).alternativeBranch===true);
  return fauxAssistantMessage(JSON.stringify({operations:[{action:'update',id,expectedRevision:revision,content:phase==='update'?'Latest Cedar condition.':'Sibling must not overwrite Cedar.',sourceKeys}]}));
 }}};
 const c=new ConsolidationCoordinator({openStore:()=>new SQLiteMemoryStore(path),retryDelayMs:100000});
 t.after(async()=>{await c.shutdown(ctx,'reload');await rm(root,{recursive:true,force:true});});
 let compactId=0;
 const compact=async()=>{compactId++;c.beforeCompact(ctx);c.afterCompact(ctx,`compact-record-${compactId}`);await c.whenIdle();};
 await compact();
 const db=new SQLiteMemoryStore(path);t.after(()=>db.close());
 id=db.searchBatch({terms:['Cedar'],match:'any'})[0].memory.id;
 phase='update';
 manager.appendContextEdit(shared,{content:[{type:'text',text:'Latest Cedar condition.'}]});
 await compact();
 assert.equal(db.get(id).revision,2);
 assert.equal(db.get(id).content,'Latest Cedar condition.');
 phase='sibling';
 manager.branch(forkPoint);
 manager.appendMessage({role:'user',content:'The sibling still uses the original condition.',timestamp:3});
 await compact();
 assert.equal(db.get(id).revision,2);
 assert.equal(db.get(id).content,'Latest Cedar condition.');
 assert.deepEqual(flags,[false,true]);
 assert.equal(db.listConsolidationJobs().length,1);
});
