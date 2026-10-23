import {fork} from 'node:child_process';
import {once} from 'node:events';
import {DatabaseSync} from 'node:sqlite';
import assert from 'node:assert/strict';
import { link, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { loadSnapshot, planSources } from '../src/consolidation-source.ts';
import { CleanupCoordinator } from '../src/cleanup.ts';
import { DistillFailure } from '../src/distill-failure.ts';
import { readSessionHeader } from '../src/session-header.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-clean-'));
  const path = join(root, 'memory.sqlite');
  const file = join(root, 'session.jsonl');
  const manager = SessionManager.inMemory(root);
  manager.appendMessage({role:'user',content:'Synthetic Cedar uses local tests.',timestamp:1});
  const save = () => writeFile(file, [manager.getHeader(), ...manager.getEntries()].map(JSON.stringify).join('\n')+'\n');
  await save();
  const store = new SQLiteMemoryStore(path);
  const workers = [];
  t.after(async () => { for (const worker of workers) worker.close(); store.close(); await rm(root,{recursive:true,force:true}); });
  const input = () => ({sessionId:manager.getSessionId(),sessionFile:file,leafId:manager.getLeafId(),cwd:root,reason:'close',intentId:'fixture'});
  const worker = (options={}) => {const c = new CleanupCoordinator({openStore:()=>new SQLiteMemoryStore(path),retryDelayMs:100_000,...options});workers.push(c);return c;};
  const task = () => store.listCleanTasks({limit:10}).items.find(item=>item.sessionFile===file);
  async function complete() {
    for (const {evidence,context,...job} of planSources(await loadSnapshot(file),new Set())) {
      store.enqueueConsolidation(job);
      assert.equal(store.claimConsolidation(job.id,'fixture',Date.now(),60_000),true);
      store.commitConsolidation({jobId:job.id,owner:'fixture',now:Date.now(),creates:[],updates:[],deletes:[]});
      store.releaseConsolidation(job.id,'fixture');
    }
    const {reason,intentId,...source}=input();
    store.saveSessionUsage({ownerId:'fixture',processToken:'fixture',ownerPid:process.pid,state:'active',...source});
  }
  return {root,path,file,manager,save,store,worker,task,input,complete};
}

test('clean needs no model and records unprocessed work while retaining exact source bytes',async t=>{
  const f=await fixture(t); f.store.enqueueCleanTask(f.input());
  const before=await readFile(f.file); const c=f.worker(); c.wake(); await c.whenIdle();
  assert.equal(f.task().status,'waiting'); assert.equal(f.task().waitReason,'unprocessed');
  assert.deepEqual(await readFile(f.file),before);
});

test('processed and released source is automatically deleted without changing memories',async t=>{
  const f=await fixture(t); await f.complete();
  const owner=f.store.listSessionUsages()[0];
  f.store.saveSessionUsage({...owner,state:'closing'}); f.store.saveSessionUsage({...owner,state:'released'});
  f.store.enqueueCleanTask(f.input());
  const c=f.worker(); c.wake(); await c.whenIdle();
  await assert.rejects(readFile(f.file), {code:'ENOENT'});
  assert.equal(f.task(),undefined);
  c.wake(); await c.whenIdle();
  assert.equal(f.task(),undefined);
});

test('unreadable sources remain waiting and a later wake retries after repair',async t=>{
  const f=await fixture(t); f.store.enqueueCleanTask(f.input());
  await writeFile(f.file,'broken synthetic data'); const c=f.worker(); c.wake(); await c.whenIdle();
  assert.equal(f.task().waitReason,'unavailable');
  await f.save(); c.wake(); await c.whenIdle(); assert.equal(f.task().waitReason,'unprocessed');
});

test('unreadable known dependencies prevent an otherwise processed source from reaching host-only waiting',async t=>{
  const f=await fixture(t);await f.complete();
  const owner=f.store.listSessionUsages()[0];
  f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
  f.store.enqueueCleanTask(f.input());
  f.store.enqueueCleanTask({...f.input(),sessionFile:join(f.root,'unavailable-known-session.jsonl'),intentId:'other'});
  const c=f.worker();c.wake();await c.whenIdle();
  assert.equal(f.task().waitReason,'dependencies');
});

test('clean queue advances beyond one bounded pass without starving later tasks',async t=>{
  const f=await fixture(t);
  for(let n=0;n<11;n++)f.store.enqueueCleanTask({...f.input(),sessionFile:join(f.root,`missing-${n}.jsonl`)});
  const c=f.worker();
  for(let n=0;n<3;n++){c.wake();await c.whenIdle();}
  const tasks=f.store.listCleanTasks({limit:20}).items;
  assert.equal(tasks.length,11);assert.ok(tasks.every(task=>task.status==='waiting'&&task.waitReason==='unavailable'));
});


test('new unprocessed content keeps a previously processed released source',async t=>{
  const f=await fixture(t);await f.complete();
  const owner=f.store.listSessionUsages()[0];
  f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
  f.manager.appendMessage({role:'user',content:'Now use integration tests too.',timestamp:2});await f.save();
  f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
  assert.equal(f.task().waitReason,'unprocessed');assert.ok(await readFile(f.file));
});

test('a live terminal keeps its processed source even while another worker cleans',async t=>{
  const f=await fixture(t);await f.complete();f.store.enqueueCleanTask(f.input());
  const before=await readFile(f.file);const c=f.worker();c.wake();await c.whenIdle();
  assert.equal(f.task().waitReason,'usage');assert.deepEqual(await readFile(f.file),before);
});

async function releaseFixture(f) {
 const owner=f.store.listSessionUsages()[0];
 f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
}

test('deletion does not change an independently stored memory',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 f.store.save({id:'remember',content:'Synthetic Cedar uses local tests.',createdAt:'2026-10-23T00:00:00.000Z'});
 const before=f.store.get('remember');f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});assert.deepEqual(f.store.get('remember'),before);
});

test('a source access reservation prevents cleanup until the actual read finishes',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const access=f.store.acquireSourceAccess();const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
 f.store.releaseSourceAccess(access);c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('a replacement file is retained when recovering an earlier deletion intent',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const access=f.store.acquireSourceAccess();f.store.claimCleanTask(task.id,'intent',Date.now(),60_000);
 f.store.prepareDeletion(task.id,'intent',access,f.store.retentionVersion(),'1:2:3:4:5');
 f.store.releaseCleanTask(task.id,'intent');f.store.releaseSourceAccess(access);
 const bytes=await readFile(f.file);const c=f.worker();c.wake();await c.whenIdle();assert.deepEqual(await readFile(f.file),bytes);
 assert.equal(f.store.getDeletion(f.file).state,'deleting');
});

test('cleanup removes processed children before their processed ancestor',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const child=SessionManager.forkFrom(f.file,f.root,join(f.root,'forks'));
 const file=child.getSessionFile();
 for(const {evidence,context,...job} of planSources(await loadSnapshot(file),new Set())) {
  f.store.enqueueConsolidation(job);f.store.claimConsolidation(job.id,'child',Date.now(),60_000);
  f.store.commitConsolidation({jobId:job.id,owner:'child',now:Date.now(),creates:[],updates:[],deletes:[]});f.store.releaseConsolidation(job.id,'child');
 }
 const source={sessionId:child.getSessionId(),sessionFile:file,cwd:f.root,leafId:child.getLeafId()};
 const owner={...source,ownerId:'fork',processToken:'fork',ownerPid:process.pid};
 f.store.saveSessionUsage({...owner,state:'active'});f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
 f.store.enqueueCleanTask({...source,reason:'close',intentId:'fork'});
 const c=f.worker();c.wake();await c.whenIdle();await assert.rejects(readFile(file),{code:'ENOENT'});
 c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('a dependency beyond the first discovery page protects its ancestor',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 for(let i=0;i<130;i++) {
  const file=join(f.root,`a-${i}.jsonl`);
  const manager=SessionManager.inMemory(f.root);
  await writeFile(file,JSON.stringify(manager.getHeader())+'\n');
  f.store.saveSessionUsage({sessionId:manager.getSessionId(),sessionFile:file,cwd:f.root,leafId:null,ownerId:`owner-${i}`,processToken:'live',ownerPid:process.pid,state:'active'});
 }
 const child=SessionManager.forkFrom(f.file,f.root,join(f.root,'z-forks'));
 f.store.saveSessionUsage({sessionId:child.getSessionId(),sessionFile:child.getSessionFile(),cwd:f.root,leafId:child.getLeafId(),ownerId:'child',processToken:'live',ownerPid:process.pid,state:'active'});
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'dependencies');
});


function fixtureChild(t,f,mode,task) {
 const child=fork(new URL('./retention-process-child.mjs',import.meta.url),[mode,f.path,JSON.stringify(task)],{stdio:['ignore','ignore','pipe','ipc']});
 let stderr='';child.stderr.on('data',data=>{stderr+=data;});
 t.after(()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');});
 return {child,stderr:()=>stderr};
}

test('real process usage and source access survive pause, then recover after confirmed exit',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const {child}=fixtureChild(t,f,'hold',task);await once(child,'message');
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
 child.kill('SIGSTOP');c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
 const exited=once(child,'exit');child.kill('SIGKILL');await exited;
 c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
 assert.equal(f.store.listSessionUsages().find(owner=>owner.ownerId==='child'),undefined,
   'confirmed release and deletion allow maintenance to reclaim the departed owner');
 assert.equal(f.store.getDeletion(f.file).state,'complete');
});

test('actual process death after unlink recovers completion without recreating the source',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const {child,stderr}=fixtureChild(t,f,'crash-delete',task);
 const [code,signal]=await once(child,'exit');assert.equal(signal,'SIGKILL',stderr());assert.equal(code,null);
 assert.equal(f.store.getDeletion(f.file).state,'deleting');await assert.rejects(readFile(f.file),{code:'ENOENT'});
 const c=f.worker();const deadline=Date.now()+1_000;
 while(f.task()&&Date.now()<deadline){c.wake();await c.whenIdle();if(f.task())await new Promise(resolve=>setTimeout(resolve,10));}
 assert.equal(f.store.getDeletion(f.file).state,'complete');assert.equal(f.task(),undefined);
 c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('concurrent cleanup coordinators complete each source at most once',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const a=f.worker(),b=f.worker();a.wake();b.wake();await Promise.all([a.whenIdle(),b.whenIdle()]);
 await assert.rejects(readFile(f.file),{code:'ENOENT'});assert.equal(f.store.getDeletion(f.file).state,'complete');
});

test('a changed same-file intent can be reassessed after the new content is processed',async t=>{
 const {lstatSync}=await import('node:fs');
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const info=lstatSync(f.file,{bigint:true});const version=[info.dev,info.ino,info.size,info.mtimeNs,info.ctimeNs].join(':');
 const access=f.store.acquireSourceAccess();f.store.claimCleanTask(task.id,'old-intent',Date.now(),60_000);
 f.store.prepareDeletion(task.id,'old-intent',access,f.store.retentionVersion(),version);
 f.store.releaseCleanTask(task.id,'old-intent');f.store.releaseSourceAccess(access);
 f.manager.appendMessage({role:'user',content:'New synthetic conclusion after the old assessment.',timestamp:3});await f.save();
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
 const snapshot=await loadSnapshot(f.file);const allKeys=planSources(snapshot,new Set()).flatMap(job=>job.sourceKeys);
 for(const {evidence,context,...job} of planSources(snapshot,new Set(f.store.processedSources(allKeys)))) {
  f.store.enqueueConsolidation(job);f.store.claimConsolidation(job.id,'new-range',Date.now(),60_000);
  f.store.commitConsolidation({jobId:job.id,owner:'new-range',now:Date.now(),creates:[],updates:[],deletes:[]});f.store.releaseConsolidation(job.id,'new-range');
 }
 f.store.enqueueCleanTask({...f.input(),intentId:'new-range'});c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('an exited empty session that never created a file does not block old-source cleanup',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const unused={sessionId:'empty',sessionFile:join(f.root,'never-written.jsonl'),cwd:f.root,leafId:'model-setting',ownerPid:process.pid,ownerId:'empty',processToken:'empty'};
 f.store.saveSessionUsage({...unused,state:'active'});f.store.saveSessionUsage({...unused,state:'closing'});f.store.saveSessionUsage({...unused,state:'released'});
 f.store.reserveSessionSource({sessionId:unused.sessionId,sessionFile:unused.sessionFile,cwd:unused.cwd});
 const c=f.worker();c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('an uncaptured close boundary must be recovered before its processed source is deleted',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const {reason,intentId,...source}=f.input();f.store.observeSession({...source,ownerPid:process.pid});
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'unprocessed');
 f.store.acknowledgeObservation(source.sessionId,source.leafId);c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

function registerHeader(store, sessionFile, sessionId, cwd) {
 const header=readSessionHeader(sessionFile);assert.equal(header.state,'known');assert.equal(header.sessionId,sessionId);
 store.sources.register({sessionId,sessionFile,cwd});
 store.sources.recordHeader({sessionFile,sessionId,fileVersion:header.fileVersion,...(header.parentSession===undefined?{}:{parentSession:header.parentSession})});
}

test('a malformed managed header leaves global topology unknown and blocks cleanup',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 const malformed=join(f.root,'malformed-header.jsonl');await writeFile(malformed,'not a Pi session header\n');
 f.store.sources.register({sessionId:'malformed',sessionFile:malformed,cwd:f.root});
 f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
 assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'dependencies');
 assert.equal(f.store.sources.isTopologyComplete(),false);
});

test('a child with a valid header but damaged body still protects its parent',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 registerHeader(f.store,f.file,f.manager.getSessionId(),f.root);
 const child=join(f.root,'fork-child.jsonl');
 const childId='damaged-child';
 await writeFile(child,JSON.stringify({type:'session',id:childId,timestamp:'2026-10-23T00:00:00.000Z',cwd:f.root,parentSession:'session.jsonl'})+'\n{damaged body\n');
 f.store.sources.register({sessionId:childId,sessionFile:child,cwd:f.root});
 f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
 assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'dependencies');
 assert.deepEqual(f.store.sources.reverseDependencies(f.file),[child]);
});

test('an unrelated oversized body does not consume the clean target source budget',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 const unrelated=join(f.root,'oversized.jsonl');
 await writeFile(unrelated,JSON.stringify({type:'session',id:'oversized',timestamp:'2026-10-23T00:00:00.000Z',cwd:f.root})+'\n'+
   JSON.stringify({type:'message',id:'too-large',payload:'x'.repeat(17*1024*1024)})+'\n');
 f.store.sources.register({sessionId:'oversized',sessionFile:unrelated,cwd:f.root});
 const c=f.worker();c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
 assert.ok((await readFile(unrelated)).length>16*1024*1024);
 assert.equal(f.store.getDeletion(unrelated),undefined);
});

test('a manager-reserved missing fork still protects its registered parent',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 registerHeader(f.store,f.file,f.manager.getSessionId(),f.root);
 const child=join(f.root,'reserved-fork.jsonl');
 f.store.reserveSessionSource({sessionId:'reserved-fork',sessionFile:child,cwd:f.root,parentSession:'session.jsonl'});
 f.store.enqueueCleanTask({sessionId:'reserved-fork',sessionFile:child,leafId:null,cwd:f.root,reason:'close',intentId:'reserved-close'});
 f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
 assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'dependencies');
 assert.equal(f.store.sources.get(child).reserved,true);
 assert.equal(f.store.sources.isTopologyComplete(),true);
 assert.equal(f.store.listCleanTasks({limit:10}).items.find(task=>task.sessionFile===child).waitReason,'unavailable');
 assert.deepEqual(f.store.sources.reverseDependencies(f.file),[child]);
});

test('a fork registered after dependency discovery invalidates the cleanup scope before assessment',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 registerHeader(f.store,f.file,f.manager.getSessionId(),f.root);
 f.store.enqueueCleanTask(f.input());
 const child=join(f.root,'late-reserved-fork.jsonl');
 const concurrentStore=new SQLiteMemoryStore(f.path);t.after(()=>concurrentStore.close());
 let interleaved=false;
 const c=new CleanupCoordinator({openStore:()=>{
  const workerStore=new SQLiteMemoryStore(f.path);
  const registry=workerStore.sources;
  const hasDependents=registry.hasDependents.bind(registry);
  registry.hasDependents=path=>{
   const result=hasDependents(path);
   if(path===f.file&&!result&&!interleaved){
    interleaved=true;
    concurrentStore.reserveSessionSource({sessionId:'late-reserved-fork',sessionFile:child,cwd:f.root,parentSession:basename(f.file)});
   }
   return result;
  };
  return workerStore;
 },retryDelayMs:100_000});
 t.after(()=>c.close());c.wake();await c.whenIdle();
 assert.equal(interleaved,true);
 assert.ok(await readFile(f.file));
 assert.equal(f.task().waitReason,'dependencies');
 assert.equal(concurrentStore.sources.get(child).reserved,true);
 assert.equal(concurrentStore.sources.hasDependents(f.file),true);
});

test('cleanup assesses one source chain when more than 1024 unrelated headers are registered',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 registerHeader(f.store,f.file,f.manager.getSessionId(),f.root);
 for(let i=0;i<1025;i++) {
  const sessionId=`unrelated-${i}`;const file=join(f.root,`${sessionId}.jsonl`);
  await writeFile(file,JSON.stringify({type:'session',id:sessionId,timestamp:'2026-10-23T00:00:00.000Z',cwd:f.root})+'\n');
  registerHeader(f.store,file,sessionId,f.root);
 }
 f.store.enqueueCleanTask(f.input());const c=f.worker();c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('hardlink aliases share active usage protection without reciprocal dependencies',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 const alias=join(f.root,'session-alias.jsonl');await link(f.file,alias);
 const sessionId=f.manager.getSessionId();registerHeader(f.store,alias,sessionId,f.root);
 const owner={sessionId,sessionFile:alias,cwd:f.root,leafId:f.manager.getLeafId(),ownerId:'alias-owner',processToken:'alias-owner',ownerPid:process.pid};
 f.store.saveSessionUsage({...owner,state:'active'});
 const aliasTask={sessionId,sessionFile:alias,leafId:owner.leafId,cwd:f.root,reason:'close',intentId:'alias-clean'};
 f.store.enqueueCleanTask(aliasTask);
 f.store.enqueueCleanTask(f.input());
 assert.deepEqual(f.store.sources.reverseDependencies(f.file),[]);
 assert.deepEqual(f.store.sources.reverseDependencies(alias),[]);
 const c=f.worker();c.wake();await c.whenIdle();
 assert.ok(await readFile(f.file));assert.ok(await readFile(alias));
 const waiting=f.store.listCleanTasks({limit:10}).items;
 assert.equal(waiting.length,2);
 assert.ok(waiting.every(task=>task.waitReason==='usage'));

 f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
 c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
 await assert.rejects(readFile(alias),{code:'ENOENT'});
 assert.equal(f.store.getDeletion(f.file).state,'complete');
 assert.equal(f.store.getDeletion(alias).state,'complete');
});

test('an active usage registered through a hardlink alias protects only the target clean task',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);
 const alias=join(f.root,'session-alias.jsonl');await link(f.file,alias);
 const sessionId=f.manager.getSessionId();registerHeader(f.store,alias,sessionId,f.root);
 const owner={sessionId,sessionFile:alias,cwd:f.root,leafId:f.manager.getLeafId(),ownerId:'alias-owner',processToken:'alias-owner',ownerPid:process.pid};
 f.store.saveSessionUsage({...owner,state:'active'});
 f.store.enqueueCleanTask(f.input());
 assert.deepEqual(f.store.sources.reverseDependencies(f.file),[]);
 assert.deepEqual(f.store.sources.reverseDependencies(alias),[]);
 const c=f.worker();c.wake();await c.whenIdle();
 assert.ok(await readFile(f.file));assert.ok(await readFile(alias));
 assert.equal(f.task().waitReason,'usage');
 assert.equal(f.store.listCleanTasks({limit:10}).items.length,1);

 f.store.saveSessionUsage({...owner,state:'closing'});f.store.saveSessionUsage({...owner,state:'released'});
 c.wake();await c.whenIdle();
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
 assert.ok(await readFile(alias));
 assert.equal(f.store.getDeletion(f.file).state,'complete');
 assert.equal(f.store.getDeletion(alias),undefined);
});


test('two actual Pi-side worker processes coordinate one deletion',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const a=fixtureChild(t,f,'clean',task),b=fixtureChild(t,f,'clean',task);
 await Promise.all([once(a.child,'message'),once(b.child,'message')]);
 const doneA=once(a.child,'message'),doneB=once(b.child,'message');
 a.child.send({go:true});b.child.send({go:true});
 assert.deepEqual((await doneA)[0],{done:true,completed:true});
 assert.deepEqual((await doneB)[0],{done:true,completed:true});
 const fileState=await readFile(f.file).then(()=> 'present',error=>error.code);
 assert.equal(fileState,'ENOENT',JSON.stringify({task:f.task(),deletion:f.store.getDeletion(f.file),stderrA:a.stderr(),stderrB:b.stderr()}));
 assert.equal(f.store.getDeletion(f.file).state,'complete');
});

test('cleanup automatically resumes after a source reservation is released without another wake', async t => {
  const f = await fixture(t); await f.complete(); await releaseFixture(f);
  const access = f.store.acquireSourceAccess(); assert.ok(access);
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c = f.worker({retryDelayMs:undefined}); c.wake(); await c.whenIdle();
  assert.ok(await readFile(f.file));
  f.store.releaseSourceAccess(access);
  t.mock.timers.tick(100); await c.whenIdle();
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('a held reservation uses bounded backoff and a later release resumes cleanup without host activity', async t => {
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  const access=f.store.acquireSourceAccess();let probes=0;
  class ObservedStore extends SQLiteMemoryStore { acquireSourceAccess(){probes++;return super.acquireSourceAccess();} }
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined,openStore:()=>new ObservedStore(f.path)});c.wake();await c.whenIdle();
  const initial=probes;
  t.mock.timers.tick(100);await c.whenIdle();assert.ok(probes>initial,'temporary contention must retry promptly');
  const afterFirst=probes;
  t.mock.timers.tick(100);await c.whenIdle();assert.equal(probes,afterFirst,'the second retry backs off');
  for(let i=0;i<30;i++){t.mock.timers.tick(100);await c.whenIdle();}
  assert.ok(probes<40,'held access must not cause a tight poll loop');assert.ok(await readFile(f.file));
  f.store.releaseSourceAccess(access);t.mock.timers.tick(5000);await c.whenIdle();
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('a wake bypassing an old assessment survives temporary contention without a second wake',async t=>{
  const f=await fixture(t);await f.complete();
  const previous=f.worker();previous.wake();await previous.whenIdle();previous.close();
  assert.equal(f.task().waitReason,'usage');await releaseFixture(f);
  const access=f.store.acquireSourceAccess();
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();
  f.store.releaseSourceAccess(access);
  for(let i=0;i<10;i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('temporary source projection conflicts retry automatically without marking the source unavailable',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  let conflicts=0;
  class ContendedStore extends SQLiteMemoryStore {
    get sourceCoverage(){
      const coverage=super.sourceCoverage,advance=coverage.advance.bind(coverage);
      coverage.advance=async(...args)=>{
        if(conflicts++<3)throw new DistillFailure('conflict','Another source projection writer committed first.');
        return advance(...args);
      };
      return coverage;
    }
  }
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined,openStore:()=>new ContendedStore(f.path)});c.wake();await c.whenIdle();
  assert.notEqual(f.task()?.waitReason,'unavailable','a concurrent writer is not an invalid source');
  for(let i=0;i<20;i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('a temporary SQLite writer lock resumes maintenance after release without a second wake',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  const workerStore=new SQLiteMemoryStore(f.path),writer=new DatabaseSync(f.path);
  writer.exec('BEGIN IMMEDIATE');t.after(()=>writer.close());
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined,openStore:()=>workerStore});c.wake();await c.whenIdle();
  assert.ok(await readFile(f.file));writer.exec('ROLLBACK');
  for(let i=0;i<10;i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('one wake examines later clean pages automatically and then stops fast polling blocked sources', async t => {
  const f=await fixture(t);
  for(let i=0;i<11;i++)f.store.enqueueCleanTask({...f.input(),sessionFile:join(f.root,`missing-${i}.jsonl`)});
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();
  for(let i=0;i<20;i++){t.mock.timers.tick(100);await c.whenIdle();}
  const before=f.store.listCleanTasks({limit:20}).items;
  assert.ok(before.every(x=>x.status==='waiting'),'all bounded pages must be visited without extra wakes');
  for(let i=0;i<20;i++){t.mock.timers.tick(100);await c.whenIdle();}
  assert.deepEqual(f.store.listCleanTasks({limit:20}).items,before,'blocked tasks must not be reassessed on every short tick');
});

test('one wake deletes eligible sources beyond the clean page limit without changing memories',async t=>{
  const f=await fixture(t),files=[];
  f.store.save({id:'retained',content:'Synthetic Cedar keeps local tests.',createdAt:'2026-10-23T00:00:00.000Z'});
  for(let i=0;i<11;i++){
    const manager=SessionManager.inMemory(f.root),file=join(f.root,`ready-${i}.jsonl`);files.push(file);
    await writeFile(file,JSON.stringify(manager.getHeader())+'\n');
    const source={sessionId:manager.getSessionId(),sessionFile:file,cwd:f.root,leafId:null};
    for(const state of ['active','closing','released'])f.store.saveSessionUsage({...source,ownerId:`ready-${i}`,processToken:`ready-${i}`,ownerPid:process.pid,state});
    f.store.enqueueCleanTask({...source,reason:'close',intentId:`ready-${i}`});
  }
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();
  assert.ok(await readFile(files.at(-1)),'the first pass must leave later work for a bounded continuation');
  for(let i=0;i<20;i++){t.mock.timers.tick(100);await c.whenIdle();}
  for(const file of files)await assert.rejects(readFile(file),{code:'ENOENT'});
  assert.equal(f.store.get('retained').content,'Synthetic Cedar keeps local tests.');
});

test('header discovery automatically crosses a page boundary and reevaluates the now-known clean source', async t => {
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  for(let i=0;i<130;i++){
    const manager=SessionManager.inMemory(f.root),file=join(f.root,`header-${i}.jsonl`);
    await writeFile(file,JSON.stringify(manager.getHeader())+'\n');
    f.store.sources.register({sessionId:manager.getSessionId(),sessionFile:file,cwd:f.root});
  }
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();
  for(let i=0;i<20&&f.task();i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('an empty clean queue still drains metadata pages automatically and then settles', async t => {
  const f=await fixture(t);
  for(let i=0;i<180;i++){
    const job={id:`gc-${i}`,sessionId:`gc-${i}`,sessionFile:null,cwd:f.root,leafId:null,sourceKeys:[`key-${i}`]};
    f.store.enqueueConsolidation(job);f.store.claimConsolidation(job.id,'gc',1,100);
    f.store.commitConsolidation({jobId:job.id,owner:'gc',now:2,creates:[],updates:[],deletes:[]});
  }
  let removed=0,passes=0;
  class ObservedStore extends SQLiteMemoryStore { collectMetadata(limit){passes++;const result=super.collectMetadata(limit);removed+=result.removed;return result;} }
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined,openStore:()=>new ObservedStore(f.path)});c.wake();await c.whenIdle();
  for(let i=0;i<60;i++){t.mock.timers.tick(100);await c.whenIdle();}
  assert.ok(removed>=180,'unreferenced evidence must be reclaimed beyond the first page');
  const settled=passes;
  for(let i=0;i<20;i++){t.mock.timers.tick(100);await c.whenIdle();}
  assert.equal(passes,settled,'retained jobs must not keep the completed sweep in a fast loop');
});

test('automatic child cleanup makes its parent eligible without another wake',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  const child=SessionManager.forkFrom(f.file,f.root,join(f.root,'forks'));
  const file=child.getSessionFile();
  for(const {evidence,context,...job} of planSources(await loadSnapshot(file),new Set())){
    f.store.enqueueConsolidation(job);f.store.claimConsolidation(job.id,'child',Date.now(),60000);
    f.store.commitConsolidation({jobId:job.id,owner:'child',now:Date.now(),creates:[],updates:[],deletes:[]});
  }
  const source={sessionId:child.getSessionId(),sessionFile:file,cwd:f.root,leafId:child.getLeafId()};
  for(const state of ['active','closing','released'])f.store.saveSessionUsage({...source,ownerId:'child',processToken:'child',ownerPid:process.pid,state});
  f.store.enqueueCleanTask({...source,reason:'close',intentId:'child'});
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();
  for(let i=0;i<8;i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(file),{code:'ENOENT'});await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('an exited process releases contention for automatic cleanup without another lifecycle event',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  const {child,stderr}=fixtureChild(t,f,'hold',f.input());const [ready]=await once(child,'message');assert.ok(ready.ready,stderr());
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
  const exit=once(child,'exit');child.kill('SIGKILL');await exit;
  for(let i=0;i<8&&f.task();i++){t.mock.timers.tick(100);await c.whenIdle();}
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('closing while maintenance is waiting cancels automatic continuation and a new worker resumes',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);const access=f.store.acquireSourceAccess();
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const first=f.worker({retryDelayMs:undefined});first.wake();await first.whenIdle();first.close();
  f.store.releaseSourceAccess(access);t.mock.timers.tick(30000);await first.whenIdle();assert.ok(await readFile(f.file));
  const next=f.worker({retryDelayMs:undefined});next.wake();await next.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('repeated wakes coalesce and still yield between maintenance passes',async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);const access=f.store.acquireSourceAccess();
  t.mock.timers.enable({apis:['setTimeout','Date'],now:Date.now()});
  const c=f.worker({retryDelayMs:undefined});for(let i=0;i<100;i++)c.wake();await c.whenIdle();
  f.store.releaseSourceAccess(access);assert.ok(await readFile(f.file));
  t.mock.timers.tick(100);await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('two actual cleanup processes retry a released reservation without a second wake', {timeout:15000}, async t=>{
  const f=await fixture(t);await f.complete();await releaseFixture(f);
  const access=f.store.acquireSourceAccess();
  const a=fixtureChild(t,f,'clean-auto',f.input()),b=fixtureChild(t,f,'clean-auto',f.input());
  await Promise.all([once(a.child,'message'),once(b.child,'message')]);
  const replies=[once(a.child,'message'),once(b.child,'message')];
  a.child.send({start:true});b.child.send({start:true});
  await new Promise(resolve=>setTimeout(resolve,200));assert.ok(await readFile(f.file));
  f.store.releaseSourceAccess(access);
  const results=await Promise.all(replies);
  assert.ok(results.every(([result])=>result.done&&result.completed),a.stderr()+b.stderr());
  await assert.rejects(readFile(f.file),{code:'ENOENT'});
});
