import {fork} from 'node:child_process';
import {once} from 'node:events';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SessionManager } from '@earendil-works/pi-coding-agent';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { loadSnapshot, planSources } from '../src/consolidation-source.ts';
import { CleanupCoordinator } from '../src/cleanup.ts';

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
 assert.equal(f.store.listSessionUsages().find(owner=>owner.ownerId==='child').state,'released');
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
 const c=f.worker();c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('an uncaptured close boundary must be recovered before its processed source is deleted',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);f.store.enqueueCleanTask(f.input());
 const {reason,intentId,...source}=f.input();f.store.observeSession({...source,ownerPid:process.pid});
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));assert.equal(f.task().waitReason,'unprocessed');
 f.store.acknowledgeObservation(source.sessionId,source.leafId);c.wake();await c.whenIdle();await assert.rejects(readFile(f.file),{code:'ENOENT'});
});


test('two actual Pi-side worker processes coordinate one deletion',async t=>{
 const f=await fixture(t);await f.complete();await releaseFixture(f);const task=f.store.enqueueCleanTask(f.input());
 const a=fixtureChild(t,f,'clean',task),b=fixtureChild(t,f,'clean',task);
 await Promise.all([once(a.child,'message'),once(b.child,'message')]);
 const doneA=once(a.child,'message'),doneB=once(b.child,'message');
 a.child.send({go:true});b.child.send({go:true});
 assert.deepEqual((await doneA)[0],{done:true});assert.deepEqual((await doneB)[0],{done:true});
 await assert.rejects(readFile(f.file),{code:'ENOENT'});assert.equal(f.store.getDeletion(f.file).state,'complete');
});
