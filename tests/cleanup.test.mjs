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

test('processed and released source waits for host coordination; new source invalidates the assessment',async t=>{
  const f=await fixture(t); await f.complete();
  const owner=f.store.listSessionUsages()[0];
  f.store.saveSessionUsage({...owner,state:'closing'}); f.store.saveSessionUsage({...owner,state:'released'});
  f.store.enqueueCleanTask(f.input());
  const c=f.worker(); c.wake(); await c.whenIdle();
  assert.equal(f.task().waitReason,'host_coordination'); assert.equal(f.task().status,'waiting');
  f.manager.appendMessage({role:'user',content:'Now use local integration tests too.',timestamp:2}); await f.save();
  c.wake(); await c.whenIdle();
  assert.equal(f.task().waitReason,'unprocessed'); assert.ok(await readFile(f.file));
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
