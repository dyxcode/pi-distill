import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../src/memory-store.ts';

test('two Pi worker processes share one durable claim and recover a killed owner with a new fence', {timeout:15_000}, async t=>{
  const root=await mkdtemp(join(tmpdir(),'pi-distill-queue-process-'));
  const db=join(root,'memory.sqlite'); const children=[];
  t.after(async()=>{for(const child of children)if(child.exitCode===null&&!child.killed)child.kill('SIGKILL');await rm(root,{recursive:true,force:true});});
  const store=new SQLiteMemoryStore(db);
  t.after(()=>store.close());
  for(const id of ['first','second'])store.enqueueDistillTrigger({id,sessionId:id,sessionFile:join(root,id+'.jsonl'),cwd:root,leafId:'leaf',reason:'close'});
  async function start(){const child=fork(new URL('./task-queue-child.mjs',import.meta.url),[db],{stdio:['ignore','ignore','pipe','ipc']});children.push(child);await once(child,'message');return child;}
  const [a,b]=await Promise.all([start(),start()]);
  function request(child,message){const result=once(child,'message');child.send(message);return result.then(([value])=>value);}
  const outcomes=await Promise.all([request(a,{action:'claim',now:100}),request(b,{action:'claim',now:100})]);
  assert.equal(outcomes.filter(result=>result.claim).length,1);
  const winner=outcomes[0].claim?a:b;const survivor=winner===a?b:a;
  const first=outcomes.find(result=>result.claim).claim;
  assert.equal((await request(survivor,{action:'claim',now:199})).claim,null);
  const exited=once(winner,'exit');winner.kill('SIGKILL');await exited;
  const next=(await request(survivor,{action:'claim',now:200})).claim;
  assert.ok(next);assert.equal(next.trigger.id,first.trigger.id);assert.notEqual(next.claimToken,first.claimToken);
  assert.throws(()=>store.prepareDistillTrigger({id:first.trigger.id,claimToken:first.claimToken,now:201,jobs:[]}));
  assert.equal((await request(survivor,{action:'prepare',now:201,claimToken:next.claimToken})).prepared,true);
  assert.equal(store.listCleanTasks({limit:10}).items.length,1);
  assert.equal((await request(survivor,{action:'claim',now:202})).claim.trigger.id,'second');
  survivor.send({action:'stop'});await once(survivor,'exit');
});

test('session rotation survives worker death and shares atomic selection across real processes', {timeout:15000}, async t=>{
 const root=await mkdtemp(join(tmpdir(),'pi-distill-schedule-process-')),db=join(root,'memory.sqlite'),children=[];
 const store=new SQLiteMemoryStore(db);
 t.after(async()=>{for(const child of children){if(child.exitCode===null&&child.signalCode===null){const exit=once(child,'exit');child.kill('SIGKILL');await exit;}}store.close();await rm(root,{recursive:true,force:true});});
 for(const id of ['A','B','C'])store.enqueueDistillTrigger({id,sessionId:id,sessionFile:join(root,id+'.jsonl'),cwd:root,leafId:'leaf',reason:'close'});
 const start=async()=>{const child=fork(new URL('./task-queue-child.mjs',import.meta.url),[db],{stdio:['ignore','ignore','pipe','ipc']});children.push(child);await once(child,'message');return child;};
 const request=async(child,message)=>{const reply=once(child,'message');child.send(message);const [result]=await reply;assert.equal(result.error,undefined);return result;};
 const a=await start(),b=await start();
 const results=await Promise.all([request(a,{action:'schedule',now:100}),request(b,{action:'schedule',now:100})]);
 assert.equal(results.filter(x=>x.selected.kind==='trigger').length,1);
 const winner=results[0].selected.kind==='trigger'?a:b, survivor=winner===a?b:a;
 const old=results.find(x=>x.selected.kind==='trigger').selected;
 assert.equal(old.claim.trigger.sessionId,'A');
 assert.equal((await request(survivor,{action:'schedule',now:199})).selected.kind,'busy');
 const exit=once(winner,'exit');winner.kill('SIGKILL');await exit;
 const next=(await request(survivor,{action:'schedule',now:200})).selected;
 assert.equal(next.claim.trigger.sessionId,'B','a crash cannot reset selection to the old first session');
 assert.throws(()=>store.prepareDistillTrigger({id:'A',claimToken:old.claim.claimToken,now:201,jobs:[]}));
 await request(survivor,{action:'scheduled-complete',id:'B',claimToken:next.claim.claimToken,now:201});
 const c=await start();
 const third=(await request(c,{action:'schedule',now:202})).selected;assert.equal(third.claim.trigger.sessionId,'C');
 await request(c,{action:'scheduled-complete',id:'C',claimToken:third.claim.claimToken,now:203});
 const resumed=(await request(survivor,{action:'schedule',now:204})).selected;assert.equal(resumed.claim.trigger.sessionId,'A');
 await request(survivor,{action:'scheduled-complete',id:'A',claimToken:resumed.claim.claimToken,now:205});
 assert.ok(store.listDistillTriggers({limit:10}).items.every(x=>x.status==='complete'));
 assert.deepEqual(store.listScheduledDistill(206).items,[]);
});
