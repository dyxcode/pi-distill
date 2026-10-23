import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
async function fixture(t){const root=await mkdtemp(join(tmpdir(),'pi-distill-prepare-'));const store=new SQLiteMemoryStore(join(root,'memory.sqlite'));t.after(async()=>{store.close();await rm(root,{recursive:true,force:true});});
 const trigger={id:'trigger',sessionId:'session',sessionFile:join(root,'source.jsonl'),leafId:'leaf',cwd:root,reason:'close'};store.enqueueDistillTrigger(trigger);return {store,trigger};}
const claim=(store,now=1)=>store.claimDistillTrigger({id:'trigger',workerId:'worker',now,leaseMs:100});

test('finishing the currently prepared page cannot complete an open source range',async t=>{
 const {store,trigger}=await fixture(t);const c=claim(store);
 const job={id:'job',sessionId:trigger.sessionId,sessionFile:trigger.sessionFile,leafId:trigger.leafId,cwd:trigger.cwd,sourceKeys:['source-key']};
 store.prepareDistillPage({id:trigger.id,claimToken:c.claimToken,now:2,viewId:'a'.repeat(64),after:0,cursor:1,jobs:[job],complete:false});
 assert.equal(store.readDistillTrigger(trigger.id).status,'pending');
 const work=store.claimDistillJob({id:job.id,workerId:'model',now:3,leaseMs:100});
 store.commitConsolidation({jobId:job.id,owner:work.claimToken,now:4,creates:[],updates:[],deletes:[]});
 assert.equal(store.readDistillTrigger(trigger.id).status,'pending');assert.equal(store.listCleanTasks({limit:10}).items.length,0);
 const next=claim(store,5);
 store.prepareDistillPage({id:trigger.id,claimToken:next.claimToken,now:6,viewId:'a'.repeat(64),after:1,cursor:1,jobs:[],complete:true});
 assert.equal(store.readDistillTrigger(trigger.id).status,'complete');assert.equal(store.listCleanTasks({limit:10}).items.length,1);
});

test('preparation cursors are atomic and a stale worker cannot advance them',async t=>{
 const {store}=await fixture(t);const old=claim(store);
 const newer=claim(store,102);assert.ok(newer);
 assert.throws(()=>store.prepareDistillPage({id:'trigger',claimToken:old.claimToken,now:103,viewId:'a'.repeat(64),after:0,cursor:128,jobs:[],complete:false}));
 assert.equal(store.readSourcePreparation('trigger','a'.repeat(64)),0);
 store.prepareDistillPage({id:'trigger',claimToken:newer.claimToken,now:103,viewId:'a'.repeat(64),after:0,cursor:128,jobs:[],complete:false});
 assert.equal(store.readSourcePreparation('trigger','a'.repeat(64)),128);
 assert.equal(store.readDistillTrigger('trigger').status,'pending');
});
