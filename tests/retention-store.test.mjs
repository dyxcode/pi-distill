import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {SQLiteMemoryStore} from '../src/memory-store.ts';

async function fixture(t){
 const root=await mkdtemp(join(tmpdir(),'pi-retention-store-'));const path=join(root,'memory.sqlite');
 const a=new SQLiteMemoryStore(path),b=new SQLiteMemoryStore(path);
 t.after(async()=>{a.close();b.close();await rm(root,{recursive:true,force:true});});
 const task=a.enqueueCleanTask({sessionId:'session',sessionFile:join(root,'s.jsonl'),leafId:null,cwd:root,reason:'close',intentId:'range'});
 return {a,b,task};
}
test('source access excludes another reader or cleaner until its actual release',async t=>{
 const {a,b}=await fixture(t);const access=a.acquireSourceAccess();assert.ok(access);
 assert.equal(b.acquireSourceAccess(),undefined);assert.equal(b.releaseSourceAccess('wrong'),false);
 assert.equal(a.releaseSourceAccess(access),true);const next=b.acquireSourceAccess();assert.ok(next);b.releaseSourceAccess(next);
});
test('durable deletion intent is fenced by metadata changes and current task ownership',async t=>{
 const {a,b,task}=await fixture(t);const access=a.acquireSourceAccess();
 assert.equal(a.claimCleanTask(task.id,'attempt',Date.now(),60_000),true);
 let version=a.retentionVersion();const {id,status,...input}=task;b.enqueueCleanTask({...input,intentId:'new-range'});
 assert.equal(a.prepareDeletion(task.id,'attempt',access,version,'1:2:3:4:5'),false);
 assert.equal(a.claimCleanTask(task.id,'next',Date.now(),60_000),true);
 version=a.retentionVersion();assert.equal(a.prepareDeletion(task.id,'next',access,version,'1:2:3:4:5'),true);
 let mutations=0;
 assert.equal(a.finishDeletion(task.id,'attempt',access,a.retentionVersion(),()=>{mutations++;return true;}),false);
 assert.equal(mutations,0);
 assert.equal(a.finishDeletion(task.id,'next',access,a.retentionVersion(),()=>{mutations++;return true;}),true);
 assert.equal(mutations,1);assert.equal(a.getDeletion(task.sessionFile).state,'complete');
 assert.equal(a.listCleanTasks({limit:10}).items.length,0);
 assert.equal(a.listKnownSessionFiles().includes(task.sessionFile),false);
 a.releaseSourceAccess(access);
});
test('a failed file mutation leaves a recoverable deletion intent',async t=>{
 const {a,task}=await fixture(t);const access=a.acquireSourceAccess();a.claimCleanTask(task.id,'attempt',Date.now(),60_000);
 a.prepareDeletion(task.id,'attempt',access,a.retentionVersion(),'1:2:3:4:5');
 assert.throws(()=>a.finishDeletion(task.id,'attempt',access,a.retentionVersion(),()=>{throw new Error('disk failure');}),/disk failure/);
 assert.equal(a.getDeletion(task.sessionFile).state,'deleting');
 assert.equal(a.listCleanTasks({limit:10}).items.length,1);a.releaseSourceAccess(access);
});

test('metadata changes after intent prevent any filesystem mutation',async t=>{
 const {a,b,task}=await fixture(t);const access=a.acquireSourceAccess();a.claimCleanTask(task.id,'attempt',Date.now(),60_000);
 a.prepareDeletion(task.id,'attempt',access,a.retentionVersion(),'1:2:3:4:5');const version=a.retentionVersion();
 b.save({id:'another',content:'A concurrent memory change.',createdAt:'2026-10-23T00:00:00.000Z'});
 let called=false;assert.equal(a.finishDeletion(task.id,'attempt',access,version,()=>{called=true;return true;}),false);
 assert.equal(called,false);a.releaseSourceAccess(access);
});

test('v4 migration preserves pending work and memories',async t=>{
 const {a,b,task}=await fixture(t);a.save({id:'keep',content:'Keep this memory.',createdAt:'2026-10-23T00:00:00.000Z'});
 const {DatabaseSync}=await import('node:sqlite');
 // Construct the exact prior schema by removing only the new metadata tables.
 const path=a.listKnownSessionFiles()[0].replace(/s.jsonl$/,'memory.sqlite');a.close();b.close();
 const raw=new DatabaseSync(path);raw.exec('DROP TABLE source_access; DROP TABLE session_deletions; PRAGMA user_version = 4');raw.close();
 const upgraded=new SQLiteMemoryStore(path);try {
  assert.equal(upgraded.get('keep').content,'Keep this memory.');
  assert.equal(upgraded.listCleanTasks({limit:10}).items[0].id,task.id);
  const access=upgraded.acquireSourceAccess();assert.ok(access);upgraded.releaseSourceAccess(access);
 } finally {upgraded.close();}
});
