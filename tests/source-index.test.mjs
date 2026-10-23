import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, writeFile, appendFile, readFile, rm, rename } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { scanSourcePage, readIndexedEntry } from '../src/source-index.ts';

const header={type:'session',id:'indexed',cwd:'/synthetic',version:3,timestamp:'2026-10-23T00:00:00.000Z'};
const entry=(i,content='synthetic secret 条件🧠')=>({type:'message',id:`e${i}`,parentId:i?`e${i-1}`:null,timestamp:header.timestamp,message:{role:'user',content,timestamp:i}});
async function fixture(t,entries){
 const root=await mkdtemp(join(tmpdir(),'pi-distill-index-'));
 const path=join(root,'session.jsonl'),db=join(root,'memory.sqlite');
 await writeFile(path,[header,...entries].map(JSON.stringify).join('\n')+'\n');
 const stores=[];const open=()=>{const store=new SQLiteMemoryStore(db);stores.push(store);return store;};
 t.after(async()=>{for(const store of stores)store.close();await rm(root,{recursive:true,force:true});});
 return {root,path,db,open};
}
async function finish(f,store){let result;do{result=await scanSourcePage(f.path,store.sourceIndex);}while(!result.complete&&!result.waitingForTail);return result;}

test('source indexing resumes bounded pages after reopening without storing conversation text',async t=>{
 const f=await fixture(t,Array.from({length:600},(_,i)=>entry(i)));
 const first=await scanSourcePage(f.path,f.open().sourceIndex);
 assert.equal(first.complete,false);assert.ok(first.entries>0&&first.entries<=256);
 const store=f.open();const last=await finish(f,store);
 assert.equal(last.entries,600);assert.equal(last.complete,true);assert.ok(last.offset>first.offset);
 const page=store.sourceIndex.entries(last.generation,0,128);
 assert.equal(page.length,128);assert.equal(page[0].id,'e0');
 const original=await readIndexedEntry(f.path,last,page[0]);assert.deepEqual(original,entry(0));
 assert.ok(!(await readFile(f.db)).includes(Buffer.from('synthetic secret')));
 assert.deepEqual(store.processedSources(['invented']),[]);
 assert.deepEqual(store.listConsolidationJobs(),[]);
});

test('total source size and record count do not limit index progress',async t=>{
 const f=await fixture(t,Array.from({length:20_010},(_,i)=>({type:'session_info',id:`e${i}`,parentId:i?`e${i-1}`:null,timestamp:header.timestamp,name:'x'.repeat(850)})));
 const result=await finish(f,f.open());assert.equal(result.complete,true);assert.equal(result.entries,20_010);assert.ok(result.offset>16*1024*1024);
});

test('incomplete tails stay pending and a later completed version is revalidated',async t=>{
 const f=await fixture(t,[entry(0)]);await appendFile(f.path,'{"type":');
 const store=f.open();const first=await finish(f,store);
 assert.equal(first.complete,false);assert.equal(first.waitingForTail,true);assert.equal(first.entries,1);
 await writeFile(f.path,[header,entry(0),entry(1)].map(JSON.stringify).join('\n')+'\n');
 const next=await finish(f,store);assert.equal(next.entries,2);assert.notEqual(next.generation,first.generation);
});

test('changed or replaced files invalidate old ranges and stale page writers',async t=>{
 const f=await fixture(t,[entry(0)]);const store=f.open();const old=await finish(f,store);
 const row=store.sourceIndex.entries(old.generation,0,1)[0];
 await writeFile(join(f.root,'replacement'),[header,entry(0,'replacement')].map(JSON.stringify).join('\n')+'\n');
 await rename(join(f.root,'replacement'),f.path);
 await assert.rejects(readIndexedEntry(f.path,old,row),/changed|version/i);
 const next=await finish(f,store);assert.notEqual(old.generation,next.generation);
 assert.throws(()=>store.sourceIndex.commitPage(old,[],{...old,complete:true}),/changed|stale|conflict/i);
});

test('invalid graphs and malformed complete records cannot complete indexing',async t=>{
 for(const records of [[entry(0),entry(0)], [{...entry(0),parentId:'missing'}], [{...entry(0),parentId:'e0'}]]){
  const f=await fixture(t,records);const store=f.open();
  await assert.rejects(finish(f,store),/duplicate|parent|cycle/i);
 }
 const f=await fixture(t,[entry(0)]);await appendFile(f.path,'{broken}\n');
 await assert.rejects(finish(f,f.open()),/malformed/i);
});

test('cancellation and concurrent page attempts cannot skip or duplicate records',async t=>{
 const f=await fixture(t,Array.from({length:300},(_,i)=>entry(i)));
 const a=f.open(),b=f.open();const cancelled=AbortSignal.abort();
 await assert.rejects(scanSourcePage(f.path,a.sourceIndex,cancelled));
 await Promise.allSettled([scanSourcePage(f.path,a.sourceIndex),scanSourcePage(f.path,b.sourceIndex)]);
 const done=await finish(f,a);assert.equal(done.entries,300);
 const ids=[];for(let seq=0;seq<done.entries;seq+=128)ids.push(...a.sourceIndex.entries(done.generation,seq,128).map(row=>row.id));
 assert.equal(new Set(ids).size,300);
});

test('a Unicode record larger than a scan slice finishes before yielding',async t=>{
 const content='条件🧠\n'.repeat(450_000);
 const f=await fixture(t,[entry(0,content),entry(1)]);const store=f.open();
 const first=await scanSourcePage(f.path,store.sourceIndex);
 assert.equal(first.entries,1);assert.equal(first.complete,false);
 assert.equal((await readIndexedEntry(f.path,first,store.sourceIndex.entries(first.generation,0,1)[0])).message.content,content);
 assert.equal((await finish(f,store)).entries,2);
});

test('single oversized records remain unsupported without acknowledging a partial record',async t=>{
 const f=await fixture(t,[entry(0,'x'.repeat(16*1024*1024))]);const store=f.open();
 await assert.rejects(scanSourcePage(f.path,store.sourceIndex),/record.*limit/i);
 assert.equal(store.sourceIndex.current(f.path).entries,0);
});

test('v8 upgrade preserves memories and pending work while starting with a rebuildable empty index',async t=>{
 const f=await fixture(t,[entry(0)]);const old=f.open();
 old.save({id:'kept',content:'Preserved condition.',createdAt:header.timestamp});
 const job={id:'pending',sessionId:header.id,sessionFile:f.path,leafId:'e0',cwd:header.cwd,sourceKeys:['existing-key']};
 old.enqueueConsolidation(job);old.close();
 const raw=new DatabaseSync(f.db);raw.exec('DROP TRIGGER source_fence_session_observations_insert; DROP TRIGGER source_fence_session_observations_update; DROP TRIGGER source_fence_session_observations_delete; DROP TRIGGER source_fence_session_usages_insert; DROP TRIGGER source_fence_session_usages_update; DROP TRIGGER source_fence_session_usages_delete; DROP TRIGGER source_fence_consolidation_jobs_insert; DROP TRIGGER source_fence_consolidation_jobs_update; DROP TRIGGER source_fence_consolidation_jobs_delete; DROP TRIGGER source_fence_distill_triggers_insert; DROP TRIGGER source_fence_distill_triggers_update; DROP TRIGGER source_fence_distill_triggers_delete; DROP TRIGGER source_fence_clean_tasks_insert; DROP TRIGGER source_fence_clean_tasks_update; DROP TRIGGER source_fence_clean_tasks_delete; DROP INDEX source_index_parents; DROP TABLE source_coverage; DROP TABLE source_fences; DROP TABLE source_derived_gc; DROP TABLE source_view_parts; DROP TABLE source_view_edits; DROP TABLE source_view_entries; DROP TABLE source_preparation; DROP TABLE source_views; DROP TABLE source_index_entries; DROP TABLE source_index_files; PRAGMA user_version=8');raw.close();
 const upgraded=f.open();assert.equal(upgraded.get('kept').content,'Preserved condition.');assert.deepEqual(upgraded.listConsolidationJobs(),[job]);
 assert.equal(upgraded.sourceIndex.current(f.path),undefined);assert.equal((await finish(f,upgraded)).entries,1);
});
