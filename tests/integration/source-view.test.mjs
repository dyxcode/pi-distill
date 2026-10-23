import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { loadSnapshot } from '../../src/sources/snapshot.ts';
import { planSources } from '../../src/distill/preparation.ts';

const at='2026-10-23T00:00:00.000Z';
const message=(id,parentId,text)=>({type:'message',id,parentId,timestamp:at,message:{role:'user',content:text,timestamp:1}});
async function fixture(t,entries,parent){
 const root=await mkdtemp(join(tmpdir(),'pi-distill-view-'));const path=join(root,'source.jsonl');
 await writeFile(path,[{type:'session',id:'view',cwd:root,version:3,timestamp:at,...(parent?{parentSession:parent}:{})},...entries].map(JSON.stringify).join('\n')+'\n');
 const db=join(root,'memory.sqlite');const stores=[];
 const open=()=>{const store=new SQLiteMemoryStore(db);stores.push(store);return store;};
 t.after(async()=>{for(const store of stores)store.close();await rm(root,{recursive:true,force:true});});
 return {root,path,open};
}
async function view(f,store,leaf){let result;do{result=await store.sourceViews.advance(f.path,leaf);}while(!result.ready);return result.view;}
async function parts(view){const result=[];let cursor=0;do{const page=await view.page(cursor,128);result.push(...page.items.map(item=>item.part));cursor=page.nextCursor;}while(cursor!==undefined);return result;}

test('paged views preserve exact existing evidence keys across edits, compaction and sibling branches',async t=>{
 const entries=[message('base',null,'Old decision.'),message('other','base','Branch A.'),
  {type:'context_edit',id:'edit',parentId:'other',timestamp:at,targetId:'base',replacement:{content:[{type:'text',text:'Corrected 条件🧠.'}]}},
  {type:'compaction',id:'compact',parentId:'edit',timestamp:at,summary:'synthetic summary',firstKeptEntryId:'other',tokensBefore:100},
  message('tail','compact','Keep the exception.'),message('sibling','base','Branch B.')];
 const f=await fixture(t,entries),store=f.open();
 for(const leaf of ['tail','sibling','edit']){
  const expected=planSources(await loadSnapshot(f.path,leaf),new Set()).flatMap(batch=>batch.evidence);
  assert.deepEqual(await parts(await view(f,store,leaf)),expected);
 }
});

test('a large indexed view resumes projection without materializing the whole conversation',async t=>{
 const entries=[message('base',null,'Keep synthetic memory with its exception.')];
 for(let i=0;i<20_010;i++)entries.push({type:'session_info',id:`meta${i}`,parentId:i?`meta${i-1}`:'base',timestamp:at,name:'x'.repeat(850)});
 entries.push(message('tail','meta20009','A final new fact.'));
 const f=await fixture(t,entries);let store=f.open();
 const first=await store.sourceViews.advance(f.path,'tail');assert.equal(first.ready,false);
 store.close();store=f.open();const result=await parts(await view(f,store,'tail'));
 assert.deepEqual(result.map(part=>part.text),['Keep synthetic memory with its exception.','A final new fact.']);
 assert.deepEqual(store.processedSources(result.map(part=>part.key)),[]);
});

test('a later removal invalidates earlier content before any evidence page becomes ready',async t=>{
 const entries=[message('base',null,'Must disappear.')];
 for(let i=0;i<300;i++)entries.push({type:'session_info',id:`meta${i}`,parentId:i?`meta${i-1}`:'base',timestamp:at});
 entries.push({type:'context_edit',id:'remove',parentId:'meta299',targetId:'base',timestamp:at,replacement:null});
 const f=await fixture(t,entries);const v=await view(f,f.open(),'remove');
 const result=await parts(v);assert.equal(result.length,1);assert.equal(result[0].role,'context_edit');assert.ok(!result[0].text.includes('Must disappear'));
});

test('the production coordinator drains a long source in resumable pages before enabling cleanup',async t=>{
 const {ConsolidationCoordinator}=await import('../../src/distill/coordinator.ts');
 const entries=[message('base',null,'Keep the initial synthetic fact.')];
 for(let i=0;i<20_010;i++)entries.push({type:'session_info',id:`meta${i}`,parentId:i?`meta${i-1}`:'base',timestamp:at,name:'x'.repeat(850)});
 entries.push(message('tail','meta20009','Keep the final synthetic fact.'));
 const f=await fixture(t,entries),store=f.open();
 store.enqueueDistillTrigger({id:'long',sessionId:'view',sessionFile:f.path,cwd:f.root,leafId:'tail',reason:'close'});
 const seen=[];const ctx={model:{id:'synthetic',contextWindow:128000,maxTokens:4096},modelRegistry:{},sessionManager:{getSessionId:()=> 'live',getSessionFile:()=>undefined,getCwd:()=>f.root,getLeafId:()=>null}};
 const worker=new ConsolidationCoordinator({openStore:f.open,retryDelayMs:1,runModel:async batch=>{seen.push(...batch.evidence.map(part=>part.text));return {creates:[],updates:[],deletes:[]};}});
 t.after(()=>worker.shutdown(ctx,'reload'));
 await worker.start(ctx);await worker.whenIdle();assert.equal(seen.length,0);assert.equal(store.listCleanTasks({limit:10}).items.length,0);
 const deadline=Date.now()+15000;
 while(store.readDistillTrigger('long').status!=='complete'&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,10));
 assert.equal(store.readDistillTrigger('long').status,'complete');
 assert.deepEqual(seen,['Keep the initial synthetic fact.','Keep the final synthetic fact.']);
 assert.equal(store.listCleanTasks({limit:10}).items.length,1);
 await worker.shutdown(ctx,'reload');
});

test('paged projection matches original semantics for repeated edits and compactions and fork provenance',async t=>{
 const entries=[message('a',null,'Original.'),message('b','a','Second.'),
 {type:'context_edit',id:'e1',parentId:'b',timestamp:at,targetId:'a',replacement:{content:[{type:'text',text:'One.'}]}},
 {type:'compaction',id:'c1',parentId:'e1',timestamp:at,summary:'summary one',firstKeptEntryId:'b',tokensBefore:100},
 {type:'context_edit',id:'e2',parentId:'c1',timestamp:at,targetId:'a',replacement:{content:[]}},
 {type:'compaction',id:'c2',parentId:'e2',timestamp:at,summary:'summary two',firstKeptEntryId:'e2',tokensBefore:100},
 {type:'context_edit',id:'e3',parentId:'c2',timestamp:at,targetId:'b',replacement:null},message('end','e3','Final.')];
 const f=await fixture(t,entries),store=f.open();
 for(const leaf of entries.map(entry=>entry.id))assert.deepEqual(await parts(await view(f,store,leaf)),planSources(await loadSnapshot(f.path,leaf),new Set()).flatMap(batch=>batch.evidence),leaf);
 const child=join(f.root,'child.jsonl');await writeFile(child,[{type:'session',id:'child',cwd:f.root,timestamp:at,version:3,parentSession:f.path},...entries,message('fork','end','Child fact.')].map(JSON.stringify).join('\n')+'\n');
 const childFixture={...f,path:child};const v=await view(childFixture,store,'fork');
 assert.deepEqual(await parts(v),planSources(await loadSnapshot(child,'fork'),new Set()).flatMap(batch=>batch.evidence));
 const parentView=await view(f,store,'end');const last=(await parts(parentView)).at(-1);
 assert.equal(v.isSibling({sessionId:'view',sourceKeys:[last.key],...parentView.boundary([last])}),false);
});

test('an ancestry path without copied branch evidence does not create shared conversational lineage',async t=>{
 const f=await fixture(t,[message('base',null,'Parent fact.')]),store=f.open();
 const path=join(f.root,'independent.jsonl');await writeFile(path,[{type:'session',id:'independent',cwd:f.root,timestamp:at,version:3,parentSession:f.path},message('new',null,'Independent fact.')].map(JSON.stringify).join('\n')+'\n');
 const v=await view({...f,path},store,'new');
 assert.deepEqual(v.lineageSessionIds,['independent']);
 assert.equal(v.isSibling({sessionId:'view',sourceKeys:['unrelated']}),false);
});

test('combined fork ancestry can exceed the former aggregate byte limit while preserving shared evidence',async t=>{
 const entries=[message('base',null,'Shared synthetic ancestry fact.')];
 for(let i=0;i<10000;i++)entries.push({type:'session_info',id:`meta${i}`,parentId:i?`meta${i-1}`:'base',timestamp:at,name:'x'.repeat(850)});
 const f=await fixture(t,entries),store=f.open();const parent=await view(f,store,'meta9999');
 const child=join(f.root,'large-child.jsonl');await writeFile(child,[{type:'session',id:'child',cwd:f.root,timestamp:at,version:3,parentSession:f.path},...entries,message('child-tail','meta9999','Child-specific fact.')].map(JSON.stringify).join('\n')+'\n');
 const childView=await view({...f,path:child},store,'child-tail');const result=await parts(childView);
 assert.deepEqual(result.slice(0,1),await parts(parent));assert.equal(result.at(-1).text,'Child-specific fact.');
});
