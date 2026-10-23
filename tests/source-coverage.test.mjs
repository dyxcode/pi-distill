import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp,writeFile,readFile,rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { CleanupCoordinator } from '../src/cleanup.ts';
const at='2026-10-23T00:00:00.000Z';
const msg=(id,parentId,text)=>({type:'message',id,parentId,timestamp:at,message:{role:'user',content:text,timestamp:1}});
async function fixture(t,long=false){
 const root=await mkdtemp(join(tmpdir(),'pi-distill-coverage-')),file=join(root,'source.jsonl'),db=join(root,'memory.sqlite');
 const entries=[msg('base',null,'Synthetic Cedar keeps local tests.')];
 if(long)for(let i=0;i<20_010;i++)entries.push({type:'session_info',id:`meta${i}`,parentId:i?`meta${i-1}`:'base',timestamp:at,name:'x'.repeat(850)});
 entries.push(msg('left',entries.at(-1).id,'Left branch fact.'),msg('right','base','Right branch fact.'));
 await writeFile(file,[{type:'session',id:'source',cwd:root,timestamp:at,version:3},...entries].map(JSON.stringify).join('\n')+'\n');
 const opened=[],workers=[];const open=()=>{const s=new SQLiteMemoryStore(db);opened.push(s);return s;};let store=open();
 const source={sessionId:'source',sessionFile:file,cwd:root,leafId:'left'};
 store.saveSessionUsage({...source,ownerId:'owner',ownerPid:process.pid,processToken:'owner',state:'active'});
 store.saveSessionUsage({...source,ownerId:'owner',ownerPid:process.pid,processToken:'owner',state:'closing'});
 store.saveSessionUsage({...source,ownerId:'owner',ownerPid:process.pid,processToken:'owner',state:'released'});
 store.enqueueCleanTask({...source,reason:'close',intentId:'test'});
 t.after(async()=>{for(const worker of workers)worker.close();for(const s of opened)s.close();await rm(root,{recursive:true,force:true});});
 const view=async leaf=>{let result;do{result=await store.sourceViews.advance(file,leaf);}while(!result.ready);return result.view;};
 const complete=async leaf=>{
  // Synthetic distillation participates in the same source-access protocol as
  // the running cleaner, so its fixture writes cannot race a coverage page.
  const deadline=Date.now()+5000;let access;
  while(!(access=store.acquireSourceAccess())&&Date.now()<deadline)await new Promise(r=>setTimeout(r,10));
  assert.ok(access,'synthetic distillation must acquire source access');
  try{
   const v=await view(leaf);let cursor=0;
   do{const page=await v.page(cursor);const done=new Set(store.processedSources(page.items.map(i=>i.part.key)));const keys=page.items.map(i=>i.part.key).filter(k=>!done.has(k));if(keys.length){const id=`job-${leaf}-${cursor}`;store.enqueueConsolidation({id,...source,leafId:leaf,sourceKeys:keys});store.claimConsolidation(id,'test',Date.now(),60000);store.commitConsolidation({jobId:id,owner:'test',now:Date.now(),creates:[],updates:[],deletes:[]});}cursor=page.nextCursor;}while(cursor!==undefined);
  }finally{store.releaseSourceAccess(access);}
 };
 const worker=()=>{const c=new CleanupCoordinator({openStore:open,retryDelayMs:1});workers.push(c);return c;};
 return {root,file,source,open,store,view,complete,worker,entries};
}
test('incremental cleanup resumes a long source, preserves an unresolved sibling, then removes only the fully covered file',async t=>{
 const f=await fixture(t,true);await f.complete('left');
 f.store.save({id:'retained',content:'Synthetic Cedar keeps local tests.',createdAt:at});
 await f.view(null);
 await f.store.sourceCoverage.advance(f.file,[f.file],f.store.retentionVersion([f.file]));
 const first=f.store.sourceCoverage.current(f.file);assert.equal(first.phase,'leaves');assert.ok(first.cursor<=2048,'one coverage call must examine bounded structural pages');
 let c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));c.close();
 c=f.worker();c.wake();await c.whenIdle();
 const deadline=Date.now()+15000;
 while(Date.now()<deadline){const task=f.store.listCleanTasks({limit:10}).items.find(t=>t.sessionFile===f.file);if(task?.waitReason==='unprocessed'&&f.store.sourceCoverage.current(f.file)?.phase==='leaves')break;await new Promise(r=>setTimeout(r,10));}
 assert.ok(await readFile(f.file));await f.complete('right');c.wake();
 const end=Date.now()+15000;let removed=false;
 while(Date.now()<end){try{await readFile(f.file);}catch(e){if(e.code==='ENOENT'){removed=true;break;}throw e;}await new Promise(r=>setTimeout(r,10));}
 assert.equal(removed,true);c.close();await c.whenIdle();assert.equal(f.store.get('retained').content,'Synthetic Cedar keeps local tests.');
 for(let i=0;i<5000;i++){const result=f.store.collectMetadata(128);assert.ok(result.removed<=128);if(!result.hasMore)break;}
 assert.equal(f.store.sourceIndex.current(f.file),undefined);assert.equal(f.store.sourceCoverage.current(f.file),undefined);
 assert.equal(f.store.get('retained').content,'Synthetic Cedar keeps local tests.');
});

test('a recorded historical boundary retains evidence removed from the terminal branch',async t=>{
 const f=await fixture(t);
 const entries=[msg('base',null,'Historical original fact.'),{type:'context_edit',id:'left',parentId:'base',timestamp:at,targetId:'base',replacement:null}];
 await writeFile(f.file,[{type:'session',id:'source',cwd:f.root,timestamp:at,version:3},...entries].map(JSON.stringify).join('\n')+'\n');
 await f.complete('left');
 f.store.observeSession({...f.source,leafId:'base',ownerPid:2147483647});f.store.acknowledgeObservation('source','base');
 const c=f.worker();c.wake();await c.whenIdle();assert.ok(await readFile(f.file));
 assert.equal(f.store.listCleanTasks({limit:10}).items.find(t=>t.sessionFile===f.file).waitReason,'unprocessed');
 await f.complete('base');c.wake();await c.whenIdle();
 const deadline=Date.now()+2000;
 while(Date.now()<deadline){try{await readFile(f.file);}catch(e){if(e.code==='ENOENT')break;throw e;}await new Promise(r=>setTimeout(r,10));}
 await assert.rejects(readFile(f.file),{code:'ENOENT'});
});

test('source fences scale beyond the prior metadata cap and detect other connections without changing for lease bookkeeping',async t=>{
 const f=await fixture(t);const other=f.open();
 // Synthetic raw fixture bulk load avoids making the test about observation insertion speed.
 const {DatabaseSync}=await import('node:sqlite');const raw=new DatabaseSync(join(f.root,'memory.sqlite'));
 raw.exec('BEGIN');const insert=raw.prepare("INSERT INTO session_usages(owner_id,process_token,session_id,session_file,cwd,leaf_id,owner_pid,state) VALUES (?,?,?,?,?,?,?,'released')");
 for(let i=0;i<16400;i++)insert.run(`owner${i}`,`token${i}`,'source',f.file,f.root,`checkpoint${i}`,2147483647);
 raw.exec('COMMIT');raw.close();
 const before=f.store.retentionVersion([f.file]);assert.equal(other.retentionVersion([f.file]),before);
 const task=f.store.listCleanTasks({limit:10}).items[0];assert.ok(other.claimCleanTask(task.id,'test',Date.now(),60000,true));
 assert.equal(f.store.retentionVersion([f.file]),before);
 other.releaseCleanTask(task.id,'test');
 other.observeSession({...f.source,leafId:'new-boundary',ownerPid:2147483647});assert.notEqual(f.store.retentionVersion([f.file]),before);
});

test('v10 upgrade preserves an indexed source and pending work while adding incremental coverage',async t=>{
 const f=await fixture(t);const v=await f.view('left');const expected=(await v.page()).items.map(i=>i.part.key);
 f.store.save({id:'before-upgrade',content:'Retain this synthetic memory.',createdAt:at});f.store.close();
 const {SOURCE_COVERAGE_SCHEMAS}=await import('../src/source-coverage.ts');const {DatabaseSync}=await import('node:sqlite');const raw=new DatabaseSync(join(f.root,'memory.sqlite'));
 for(const schema of [...SOURCE_COVERAGE_SCHEMAS,...(await import('../src/distill-schedule.ts')).DISTILL_SCHEDULE_SCHEMAS].reverse()){const type=schema.sql.split(' ')[1];raw.exec(`DROP ${type} ${schema.name}`);}raw.exec('PRAGMA user_version=10');raw.close();
 const upgraded=f.open();assert.equal(upgraded.get('before-upgrade').content,'Retain this synthetic memory.');
 const result=await upgraded.sourceViews.advance(f.file,'left');assert.ok(result.ready);assert.deepEqual((await result.view.page()).items.map(i=>i.part.key),expected);
 assert.ok(upgraded.listCleanTasks({limit:10}).items.length);assert.doesNotThrow(()=>upgraded.retentionVersion([f.file]));
});
