import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { MemoryTools } from '../src/memory-tools.ts';

const time = '2026-10-21T08:00:00.000Z';
async function fixture(t, records = []) {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-tools-'));
  const path = join(root, 'memories.sqlite');
  const writer = new SQLiteMemoryStore(path);
  for (const [id, content] of records) writer.save({id,content,createdAt:time});
  const tools = new MemoryTools(() => new SQLiteMemoryStore(path));
  t.after(async () => { tools.close(); writer.close(); await rm(root,{recursive:true,force:true}); });
  return {tools,writer,path};
}
const data = result => { assert.deepEqual(JSON.parse(result.content[0].text),result.details); return result.details; };
const bytes = result => Buffer.byteLength(JSON.stringify(result));

test('search uses literal alternatives, returns exact original previews and leaves records unchanged', async t => {
 const {tools,writer} = await fixture(t,[['a','本地单元测试 TESTS'],['b','Other project: unit test stays here.'],['c','Nothing relevant']]);
 const before=writer.get('a');
 const r=data(await tools.search({terms:['测试','TESTS','tests']}));
 assert.equal(r.status,'ok'); assert.deepEqual(r.candidates.map(x=>x.id),['a']);
 assert.deepEqual(r.candidates[0].matchedTerms,['测试','tests']);
 assert.equal(r.candidates[0].preview.text,before.content);
 assert.equal(r.candidates[0].preview.omittedAfter,false);
 assert.deepEqual(writer.get('a'),before);
 assert.deepEqual(data(await tools.search({terms:['unit test']})).candidates.map(x=>x.id),['b']);
 assert.equal(data(await tools.search({terms:['测试','absent'],match:'all'})).candidates.length,0);
});

test('an exhausted literal miss advises a broader query that can recover the policy', async t => {
 const {tools,writer}=await fixture(t,[['policy','For fictional Cedar-72, retain logs for 14 days by default; 30 days during an incident investigation.']]);
 const before=writer.get('policy');
 const miss=data(await tools.search({terms:['Cedar-72 log retention incident investigation','Cedar 72 retention period','incident investigation exception log'],match:'any'}));
 assert.equal(miss.status,'ok');assert.equal(miss.hasMore,false);assert.deepEqual(miss.candidates,[]);
 assert.match(miss.hint,/literal/i);assert.match(miss.hint,/short|broad/i);
 const found=data(await tools.search({terms:['Cedar-72'],match:'any'}));
 assert.deepEqual(found.candidates.map(candidate=>candidate.id),['policy']);
 assert.equal(found.hint,undefined);
 assert.deepEqual(writer.get('policy'),before);
});

test('retrieval failures and budget exhaustion do not advise another search', async t => {
 const unavailable=new MemoryTools(()=>{throw Error('storage unavailable');});
 t.after(()=>unavailable.close());
 const failed=data(await unavailable.search({terms:['policy']}));
 assert.equal(failed.status,'unavailable');assert.equal(failed.hint,undefined);
 const {tools}=await fixture(t);
 for(let index=0;index<11;index++)await tools.search({terms:['missing']});
 const last=data(await tools.search({terms:['missing']}));
 assert.equal(last.status,'ok');assert.equal(last.remaining.calls,0);assert.equal(last.hint,undefined);
 const exhausted=data(await tools.search({terms:['missing']}));
 assert.equal(exhausted.status,'budget_exhausted');assert.equal(exhausted.hint,undefined);
});

test('stable pagination reaches later matches after many nonmatches without duplicates', async t => {
 const records=Array.from({length:350},(_,i)=>[String(i).padStart(4,'0'),i%3===0?'needle':'noise']);
 const {tools}=await fixture(t,records); const ids=[]; let cursor;
 do { tools.reset(); const r=data(await tools.search({terms:['needle'],limit:7,...(cursor?{cursor}:{})}));
 assert.equal(r.status,'ok'); ids.push(...r.candidates.map(x=>x.id)); cursor=r.nextCursor;
 assert.equal(r.hasMore,Boolean(cursor)); } while(cursor);
 assert.deepEqual(ids,records.filter(x=>x[1]==='needle').map(x=>x[0]));
});

test('cursor rejects changed query and corruption; changed limit is permitted', async t => {
 const {tools}=await fixture(t,[['a','needle'],['b','needle'],['c','needle']]);
 const first=data(await tools.search({terms:['needle'],limit:1}));
 assert.equal(data(await tools.search({terms:['other'],cursor:first.nextCursor})).status,'invalid_input');
 assert.equal(data(await tools.search({terms:['needle'],cursor:first.nextCursor+'a'})).status,'invalid_input');
 assert.deepEqual(data(await tools.search({terms:['NEEDLE'],limit:2,cursor:first.nextCursor})).candidates.map(x=>x.id),['b','c']);
});

test('long Unicode/control content is returned in exact revision-bound chunks', async t => {
 const content=('条件🧠\u0000TEST\\\n"').repeat(2600);
 const {tools,writer}=await fixture(t,[['a',content]]); const chunks=[];let cursor;
 do { tools.reset();const result=await tools.read({id:'a',expectedRevision:1,...(cursor?{cursor}:{})});
 assert.ok(bytes(result)<=16384);const r=data(result); assert.equal(r.status,'ok');
 chunks.push(r.content);cursor=r.nextCursor;
 assert.equal(r.createdAt,time);assert.equal(r.updatedAt,time);
 } while(cursor);
 assert.equal(chunks.join(''),content); assert.equal(writer.get('a').revision,1);
});

test('current revisions prevent mixed reads and new searches observe correction/deletion', async t => {
 const {tools,writer}=await fixture(t,[['a','needle '+('text'.repeat(4000))]]);
 const first=data(await tools.read({id:'a',expectedRevision:1}));assert.ok(first.nextCursor);
 writer.update({id:'a',expectedRevision:1,content:'replacement',updatedAt:'2026-10-22T08:00:00.000Z'});
 const stale=data(await tools.read({id:'a',expectedRevision:1,cursor:first.nextCursor}));
 assert.equal(stale.status,'stale');assert.equal(stale.currentRevision,2);
 assert.equal(data(await tools.read({id:'a',expectedRevision:2,cursor:first.nextCursor})).status,'invalid_input');
 assert.equal(data(await tools.search({terms:['needle']})).candidates.length,0);
 writer.forget({id:'a',expectedRevision:2});
 assert.equal(data(await tools.read({id:'a',expectedRevision:2})).status,'not_found');
});

test('invalid external inputs fail clearly without opening storage', async () => {
 let opens=0;const tools=new MemoryTools(()=>{opens++;throw Error('not expected');});
 for(const request of [{terms:[]},{terms:[' ']},{terms:['\ud800']},{terms:['x'.repeat(129)]},{terms:['x'],sql:'SELECT'}, {terms:['x'],limit:0},{terms:['x'],match:'both'},null]) {
 tools.reset(); assert.equal(data(await tools.search(request)).status,'invalid_input');
 }
 for(const request of [{id:'a',expectedRevision:0},{id:'a',expectedRevision:1,offset:0},{id:'\udfff',expectedRevision:1}]) {
 tools.reset();assert.equal(data(await tools.read(request)).status,'invalid_input');
 } assert.equal(opens,0);
});

test('limits count invalid attempts, reset once per run and keep later refusals small', async t => {
 const {tools}=await fixture(t,[['a','needle']]);
 for(let i=0;i<12;i++) assert.equal(data(await tools.read({id:'missing',expectedRevision:1})).status,'not_found');
 const stopped=await tools.search({terms:['needle']});assert.equal(data(stopped).status,'budget_exhausted');
 for(let i=0;i<3;i++) assert.ok(bytes(await tools.search({terms:['needle']}))<=256);
 tools.reset();assert.equal(data(await tools.search({terms:['needle']})).status,'ok');
});

test('serialized response and aggregate budgets include escaped content and metadata', async t => {
 const {tools}=await fixture(t,[['a','"\\\n🧠'.repeat(10000)]]);
 let total=0;let exhausted=false;
 for(let i=0;i<12;i++) {const result=await tools.read({id:'a',expectedRevision:1});total+=bytes(result);assert.ok(bytes(result)<=16384);
 if(data(result).status==='budget_exhausted'){exhausted=true;break;}}
 assert.ok(exhausted);assert.ok(total<=65536,`${total}`);
});

test('concurrent attempts cannot bypass run budgets', async t => {
 const {tools}=await fixture(t,[['a','needle']]);
 const results=await Promise.all(Array.from({length:20},()=>tools.search({terms:['needle']})));
 assert.equal(results.filter(x=>data(x).status==='ok').length,12);
});

test('already-cancelled requests do not open storage and cancellation leaves service usable', async t => {
 const {tools}=await fixture(t,[['a','needle']]); const controller=new AbortController();controller.abort();
 assert.equal(data(await tools.search({terms:['needle']},controller.signal)).status,'cancelled');
 assert.equal(data(await tools.search({terms:['needle']})).status,'ok');
});

test('unavailable/corrupt storage is preserved and later requests can recover', async t => {
 const root=await mkdtemp(join(tmpdir(),'pi-distill-bad-'));const path=join(root,'broken.sqlite');
 const original=Buffer.from('not sqlite');await writeFile(path,original);
 const tools=new MemoryTools(()=>new SQLiteMemoryStore(path));
 t.after(async()=>{tools.close();await rm(root,{recursive:true,force:true});});
 assert.equal(data(await tools.search({terms:['x']})).status,'unavailable');
 assert.deepEqual(await readFile(path),original);
 await rm(path);assert.equal(data(await tools.search({terms:['x']})).status,'ok');
});

test('oversized mandatory metadata yields explicit incomplete failure', async t => {
 const {tools}=await fixture(t,[['a'.repeat(20000),'needle']]);
 const result=await tools.search({terms:['needle']});assert.ok(bytes(result)<=16384);
 assert.equal(data(result).status,'result_too_large');assert.equal(data(result).incomplete,true);
});

test('closing during a run cancels in-flight work and subsequent use reopens lazily', async t => {
 const {tools}=await fixture(t,Array.from({length:400},(_,i)=>[String(i),'noise']));
 const pending=tools.search({terms:['absent']});tools.close();
 assert.equal(data(await pending).status,'cancelled');
 tools.reset();assert.equal(data(await tools.search({terms:['noise']})).status,'ok');
});

test('output-limited pages replay the next match and preserve all IDs', async t => {
 const records=Array.from({length:85},(_,i)=>[String(i).padStart(3,'0'),'needle '+('"\\\n🧠'.repeat(500))]);
 const {tools}=await fixture(t,records);let cursor;const ids=[];let limited=false;
 do {tools.reset();const response=await tools.search({terms:['needle'],limit:50,...(cursor?{cursor}:{})});
 assert.ok(bytes(response)<=16384);const r=data(response);assert.equal(r.status,'ok');
 limited ||= r.stopReason==='output_limit';ids.push(...r.candidates.map(x=>x.id));cursor=r.nextCursor;
 }while(cursor);
 assert.ok(limited);assert.deepEqual(ids,records.map(x=>x[0]));
});

test('cancellation during scanning yields and a new run remains usable', async t => {
 const {tools}=await fixture(t,Array.from({length:600},(_,i)=>[String(i).padStart(4,'0'),'noise']));
 const controller=new AbortController();const pending=tools.search({terms:['absent']},controller.signal);
 setImmediate(()=>controller.abort());assert.equal(data(await pending).status,'cancelled');
 tools.reset();assert.equal(data(await tools.search({terms:['noise']})).status,'ok');
});

test('explicit null options are invalid rather than treated as defaults', async t => {
 const {tools}=await fixture(t);
 for(const request of [{terms:['a'],match:null},{terms:['a'],limit:null}]) {
 assert.equal(data(await tools.search(request)).status,'invalid_input');
 }
});

test('sparse terms fail input validation before storage is opened', async () => {
 let opened=false;const tools=new MemoryTools(()=>{opened=true;throw Error('unexpected');});
 assert.equal(data(await tools.search({terms:new Array(1)})).status,'invalid_input');assert.equal(opened,false);
});

test('the last candidate on a short final batch is marked exhausted', async t => {
 const {tools}=await fixture(t,[['a','needle']]);const r=data(await tools.search({terms:['needle'],limit:1}));
 assert.equal(r.hasMore,false);assert.equal(r.nextCursor,undefined);assert.equal(r.stopReason,'exhausted');
});

test('repeated query matches count unique memory IDs across the run', async t => {
 const {tools}=await fixture(t,[['a','needle thread'],['b','thread']]);
 assert.equal(data(await tools.search({terms:['needle']})).uniqueCandidates,1);
 assert.equal(data(await tools.search({terms:['thread']})).uniqueCandidates,2);
 assert.equal(data(await tools.search({terms:['needle']})).uniqueCandidates,2);
});

test('chunk shrinking retains a complete leading emoji when one code point still fits', async t => {
 const id='a'.repeat(3359);const {tools}=await fixture(t,[[id,'🧠'+'\u0000'.repeat(9000)]]);
 const response=await tools.read({id,expectedRevision:1});const r=data(response);
 assert.equal(r.status,'ok');assert.equal(r.content,'🧠');assert.ok(r.nextCursor);assert.ok(bytes(response)<=16384);
});

test('previews preserve earliest literal match across long Unicode content', async t => {
 const content='语境'.repeat(4093)+'🧠NEEDLE中文 END '+('X'.repeat(100000));
 const {tools}=await fixture(t,[['a',content]]);
 const r=data(await tools.search({terms:['needle中文','end']}));const preview=r.candidates[0].preview;
 assert.ok(preview.text.includes('NEEDLE中文'));assert.ok(content.includes(preview.text));
 assert.equal(preview.omittedBefore,true);assert.equal(preview.omittedAfter,true);
});
