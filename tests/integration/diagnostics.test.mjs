import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, rmSync, readFileSync, statSync, mkdirSync, writeFileSync, symlinkSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { LocalDiagnostics } from '../../src/diagnostics/recorder.ts';
import { MemoryTools } from '../../src/memory/retrieval.ts';

function fixture(t, options = {}) {
  const root = mkdtempSync(join(tmpdir(), 'pi-distill-diagnostics-'));
  const path = join(root, 'distill', 'diagnostics.sqlite');
  const logs = [];
  const open = () => { const log = new LocalDiagnostics({ agentDir: root, extensionVersion: '0.0.0', piVersion: '1.3.0', ...options }); logs.push(log); return log; };
  t.after(() => { for (const log of logs) log.close(); rmSync(root, {recursive: true, force: true}); });
  return {root, path, open};
}
function rows(path) {
  const db = new DatabaseSync(path, {readOnly: true});
  try { return db.prepare('SELECT payload FROM events ORDER BY sequence').all().map(row => JSON.parse(row.payload)); }
  finally { db.close(); }
}

test('local records survive restart, correlate without identifiers, and exclude arbitrary content and errors', t => {
  const f = fixture(t); const first = f.open();
  const secret = 'PRIVATE conversation /home/person/key TOKEN123';
  const error = new TypeError(secret); error.code = 'EACCES'; error.cause = error;
  const event = {operation:'distill.commit', outcome:'failed', task:secret, source:secret, model:secret, count:2, durationMs:12, error, content:secret};
  first.record(event); first.close();
  const second = f.open(); second.record({...event,outcome:'ok'});
  const records = rows(f.path);
  assert.equal(records.length, 2);
  assert.equal(records[0].task, records[1].task);
  assert.notEqual(records[0].task, records[0].source);
  assert.notEqual(records[0].runtime, records[1].runtime);
  assert.equal(records[0].errorCode, 'EACCES');
  assert.equal(records[0].errorClass, 'TypeError');
  assert.equal(records[1].outcome, 'recovered');
  assert.equal(records[0].piVersion, '1.3.0');
  assert.equal(records[0].extensionVersion, '0.0.0');
  assert.equal(records[0].nodeVersion, process.versions.node);
  assert.ok(!readFileSync(f.path).includes(Buffer.from(secret)));
  const hostile = new Proxy({}, {get() {throw new Error(secret);}});
  assert.doesNotThrow(() => second.record(hostile));
  second.record({operation:'memory.search', outcome:'failed', error:hostile, count:Infinity, bytes:-1, reason:secret});
  assert.ok(!readFileSync(f.path).includes(Buffer.from(secret)));
  assert.equal(rows(f.path).at(-1).operation, 'memory.search');
});

test('retention bounds records, payloads and physical size and expires old observations on next use', t => {
  let now = Date.now(); const f=fixture(t,{now:()=>now}); const log=f.open();
  for (let i=0;i<2200;i++) log.record({operation:'distill.commit',outcome:'failed',task:`job-${i}`,count:i});
  let records=rows(f.path); assert.equal(records.length,2048); assert.equal(records.at(-1).outcome,'failed');
  assert.ok(records.every(event=>Buffer.byteLength(JSON.stringify(event))<2048));
  assert.ok(statSync(f.path).size<=8*1024*1024);
  now += 8*24*60*60*1000;
  log.record({operation:'memory.read',outcome:'ok'});
  records=rows(f.path);assert.equal(records.length,0);
  log.close(); log.record({operation:'runtime.stop',outcome:'failed'});assert.equal(rows(f.path).length,0);
});

test('normal outcomes are quiet; repeated issues merge and recovery survives restart without duplicate resolution', t => {
  let now=Date.now();const f=fixture(t,{now:()=>now});let log=f.open();
  log.record({operation:'memory.read',outcome:'ok'});assert.equal(existsSync(f.path),false);
  const event={operation:'distill.model',outcome:'blocked',reason:'model_budget',task:'job'};
  log.record(event);for(let i=0;i<100;i++)log.record(event);
  assert.equal(rows(f.path).length,1);
  now+=30001;log.record(event);assert.equal(rows(f.path).at(-1).suppressed,100);
  log.close();log=f.open();
  log.record({...event,outcome:'ok',task:'unrelated'});assert.equal(rows(f.path).length,2);
  log.record({...event,outcome:'ok'});log.record({...event,outcome:'ok'});
  assert.equal(rows(f.path).length,3);assert.equal(rows(f.path).at(-1).outcome,'recovered');
  const lock=new DatabaseSync(f.path); lock.exec('BEGIN IMMEDIATE');
  const started=performance.now();log.record({operation:'memory.read',outcome:'failed'});
  assert.ok(performance.now()-started<500);lock.exec('ROLLBACK');lock.close();
  log.record({operation:'memory.read',outcome:'failed'});assert.equal(rows(f.path).at(-1).dropped,1);
});

test('unavailable, corrupt and unknown-schema diagnostics never block operations or reset existing data', t => {
  const f=fixture(t);mkdirSync(join(f.root,'distill'));writeFileSync(f.path,'unrelated data');
  const log=f.open(); assert.doesNotThrow(()=>log.record({operation:'cleanup.pass',outcome:'failed'}));
  assert.equal(readFileSync(f.path,'utf8'),'unrelated data');log.close();rmSync(f.path);
  const db=new DatabaseSync(f.path);db.exec('PRAGMA user_version=99;CREATE TABLE preserved(value);INSERT INTO preserved VALUES(42)');db.close();
  f.open().record({operation:'cleanup.pass',outcome:'failed'});
  const check=new DatabaseSync(f.path);assert.equal(check.prepare('SELECT value FROM preserved').get().value,42);assert.equal(check.prepare('PRAGMA user_version').get().user_version,99);check.close();
});

test('diagnostics refuse vault/repository locations and symlinks into protected storage', t => {
  const f=fixture(t);const protectedRoot=join(f.root,'vault');mkdirSync(join(protectedRoot,'.obsidian'),{recursive:true});
  const options={agentDir:protectedRoot,extensionVersion:'0.0.0',piVersion:'1.3.0'};
  const log=new LocalDiagnostics(options);log.record({operation:'cleanup.pass',outcome:'failed'});log.close();
  assert.throws(()=>statSync(join(protectedRoot,'distill')));
  mkdirSync(join(f.root,'distill'));symlinkSync(join(protectedRoot,'private.sqlite'),f.path);
  f.open().record({operation:'cleanup.pass',outcome:'failed'});assert.throws(()=>statSync(join(protectedRoot,'private.sqlite')));
});

test('independent processes share the bounded store without collisions or unbounded files', async t => {
  const f=fixture(t);f.open().record({operation:'cleanup.pass',outcome:'failed'});
  const module=new URL('../../src/diagnostics/recorder.ts',import.meta.url).href;
  await Promise.all(Array.from({length:3},()=>new Promise((resolve,reject)=>{
    const script=`import {LocalDiagnostics} from ${JSON.stringify(module)};const log=new LocalDiagnostics({agentDir:process.argv[1],extensionVersion:'0.0.0',piVersion:'1.3.0'});for(let i=0;i<1200;i++)log.record({operation:'distill.commit',outcome:'failed',task:process.pid+'-'+i,source:'shared-source'});log.close();`;
    const child=spawn(process.execPath,['--input-type=module','-e',script,f.root],{stdio:['ignore','ignore','pipe']});let stderr='';child.stderr.on('data',chunk=>stderr+=chunk);child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(new Error(stderr)));
  })));
  const records=rows(f.path);assert.ok(records.length>1 && records.length<=2048);
  const commits=records.filter(event=>event.operation==='distill.commit');assert.ok(commits.length>0);assert.equal(new Set(commits.map(event=>event.source)).size,1);
  assert.ok(statSync(f.path).size<=8*1024*1024);
});

test('retrieval retains exact responses with working and throwing diagnostics', async t => {
  const f=fixture(t);const log=f.open();
  const emptyStore={searchBatch:()=>{throw new Error('private database failure');},get:()=>undefined,close:()=>{}};
  const plain=new MemoryTools(()=>emptyStore);const logged=new MemoryTools(()=>emptyStore,log);
  const broken=new MemoryTools(()=>emptyStore,{record(){throw new Error('broken logger');}});
  for(const input of [{terms:['private keyword']},{terms:[]}]){
    const expected=await plain.search(input);assert.deepEqual(await logged.search(input),expected);assert.deepEqual(await broken.search(input),expected);
  }
  assert.deepEqual(await logged.read({id:'private-id',expectedRevision:1}),await plain.read({id:'private-id',expectedRevision:1}));
  const records=rows(f.path);assert.equal(records.filter(event=>event.operation==='memory.search').length,1);
  assert.equal(records.at(-1).outcome,'failed');assert.ok(!records.some(event=>event.operation==='memory.read'));assert.ok(!readFileSync(f.path).includes(Buffer.from('private keyword')));
  plain.close();logged.close();broken.close();
});

test('invalid outcomes cannot create routine activity records and successful recovery requires the same operation', t=>{
  const f=fixture(t);const log=f.open();
  for(const outcome of ['started','cancelled','waiting','contended','not_found','stale']) log.record({operation:'memory.read',outcome});
  log.record({operation:'cleanup.pass',outcome:'failed',error:{code:'ERR_SQLITE_ERROR',errcode:5}});
  assert.equal(existsSync(f.path),false);
  log.record({operation:'memory.search',outcome:'failed'});
  log.record({operation:'memory.read',outcome:'ok'});assert.equal(rows(f.path).length,1);
  log.record({operation:'memory.search',outcome:'ok'});assert.equal(rows(f.path).at(-1).outcome,'recovered');
});

test('two processes can resolve one persisted issue only once',async t=>{
  const f=fixture(t);f.open().record({operation:'distill.model',outcome:'blocked',task:'shared',reason:'model_budget'});
  const module=new URL('../../src/diagnostics/recorder.ts',import.meta.url).href;
  await Promise.all(Array.from({length:2},()=>new Promise((resolve,reject)=>{
    const script=`import {LocalDiagnostics} from ${JSON.stringify(module)};const log=new LocalDiagnostics({agentDir:process.argv[1],extensionVersion:'0.0.0',piVersion:'1.3.0'});for(let i=0;i<5;i++)log.record({operation:'distill.model',outcome:'ok',task:'shared'});log.close();`;
    const child=spawn(process.execPath,['--input-type=module','-e',script,f.root],{stdio:'ignore'});child.on('error',reject);child.on('exit',code=>code===0?resolve():reject(new Error(`Exit ${code}`)));
  })));
  assert.equal(rows(f.path).filter(event=>event.outcome==='recovered').length,1);
});
