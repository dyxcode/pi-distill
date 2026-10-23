import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {DatabaseSync} from 'node:sqlite';
import {SQLiteMemoryStore} from '../../src/storage/store.ts';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-source-store-'));
  const path = join(root, 'memory.sqlite');
  const a = new SQLiteMemoryStore(path), b = new SQLiteMemoryStore(path);
  t.after(async () => {a.close(); b.close(); await rm(root, {recursive:true, force:true});});
  const input = (id) => ({sessionId:id, sessionFile:join(root, `${id}.jsonl`), cwd:root, leafId:null});
  return {root,path,a,b,input};
}

test('durable work registers its source independently of job history and survives restart', async t => {
  const {a,b,input} = await fixture(t);
  const source = input('source');
  a.enqueueConsolidation({...source, id:'job', sourceKeys:['key']});
  assert.equal(b.sources.get(source.sessionFile).sessionId, source.sessionId);
  assert.deepEqual(b.listKnownSessionFiles(), [source.sessionFile]);
});

test('scoped deletion evidence changes only for relevant state or dependency topology', async t => {
  const {a,b,input} = await fixture(t);
  const first = input('first'), second = input('second');
  for (const source of [first,second]) {
    a.enqueueCleanTask({...source,reason:'close',intentId:'done'});
    a.sources.recordHeader({sessionId:source.sessionId,sessionFile:source.sessionFile,fileVersion:source === first ? '1:2:3:4:5' : '1:3:3:4:5'});
  }
  const before = a.retentionVersion([first.sessionFile]);
  b.save({id:'memory',content:'Independent memory.',createdAt:'2026-10-23T00:00:00.000Z'});
  b.observeSession({...second,leafId:'new',ownerPid:process.pid});
  assert.equal(a.retentionVersion([first.sessionFile]), before);
  b.observeSession({...first,leafId:'new',ownerPid:process.pid});
  assert.notEqual(a.retentionVersion([first.sessionFile]), before);
});

test('source discovery does not stop at the historical task row ceiling', async t => {
  const {a,path,input} = await fixture(t);
  const source = input('source');
  a.enqueueConsolidation({...source,id:'registered',sourceKeys:['key']});
  const raw = new DatabaseSync(path);
  try {
    raw.exec('BEGIN');
    const insert = raw.prepare("INSERT INTO consolidation_jobs(id,job_json,status) VALUES(?,?,'complete')");
    for(let n=0;n<16_385;n++) {
      const job={...source,id:`history-${n}`,sourceKeys:[`old-${n}`]};
      insert.run(job.id,JSON.stringify(job));
    }
    raw.exec('COMMIT');
  } finally {raw.close();}
  assert.deepEqual(a.listKnownSessionFiles(),[source.sessionFile]);
  const status=a.readConsolidationStatus([], [source.sessionFile]);
  assert.equal(status.pendingJobs.length,1);
});
