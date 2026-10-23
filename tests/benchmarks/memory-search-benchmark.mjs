import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { SQLiteMemoryStore } from '../../src/storage/store.ts';
import { MemoryTools } from '../../src/memory/retrieval.ts';
const dir=await mkdtemp(join(tmpdir(),'pi-distill-bench-'));
try {
 for(const count of [2000,20000,80000]) {
  const path=join(dir,`${count}.sqlite`);new SQLiteMemoryStore(path).close();
  const db=new DatabaseSync(path); const statement=db.prepare('INSERT INTO memories VALUES (?,?,?,1,?)');
  const time='2026-10-21T00:00:00.000Z';db.exec('BEGIN');
  for(let i=0;i<count;i++)statement.run(String(i).padStart(8,'0'),'Synthetic unrelated context. '.repeat(30),time,time);
  db.exec('COMMIT');db.close();
  const tools=new MemoryTools(()=>new SQLiteMemoryStore(path));let cursor;let pages=0;const timings=[];let partialEmpty=false;
  do {tools.reset();const start=performance.now();const response=await tools.search({terms:['absent','missing','needle','测试'],...(cursor?{cursor}:{})});
  timings.push(Math.round(performance.now()-start));const r=response.details;
  if(r.status!=='ok')throw Error(JSON.stringify(r));partialEmpty ||= r.hasMore&&r.candidates.length===0;
  cursor=r.nextCursor;pages++;if(pages>100)throw Error('No progress');}while(cursor);
  tools.close();console.log(JSON.stringify({count,pages,timingsMs:timings,partialEmpty}));
 }
}finally{await rm(dir,{recursive:true,force:true});}
