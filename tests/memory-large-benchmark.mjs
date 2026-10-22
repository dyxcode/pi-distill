import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { SQLiteMemoryStore } from '../src/memory-store.ts';
import { MemoryTools } from '../src/memory-tools.ts';
const root=await mkdtemp(join(tmpdir(),'pi-distill-large-'));const path=join(root,'memory.sqlite');
const writer=new SQLiteMemoryStore(path); const tools=new MemoryTools(()=>new SQLiteMemoryStore(path));
try {
 writer.save({id:'large',content:'NEEDLE '+'X'.repeat(8*1024*1024),createdAt:'2026-10-21T00:00:00.000Z'});
 const start=performance.now();const result=await tools.search({terms:['needle']});
 console.log(JSON.stringify({contentMiB:8,elapsedMs:Math.round(performance.now()-start),bytes:Buffer.byteLength(JSON.stringify(result)),status:result.details.status,candidates:result.details.candidates?.length}));
}finally{tools.close();writer.close();await rm(root,{recursive:true,force:true});}
