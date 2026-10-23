import {unlinkSync,lstatSync} from 'node:fs';
import {SQLiteMemoryStore} from '../../src/storage/store.ts';
const [mode,path,payload]=process.argv.slice(2);
const task=JSON.parse(payload);
const store=new SQLiteMemoryStore(path);
if (mode === 'clean' || mode === 'clean-auto') {
 const {CleanupCoordinator}=await import('../../src/retention/coordinator.ts');
 process.send({ready:true});
 await new Promise(resolve=>process.once('message',resolve));
 const cleaner=new CleanupCoordinator({openStore:()=>new SQLiteMemoryStore(path),...(mode==='clean'?{retryDelayMs:10}:{})});
 cleaner.wake();await cleaner.whenIdle();
 const deadline=Date.now()+(mode==='clean'?1_500:8_000);
 while(Date.now()<deadline&&store.getDeletion(task.sessionFile)?.state!=='complete') {
  await new Promise(resolve=>setTimeout(resolve,5));
 }
 const completed=store.getDeletion(task.sessionFile)?.state==='complete';
 cleaner.close();store.close();
 process.send({done:true,completed},()=>process.disconnect());
} else {
const access=store.acquireSourceAccess();
if (!access) throw new Error('Fixture source access unavailable');
if (mode==='hold') {
 const {reason,intentId,id,status,...source}=task;
 store.saveSessionUsage({...source,ownerPid:process.pid,ownerId:'child',processToken:'child',state:'active'});
 process.send({ready:true});
 setInterval(()=>{},1000);
} else if (mode==='crash-delete') {
 store.claimCleanTask(task.id,'crash',Date.now(),60_000);
 const info=lstatSync(task.sessionFile,{bigint:true});
 const version=[info.dev,info.ino,info.size,info.mtimeNs,info.ctimeNs].join(':');
 if (!store.prepareDeletion(task.id,'crash',access,store.retentionVersion(),version)) throw new Error('No intent');
 // Fresh claim allows the next call to enter its transaction; expiry is only a worker scheduling bound.
 store.releaseCleanTask(task.id,'crash');store.claimCleanTask(task.id,'crash-final',Date.now(),10);
 store.finishDeletion(task.id,'crash-final',access,store.retentionVersion(),()=>{
  unlinkSync(task.sessionFile);
  process.kill(process.pid,'SIGKILL');
  return true;
 });
 throw new Error('Crash injection failed');
}

}
