import { SQLiteMemoryStore } from '../src/memory-store.ts';

const store = new SQLiteMemoryStore(process.argv[2]);
process.send({ready:true});
process.on('message', ({action,now,claimToken,id}) => {
  try {
    if(action==='claim')process.send({claim:store.claimDistillTrigger({workerId:String(process.pid),now,leaseMs:100})??null});
    else if(action==='schedule') {
      const page=store.listScheduledDistill(now,true);
      const candidate=page.items[0];
      process.send({selected:candidate?store.claimScheduledDistill(candidate,{workerId:String(process.pid),now,leaseMs:100}):null});
    } else if(action==='scheduled-complete') {
      store.prepareDistillTrigger({id,claimToken,now,jobs:[]});process.send({prepared:true});
    }
    else if(action==='prepare') {
      const trigger=store.listDistillTriggers({limit:10}).items[0];
      store.prepareDistillTrigger({id:trigger.id,claimToken,now,jobs:[]});process.send({prepared:true});
    } else if(action==='stop'){store.close();process.disconnect();}
  } catch(error){process.send({error:error.message});}
});
