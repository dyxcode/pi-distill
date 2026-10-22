import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
const [mode,externalRoot]=process.argv.slice(2);
const root=externalRoot??await mkdtemp(join(tmpdir(),'pi-distill-consolidation-sdk-'));
process.env.PI_CODING_AGENT_DIR=join(root,'agent');
process.env.PI_CODING_AGENT_SESSION_DIR=join(root,'sessions');
process.env.PI_OFFLINE='1'; process.env.PI_SKIP_VERSION_CHECK='1'; process.env.PI_TELEMETRY='0';
const pi=await import('@earendil-works/pi-coding-agent');
const ai=await import('@earendil-works/pi-ai');
const {SQLiteMemoryStore}=await import('../src/memory-store.ts');
const agentDir=process.env.PI_CODING_AGENT_DIR;
const cwd=join(root,'project'); await mkdir(cwd,{recursive:true});
const faux=ai.fauxProvider({tokensPerSecond:100000,models:[{id:'synthetic',contextWindow:32768,maxTokens:8192}]});
let extractionCalls=0;
const response=(context)=>{
  const text=context.messages.flatMap(message=>typeof message.content==='string'?[message.content]:message.content?.filter(block=>block.type==='text').map(block=>block.text)??[]).join('\n');
  const key=text.match(/"key"\s*:\s*"([^"\n]+)"/u)?.[1];
  if(key){extractionCalls++;
    if(mode==='update'){
      if(!context.messages.some(m=>m.role==='toolResult'))return ai.fauxAssistantMessage([ai.fauxToolCall('memory_read',{id:'keep',expectedRevision:1}),ai.fauxToolCall('memory_read',{id:'remove',expectedRevision:1})],{stopReason:'toolUse'});
      return ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'update',id:'keep',expectedRevision:1,content:'Cedar uses local tests.',sourceKeys:[key]},{action:'delete',id:'remove',expectedRevision:1,sourceKeys:[key]}]}));
    }
    return ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'For fictional Cedar, keep tests local.',sourceKeys:[key]}]}));}
  const full=JSON.stringify(context);
  if(/summariz|summary|compaction/i.test(full)&&context.messages.length===1)return ai.fauxAssistantMessage('Cedar keeps tests local.');
  return ai.fauxAssistantMessage('Cedar will use local tests.');
};
faux.setResponses(Array.from({length:40},()=>response));
const modelRuntime=await pi.ModelRuntime.create({authPath:join(agentDir,'auth.json'),modelsPath:null,refreshOnCreate:false});
modelRuntime.registerNativeProvider(faux.provider);
const settings=pi.SettingsManager.inMemory({compaction:{enabled:mode==='auto',reserveTokens:mode==='auto'?32600:512,keepRecentTokens:0}});
const factory=async options=>{
  const services=await pi.createAgentSessionServices({cwd:options.cwd,agentDir,settingsManager:settings,modelRuntime,
    resourceLoaderOptions:{additionalExtensionPaths:[resolve('src/index.ts')],noSkills:true,noPromptTemplates:true,noThemes:true,noContextFiles:true}});
  const created=await pi.createAgentSessionFromServices({services,sessionManager:options.sessionManager,model:faux.getModel(),noTools:'builtin',sessionStartEvent:options.sessionStartEvent});
  assert.deepEqual(created.extensionsResult.errors,[]);
  await created.session.bindExtensions({mode:'print',onError:error=>{throw new Error(error.error);}});
  return {...created,services,diagnostics:services.diagnostics};
};
let runtime;
try{
  if(mode==='update'){await mkdir(join(agentDir,'distill'),{recursive:true});const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));
    db.save({id:'keep',content:'Cedar uses remote tests.',createdAt:'2026-10-21T00:00:00.000Z'});
    db.save({id:'remove',content:'Cedar remote test setup is pending.',createdAt:'2026-10-21T00:00:00.000Z'});db.close();}

  runtime=await pi.createAgentSessionRuntime(factory,{cwd,agentDir,sessionManager:pi.SessionManager.create(cwd,join(root,'sessions'))});
  const session=runtime.session;
  await session.prompt('For fictional Cedar, our tests live only on this computer.' + (mode==='auto' ? ' The fictional Cedar convention applies to its tests.'.repeat(40) : ''));
  if(mode==='crash-capture'){console.log(JSON.stringify({passed:true,extractionCalls}));process.exit(0);}
  if(mode==='compact'){
    await session.prompt('This convention applies to Cedar.');
    await session.compact();
  }else if(mode==='reload'){
    await session.reload();
    assert.equal(extractionCalls,0,'reload must not create a consolidation trigger');
  }else if(mode==='new'){
    await runtime.newSession();
  }else if(mode==='fork'){
    const entry=session.sessionManager.getBranch().find(entry=>entry.type==='message'&&entry.message.role==='user');
    assert.ok(entry); await runtime.fork(entry.id,{position:'at'});
  }else if(mode==='resume'){
    const path=session.sessionManager.getSessionFile(); assert.ok(path);
    await runtime.newSession(); await runtime.switchSession(path);
  }else if(mode==='quit'||mode==='update'){
    await runtime.dispose(); runtime=undefined;
  }
  if(mode!=='reload'){
    if(mode==='roundtrip')await runtime.newSession();
    const deadline=Date.now()+4000;
    let found;
    while(Date.now()<deadline){
      try{const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));
        found=db.searchBatch({terms:['Cedar'],match:'any'}).find(row=>row.matchedTerms.length)?.memory;
        db.close();}catch{}
      if(found)break;
      await new Promise(resolve=>setTimeout(resolve,20));
    }
    if(!found){
      const {loadSnapshot,planSources}=await import('../src/consolidation-source.ts');
      const file=session.sessionManager.getSessionFile();
      const snap=file?await loadSnapshot(file):undefined;
      const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));
      const jobs=db.listConsolidationJobs();db.close();
      console.error(JSON.stringify({extractionCalls,providerCalls:faux.state.callCount,sourcePending:snap?.pending,reason:snap?.pendingReason,batches:snap?planSources(snap,new Set()).length:0,jobs:jobs.map(j=>({id:j.id,sourceKeys:j.sourceKeys}))}));
    }
    assert.ok(found,`automatic ${mode} must persist memory without a remember command`);
    assert.ok(extractionCalls>=1);
    if(mode==='update'){const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));assert.equal(db.get('keep')?.content,'Cedar uses local tests.');assert.equal(db.get('keep')?.revision,2);assert.equal(db.get('remove'),undefined);db.close();}
    if(mode==='roundtrip'){
      const events=[];runtime.session.subscribe(e=>{if(e.type==='tool_execution_end')events.push(e);});
      faux.setResponses([ai.fauxAssistantMessage(ai.fauxToolCall('memory_search',{terms:['Cedar']}),{stopReason:'toolUse'}),ai.fauxAssistantMessage(ai.fauxToolCall('memory_read',{id:found.id,expectedRevision:found.revision}),{stopReason:'toolUse'}),ai.fauxAssistantMessage('Cedar uses local tests.')]);
      await runtime.session.prompt('What testing convention applies to Cedar?');
      assert.equal(events.length,2);assert.equal(events[0].toolName,'memory_search');assert.equal(events[1].result.details.content,'For fictional Cedar, keep tests local.');
    }
  }
  console.log(JSON.stringify({mode,extractionCalls,calls:faux.state.callCount,passed:true}));
}finally{if(runtime)await runtime.dispose();if(!externalRoot)await rm(root,{recursive:true,force:true});}
