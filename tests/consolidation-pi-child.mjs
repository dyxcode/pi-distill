import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
const [mode,externalRoot,externalSessionFile]=process.argv.slice(2);
const root=externalRoot??await mkdtemp(join(tmpdir(),'pi-distill-consolidation-sdk-'));
process.env.PI_CODING_AGENT_DIR=join(root,'agent');
process.env.PI_CODING_AGENT_SESSION_DIR=join(root,'sessions');
process.env.PI_OFFLINE='1'; process.env.PI_SKIP_VERSION_CHECK='1'; process.env.PI_TELEMETRY='0';
const pi=await import('@earendil-works/pi-coding-agent');
const ai=await import('@earendil-works/pi-ai');
const {SQLiteMemoryStore}=await import('../src/memory-store.ts');
const agentDir=process.env.PI_CODING_AGENT_DIR;
const cwd=join(root,'project'); await mkdir(cwd,{recursive:true});
const memoryPath=join(agentDir,'distill','memories.sqlite');
const queuedTrigger=(sessionId,reason='close')=>{
  const db=new SQLiteMemoryStore(memoryPath);
  try{return db.listDistillTriggers({limit:100}).items.find(trigger=>trigger.sessionId===sessionId&&trigger.reason===reason);}
  finally{db.close();}
};
const assertQueuedClose=(sessionId,sessionFile,leafId,{pending=false}={})=>{
  const trigger=queuedTrigger(sessionId);
  assert.ok(trigger,`session ${sessionId} should have a durable close trigger`);
  assert.equal(trigger.sessionId,sessionId);
  assert.equal(trigger.sessionFile,sessionFile);
  assert.equal(trigger.leafId,leafId);
  assert.equal(trigger.cwd,cwd);
  assert.equal(trigger.compactionRecordId,null);
  assert.doesNotMatch(JSON.stringify(trigger),/transcript|newEvidence|message content/iu);
  if(pending){assert.equal(trigger.status,'pending');assert.deepEqual(trigger.jobIds,[]);}
  return trigger;
};
const faux=ai.fauxProvider({tokensPerSecond:100000,models:[{id:'synthetic',contextWindow:32768,maxTokens:8192}]});
let extractionCalls=0;
let extractionSignal;
let resolveExtractionStarted;
const extractionStarted=new Promise(resolve=>{resolveExtractionStarted=resolve;});
let resolveLateReply;
const lateReply=new Promise(resolve=>{resolveLateReply=resolve;});
const slowModes=new Set(['quit','new','resume','fork']);
const response=(context,streamOptions)=>{
  const text=context.messages.flatMap(message=>typeof message.content==='string'?[message.content]:message.content?.filter(block=>block.type==='text').map(block=>block.text)??[]).join('\n');
  const key=text.match(/"key"\s*:\s*"([^"\n]+)"/u)?.[1];
  if(key){extractionCalls++;extractionSignal=streamOptions?.signal;resolveExtractionStarted();
    if(mode==='late-reply')return lateReply.then(()=>ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'For fictional Cedar, keep tests local.',sourceKeys:[key]}]})));
    if(mode==='foreground'&&extractionCalls===1)return lateReply.then(()=>ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'For fictional Cedar, keep tests local.',sourceKeys:[key]}]})));
    if(mode==='update'){
      if(!context.messages.some(m=>m.role==='toolResult'))return ai.fauxAssistantMessage([ai.fauxToolCall('memory_read',{id:'keep',expectedRevision:1}),ai.fauxToolCall('memory_read',{id:'remove',expectedRevision:1})],{stopReason:'toolUse'});
      return ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'update',id:'keep',expectedRevision:1,content:'Cedar uses local tests.',sourceKeys:[key]},{action:'delete',id:'remove',expectedRevision:1,sourceKeys:[key]}]}));
    }
    if(slowModes.has(mode))return new Promise(resolve=>setTimeout(()=>resolve(ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'For fictional Cedar, keep tests local.',sourceKeys:[key]}]}))),2200));
    return ai.fauxAssistantMessage(JSON.stringify({operations:[{action:'create',content:'For fictional Cedar, keep tests local.',sourceKeys:[key]}]}));}
  const full=JSON.stringify(context);
  if(/summariz|summary|compaction/i.test(full)&&context.messages.length===1)return ai.fauxAssistantMessage('Cedar keeps tests local.');
  return ai.fauxAssistantMessage('Cedar will use local tests.');
};
faux.setResponses(Array.from({length:80},()=>response));
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
let session;
let shutdownMs;
try{
  if(mode==='update'){await mkdir(join(agentDir,'distill'),{recursive:true});const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));
    db.save({id:'keep',content:'Cedar uses remote tests.',createdAt:'2026-10-21T00:00:00.000Z'});
    db.save({id:'remove',content:'Cedar remote test setup is pending.',createdAt:'2026-10-21T00:00:00.000Z'});db.close();}

  const manager=['close-recover','crash-recover'].includes(mode)&&externalSessionFile
    ?pi.SessionManager.open(externalSessionFile,join(root,'sessions'),cwd)
    :pi.SessionManager.create(cwd,join(root,'sessions'));
  runtime=await pi.createAgentSessionRuntime(factory,{cwd,agentDir,sessionManager:manager});
  session=runtime.session;
  if(!['close-recover','crash-recover'].includes(mode))await session.prompt('For fictional Cedar, our tests live only on this computer.' + (mode==='auto' ? ' The fictional Cedar convention applies to its tests.'.repeat(40) : ''));
  if(mode==='crash-capture'){
    const sessionFile=session.sessionManager.getSessionFile();
    assert.ok(sessionFile);
    assert.equal(extractionCalls,0,'an unannounced crash must not have started model work');
    const db=new SQLiteMemoryStore(memoryPath);
    assert.deepEqual(db.listDistillTriggers({limit:100}).items,[],'the source should still be unqueued when the process crashes without shutdown');
    db.close();
    console.log(JSON.stringify({mode,sessionFile,extractionCalls,passed:true}));
    process.exit(0);
  }
  if(mode==='close-capture'){
    const before={id:session.sessionManager.getSessionId(),file:session.sessionManager.getSessionFile(),leaf:session.sessionManager.getLeafId()};
    const start=Date.now();await runtime.dispose();shutdownMs=Date.now()-start;runtime=undefined;
    assert.ok(shutdownMs<1000,`clean shutdown waited ${shutdownMs}ms for consolidation`);
    assert.equal(extractionCalls,0,'clean shutdown must not invoke the model');
    assertQueuedClose(before.id,before.file,before.leaf,{pending:true});
    console.log(JSON.stringify({mode,sessionFile:before.file,extractionCalls,shutdownMs,passed:true}));
    process.exit(0);
  }
  if(mode==='compact'){
    await session.prompt('This convention applies to Cedar.');
    const source={sessionId:session.sessionManager.getSessionId(),sessionFile:session.sessionManager.getSessionFile(),leafId:session.sessionManager.getLeafId()};
    await session.compact();
    const trigger=queuedTrigger(session.sessionManager.getSessionId(),'compact');
    assert.ok(trigger,'successful compaction should durably register its source boundary');
    assert.equal(trigger.reason,'compact');
    assert.equal(trigger.sessionId,source.sessionId);
    assert.equal(trigger.sessionFile,source.sessionFile);
    assert.equal(trigger.leafId,source.leafId,'the queued source leaf must precede the compaction summary');
    assert.equal(trigger.cwd,cwd);
    assert.notEqual(trigger.status,'candidate','a saved compaction record should confirm the durable source trigger');
    assert.doesNotMatch(JSON.stringify(trigger),/transcript|newEvidence|message content/iu);
    assert.ok(trigger.compactionRecordId,'a compact trigger is ready only with its saved compaction record');
  }else if(mode==='reload'){
    await session.reload();
    assert.equal(extractionCalls,0,'reload must not create a consolidation trigger');
    assert.equal(queuedTrigger(session.sessionManager.getSessionId()),undefined,'reload must not register a close trigger');
  }else if(mode==='new'){
    const before={id:session.sessionManager.getSessionId(),file:session.sessionManager.getSessionFile(),leaf:session.sessionManager.getLeafId()};
    const start=Date.now(); await runtime.newSession(); shutdownMs=Date.now()-start;
    assert.ok(shutdownMs<1000,`session replacement waited ${shutdownMs}ms for consolidation`);
    assertQueuedClose(before.id,before.file,before.leaf);
  }else if(mode==='fork'){
    const before={id:session.sessionManager.getSessionId(),file:session.sessionManager.getSessionFile(),leaf:session.sessionManager.getLeafId()};
    const entry=session.sessionManager.getBranch().find(entry=>entry.type==='message'&&entry.message.role==='user');
    assert.ok(entry); const start=Date.now(); await runtime.fork(entry.id,{position:'at'}); shutdownMs=Date.now()-start;
    assert.ok(shutdownMs<1000,`fork waited ${shutdownMs}ms for consolidation`);
    assertQueuedClose(before.id,before.file,before.leaf);
  }else if(mode==='resume'){
    const path=session.sessionManager.getSessionFile(); assert.ok(path);
    const before={id:session.sessionManager.getSessionId(),file:path,leaf:session.sessionManager.getLeafId()};
    const start=Date.now(); await runtime.newSession(); shutdownMs=Date.now()-start;
    assert.ok(shutdownMs<1000,`session replacement waited ${shutdownMs}ms for consolidation`);
    assertQueuedClose(before.id,before.file,before.leaf);
    await runtime.switchSession(path);
  }else if(mode==='quit'||mode==='update'){
    const before={id:session.sessionManager.getSessionId(),file:session.sessionManager.getSessionFile(),leaf:session.sessionManager.getLeafId()};
    const start=Date.now(); await runtime.dispose(); shutdownMs=Date.now()-start; runtime=undefined;
    assert.ok(shutdownMs<1000,`session shutdown waited ${shutdownMs}ms for consolidation`);
    assert.equal(extractionCalls,0,'closing a runtime must queue source references without starting model work');
    assertQueuedClose(before.id,before.file,before.leaf,{pending:true});
    runtime=await pi.createAgentSessionRuntime(factory,{cwd,agentDir,sessionManager:pi.SessionManager.open(before.file,join(root,'sessions'),cwd)});
    session=runtime.session;
  }else if(mode==='late-reply'){
    await session.compact();
    await Promise.race([extractionStarted,new Promise((_,reject)=>setTimeout(()=>reject(new Error('background extraction did not start')),3000))]);
    assert.ok(extractionSignal,'the scripted provider should receive the background request signal');
    const before={id:session.sessionManager.getSessionId(),file:session.sessionManager.getSessionFile(),leaf:session.sessionManager.getLeafId()};
    const start=Date.now(); await runtime.dispose(); shutdownMs=Date.now()-start; runtime=undefined;
    assert.ok(shutdownMs<1000,`runtime teardown waited ${shutdownMs}ms for a late provider reply`);
    resolveLateReply();
    await new Promise(resolve=>setTimeout(resolve,50));
    const db=new SQLiteMemoryStore(memoryPath);
    assert.equal(db.searchBatch({terms:['Cedar'],match:'any'}).length,0,'a reply from a disposed runtime must not commit');
    assert.ok(db.listConsolidationJobs().length>0,'the source remains recoverable after the late reply');
    db.close();
    assertQueuedClose(before.id,before.file,before.leaf);
  }else if(mode==='foreground'){
    await session.compact();
    await Promise.race([extractionStarted,new Promise((_,reject)=>setTimeout(()=>reject(new Error('background extraction did not start')),3000))]);
    assert.ok(extractionSignal&&!extractionSignal.aborted,'the background request should remain live after it starts');
    await session.prompt('A later synthetic Cedar example adds another ordinary message.');
    assert.equal(extractionCalls,1,'ordinary foreground work should not start another distillation request');
    assert.equal(extractionSignal.aborted,false,'sending a user message must not cancel an already-issued distillation request');
    await session.compact();
    assert.equal(extractionCalls,1,'compaction while background work is in flight must not overlap another distillation request');
    resolveLateReply();
  }
  if(mode!=='reload'&&mode!=='close-capture'&&mode!=='late-reply'){
    if(mode==='roundtrip')await runtime.newSession();
    const deadline=Date.now()+8000;
    let found;
    while(Date.now()<deadline){
      try{const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));
        const matches=db.searchBatch({terms:['Cedar'],match:'any'}).filter(row=>row.matchedTerms.length).map(row=>row.memory);
        found=mode==='update'
          ?matches.find(memory=>memory.id==='keep'&&memory.content==='Cedar uses local tests.')
          :matches[0];
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
    if(!['late-reply'].includes(mode)){
      assert.ok(found,`automatic ${mode} must persist memory without a remember command`);
      assert.ok(extractionCalls>=1);
    }
    if(mode==='update'){const db=new SQLiteMemoryStore(join(agentDir,'distill','memories.sqlite'));assert.equal(db.get('keep')?.content,'Cedar uses local tests.');assert.equal(db.get('keep')?.revision,2);assert.equal(db.get('remove'),undefined);db.close();}
    if(mode==='roundtrip'){
      const events=[];runtime.session.subscribe(e=>{if(e.type==='tool_execution_end')events.push(e);});
      faux.setResponses([ai.fauxAssistantMessage(ai.fauxToolCall('memory_search',{terms:['Cedar']}),{stopReason:'toolUse'}),ai.fauxAssistantMessage(ai.fauxToolCall('memory_read',{id:found.id,expectedRevision:found.revision}),{stopReason:'toolUse'}),ai.fauxAssistantMessage('Cedar uses local tests.')]);
      await runtime.session.prompt('What testing convention applies to Cedar?');
      assert.equal(events.length,2);assert.equal(events[0].toolName,'memory_search');assert.equal(events[1].result.details.content,'For fictional Cedar, keep tests local.');
    }
  }
  if(mode==='crash-recover'){
    assertQueuedClose(session.sessionManager.getSessionId(),session.sessionManager.getSessionFile(),session.sessionManager.getLeafId());
    assert.ok(extractionCalls>=1,'a new Pi runtime should process the source recovered from the crashed owner');
  }
  console.log(JSON.stringify({mode,extractionCalls,calls:faux.state.callCount,passed:true}));
}finally{if(runtime)await runtime.dispose();if(!externalRoot)await rm(root,{recursive:true,force:true});}
