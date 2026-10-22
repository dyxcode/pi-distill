import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {test} from 'node:test';
for(const mode of ['compact','auto','quit','new','resume','fork','reload','update','roundtrip']){
 test(`real Pi SDK lifecycle automatically handles ${mode}`,()=>{
   const result=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs',mode],{encoding:'utf8',timeout:20000});
   assert.ifError(result.error); assert.equal(result.status,0,result.stderr||result.stdout);
   const report=JSON.parse(result.stdout.trim().split('\n').at(-1)); assert.equal(report.passed,true);
 });
}

import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
test('real Pi SDK recovers a missed shutdown after the original process has exited',async t=>{
 const root=await mkdtemp(join(tmpdir(),'pi-distill-crash-recovery-'));t.after(()=>rm(root,{recursive:true,force:true}));
 for(const mode of ['crash-capture','crash-recover']){
  const child=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs',mode,root],{encoding:'utf8',timeout:20000});
  assert.ifError(child.error);assert.equal(child.status,0,child.stderr||child.stdout);
  const report=JSON.parse(child.stdout.trim().split('\n').at(-1));assert.equal(report.passed,true);
  if(mode==='crash-capture')assert.equal(report.extractionCalls,0);
 }
});
