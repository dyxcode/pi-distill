import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {test} from 'node:test';
for(const mode of ['compact','auto','quit','new','resume','fork','reload','update','roundtrip','foreground']){
 test(`real Pi SDK lifecycle automatically handles ${mode}`,()=>{
   const result=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs',mode],{encoding:'utf8',timeout:20000});
   assert.ifError(result.error); assert.equal(result.status,0,result.stderr||result.stdout);
   const report=JSON.parse(result.stdout.trim().split('\n').at(-1)); assert.equal(report.passed,true);
 });
}

test('real Pi SDK runtime teardown fences a late background model reply',()=>{
 const result=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs','late-reply'],{encoding:'utf8',timeout:20000});
 assert.ifError(result.error);assert.equal(result.status,0,result.stderr||result.stdout);
 const report=JSON.parse(result.stdout.trim().split('\n').at(-1));assert.equal(report.passed,true);
});

import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
test('real Pi SDK recovers a missed shutdown after the original process has exited',async t=>{
 const root=await mkdtemp(join(tmpdir(),'pi-distill-crash-recovery-'));t.after(()=>rm(root,{recursive:true,force:true}));
 const crashed=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs','crash-capture',root],{encoding:'utf8',timeout:20000});
 assert.ifError(crashed.error);assert.equal(crashed.status,0,crashed.stderr||crashed.stdout);
 const before=JSON.parse(crashed.stdout.trim().split('\n').at(-1));assert.equal(before.passed,true);assert.equal(before.extractionCalls,0);
 const recovered=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs','crash-recover',root,before.sessionFile],{encoding:'utf8',timeout:20000});
 assert.ifError(recovered.error);assert.equal(recovered.status,0,recovered.stderr||recovered.stdout);
 const after=JSON.parse(recovered.stdout.trim().split('\n').at(-1));assert.equal(after.passed,true);assert.ok(after.extractionCalls>=1);
});

test('a clean quit leaves only a durable source trigger for the next process to reopen and distill',async t=>{
 const root=await mkdtemp(join(tmpdir(),'pi-distill-queued-reopen-'));t.after(()=>rm(root,{recursive:true,force:true}));
 const captured=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs','close-capture',root],{encoding:'utf8',timeout:20000});
 assert.ifError(captured.error);assert.equal(captured.status,0,captured.stderr||captured.stdout);
 const first=JSON.parse(captured.stdout.trim().split('\n').at(-1));
 assert.equal(first.passed,true);assert.equal(first.extractionCalls,0);
 const reopened=spawnSync(process.execPath,['tests/consolidation-pi-child.mjs','close-recover',root,first.sessionFile],{encoding:'utf8',timeout:20000});
 assert.ifError(reopened.error);assert.equal(reopened.status,0,reopened.stderr||reopened.stdout);
 const second=JSON.parse(reopened.stdout.trim().split('\n').at(-1));
 assert.equal(second.passed,true);assert.ok(second.extractionCalls>=1);
});
