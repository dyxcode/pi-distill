import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const [root, artifact]=process.argv.slice(2);
assert.ok(root&&artifact);
const agentDir=join(root,'agent');const cwd=join(root,'project');
process.env.PI_CODING_AGENT_DIR=agentDir;
process.env.PI_CODING_AGENT_SESSION_DIR=join(root,'sessions');
process.env.PI_OFFLINE='1';process.env.PI_SKIP_VERSION_CHECK='1';process.env.PI_TELEMETRY='0';
await mkdir(agentDir,{recursive:true});await mkdir(cwd,{recursive:true});
await writeFile(join(agentDir,'settings.json'),JSON.stringify({enableInstallTelemetry:false,enableAnalytics:false}));
const {createAgentSession,SessionManager}=await import('@earendil-works/pi-coding-agent');
const cli=fileURLToPath(new URL('../../node_modules/.bin/pi',import.meta.url));
function manage(command) {
  const result=spawnSync(process.execPath,[cli,command,artifact],{cwd,env:process.env,encoding:'utf8',timeout:30000});
  assert.ifError(result.error);assert.equal(result.status,0,result.stderr||result.stdout);
}
manage('install');
const {session}=await createAgentSession({cwd,agentDir,sessionManager:SessionManager.inMemory(cwd),noTools:'builtin'});
try {
  for(let load=0;load<2;load++) {
    const extensions=session.resourceLoader.getExtensions();
    assert.deepEqual(extensions.errors,[]);assert.deepEqual(extensions.warnings??[],[]);
    assert.equal(extensions.extensions.filter(item=>item.resolvedPath===resolve(artifact,'src/index.ts')).length,1);
    assert.deepEqual(session.getAllTools().filter(tool=>tool.name.startsWith('memory_')).map(tool=>tool.name).sort(),['memory_read','memory_search']);
    if(load===0)await session.reload();
  }
  assert.equal(existsSync(join(agentDir,'distill','memories.sqlite')),false);
  assert.equal(existsSync(join(agentDir,'distill','diagnostics.sqlite')),false);
  manage('remove');await session.reload();
  assert.equal(session.resourceLoader.getExtensions().extensions.length,0);
  assert.equal(session.getAllTools().filter(tool=>tool.name.startsWith('memory_')).length,0);
  console.log(JSON.stringify({passed:true}));
} finally {session.dispose();}
