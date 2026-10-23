import assert from 'node:assert/strict';
import { before, after, test } from 'node:test';
import { spawnSync } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = fileURLToPath(new URL('../..', import.meta.url));
let root, artifact, inventory;
function run(command, args, options = {}) {
  const result = spawnSync(command, args, {encoding:'utf8',timeout:60_000,...options});
  assert.ifError(result.error);
  assert.equal(result.status,0,result.stderr || result.stdout);
  return result.stdout;
}
function environment(agentDir, sessions) {
  return {PATH:process.env.PATH,PI_CODING_AGENT_DIR:agentDir,PI_CODING_AGENT_SESSION_DIR:sessions,
    PI_OFFLINE:'1',PI_SKIP_VERSION_CHECK:'1',PI_TELEMETRY:'0'};
}
async function files(directory) {
  const result=[];
  for(const item of await readdir(directory,{withFileTypes:true})) {
    const path=join(directory,item.name);
    if(item.isDirectory()) result.push(...await files(path));
    else if(item.isFile()) result.push(path);
  }
  return result;
}
before(async()=>{
  root=await mkdtemp(join(tmpdir(),'pi-distill-distribution-'));
  const source=join(root,'source');await mkdir(source);
  // Copy public inputs only. Exclusion probes are synthetic, never real local state.
  for(const name of ['src','docs','README.md','package.json','LICENSE']) {
    if(existsSync(join(repo,name))) await cp(join(repo,name),join(source,name),{recursive:true,
      filter:path=>!path.endsWith('.sqlite')&&!path.endsWith('.log')});
  }
  for(const name of ['AGENTS.md','agent-docs/plan.md','tests/unit/synthetic.test.mjs',
    '.pi/auth.json','sessions/synthetic.jsonl','src/synthetic.sqlite','docs/agent-local/plan.md','docs/assets/synthetic.log']) {
    const path=join(source,name);await mkdir(dirname(path),{recursive:true});await writeFile(path,'synthetic exclusion probe');
  }
  const packed=JSON.parse(run('npm',['pack','--json','--ignore-scripts','--offline','--cache',join(root,'cache'),'--pack-destination',root],{cwd:source}))[0];
  inventory=packed.files.map(file=>file.path);
  const extracted=join(root,'extracted');await mkdir(extracted);
  run('tar',['-xzf',join(root,packed.filename),'-C',extracted]);artifact=join(extracted,'package');
});
after(async()=>{if(root)await rm(root,{recursive:true,force:true});});

test('distribution contains runtime and bilingual reader materials without development or personal data',async()=>{
  assert.ok(!inventory.some(path=>path.startsWith('tests/')),'test source belongs in GitHub, not the install tarball');
  for(const required of ['package.json','README.md','LICENSE','docs/installation.md','docs/i18n/installation.zh-CN.md','docs/i18n/README.zh-CN.md','docs/assets/pi-distill-workflow.svg','docs/assets/pi-distill-workflow.zh-CN.svg']) assert.ok(inventory.includes(required),required);
  assert.ok(inventory.every(path=>/^(?:package\.json|README\.md|LICENSE|src\/.+\.ts|docs\/[^/]+\.md|docs\/i18n\/[^/]+\.md|docs\/assets\/[^/]+\.svg)$/.test(path)),JSON.stringify(inventory));
  for(const path of await files(join(repo,'src'))) {
    if(!path.endsWith('.ts'))continue;
    const relative=path.slice(repo.length).replace(/^\//,'');
    assert.deepEqual(await readFile(join(artifact,relative)),await readFile(path),relative);
  }
  assert.equal(existsSync(join(artifact,'node_modules')),false,'Pi must supply its own host modules');
  const manifest=JSON.parse(await readFile(join(artifact,'package.json'),'utf8'));
  assert.equal(manifest.license,'MIT');assert.equal(manifest.private,true);
  assert.ok(manifest.keywords.includes('pi-package'));
  assert.equal(manifest.peerDependencies['@earendil-works/pi-ai'],'*');
  const license=await readFile(join(artifact,'LICENSE'),'utf8');assert.match(license,/MIT License/);assert.match(license,/Copyright \(c\) 2026 dyxcode/);
});

test('extracted package installs, loads once, reloads and removes through real Pi',()=>{
  const fixture=join(root,'registration');
  const report=JSON.parse(run(process.execPath,[join(repo,'tests/helpers/distribution-pi-child.mjs'),fixture,artifact],{cwd:root}));
  assert.equal(report.passed,true);
});

test('extracted package distills, cleans, recalls and updates memory after source deletion',()=>{
  const fixture=join(root,'lifecycle');const agent=join(fixture,'agent');const sessions=join(fixture,'sessions');
  const output=run(process.execPath,[join(repo,'tests/helpers/session-independence-child.mjs'),fixture,agent,sessions,join(fixture,'project'),join(artifact,'src/index.ts')],{
    cwd:root,env:environment(agent,sessions)});
  assert.equal(JSON.parse(output.trim().split('\n').at(-1)).passed,true);
});

test('extracted package records actionable diagnostics with its own release version',async()=>{
  const fixture=join(root,'diagnostics');const agent=join(fixture,'agent');
  const output=run(process.execPath,[join(repo,'tests/helpers/memory-pi-child.mjs'),'diagnostics',fixture,agent,join(fixture,'project'),join(artifact,'src/index.ts')],{
    cwd:root,env:environment(agent,join(fixture,'sessions'))});
  const report=JSON.parse(output.trim().split('\n').at(-1));
  assert.deepEqual(report.statuses,['unavailable','ok']);
  const events=report.records.filter(event=>event.operation==='memory.search');
  assert.deepEqual(events.map(event=>event.outcome),['failed','recovered']);
  const manifest=JSON.parse(await readFile(join(artifact,'package.json'),'utf8'));
  assert.ok(events.every(event=>event.extensionVersion===manifest.version));
});

test('distributed reader documents resolve their local links and images',async()=>{
  for(const path of [join(artifact,'README.md'),...await files(join(artifact,'docs'))].filter(path=>path.endsWith('.md'))) {
    const markdown=await readFile(path,'utf8');
    for(const match of markdown.matchAll(/!?\[[^\]]*\]\(([^\s)]+)(?:\s+"[^"]*")?\)/g)) {
      const target=match[1].split('#')[0];
      if(!target||/^[a-z]+:/i.test(target))continue;
      assert.ok(existsSync(resolve(dirname(path),decodeURIComponent(target))),`${path}: ${target}`);
    }
  }
});
