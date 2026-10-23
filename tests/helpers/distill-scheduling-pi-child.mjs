import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const [root] = process.argv.slice(2);
const agentDir = join(root, 'agent'), sessionDir = join(root, 'sessions'), cwd = join(root, 'project');
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_CODING_AGENT_SESSION_DIR = sessionDir;
process.env.PI_OFFLINE = '1';
process.env.PI_SKIP_VERSION_CHECK = '1';
process.env.PI_TELEMETRY = '0';
await mkdir(cwd, { recursive: true });
const pi = await import('@earendil-works/pi-coding-agent');
const ai = await import('@earendil-works/pi-ai');
const { SQLiteMemoryStore } = await import('../../src/storage/store.ts');
await mkdir(join(agentDir, 'distill'), { recursive: true });
const path = join(agentDir, 'distill', 'memories.sqlite');
const inspect = operation => { const store = new SQLiteMemoryStore(path); try { return operation(store); } finally { store.close(); } };
const queue = (label, count) => {
  const manager = pi.SessionManager.create(cwd, sessionDir);
  for (let i = 0; i < count; i++) manager.appendMessage({ role: 'user', content: `${label} ${i}: ${'Conditional synthetic policy. '.repeat(count === 1 ? 1 : 40)}`, timestamp: i + 1 });
  manager.appendMessage(ai.fauxAssistantMessage('Synthetic acknowledgement.'));
  const source = { id: label, sessionId: manager.getSessionId(), sessionFile: manager.getSessionFile(), cwd, leafId: manager.getLeafId(), reason: 'close' };
  assert.ok(source.sessionFile && existsSync(source.sessionFile));
  inspect(store => {
    for (const state of ['active', 'closing', 'released']) store.saveSessionUsage({ sessionId: source.sessionId, sessionFile: source.sessionFile, cwd, leafId: source.leafId,
      ownerId: label, processToken: label, ownerPid: process.pid, state });
    store.enqueueDistillTrigger(source);
  });
  return source;
};
const long = queue('Long Cedar', 36), short = queue('Short Cedar', 1);
const faux = ai.fauxProvider({ tokensPerSecond: 100000, models: [{ id: 'synthetic', contextWindow: 10000, maxTokens: 1024 }] });
const remembered = 'For Cedar, use local synthetic tests.';
let longRequests = 0;
const order = [];
function response(context, options) {
  let payload;
  for (const message of context.messages) {
    if (message.role !== 'user') continue;
    const text = typeof message.content === 'string' ? message.content : message.content.filter(x => x.type === 'text').map(x => x.text).join('\n');
    try { const parsed = JSON.parse(text); if (Array.isArray(parsed.newEvidence)) payload = parsed; } catch { /* Normal foreground prompt. */ }
  }
  if (payload) {
    const isShort = payload.newEvidence.some(x => x.text.includes('Short Cedar'));
    order.push(isShort ? 'short' : 'long');
    if (!isShort && ++longRequests > 1) return new Promise((_, reject) => {
      assert.ok(options.signal);
      if (options.signal.aborted) reject(options.signal.reason);
      else options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true });
    });
    return ai.fauxAssistantMessage(JSON.stringify({ operations: isShort ? [{ action: 'create', content: remembered, sourceKeys: [payload.newEvidence[0].key] }] : [] }));
  }
  const last = context.messages.filter(x => x.role === 'toolResult').at(-1);
  if (!last) return ai.fauxAssistantMessage(ai.fauxToolCall('memory_search', { terms: ['local synthetic'] }), { stopReason: 'toolUse' });
  if (last.toolName === 'memory_search') {
    const memory = inspect(store => store.searchBatch({ terms: ['local synthetic'], match: 'any' }).map(x => x.memory).find(x => x.content === remembered));
    assert.ok(memory);
    return ai.fauxAssistantMessage(ai.fauxToolCall('memory_read', { id: memory.id, expectedRevision: memory.revision }), { stopReason: 'toolUse' });
  }
  return ai.fauxAssistantMessage(remembered);
}
faux.setResponses(Array.from({ length: 100 }, () => response));
const modelRuntime = await pi.ModelRuntime.create({ authPath: join(agentDir, 'auth.json'), modelsPath: null, refreshOnCreate: false });
modelRuntime.registerNativeProvider(faux.provider);
const settings = pi.SettingsManager.inMemory({ compaction: { enabled: false } });
const factory = async options => {
  const services = await pi.createAgentSessionServices({ cwd, agentDir, settingsManager: settings, modelRuntime,
    resourceLoaderOptions: { additionalExtensionPaths: [resolve('src/index.ts')], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true } });
  const created = await pi.createAgentSessionFromServices({ services, sessionManager: options.sessionManager, model: faux.getModel(), noTools: 'builtin', sessionStartEvent: options.sessionStartEvent });
  assert.deepEqual(created.extensionsResult.errors, []);
  await created.session.bindExtensions({ mode: 'print', onError: error => { throw new Error(error.error); } });
  return { ...created, services, diagnostics: services.diagnostics };
};
const waitFor = async predicate => {
  const deadline = Date.now() + 10000;
  while (!predicate()) { assert.ok(Date.now() < deadline, `Pi scheduling did not settle: ${JSON.stringify({order,tasks:inspect(s=>s.listCleanTasks({limit:10}).items),triggers:inspect(s=>s.listDistillTriggers({limit:10}).items),usages:inspect(s=>s.listSessionUsages())})}`); await new Promise(resolve => setTimeout(resolve, 20)); }
};
let runtime;
try {
  runtime = await pi.createAgentSessionRuntime(factory, { cwd, agentDir, sessionManager: pi.SessionManager.create(cwd, sessionDir) });
  await waitFor(() => inspect(store => store.searchBatch({ terms: ['local synthetic'], match: 'any' }).map(x => x.memory).some(x => x.content === remembered)));
  assert.ok(inspect(store => store.readDistillTrigger(long.id).status !== 'complete'));
  assert.ok(existsSync(long.sessionFile), 'partial long work must retain its source');
  const tools = [];
  runtime.session.subscribe(event => { if (event.type === 'tool_execution_end') tools.push(event); });
  await runtime.session.prompt('What testing convention applies to Cedar?');
  assert.deepEqual(tools.map(x => x.toolName), ['memory_search', 'memory_read']);
  assert.equal(tools[1].result.details.content, remembered);
  await waitFor(() => !existsSync(short.sessionFile));
  assert.ok(existsSync(long.sessionFile));
  assert.ok(inspect(store => store.readDistillTrigger(long.id).status !== 'complete'));
  console.log(JSON.stringify({ passed: true, recalled: true, shortDeleted: true, longRetained: true }));
} finally {
  await runtime?.dispose();
}
