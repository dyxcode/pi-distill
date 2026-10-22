import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join, sep } from 'node:path';
import { stripVTControlCharacters } from 'node:util';

const [root, agentDir, sessionDir, cwd, extensionPath] = process.argv.slice(2);
assert.ok(root && agentDir && sessionDir && cwd && extensionPath);

const pi = await import('@earendil-works/pi-coding-agent');
const ai = await import('@earendil-works/pi-ai');
const { SQLiteMemoryStore } = await import('../src/memory-store.ts');
const databasePath = join(agentDir, 'distill', 'memories.sqlite');
const originalSessionPhrase = 'For fictional Cedar, retain logs for 14 days by default and 30 days during incident investigations. Separately, my preferred afternoon drink is oolong tea.';
const preferenceContent = 'The user prefers oolong tea in the afternoon.';
const originalContent = 'For fictional Cedar, retain logs for 14 days by default and 30 days during incident investigations.';
const correctedContent = 'For fictional Cedar, retain logs for 7 days by default and 30 days during incident investigations.';
let memoryId;
let memoryRevision;
const faux = ai.fauxProvider({
  tokensPerSecond: 100_000,
  models: [{ id: 'synthetic', contextWindow: 32_768, maxTokens: 8_192 }],
});

function contentText(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.filter(part => part.type === 'text').map(part => part.text).join('\n');
}

function payloadOf(messages) {
  for (const message of messages) {
    if (message.role !== 'user') continue;
    try {
      const value = JSON.parse(contentText(message.content));
      if (Array.isArray(value.newEvidence)) return value;
    } catch { /* Ordinary Pi messages are not consolidation payloads. */ }
  }
  return undefined;
}

function latestUserIndex(messages) {
  for (let index = messages.length - 1; index >= 0; index--) {
    if (messages[index].role === 'user') return index;
  }
  return -1;
}

function scriptedResponse(context) {
  const messages = context.messages;
  const payload = payloadOf(messages);
  if (payload) {
    if (!memoryId) {
      const evidence = payload.newEvidence.find(part => part.role === 'user' && part.text.includes('14 days by default'));
      assert.ok(evidence, 'the original synthetic policy must reach consolidation');
      return ai.fauxAssistantMessage(JSON.stringify({ operations: [
        { action: 'create', content: originalContent, sourceKeys: [evidence.key] },
        { action: 'create', content: preferenceContent, sourceKeys: [evidence.key] },
      ] }));
    }

    const toolResults = messages.filter(message => message.role === 'toolResult');
    if (toolResults.length === 0) {
      return ai.fauxAssistantMessage(ai.fauxToolCall('memory_search', { terms: ['Cedar'] }), { stopReason: 'toolUse' });
    }
    if (toolResults.length === 1) {
      return ai.fauxAssistantMessage(ai.fauxToolCall('memory_read', {
        id: memoryId,
        expectedRevision: memoryRevision,
      }), { stopReason: 'toolUse' });
    }
    if (toolResults.length === 2) {
      const evidence = [...payload.newEvidence].reverse().find(part => part.role === 'user' && part.text.includes('Correction:'));
      assert.ok(evidence, 'the ordinary correction must reach consolidation');
      return ai.fauxAssistantMessage(JSON.stringify({ operations: [
        {
          action: 'update',
          id: memoryId,
          expectedRevision: memoryRevision,
          content: correctedContent,
          sourceKeys: [evidence.key],
        },
      ] }));
    }
    throw new Error('Unexpected extra consolidation tool result.');
  }

  const userIndex = latestUserIndex(messages);
  const userText = userIndex < 0 ? '' : contentText(messages[userIndex].content);
  const currentTurnResults = messages.slice(userIndex + 1).filter(message => message.role === 'toolResult');
  const lastResult = currentTurnResults.at(-1);
  if (lastResult?.toolName === 'memory_search') {
    return ai.fauxAssistantMessage(ai.fauxToolCall('memory_read', {
      id: memoryId,
      expectedRevision: memoryRevision,
    }), { stopReason: 'toolUse' });
  }
  if (lastResult?.toolName === 'memory_read') {
    return ai.fauxAssistantMessage(originalContent);
  }
  if (/retrieve Cedar's policy/iu.test(userText)) {
    return ai.fauxAssistantMessage(ai.fauxToolCall('memory_search', { terms: ['Cedar'] }), { stopReason: 'toolUse' });
  }
  if (/^Correction:/u.test(userText)) {
    return ai.fauxAssistantMessage('Understood: Cedar now keeps logs for 7 days by default, with the 30-day incident exception.');
  }
  return ai.fauxAssistantMessage('For fictional Cedar, the original default is 14 days, with 30 days during incident investigations.');
}

function setScript() {
  faux.setResponses(Array.from({ length: 40 }, () => scriptedResponse));
}

async function createRuntime(modelRuntime, settings) {
  const factory = async options => {
    const services = await pi.createAgentSessionServices({
      cwd: options.cwd,
      agentDir,
      settingsManager: settings,
      modelRuntime,
      resourceLoaderOptions: {
        additionalExtensionPaths: [extensionPath],
        noSkills: true,
        noPromptTemplates: true,
        noThemes: true,
        noContextFiles: true,
      },
    });
    const created = await pi.createAgentSessionFromServices({
      services,
      sessionManager: options.sessionManager,
      model: faux.getModel(),
      noTools: 'builtin',
      sessionStartEvent: options.sessionStartEvent,
    });
    assert.deepEqual(created.extensionsResult.errors, []);
    await created.session.bindExtensions({ mode: 'print', onError: error => { throw new Error(error.error); } });
    return { ...created, services, diagnostics: services.diagnostics };
  };
  return pi.createAgentSessionRuntime(factory, {
    cwd,
    agentDir,
    sessionManager: pi.SessionManager.create(cwd, sessionDir),
  });
}

function readState() {
  const store = new SQLiteMemoryStore(databasePath);
  try {
    return {
      memory: store.searchBatch({ terms: ['Cedar'], match: 'any' }).find(row => row.matchedTerms.length)?.memory,
      preference: store.searchBatch({ terms: ['oolong'], match: 'any' }).find(row => row.matchedTerms.length)?.memory,
      pendingJobs: store.listConsolidationJobs(),
      clean: store.listCleanTasks({limit:100}),
      usages: store.listSessionUsages(),
      pendingObservations: store.observedSessions(),
    };
  } finally {
    store.close();
  }
}

async function waitForSettledMemory(predicate) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const state = readState();
    if (state.memory && state.pendingJobs.length === 0 && state.pendingObservations.length === 0 && predicate(state.memory)) {
      return state;
    }
    await new Promise(resolvePromise => setTimeout(resolvePromise, 20));
  }
  return readState();
}

let runtime;
try {
  await Promise.all([mkdir(agentDir, { recursive: true }), mkdir(sessionDir, { recursive: true }), mkdir(cwd, { recursive: true })]);
  pi.initTheme('dark');
  const modelRuntime = await pi.ModelRuntime.create({
    authPath: join(agentDir, 'auth.json'),
    modelsPath: null,
    refreshOnCreate: false,
  });
  modelRuntime.registerNativeProvider(faux.provider);
  const settings = pi.SettingsManager.inMemory({ compaction: { enabled: false } });
  setScript();

  runtime = await createRuntime(modelRuntime, settings);
  await runtime.session.prompt(originalSessionPhrase);
  const originalSessionFile = runtime.session.sessionManager.getSessionFile();
  const originalSessionId = runtime.session.sessionManager.getSessionId();
  assert.ok(originalSessionFile);
  assert.ok(originalSessionFile.startsWith(`${root}${sep}`), 'the original source must be inside the disposable fixture');
  await runtime.newSession();

  const originalState = await waitForSettledMemory(memory => memory.content === originalContent);
  assert.equal(originalState.memory?.content, originalContent);
  assert.equal(originalState.memory?.revision, 1);
  assert.deepEqual(originalState.pendingJobs, [], 'initial consolidation must have no pending work');
  assert.deepEqual(originalState.pendingObservations, [], 'initial capture must have no pending observations');
  assert.equal(originalState.preference?.content, preferenceContent);
  memoryId = originalState.memory.id;
  memoryRevision = originalState.memory.revision;

  const cleanupDeadline = Date.now() + 5_000;
  while (existsSync(originalSessionFile) && Date.now() < cleanupDeadline) await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(existsSync(originalSessionFile), false, "extension cleanup must remove the processed inactive source: " + JSON.stringify(readState()));

  setScript();
  const freshSessionId = runtime.session.sessionManager.getSessionId();
  const retrievalEvents = [];
  runtime.session.subscribe(event => {
    if (event.type === 'tool_execution_end') retrievalEvents.push(event);
  });
  await runtime.session.prompt('Please retrieve Cedar\'s policy and explain the current log-retention periods.');
  assert.deepEqual(retrievalEvents.map(event => event.toolName), ['memory_search', 'memory_read']);
  assert.equal(retrievalEvents[0].result.details.candidates[0].id, memoryId);
  assert.equal(retrievalEvents[1].result.details.content, originalContent);

  const readDefinition = runtime.session.getToolDefinition('memory_read');
  assert.ok(readDefinition, 'the active Pi session must expose its registered memory_read definition');
  const renderedRead = new pi.ToolExecutionComponent(
    'memory_read',
    'synthetic-memory-read',
    { id: memoryId, expectedRevision: memoryRevision },
    {},
    readDefinition,
    { requestRender() {} },
    cwd,
  );
  renderedRead.updateResult(retrievalEvents[1].result);
  renderedRead.setExpanded(true);
  const renderedText = renderedRead.render(110).map(stripVTControlCharacters).join('\n');
  assert.ok(renderedText.includes(originalContent), 'expanded Pi output must show the memory content');
  assert.doesNotMatch(renderedText, /session\s*id|memory\s*id/iu);
  for (const privateId of [originalSessionId, freshSessionId, memoryId]) {
    assert.equal(renderedText.includes(privateId), false, 'expanded Pi output must not expose stored identifiers');
  }
  assert.equal(existsSync(originalSessionFile), false, 'fresh-session retrieval must not recreate its deleted source');

  await runtime.session.prompt('Correction: For fictional Cedar, retain logs for 7 days by default and 30 days during incident investigations.');
  await runtime.newSession();

  const correctedState = await waitForSettledMemory(memory => memory.content === correctedContent);
  assert.equal(correctedState.memory?.id, memoryId, 'the correction must update the same durable record');
  assert.equal(correctedState.memory?.content, correctedContent);
  assert.equal(correctedState.memory?.revision, 2);
  assert.deepEqual(correctedState.preference, originalState.preference, 'an unrelated preference from the same old chat must survive correction');
  assert.deepEqual(correctedState.pendingJobs, [], 'correction consolidation must have no pending work');
  assert.deepEqual(correctedState.pendingObservations, [], 'correction capture must have no pending observations');
  assert.equal(existsSync(originalSessionFile), false, 'the original source file must remain absent after correction');
  console.log(JSON.stringify({ passed: true, memoryId, revision: correctedState.memory.revision, providerCalls: faux.state.callCount }));
} finally {
  if (runtime) await runtime.dispose();
}
