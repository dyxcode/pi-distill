import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const [mode, root, sourceFile] = process.argv.slice(2);
assert.ok(['prepare', 'partial', 'resume-and-recall', 'retrieval-budget', 'retrieval-finish'].includes(mode));
assert.ok(root);

const agentDir = join(root, 'agent');
const sessionDir = join(root, 'sessions');
const cwd = join(root, 'project');
const databasePath = join(agentDir, 'distill', 'memories.sqlite');
const extensionPath = resolve('src/index.ts');
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_CODING_AGENT_SESSION_DIR = sessionDir;
process.env.PI_OFFLINE = '1';
process.env.PI_SKIP_VERSION_CHECK = '1';
process.env.PI_TELEMETRY = '0';

const pi = await import('@earendil-works/pi-coding-agent');
const ai = await import('@earendil-works/pi-ai');
const { SQLiteMemoryStore } = await import('../../src/storage/store.ts');
const { loadSnapshot } = await import('../../src/sources/snapshot.ts');
const { planSources } = await import('../../src/distill/preparation.ts');

const models = [
  { id: 'synthetic-large', contextWindow: 128_000, maxTokens: 4_096 },
  { id: 'synthetic-small', contextWindow: 10_000, maxTokens: 1_024 },
  { id: 'synthetic-small-wake', contextWindow: 10_000, maxTokens: 1_024 },
  { id: 'synthetic-retrieval', contextWindow: 12_000, maxTokens: 1_024 },
];
const faux = ai.fauxProvider({ tokensPerSecond: 100_000, models });
const largeModel = faux.getModel('synthetic-large');
const smallModel = faux.getModel('synthetic-small');
const wakeModel = faux.getModel('synthetic-small-wake');
assert.ok(largeModel && smallModel && wakeModel);

const providerRecords = [];
let resolveFirstExtraction;
const firstExtractionStarted = new Promise(resolvePromise => { resolveFirstExtraction = resolvePromise; });
let largeHeldSignal;
let primaryHeldSignal;
let contenderHeldSignal;
let contenderHeldRequest;
let primaryHoldClaimed = false;
let contenderHoldArmed = false;
let primaryLateReplyResolve;
let primaryLateProviderReply;
let primaryLateContent;
const prefixOrder = [];
const prefixPositions = new Map();
let firstPrefixRequest;
let secondPrefixRequest;
let rememberedMemory;
let runtime;
let contenderRuntime;
let activeModelRuntime;
let holdRetrieval = true;
let heldRetrievalSignal;
let retrievalHeld;
const retrievalReady = new Promise(resolve => { retrievalHeld = resolve; });
let retrievalPages = 0;
const retrievalContent = 'For fictional Cedar, fixture policy 000: retain 21 days; 30 during incidents.';

function delay(milliseconds) {
  return new Promise(resolvePromise => setTimeout(resolvePromise, milliseconds));
}

function textOf(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.filter(part => part?.type === 'text').map(part => part.text).join('\n');
}

function consolidationPayload(context) {
  for (const message of [...context.messages].reverse()) {
    if (message.role !== 'user') continue;
    try {
      const parsed = JSON.parse(textOf(message.content));
      if (Array.isArray(parsed?.newEvidence)) return parsed;
    } catch { /* Normal Pi conversation messages are not extraction payloads. */ }
  }
  return undefined;
}

function abortableHold(signal, onCaptured) {
  return new Promise((resolvePromise, rejectPromise) => {
    if (signal?.aborted) {
      rejectPromise(signal.reason ?? new Error('Synthetic provider request aborted.'));
      return;
    }
    if (!signal) {
      rejectPromise(new Error('Pi did not supply a cancellation signal to the synthetic provider.'));
      return;
    }
    onCaptured?.(signal);
    signal.addEventListener('abort', () => {
      rejectPromise(signal.reason ?? new Error('Synthetic provider request aborted.'));
    }, { once: true });
  });
}

function holdPrimaryReplyPastAbort(signal, payload) {
  assert.ok(signal, 'the primary held provider request must have a cancellation signal');
  primaryHeldSignal = signal;
  const firstPolicy = payload.newEvidence.find(part => part.role === 'user' && sourceNumber(part) !== undefined);
  assert.ok(firstPolicy, 'the held prefix must contain an original synthetic user fixture');
  const policy = sourceNumber(firstPolicy);
  primaryLateContent = `For fictional Cedar, fixture policy ${policy}: keep local sample ${policy} for five days.`;
  primaryLateProviderReply = new Promise(resolvePromise => {
    primaryLateReplyResolve = () => {
      resolvePromise(extractionReply(payload));
    };
  });
  return primaryLateProviderReply;
}

function sourceNumber(part) {
  return textOf(part?.text).match(/fixture policy (\d{3})/u)?.[1];
}

function partSignature(part) {
  return JSON.stringify([part.entryId, part.role, part.text]);
}

function extractionReply(payload) {
  const firstPolicy = payload.newEvidence.find(part => part.role === 'user' && sourceNumber(part) !== undefined);
  assert.ok(firstPolicy, 'the fitting request should contain an original synthetic user fixture');
  const policy = sourceNumber(firstPolicy);
  return ai.fauxAssistantMessage(JSON.stringify({ operations: [{
    action: 'create',
    content: `For fictional Cedar, fixture policy ${policy}: keep local sample ${policy} for five days.`,
    sourceKeys: [firstPolicy.key],
  }] }));
}

function latestUserText(context) {
  for (const message of [...context.messages].reverse()) {
    if (message.role === 'user') return textOf(message.content);
  }
  return '';
}

function providerResponse(context, options, _state, model) {
  const payload = consolidationPayload(context);
  if (payload && (mode === 'retrieval-budget' || mode === 'retrieval-finish')) {
    const results = context.messages.filter(message => message.role === 'toolResult');
    const last = results.at(-1);
    const page = last ? JSON.parse(textOf(last.content)) : undefined;
    if (!last) return ai.fauxAssistantMessage(ai.fauxToolCall('memory_search', { terms: ['fixture policy 000'] }), { stopReason: 'toolUse' });
    if (last.toolName === 'memory_search') {
      assert.equal(page.status, 'ok');
      assert.equal(page.hasMore, true);
      assert.ok(page.nextCursor);
      assert.equal(page.candidates[0].id, '00-target');
      retrievalPages++;
      return ai.fauxAssistantMessage(ai.fauxToolCall('memory_read', { id: '00-target', expectedRevision: 1 }), { stopReason: 'toolUse' });
    }
    assert.equal(page.status, 'ok');
    assert.equal(page.hasMore, false);
    assert.ok(page.content.includes('30 during incidents'));
    if (holdRetrieval) return abortableHold(options?.signal, signal => {
      heldRetrievalSignal = signal;
      retrievalHeld();
    });
    return ai.fauxAssistantMessage(JSON.stringify({ operations: [{ action: 'update', id: '00-target',
      expectedRevision: 1, content: retrievalContent, sourceKeys: [payload.newEvidence[0].key] }] }));
  }
  if (payload) {
    const keys = payload.newEvidence.map(part => part.key);
    const identity = payload.newEvidence.map(partSignature).join('|');
    const toolResultCount = context.messages.filter(message => message.role === 'toolResult').length;
    const systemMessages = context.messages.filter(message => message.role === 'system');
    assert.ok(systemMessages.length > 0, 'the provider request must contain Pi’s normalized system prompt and tool declarations');
    const toolNames = [...new Set(systemMessages.flatMap(message => message.toolsAdded ?? []).map(tool => tool.name))];
    assert.ok(toolNames.includes('memory_search') && toolNames.includes('memory_read'),
      'the serialized provider context must include Pi’s registered memory tool declarations');
    const request = {
      modelId: model.id,
      contextWindow: model.contextWindow,
      serializedBytes: Buffer.byteLength(JSON.stringify({
        model: { id: model.id, contextWindow: model.contextWindow, maxTokens: model.maxTokens },
        messages: context.messages,
      }), 'utf8'),
      inputBudget: model.contextWindow - 4_096,
      toolNames,
      keys,
      parts: payload.newEvidence.map(part => ({ entryId: part.entryId, role: part.role, text: part.text })),
      toolResultCount,
    };
    providerRecords.push(request);

    if (toolResultCount === 0 && !prefixPositions.has(identity)) {
      const position = prefixOrder.length;
      prefixPositions.set(identity, position);
      prefixOrder.push(keys);
      if (mode === 'partial' || mode === 'resume-and-recall') {
        assert.ok(request.serializedBytes <= request.inputBudget,
          `initial synthetic model request exceeded its ${request.inputBudget}-byte allowance with ${request.serializedBytes} bytes`);
      }
      if (mode === 'partial' && position === 0) firstPrefixRequest = request;
      if (mode === 'prepare') resolveFirstExtraction?.(request);
      if (mode === 'partial' && position === 1) secondPrefixRequest = request;
    }

    const prefixPosition = prefixPositions.get(identity);
    if (mode === 'prepare') return abortableHold(options?.signal, signal => { largeHeldSignal = signal; });
    if (mode === 'partial') {
      if (prefixPosition === 1 && !primaryHoldClaimed) {
        primaryHoldClaimed = true;
        return holdPrimaryReplyPastAbort(options?.signal, payload);
      }
      if (prefixPosition === 2 && contenderHoldArmed) {
        contenderHoldArmed = false;
        contenderHeldRequest = request;
        return abortableHold(options?.signal, signal => { contenderHeldSignal = signal; });
      }
      if (prefixPosition === 0) return extractionReply(payload);
      return ai.fauxAssistantMessage('{"operations":[]}');
    }
    return ai.fauxAssistantMessage('{"operations":[]}');
  }

  const userText = latestUserText(context);
  const toolResults = context.messages.filter(message => message.role === 'toolResult');
  const lastTool = toolResults.at(-1);
  if (/retrieve the first Cedar fixture policy/iu.test(userText)) {
    if (lastTool?.toolName === 'memory_search') {
      const candidates = lastTool.content.map(part => part.text).join('\n');
      assert.ok(candidates.includes(rememberedMemory.id), 'the deleted-source recall should find the committed memory');
      return ai.fauxAssistantMessage(ai.fauxToolCall('memory_read', {
        id: rememberedMemory.id,
        expectedRevision: rememberedMemory.revision,
      }), { stopReason: 'toolUse' });
    }
    if (lastTool?.toolName === 'memory_read') {
      const details = lastTool.content.map(part => part.text).join('\n');
      assert.ok(details.includes(rememberedMemory.content));
      return ai.fauxAssistantMessage(rememberedMemory.content);
    }
    return ai.fauxAssistantMessage(ai.fauxToolCall('memory_search', { terms: ['fixture policy 000'] }), { stopReason: 'toolUse' });
  }
  return ai.fauxAssistantMessage('Synthetic Pi fixture acknowledgement.');
}

async function createRuntime(sessionManager, model) {
  const settings = pi.SettingsManager.inMemory({ compaction: { enabled: false } });
  const factory = async options => {
    const services = await pi.createAgentSessionServices({
      cwd: options.cwd,
      agentDir,
      settingsManager: settings,
      modelRuntime: activeModelRuntime,
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
      model,
      noTools: 'builtin',
      sessionStartEvent: options.sessionStartEvent,
    });
    assert.deepEqual(created.extensionsResult.errors, []);
    await created.session.bindExtensions({ mode: 'print', onError: error => { throw new Error(error.error); } });
    return { ...created, services, diagnostics: services.diagnostics };
  };
  return pi.createAgentSessionRuntime(factory, { cwd, agentDir, sessionManager });
}

function withStore(action) {
  const store = new SQLiteMemoryStore(databasePath);
  try { return action(store); }
  finally { store.close(); }
}

function findJobByFile(store, path) {
  return store.listConsolidationJobs().find(job => job.sessionFile === path);
}

function memoryForFixture(store) {
  return store.searchBatch({ terms: ['fixture policy 000'], match: 'any' })
    .find(row => row.matchedTerms.length > 0)?.memory;
}

function requestSummaries(requests) {
  return requests.map(request => ({
    modelId: request.modelId,
    contextWindow: request.contextWindow,
    serializedBytes: request.serializedBytes,
    inputBudget: request.inputBudget,
    toolNames: request.toolNames,
    keyCount: request.keys.length,
  }));
}

async function waitUntil(read, label, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value) return value;
    await delay(20);
  }
  throw new Error(`Timed out waiting for ${label}.`);
}

async function awaitStarted(promise, label, timeoutMs = 15_000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}.`)), timeoutMs); }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function setNextSmallModel(session, current) {
  const next = current === 'synthetic-small' ? wakeModel : smallModel;
  await session.setModel(next, { persist: false });
  return next.id;
}

async function prepareLargeRange() {
  let firstRuntime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), largeModel);
  const session = firstRuntime.session;
  for (let index = 0; index < 28; index += 1) {
    const policy = String(index).padStart(3, '0');
    const leading = `For fictional Cedar, fixture policy ${policy}: keep local sample ${policy} for five days. `;
    const padding = 'Additional synthetic fixture detail. '.repeat(10);
    const content = (leading + padding).slice(0, 400).padEnd(400, ' ');
    await session.prompt(content);
  }
  const sourceFilePath = session.sessionManager.getSessionFile();
  const sessionId = session.sessionManager.getSessionId();
  assert.ok(sourceFilePath && existsSync(sourceFilePath));
  assert.ok(sourceFilePath.startsWith(`${resolve(root)}/`));

  await firstRuntime.dispose();
  firstRuntime = undefined;

  runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), largeModel);
  const observedRequest = await awaitStarted(firstExtractionStarted, 'the large-model extraction request');
  const evidenceJobs = withStore(store => {
    const jobs = store.listConsolidationJobs().filter(job => job.sessionFile === sourceFilePath);
    return { jobs, leafId: jobs[0]?.leafId };
  });
  assert.ok(evidenceJobs.jobs.length > 0, 'the large runtime must durably prepare source work before the provider request');
  assert.equal(evidenceJobs.jobs.length, 1, 'the synthetic source should fit one durable range at 128k');
  const snapshot = await loadSnapshot(sourceFilePath, evidenceJobs.leafId);
  const partsByKey = new Map(planSources(snapshot, new Set()).flatMap(batch => batch.evidence).map(part => [part.key, part]));
  const preparedJob = evidenceJobs.jobs[0];
  const durableEvidence = preparedJob.sourceKeys.map(key => partsByKey.get(key));
  assert.ok(durableEvidence.every(Boolean));
  const evidenceBytes = Buffer.byteLength(JSON.stringify(durableEvidence), 'utf8');
  assert.ok(evidenceBytes >= 18 * 1024 && evidenceBytes <= 23 * 1024,
    `expected about 20 KiB of durable evidence, got ${evidenceBytes} bytes`);
  assert.equal(observedRequest.contextWindow, 128_000);

  await runtime.dispose();
  runtime = undefined;
  const persisted = withStore(store => {
    const job = store.listConsolidationJobs().find(item => item.id === preparedJob.id);
    assert.ok(job, 'the interrupted job must remain durable');
    assert.deepEqual(store.processedSources(job.sourceKeys), [], 'no evidence may be committed by the interrupted large request');
    const trigger = store.listDistillTriggers({ limit: 100 }).items.find(item => item.sessionFile === sourceFilePath);
    return { job, trigger, processedKeys: store.processedSources(job.sourceKeys) };
  });
  assert.ok(largeHeldSignal?.aborted, 'disposing the large Pi runtime must abort its provider request');
  assert.equal(persisted.job.sourceKeys.length, preparedJob.sourceKeys.length);
  return {
    sourceFile: sourceFilePath,
    sessionId,
    jobs: [{ id: persisted.job.id }],
    sourceKeys: [...persisted.job.sourceKeys],
    evidenceBytes,
    processedKeys: persisted.processedKeys,
    modelContextWindow: largeModel.contextWindow,
    providerSignalAborted: largeHeldSignal.aborted,
  };
}

async function interruptAfterFirstSmallPrefix(path) {
  assert.ok(path && existsSync(path));
  runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), smallModel);
  let smallFailure;
  let currentWakeModel = 'synthetic-small';
  let nextWakeAt = Date.now() + 500;
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline && !secondPrefixRequest && !smallFailure) {
    smallFailure = withStore(store => {
      const job = findJobByFile(store, path);
      return job ? store.readDistillFailure('job', job.id) : undefined;
    });
    if (secondPrefixRequest || smallFailure) break;
    if (Date.now() >= nextWakeAt) {
      currentWakeModel = await setNextSmallModel(runtime.session, currentWakeModel);
      nextWakeAt = Date.now() + 500;
    }
    await delay(15);
  }
  if (smallFailure) assert.fail(`the 10k claiming model could not begin a fitting prefix: ${smallFailure.reason}`);
  assert.ok(secondPrefixRequest, 'the Pi runtime should advance to a second bounded prefix without a long retry timer');

  const job = withStore(store => findJobByFile(store, path));
  assert.ok(job);
  const firstProgress = await waitUntil(() => withStore(store => {
    const processedKeys = store.processedSources(job.sourceKeys);
    const memory = memoryForFixture(store);
    return processedKeys.length > 0 && memory ? { processedKeys, memory, origin: store.memoryOrigin(memory.id) } : undefined;
  }), 'the first small-model prefix commit');
  assert.ok(firstProgress.processedKeys.length < job.sourceKeys.length,
    `the first request processed ${firstProgress.processedKeys.length} of ${job.sourceKeys.length} durable keys`);
  assert.equal(firstProgress.memory.content, 'For fictional Cedar, fixture policy 000: keep local sample 000 for five days.');
  assert.ok(firstPrefixRequest && secondPrefixRequest);

  const snapshot = await loadSnapshot(path, job.leafId);
  const durableBySignature = new Map(planSources(snapshot, new Set())
    .flatMap(batch => batch.evidence)
    .map(part => [partSignature(part), part.key]));
  const durableKeysForRequest = request => {
    const unresolved = request.parts.filter(part => !durableBySignature.has(partSignature(part)));
    assert.deepEqual(unresolved, [], 'each small-model prefix must reference durable source evidence');
    return request.parts.map(part => durableBySignature.get(partSignature(part)));
  };
  const firstPrefixKeys = durableKeysForRequest(firstPrefixRequest);
  const secondPrefixKeys = durableKeysForRequest(secondPrefixRequest);
  assert.deepEqual(firstProgress.processedKeys, firstPrefixKeys,
    'the first small-model response must commit exactly its selected prefix');
  assert.equal(secondPrefixKeys.some(key => firstProgress.processedKeys.includes(key)), false);

  const requestCountBeforeContender = providerRecords.length;
  contenderRuntime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), wakeModel);
  const contenderJob = withStore(store => findJobByFile(store, path));
  assert.equal(contenderJob?.id, job.id, 'the second Pi runtime must contend for the same durable job');
  await contenderRuntime.session.setModel(smallModel, { persist: false });
  await contenderRuntime.session.setModel(wakeModel, { persist: false });
  await delay(100);
  const requestsWhilePrimaryHeld = providerRecords.slice(requestCountBeforeContender)
    .filter(request => request.toolResultCount === 0);
  assert.deepEqual(requestsWhilePrimaryHeld, [],
    'a second actual Pi runtime must not issue a duplicate extraction while the first runtime holds the SQLite claim');
  assert.deepEqual(withStore(store => store.processedSources(job.sourceKeys)), firstProgress.processedKeys,
    'the competing runtime must leave durable progress unchanged while the primary claim is live');

  const primaryShutdownStarted = Date.now();
  await runtime.dispose();
  runtime = undefined;
  assert.ok(Date.now() - primaryShutdownStarted < 1_000,
    'Pi shutdown must not wait for a provider that ignores cancellation');
  assert.ok(primaryHeldSignal?.aborted, 'disposing the primary Pi runtime must abort its held provider request');
  contenderHoldArmed = true;

  let contenderModelId = wakeModel.id;
  nextWakeAt = Date.now() + 100;
  const contenderDeadline = Date.now() + 15_000;
  let contenderState;
  while (Date.now() < contenderDeadline) {
    contenderState = withStore(store => ({
      processedKeys: store.processedSources(job.sourceKeys),
      memory: store.get(firstProgress.memory.id),
      pending: store.listConsolidationJobs().find(item => item.id === job.id),
    }));
    const secondPrefixCommitted = secondPrefixKeys.every(key => contenderState.processedKeys.includes(key));
    if (secondPrefixCommitted && contenderHeldSignal) break;
    if (Date.now() >= nextWakeAt) {
      contenderModelId = await setNextSmallModel(contenderRuntime.session, contenderModelId);
      nextWakeAt = Date.now() + 250;
    }
    await delay(15);
  }
  assert.ok(contenderHeldSignal,
    'the second Pi runtime must claim the released job, commit the held prefix, and advance to the next bounded prefix');
  assert.ok(secondPrefixKeys.every(key => contenderState?.processedKeys.includes(key)),
    'the competing runtime must commit the prefix left uncommitted by the first runtime');
  assert.ok(contenderState?.processedKeys.length < job.sourceKeys.length,
    'the competing runtime must stop with the remaining job still pending');
  assert.equal(contenderState?.memory?.content, firstProgress.memory.content);

  await contenderRuntime.dispose();
  contenderRuntime = undefined;
  assert.ok(contenderHeldSignal.aborted, 'disposing the competing Pi runtime must interrupt its next request');
  assert.ok(primaryLateReplyResolve && primaryLateProviderReply,
    'the primary provider must still be holding its stale response after shutdown');
  primaryLateReplyResolve();
  const lateReply = await awaitStarted(primaryLateProviderReply, 'the intentionally late provider callback', 3_000);
  assert.ok(textOf(lateReply.content).includes(primaryLateContent),
    'the deliberately late provider response must contain its stale create operation');
  await delay(50);

  const state = withStore(store => {
    const pending = store.listConsolidationJobs().find(item => item.id === job.id);
    const memory = store.get(firstProgress.memory.id);
    const staleMemory = store.searchBatch({ terms: [`fixture policy ${sourceNumber(secondPrefixRequest.parts.find(part => part.role === 'user'))}`], match: 'any' })
      .find(row => row.memory.content === primaryLateContent)?.memory;
    return { pending, memory, staleMemory, processedKeys: store.processedSources(job.sourceKeys) };
  });
  assert.ok(state.pending, 'the partially completed job must remain pending after both runtimes stop');
  assert.ok(existsSync(path), 'the source must be retained while the range is incomplete');
  assert.deepEqual(state.processedKeys, contenderState?.processedKeys);
  assert.deepEqual(state.memory, firstProgress.memory);
  assert.equal(state.staleMemory, undefined, 'the primary runtime’s late result must not commit after losing its claim');

  return {
    jobId: job.id,
    initialRequests: requestSummaries(providerRecords.filter(request => request.contextWindow === 10_000 && request.toolResultCount === 0)),
    firstCommittedKeys: state.processedKeys,
    firstPrefixKeys,
    contenderCommittedKeys: secondPrefixKeys,
    contenderJobId: contenderJob.id,
    duplicateRequestsWhilePrimaryHeld: requestsWhilePrimaryHeld.length,
    staleReplyDelivered: true,
    staleMemoryAbsent: state.staleMemory === undefined,
    jobStatus: 'pending',
    sourceRetained: existsSync(path),
    memory: { id: state.memory.id, content: state.memory.content, revision: state.memory.revision },
    memoryOrigin: firstProgress.origin,
  };
}

async function resumeAndRecall(path) {
  assert.ok(path && existsSync(path));
  runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), smallModel);
  const job = withStore(store => store.listConsolidationJobs().find(item => item.sessionFile === path));
  assert.ok(job, 'the same interrupted durable job must be reclaimable after restart');
  const firstCommitted = withStore(store => store.processedSources(job.sourceKeys));
  assert.ok(firstCommitted.length > 0 && firstCommitted.length < job.sourceKeys.length);
  const snapshot = await loadSnapshot(path, job.leafId);
  const durableBySignature = new Map(planSources(snapshot, new Set())
    .flatMap(batch => batch.evidence)
    .map(part => [partSignature(part), part.key]));
  let lastProcessedCount = firstCommitted.length;
  let nextWakeAt = Date.now() + 150;
  let wakeModelId = 'synthetic-small';
  const deadline = Date.now() + 25_000;
  let finalState;
  while (Date.now() < deadline) {
    finalState = withStore(store => {
      const processed = store.processedSources(job.sourceKeys);
      const pendingJob = store.listConsolidationJobs().find(item => item.sourceKeys.some(key => job.sourceKeys.includes(key)));
      const trigger = store.listDistillTriggers({ limit: 100 }).items.find(item => item.sessionFile === path);
      const found = rememberedMemory ?? memoryForFixture(store);
      const memory = found ? store.get(found.id) : undefined;
      return { processed, pendingJob, trigger, memory };
    });
    if (finalState.processed.length === job.sourceKeys.length && finalState.pendingJob === undefined) break;
    if (finalState.processed.length !== lastProcessedCount) {
      lastProcessedCount = finalState.processed.length;
      nextWakeAt = Date.now() + 150;
    } else if (Date.now() >= nextWakeAt) {
      wakeModelId = await setNextSmallModel(runtime.session, wakeModelId);
      nextWakeAt = Date.now() + 250;
    }
    await delay(15);
  }
  assert.ok(finalState);
  assert.deepEqual(finalState.processed, job.sourceKeys,
    `the restarted 10k runtime must finish the remaining range; processed ${finalState.processed.length}/${job.sourceKeys.length}`);
  assert.equal(finalState.pendingJob, undefined, 'all source jobs must leave the pending queue after processing');
  if (finalState.trigger !== undefined) {
    assert.equal(finalState.trigger.status, 'complete', 'a retained historical trigger must be complete');
  }
  const memory = withStore(store => memoryForFixture(store));
  assert.ok(memory, 'the first prefix memory must survive all remaining commits');
  assert.deepEqual(memory, withStore(store => store.get(memory.id)));

  const initialRequests = providerRecords.filter(request => request.toolResultCount === 0);
  const selectedParts = initialRequests.flatMap(request => request.parts);
  const unresolvedParts = selectedParts.filter(part => !durableBySignature.has(partSignature(part)));
  assert.deepEqual(unresolvedParts, [], 'every requested evidence part must map to a durable source key');
  const selectedKeys = selectedParts.map(part => durableBySignature.get(partSignature(part)));
  const resumedKeys = selectedKeys.filter(key => !firstCommitted.includes(key));
  assert.deepEqual(new Set(resumedKeys), new Set(job.sourceKeys.filter(key => !firstCommitted.includes(key))));
  assert.equal(resumedKeys.length, new Set(resumedKeys).size,
    'the restarted runtime must not request any remaining evidence part more than once');
  assert.deepEqual(selectedKeys.filter(key => firstCommitted.includes(key)), [],
    'restart requests must exclude the already committed prefix');

  const recallTools = [];
  let recalledContent;
  runtime.session.subscribe(event => {
    if (event.type === 'tool_execution_end') recallTools.push(event.toolName);
    if (event.type === 'message_end' && event.message?.role === 'assistant') {
      const messageText = textOf(event.message.content);
      if (messageText.includes(memory.content)) recalledContent = messageText;
    }
  });
  await runtime.session.prompt('Synthetic fixture wake: please reply with a brief acknowledgement.');
  let deleted;
  try {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && existsSync(path)) {
      await delay(20);
    }
    deleted = !existsSync(path);
    assert.equal(deleted, true, 'eligible deletion after the source session ends');
  } catch (error) {
    const store = new SQLiteMemoryStore(databasePath);
    let diagnostics;
    try {
      const allEvidence = planSources(await loadSnapshot(path), new Set()).flatMap(batch => batch.evidence);
      const status = store.readConsolidationStatus(allEvidence.map(part => part.key), [path]);
      diagnostics = {
        cleanTasks: store.listCleanTasks({ limit: 20 }).items,
        pendingTriggers: status.pendingTriggers,
        pendingJobs: status.pendingJobs,
        unprocessedKeys: allEvidence.filter(part => !status.processedSources.includes(part.key)).map(part => part.key),
        observations: status.observations,
        usages: status.usages,
        coverage: store.sourceCoverage.current(path),
      };
    } finally { store.close(); }
    throw new Error(`${error.message} Cleanup state: ${JSON.stringify(diagnostics)}`);
  }
  assert.equal(deleted, true);
  rememberedMemory = memory;
  await runtime.session.prompt('Please retrieve the first Cedar fixture policy.');
  assert.deepEqual(recallTools, ['memory_search', 'memory_read']);
  assert.ok(recalledContent?.includes(memory.content), 'Pi should retrieve the committed memory after deleting the source');
  const postCleanup = withStore(store => ({
    memory: store.get(memory.id),
    origin: store.memoryOrigin(memory.id),
    processed: store.processedSources(job.sourceKeys),
  }));
  assert.deepEqual(postCleanup.memory, memory);
  assert.ok(postCleanup.origin);
  assert.deepEqual(postCleanup.processed, job.sourceKeys);
  return {
    jobId: job.id,
    initialRequests: requestSummaries(providerRecords.filter(request => request.contextWindow === 10_000 && request.toolResultCount === 0)),
    finalProcessedKeys: postCleanup.processed,
    reprocessedKeys: selectedKeys.filter(key => firstCommitted.includes(key)),
    memory: { id: postCleanup.memory.id, content: postCleanup.memory.content, revision: postCleanup.memory.revision },
    memoryOrigin: postCleanup.origin,
    sourceDeletedAfterEligibleCompletion: !existsSync(path),
    recalledContent,
    recallTools,
  };
}

async function main() {
  await Promise.all([
    mkdir(agentDir, { recursive: true }),
    mkdir(sessionDir, { recursive: true }),
    mkdir(cwd, { recursive: true }),
  ]);
  faux.setResponses(Array.from({ length: 500 }, () => providerResponse));
  activeModelRuntime = await pi.ModelRuntime.create({
    authPath: join(agentDir, 'auth.json'),
    modelsPath: null,
    refreshOnCreate: false,
  });
  activeModelRuntime.registerNativeProvider(faux.provider);

  if (mode === 'retrieval-budget') {
    await mkdir(join(agentDir, 'distill'), { recursive: true });
    withStore(store => {
      store.save({ id: '00-target', content: 'For fictional Cedar, fixture policy 000: retain 14 days; 30 during incidents.', createdAt: '2026-10-23T00:00:00.000Z' });
      for (let index = 0; index < 30; index++) store.save({ id: `z${index}`, content: `fixture policy 000: unrelated ${'background '.repeat(80)}`, createdAt: '2026-10-23T00:00:00.000Z' });
    });
    runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), largeModel);
    await runtime.session.prompt('For fictional Cedar, change fixture policy 000 to retain 21 days. Keep the 30-day incident exception.');
    const path = runtime.session.sessionManager.getSessionFile();
    assert.ok(path && existsSync(path));
    await runtime.dispose();
    const retrievalModel = faux.getModel('synthetic-retrieval');
    assert.ok(retrievalModel);
    runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), retrievalModel);
    await awaitStarted(retrievalReady, 'the bounded retrieval update');
    const job = withStore(store => findJobByFile(store, path));
    assert.ok(job);
    assert.deepEqual(withStore(store => store.processedSources(job.sourceKeys)), []);
    assert.equal(withStore(store => store.get('00-target').revision), 1);
    assert.ok(existsSync(path), 'an uncommitted update must retain its source');
    await runtime.dispose();
    assert.ok(heldRetrievalSignal.aborted);
    console.log(JSON.stringify({ passed: true, retrievalPages, sourceFile: path, sourceKeys: job.sourceKeys }));
    runtime = undefined;
    return;
  }
  if (mode === 'retrieval-finish') {
    const path = sourceFile;
    assert.ok(path && existsSync(path));
    const job = withStore(store => findJobByFile(store, path));
    assert.ok(job);
    assert.deepEqual(withStore(store => store.processedSources(job.sourceKeys)), []);
    holdRetrieval = false;
    const retrievalModel = faux.getModel('synthetic-retrieval');
    assert.ok(retrievalModel);
    runtime = await createRuntime(pi.SessionManager.create(cwd, sessionDir), retrievalModel);
    const memory = await waitUntil(() => withStore(store => {
      const current = store.get('00-target');
      return current.revision === 2 ? current : undefined;
    }), 'the corrected memory after restart');
    assert.equal(memory.content, retrievalContent);
    assert.deepEqual(withStore(store => store.processedSources(job.sourceKeys)), job.sourceKeys);
    await runtime.session.prompt('Synthetic fixture wake: please reply with a brief acknowledgement.');
    try {
      await waitUntil(() => !existsSync(path), 'cleanup after the committed correction');
    } catch (error) {
      const diagnostics = withStore(store => ({ tasks: store.listCleanTasks({ limit: 20 }).items,
        status: store.readConsolidationStatus(job.sourceKeys, [path]) }));
      throw new Error(`${error.message} ${JSON.stringify(diagnostics)}`);
    }
    rememberedMemory = memory;
    const recallTools = [];
    let recalled = false;
    runtime.session.subscribe(event => {
      if (event.type === 'tool_execution_end') recallTools.push(event.toolName);
      if (event.type === 'message_end' && event.message.role === 'assistant' && textOf(event.message.content) === retrievalContent) recalled = true;
    });
    await runtime.session.prompt('Please retrieve the first Cedar fixture policy.');
    assert.deepEqual(recallTools, ['memory_search', 'memory_read']);
    assert.ok(recalled);
    assert.ok(withStore(store => store.memoryOrigin(memory.id)));
    assert.equal(retrievalPages, 1, 'restart must replay the uncommitted retrieval exactly once');
    console.log(JSON.stringify({ passed: true, retrievalPages, recalled, sourceDeleted: !existsSync(path) }));
    return;
  }

  if (mode === 'prepare') {
    const result = await prepareLargeRange();
    console.log(JSON.stringify({ ...result, passed: true }));
    return;
  }
  if (mode === 'partial') {
    const result = await interruptAfterFirstSmallPrefix(sourceFile);
    console.log(JSON.stringify({ ...result, passed: true }));
    return;
  }
  const result = await resumeAndRecall(sourceFile);
  console.log(JSON.stringify({ ...result, passed: true }));
}

try {
  await main();
} finally {
  try {
    if (runtime) await runtime.dispose();
  } finally {
    if (contenderRuntime) await contenderRuntime.dispose();
  }
}
