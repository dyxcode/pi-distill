import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, writeFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";

const [mode, temporaryRoot, agentDir, cwd, extensionPath] = process.argv.slice(2);
assert.ok(mode && temporaryRoot && agentDir && cwd && extensionPath);
process.env.PI_CODING_AGENT_DIR = agentDir;
process.env.PI_CODING_AGENT_SESSION_DIR = join(temporaryRoot, "sessions");
process.env.PI_OFFLINE = "1";
process.env.PI_SKIP_VERSION_CHECK = "1";
process.env.PI_TELEMETRY = "0";
mkdirSync(agentDir, { recursive: true });
mkdirSync(cwd, { recursive: true });

const [pi, piAi, { SQLiteMemoryStore }] = await Promise.all([
  import("@earendil-works/pi-coding-agent"),
  import("@earendil-works/pi-ai"),
  import("../../src/storage/store.ts"),
]);

const loader = new pi.DefaultResourceLoader({
  cwd,
  agentDir,
  additionalExtensionPaths: [extensionPath],
  noSkills: true,
  noPromptTemplates: true,
  noThemes: true,
  noContextFiles: true,
});
await loader.reload();
const extensionResult = loader.getExtensions();

let faux;
let session;
let modelRuntime;

function publicTools() {
  const definitions = ["memory_read", "memory_search"].map(name => session.getToolDefinition(name));
  return definitions.map((definition, index) => {
    assert.ok(definition, `Pi must register ${index === 0 ? "memory_read" : "memory_search"}`);
    return definition;
  });
}

async function startSession(scriptedResponses) {
  const options = {
    cwd,
    agentDir,
    resourceLoader: loader,
    sessionManager: pi.SessionManager.inMemory(cwd),
    tools: ["memory_search", "memory_read"],
  };
  if (scriptedResponses) {
    faux = piAi.fauxProvider();
    faux.setResponses(scriptedResponses);
    modelRuntime = await pi.ModelRuntime.create({
      authPath: join(agentDir, "auth.json"),
      modelsPath: null,
      refreshOnCreate: false,
    });
    modelRuntime.registerNativeProvider(faux.provider);
    Object.assign(options, { modelRuntime, model: faux.getModel() });
  }
  const result = await pi.createAgentSession(options);
  session = result.session;
  return result;
}

function record(content) {
  const createdAt = "2026-10-21T08:00:00.000Z";
  return { id: "synthetic-pref", content, createdAt };
}

function makeToolCall(name, arguments_) {
  return piAi.fauxAssistantMessage(piAi.fauxToolCall(name, arguments_), { stopReason: "toolUse" });
}

function captureEvents() {
  const calls = [];
  const results = [];
  session.subscribe(event => {
    if (event.type === "tool_execution_start") calls.push(event.toolName);
    if (event.type === "tool_execution_end") {
      results.push({ name: event.toolName, isError: event.isError, result: event.result });
    }
  });
  return { calls, results };
}

function detailStatus(entry) {
  const body = entry?.result?.details;
  if (body && typeof body === "object" && typeof body.status === "string") return body.status;
  const text = entry?.result?.content?.find(item => item.type === "text")?.text;
  if (typeof text !== "string") return undefined;
  try {
    const parsed = JSON.parse(text);
    return typeof parsed.status === "string" ? parsed.status : undefined;
  } catch {
    return undefined;
  }
}

if (extensionResult.errors.length > 0) {
  throw new Error(extensionResult.errors.map(error => error.message).join("\n"));
}

const storePath = join(agentDir, "distill", "memories.sqlite");

if (mode === "load-only") {
  await startSession();
  const definitions = publicTools();
  const names = session.getAllTools().map(tool => tool.name).filter(name => name.startsWith("memory_")).sort();
  const schemas = Object.fromEntries(definitions.map(definition => [
    definition.name,
    Object.keys(definition.parameters.properties ?? {}).sort(),
  ]));
  const searchDefinition = definitions.find(definition => definition.name === "memory_search");
  const prompt = session.systemPrompt;
  await session.reload();
  const storagePath = join(agentDir, "distill", "memories.sqlite");
  console.log(JSON.stringify({
    extensionErrors: extensionResult.errors,
    toolNames: names,
    schemas,
    searchRejectsUnknown: searchDefinition.parameters.additionalProperties === false,
    searchTermMaxLength: searchDefinition.parameters.properties.terms.items.maxLength,
    prompt,
    storageExists: existsSync(storagePath),
  }));
  session.dispose();
  process.exit(0);
}

if (mode === "strict-schema") {
  await startSession([
    makeToolCall("memory_search", { terms: ["synthetic"], unexpected: true }),
    piAi.fauxAssistantMessage("Malformed tool arguments were rejected."),
  ]);
  const { calls, results } = captureEvents();
  await session.prompt("Run a synthetic invalid-arguments dispatch.");
  const result = results[0];
  const report = {
    calls,
    toolResultIsError: result?.isError,
    toolResultStatus: detailStatus(result),
    storageExists: existsSync(storePath),
  };
  session.dispose();
  console.log(JSON.stringify(report));
  process.exit(0);
}

mkdirSync(join(agentDir, "distill"), { recursive: true });
if (mode === "dispatch") {
  const writer = new SQLiteMemoryStore(storePath);
  writer.save(record("For this synthetic demo, two short check-ins per week fit better than a daily standup."));
  writer.close();
  const maxCodepointTerm = "🧠".repeat(128);
  const responses = [
    makeToolCall("memory_search", { terms: [maxCodepointTerm], match: "any" }),
    makeToolCall("memory_search", { terms: ["sync cadence", "daily standup"], match: "any" }),
    makeToolCall("memory_read", { id: "synthetic-pref", expectedRevision: 1 }),
    piAi.fauxAssistantMessage("The synthetic preference supports two short check-ins per week."),
  ];
  await startSession(responses);
  const { calls, results } = captureEvents();
  await session.prompt("For the synthetic demo, what check-in rhythm was preferred?");
  const definitions = publicTools();
  await session.reload();
  const reloadedToolCount = session.getAllTools().filter(tool => tool.name.startsWith("memory_")).length;
  faux.setResponses([
    makeToolCall("memory_search", { terms: ["check-ins"], match: "any" }),
    piAi.fauxAssistantMessage("The preference is available after reload."),
  ]);
  await session.prompt("Check the synthetic check-in preference once more.");
  const afterReloadCallCount = calls.length;
  const readResult = results.find(entry => entry.name === "memory_read")?.result;
  const readText = readResult?.details?.content;
  const unicodeSearchStatus = results[0]?.name === "memory_search" ? detailStatus(results[0]) : undefined;
  const persisted = new SQLiteMemoryStore(storePath);
  persisted.close();
  const returnedIds = results.flatMap(entry => {
    const candidates = entry.result?.details?.candidates;
    return Array.isArray(candidates) ? candidates.map(candidate => candidate.id) : [];
  });
  const report = {
    toolNames: definitions.map(definition => definition.name).sort(),
    calls,
    searchStatuses: results.filter(entry => entry.name === "memory_search").map(detailStatus),
    unicodeSearchStatus,
    candidateIds: [...new Set(returnedIds)],
    readContent: readText,
    reloadedToolCount,
    storePathIsIsolated: storePath.startsWith(agentDir),
    providerCalls: faux.state.callCount,
    afterReloadCallCount,
  };
  session.dispose();
  console.log(JSON.stringify(report));
  process.exit(0);
}

if (mode === "budget") {
  new SQLiteMemoryStore(storePath).close();
  const firstRun = [];
  for (let index = 0; index < 13; index++) {
    firstRun.push(makeToolCall("memory_search", { terms: [`no-match-${index}`], match: "any" }));
  }
  firstRun.push(piAi.fauxAssistantMessage("The first run has reached its tool budget."));
  await startSession(firstRun);
  const { calls, results } = captureEvents();
  await session.prompt("Use synthetic retrieval for this run-budget test.");
  const firstRunRefusal = results.map(detailStatus).find(status => status === "budget_exhausted");
  faux.setResponses([
    makeToolCall("memory_search", { terms: ["fresh-run"], match: "any" }),
    piAi.fauxAssistantMessage("The fresh run can retrieve normally."),
  ]);
  await session.prompt("Start a fresh run and search again.");
  const secondRunStatus = results.slice(13).map(detailStatus).find(status => status !== undefined);
  const report = {
    toolCalls: calls.length,
    firstRunRefusal,
    secondRunStatus,
    providerCalls: faux.state.callCount,
  };
  session.dispose();
  console.log(JSON.stringify(report));
  process.exit(0);
}

if (mode === "diagnostics") {
  mkdirSync(join(agentDir,'distill'),{recursive:true});
  writeFileSync(storePath,'synthetic corrupt database');
  await startSession([
    makeToolCall('memory_search',{terms:['private synthetic words']}),
    piAi.fauxAssistantMessage('Continue normally.'),
    makeToolCall('memory_search',{terms:['private synthetic words']}),
    piAi.fauxAssistantMessage('Continue normally.'),
  ]);
  const {results}=captureEvents();
  await session.prompt('Synthetic failure test.');
  unlinkSync(storePath);
  await session.prompt('Synthetic recovery test.');
  const db=new DatabaseSync(join(agentDir,'distill','diagnostics.sqlite'),{readOnly:true});
  const records=db.prepare('SELECT payload FROM events ORDER BY sequence').all().map(row=>JSON.parse(row.payload));db.close();
  console.log(JSON.stringify({statuses:results.map(detailStatus),records}));
  session.dispose();
  process.exit(0);
}

throw new Error(`Unknown test mode: ${mode}`);
