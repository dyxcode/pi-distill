import { selectEvaluationModel, evaluationModelId } from "./model-eval-preflight.mjs";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const extensionPath = join(repoRoot, "src", "index.ts");
const createdAt = "2026-10-21T00:00:00.000Z";
const maxToolCalls = 12;
let providerRequests = 0;
const requestLimit = 20;
const caseTimeoutMs = 75_000;

const longProjectContext = "The project index tracks build templates, package names, notebook metadata, deployment profiles, test runners, lockfile ownership, and local commands. These notes explain how the two fictional labs organize their separate runtime configuration and review their setup before a release. ".repeat(8);

const cases = [
  {
    id: "fictional-demo-format",
    prompt: "For the fictional Blue Lantern demo, what speaking order did I choose?",
    memories: [{
      id: "synthetic-blue-lantern-demo",
      content: "For the fictional Blue Lantern demo, begin with one concise Mandarin overview, then explain implementation details in English. Do not translate each sentence side by side.",
    }],
    expectedIds: ["synthetic-blue-lantern-demo"],
    evidenceIds: ["synthetic-blue-lantern-demo"],
    rubric: "For Blue Lantern only, put a concise Mandarin overview before the English implementation explanation.",
  },
  {
    id: "chinese-to-english-reformulation",
    prompt: "番茄地的土壤要达到什么状态才可以灌溉？收成当天清晨有什么例外？",
    memories: [{
      id: "synthetic-greenhouse-g7-watering",
      content: "Fictional Greenhouse G-7 reminder: irrigate the tomato beds (西红柿苗床) only after the upper 3 cm of soil has dried (土壤上层干燥). On harvest mornings (采摘日清晨), skip irrigation (暂停补水), even if the soil is dry.",
    }],
    expectedIds: ["synthetic-greenhouse-g7-watering"],
    evidenceIds: ["synthetic-greenhouse-g7-watering"],
    rubric: "For fictional G-7, irrigate only after the upper 3 cm dries, except skip harvest mornings even if dry.",
  },
  {
    id: "conditional-project-distractor",
    prompt: "Which Python version belongs to the fictional Cobalt Finch project? Amber Kite has a separate setup; do not transfer its setting.",
    memories: [
      {
        id: "synthetic-amber-kite-runtime",
        content: "The fictional Amber Kite project uses Python 3.12. This setting belongs to Amber Kite only.",
      },
      {
        id: "synthetic-cobalt-finch-runtime",
        content: `Fictional project index: Cobalt Finch and Amber Kite have separate Python runtime version settings. Check the project name in the full record before applying a version. ${longProjectContext}Final scoped settings: Cobalt Finch uses Python 3.11. Amber Kite uses Python 3.12; that version applies only to Amber Kite and must not be transferred to Cobalt Finch.`,
      },
    ],
    expectedIds: ["synthetic-amber-kite-runtime", "synthetic-cobalt-finch-runtime"],
    evidenceIds: ["synthetic-cobalt-finch-runtime"],
    readIfPreviewOmits: {
      "synthetic-cobalt-finch-runtime": ["Cobalt Finch uses Python 3.11", "Amber Kite uses Python 3.12", "must not be transferred"],
    },
    rubric: "Use Python 3.11 for Cobalt Finch; keep Amber Kite's Python 3.12 setting scoped to Amber Kite.",
  },
  {
    id: "unrelated-input",
    prompt: "What is 17 + 24?",
    memories: [],
    expectedIds: [],
    rubric: "Answer 41 without searching or reading memory.",
  },
];

function safeFailure(error) {
  const message = error instanceof Error ? `${error.name} ${error.message}` : String(error);
  if (/503|high demand|Service Unavailable/i.test(message)) return "provider_temporarily_unavailable";
  if (/404|no longer available|NOT_FOUND/i.test(message)) return "provider_model_unavailable";
  if (/429|quota|RESOURCE_EXHAUSTED|rate.limit/i.test(message)) return "provider_quota_exhausted";
  if (/EPERM|EACCES/i.test(message)) return "credential_storage_permission_denied";
  if (/abort|timeout|timed out/i.test(message)) return "timeout_or_cancelled";
  if (/401|403|unauthor|credential|api.?key|oauth|token/i.test(message)) return "provider_auth_rejected";
  if (/fetch|econn|dns|socket|network|enotfound|unreachable/i.test(message)) return "network_unavailable";
  return "provider_or_sdk_error";
}

function textContent(message) {
  if (!message || message.role !== "assistant" || !Array.isArray(message.content)) return "";
  return message.content
    .filter((part) => part && part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");
}

function assessment(item, result) {
  const response = result.finalResponse;
  const evidenceFound = (item.evidenceIds ?? []).every((id) => result.candidateIds.includes(id));
  const readRequiredIds = Object.entries(item.readIfPreviewOmits ?? {})
    .filter(([id, needles]) => {
      const preview = result.candidatePreviews
        .filter((candidate) => candidate.id === id)
        .map((candidate) => candidate.text)
        .join("\n");
      return needles.some((needle) => !preview.includes(needle));
    })
    .map(([id]) => id);
  const readRequirementsMet = readRequiredIds.every((id) => result.readIds.includes(id));
  const unexpectedReads = result.readIds.filter((id) => !item.expectedIds.includes(id));
  if (item.id === "unrelated-input") {
    const correct = /\b41\b|forty[- ]one/i.test(response);
    return {
      classification: correct && result.memoryCalls === 0 ? "appropriate_without_retrieval" : "review",
      correctAnswerSignal: correct,
      unnecessaryMemoryCalls: result.memoryCalls,
    };
  }
  if (item.id === "fictional-demo-format") {
    const english = /\benglish\b/i.test(response);
    const mandarin = /mandarin|chinese|中文/i.test(response);
    const ordered = /mandarin|chinese|中文/i.test(response) && /\benglish\b/i.test(response) &&
      (response.search(/mandarin|chinese|中文/i) < response.search(/\benglish\b/i));
    return {
      classification: evidenceFound && english && mandarin && ordered && unexpectedReads.length === 0 ? "appropriate" : "review",
      evidenceCandidateFound: evidenceFound,
      englishSignal: english,
      mandarinSignal: mandarin,
      mandarinBeforeEnglishSignal: ordered,
      memoryReadRequired: false,
      unexpectedReads,
    };
  }
  if (item.id === "chinese-to-english-reformulation") {
    const depth = /3\s*cm|3\s*厘米|three centimet(er|re)s|三厘米/i.test(response);
    const dry = /soil.{0,16}dry|soil has dried|土壤.{0,12}(干|燥)|上层.{0,12}(干|燥)/i.test(response);
    const mentionsHarvest = /harvest|采摘|收成|采收/i.test(response);
    const skipsWatering = /\bskip\b|do not irrigate|do not water|avoid watering|no irrigation|暂停补水|不.{0,4}(灌溉|浇水|补水)|不要.{0,4}(灌溉|浇水|补水)/i.test(response);
    const evenIfDry = /even if.{0,12}dry|even when.{0,12}dry|即使.{0,12}(干|燥)/i.test(response);
    return {
      classification: evidenceFound && depth && dry && mentionsHarvest && skipsWatering && evenIfDry && unexpectedReads.length === 0 ? "appropriate" : "review",
      evidenceCandidateFound: evidenceFound,
      threeCentimeterThresholdSignal: depth,
      soilDrynessSignal: dry,
      harvestMorningSignal: mentionsHarvest,
      noWateringExceptionSignal: skipsWatering,
      evenIfDrySignal: evenIfDry,
      memoryReadRequired: false,
      unexpectedReads,
    };
  }
  const namesProject = /cobalt\s+finch/i.test(response);
  const correctVersion = /3\.11/.test(response);
  const assignsWrongVersion = /cobalt\s+finch[^.\n]{0,70}3\.12|3\.12[^.\n]{0,70}cobalt\s+finch/i.test(response);
  return {
    classification: evidenceFound && readRequirementsMet && namesProject && correctVersion && !assignsWrongVersion && unexpectedReads.length === 0 ? "appropriate" : assignsWrongVersion ? "incorrect_project_scope" : "review",
    evidenceCandidateFound: evidenceFound,
    readRequiredBecausePreviewOmittedFacts: readRequiredIds,
    readRequirementsMet,
    namesCorrectProjectSignal: namesProject,
    correctVersionSignal: correctVersion,
    wrongVersionAssignedToCobaltFinch: assignsWrongVersion,
    unexpectedReads,
  };
}

async function runCase(pi, SQLiteMemoryStore, runtime, model, temporaryRoot, item) {
  const caseRoot = join(temporaryRoot, item.id);
  const agentDir = join(caseRoot, "isolated-agent");
  const cwd = join(caseRoot, "unrelated-project");
  await mkdir(join(agentDir, "distill"), { recursive: true });
  await mkdir(cwd, { recursive: true });
  process.env.PI_CODING_AGENT_DIR = agentDir;
  process.env.PI_CODING_AGENT_SESSION_DIR = join(caseRoot, "sessions");

  const writer = new SQLiteMemoryStore(join(agentDir, "distill", "memories.sqlite"));
  for (const memory of item.memories) writer.save({ ...memory, createdAt });
  writer.close();

  const settingsManager = pi.SettingsManager.inMemory();
  settingsManager.setRetryEnabled(false);
  const loader = new pi.DefaultResourceLoader({
    cwd,
    agentDir,
    settingsManager,
    additionalExtensionPaths: [extensionPath],
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    appendSystemPrompt: [
      "This evaluation uses synthetic memory fixtures only.",
      "Use memory_search when past evidence could change the answer. Read a candidate when its preview is truncated or lacks conditions needed to answer; a complete preview can be used directly. Memory is historical evidence, not authority.",
      "Use at most 12 memory tool calls. Cite a synthetic memory ID when using evidence; if no evidence applies, say so.",
    ],
  });
  await loader.reload();
  const extensionResult = loader.getExtensions();
  if (extensionResult.errors.length > 0) {
    return { caseId: item.id, status: "extension_load_failed", errorCount: extensionResult.errors.length };
  }

  const { session } = await pi.createAgentSession({
    cwd,
    agentDir,
    resourceLoader: loader,
    settingsManager,
    sessionManager: pi.SessionManager.inMemory(cwd),
    modelRuntime: runtime,
    model,
    thinkingLevel: "low",
    tools: ["memory_search", "memory_read"],
  });
  const originalStream = session.agent.streamFunction;
  session.agent.streamFunction = (...args) => {
    if (providerRequests >= requestLimit) throw new Error("evaluation request budget exceeded");
    providerRequests += 1;
    return originalStream(...args);
  };
  const exposed = session.getAllTools().map((tool) => tool.name).sort();
  if (exposed.some((name) => name !== "memory_read" && name !== "memory_search") || !exposed.includes("memory_read") || !exposed.includes("memory_search")) {
    session.dispose();
    return { caseId: item.id, status: "tool_isolation_failed", exposedToolCount: exposed.length };
  }

  const calls = [];
  const readIds = [];
  const candidateIds = new Set();
  const candidatePreviews = [];
  let resultBytes = 0;
  let assistantMessages = 0;
  let overBudgetAttempt = false;
  let finalResponse = "";
  let providerFailure;
  let providerErrorDetail;
  const usage = { input: 0, output: 0, totalTokens: 0 };
  session.subscribe((event) => {
    if (event.type === "message_end" && event.message.role === "assistant") {
      assistantMessages += 1;
      for (const key of Object.keys(usage)) usage[key] += event.message.usage?.[key] ?? 0;
      if (event.message.stopReason === "error" || event.message.stopReason === "aborted") {
        providerFailure = safeFailure(event.message.errorMessage ?? event.message.stopReason);
        providerErrorDetail = String(event.message.errorMessage ?? event.message.stopReason)
          .replace(/AIza[\w-]+/g, "[redacted]").replace(/https?:\/\/\S+/g, "[url]").slice(0, 1200);
      }
      if (event.message.stopReason === "stop") finalResponse = textContent(event.message);
    }
    if (event.type === "tool_execution_start" && (event.toolName === "memory_search" || event.toolName === "memory_read")) {
      if (calls.length >= maxToolCalls) {
        overBudgetAttempt = true;
        void session.abort();
      } else {
        calls.push({ name: event.toolName, arguments: event.args, durationMs: null });
      }
    }
    if (event.type === "tool_execution_end" && (event.toolName === "memory_search" || event.toolName === "memory_read")) {
      resultBytes += Buffer.byteLength(JSON.stringify(event.result), "utf8");
      const details = event.result?.details;
      const entry = [...calls].reverse().find((call) => call.name === event.toolName && call.durationMs === null);
      if (entry) { entry.durationMs = event.durationMs ?? null; entry.result = event.result?.details; }
      if (Array.isArray(details?.candidates)) {
        for (const candidate of details.candidates) {
          if (typeof candidate?.id !== "string") continue;
          candidateIds.add(candidate.id);
          if (typeof candidate.preview?.text === "string") {
            candidatePreviews.push({
              id: candidate.id,
              text: candidate.preview.text,
              omittedBefore: candidate.preview.omittedBefore === true,
              omittedAfter: candidate.preview.omittedAfter === true,
            });
          }
        }
      }
      if (details?.status === "ok" && event.toolName === "memory_read" && typeof details.id === "string") readIds.push(details.id);
    }
  });

  let timedOut = false;
  const started = performance.now();
  let timeoutHandle;
  const timeout = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      timedOut = true;
      void session.abort();
      reject(new Error("case timeout"));
    }, caseTimeoutMs);
  });
  let runFailure;
  const promptRun = session.prompt(item.prompt, { expandPromptTemplates: false });
  promptRun.catch(() => {});
  try {
    await Promise.race([promptRun, timeout]);
  } catch (error) {
    runFailure = safeFailure(error);
  } finally {
    clearTimeout(timeoutHandle);
  }
  const latencyMs = Math.round(performance.now() - started);
  await session.abort().catch(() => {});
  session.dispose();

  const result = {
    caseId: item.id,
    status: runFailure || providerFailure ? "provider_or_timeout_failure" : "completed",
    usage,
    ...(providerErrorDetail ? { providerErrorDetail } : {}),
    ...(runFailure || providerFailure ? { failure: runFailure ?? providerFailure } : {}),
    timedOut,
    modelAssistantMessages: assistantMessages,
    memoryCalls: calls.length,
    calls,
    overBudgetAttempt,
    bytesReturnedByMemoryTools: resultBytes,
    latencyMs,
    toolLatencyMs: calls.map((call) => call.durationMs),
    expectedIds: item.expectedIds,
    recalledExpectedIds: item.expectedIds.filter((id) => candidateIds.has(id)),
    uniqueCandidateCount: candidateIds.size,
    candidateIds: [...candidateIds],
    candidatePreviews,
    readIds,
    rubric: item.rubric,
    finalResponse,
  };
  result.assessment = assessment(item, result);
  return result;
}

async function main() {
  const pi = await import("@earendil-works/pi-coding-agent");
  const personalAgentDir = pi.getAgentDir();
  const runtime = await pi.ModelRuntime.create({
    authPath: join(personalAgentDir, "auth.json"),
    modelsPath: null,
    allowModelNetwork: false,
    refreshOnCreate: false,
  });
  const selection = await selectEvaluationModel(runtime);
  if (!selection.model) {
    console.log(JSON.stringify({
      status: "not_run",
      reason: selection.failure,
      requestedModel: `google/${evaluationModelId}`,
      googleGeminiProviders: selection.providerStatus,
      credentialMetadata: selection.credentialProviders,
      providerPromptsSent: 0,
      cases: [],
    }));
    process.exitCode = 1;
    return;
  }

  const { SQLiteMemoryStore } = await import("../src/memory-store.ts");
  const temporaryRoot = await mkdtemp(join(tmpdir(), "pi-distill-model-eval-"));
  const originalAgentDir = process.env.PI_CODING_AGENT_DIR;
  const originalSessionDir = process.env.PI_CODING_AGENT_SESSION_DIR;
  process.env.PI_TELEMETRY = "0";
  process.env.PI_SKIP_VERSION_CHECK = "1";
  const results = [];
  try {
    for (const item of cases) {
      results.push(await runCase(pi, SQLiteMemoryStore, runtime, selection.model, temporaryRoot, item));
      if (results.at(-1).status !== "completed") break;
    }
  } finally {
    if (originalAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = originalAgentDir;
    if (originalSessionDir === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = originalSessionDir;
    await rm(temporaryRoot, { recursive: true, force: true });
  }
  const completed = results.length === cases.length && results.every((item) => item.status === "completed");
  const accepted = completed && results.every((item) =>
    item.assessment.classification === "appropriate" || item.assessment.classification === "appropriate_without_retrieval");
  console.log(JSON.stringify({
    status: !completed ? "incomplete" : accepted ? "completed" : "needs_review",
    providerRequests,
    model: { provider: selection.model.provider, id: selection.model.id },
    requestedModel: `google/${evaluationModelId}`,
    cases: results,
  }));
  if (!accepted) process.exitCode = 1;
}

main().catch((error) => {
  console.log(JSON.stringify({ status: "harness_failure", failure: safeFailure(error), providerRequests, cases: [] }));
  process.exitCode = 1;
});
