import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { assessCurrentMemories, assessRecall, evaluationFailureStatus, sanitizeEvaluationTrace, trackStreamCompletion, waitForConsolidation } from "./consolidation-model-eval-support.mjs";
import { assessTaskMemories, assessTaskRecall } from "./task-cancellation-eval-support.mjs";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const extensionPath = join(repoRoot, "src", "index.ts");
const modelId = "gpt-6-luna";
const requestLimit = 20;
const deadlineMs = 150_000;
const cases = {
  lifecycle: {
    project: "Cedar-72", initialDays: 14, correctedDays: 7, exceptionDays: 30,
    initial: "For the fictional Cedar-72 service, normal log retention is 14 days by default. While an incident investigation is active, retain the relevant logs for 30 days. The incident period is an exception; the general default remains 14 days.",
    correction: "Correction for fictional Cedar-72: the standard log-retention period is now 7 days. The incident-investigation exception remains 30 days.",
    initialQuestion: "For Cedar-72, give the default log-retention period and the incident-investigation exception.",
    finalQuestion: "For Cedar-72, what does the ordinary telemetry policy say, including what changes during an active investigation?",
  },
  compact: {
    project: "Maple", initialDays: 10, correctedDays: 5, exceptionDays: 21,
    initial: "Our fictional Maple service keeps logs for 10 days normally, or 21 days during an incident investigation. That is the agreed policy. Briefly acknowledge.",
    correction: "Maple 的日志默认保存期限改成 5 天。事故调查期间仍然保留 21 天。新默认值替代之前的决定。简单确认即可。",
    initialQuestion: "What is Maple's normal retention duration and its investigation exception? Answer briefly in English.",
    finalQuestion: "For Maple, how long do we now keep ordinary logs, and what changes when investigating an incident? Answer briefly in English.",
  },
  cancellation: {
    kind: "task", project: "River", trigger: "close",
    initial: "For our fictional River project, we need to submit the migration proposal. The weekly log review is also required. These are our current commitments. Briefly acknowledge.",
    correction: "River's migration has been cancelled, so there is no need to submit the proposal. The weekly log review must continue. Briefly acknowledge.",
    initialQuestion: "What work is currently required for River? Answer briefly in English.",
    finalQuestion: "What work is still required for River now? Answer briefly in English.",
  },
  "cancellation-compact": {
    kind: "task", project: "Brook", trigger: "compact",
    initial: "For our fictional Brook project, submitting the migration proposal is required. We also need to carry out the weekly log review. Both are agreed commitments. Briefly acknowledge.",
    correction: "Brook 的迁移已经取消，因此迁移提案不需要提交了。每周日志检查照常进行。简单确认即可。",
    initialQuestion: "What work is currently required for Brook? Answer briefly in English.",
    finalQuestion: "What work is still required for Brook now? Answer briefly in English.",
  },
};
const usage = `Usage: node tests/consolidation-model-eval.mjs [--case ${Object.keys(cases).join("|")}]`;

function parseArgs(argv) {
  if (argv.includes("--help") || argv.includes("-h")) return { help: true };
  if (argv.length === 0) return { caseId: "lifecycle" };
  if (argv.length === 2 && argv[0] === "--case" && Object.hasOwn(cases, argv[1])) return { caseId: argv[1] };
  throw new Error(usage);
}

function failureCode(error, requestLimitHit) {
  if (requestLimitHit) return "request_budget_exhausted";
  const message = error instanceof Error ? `${error.name} ${error.message}` : String(error);
  for (const code of ["extension_load_failed", "extension_runtime_failed", "unexpected_tool_exposure", "session_replacement_cancelled"]) {
    if (message.includes(code)) return code;
  }
  if (message.includes("openai_oauth_unavailable")) return "openai_oauth_unavailable";
  if (message.includes("requested_model_unavailable")) return "provider_model_unavailable";
  if (/503|high demand|service unavailable/iu.test(message)) return "provider_temporarily_unavailable";
  if (/404|no longer available|not_found/iu.test(message)) return "provider_model_unavailable";
  if (/429|quota|resource_exhausted|rate.limit/iu.test(message)) return "provider_quota_exhausted";
  if (/401|403|unauthor|credential|api.?key|oauth|token/iu.test(message)) return "provider_auth_rejected";
  if (/timeout|timed out|abort|deadline/iu.test(message)) return "timeout_or_cancelled";
  if (/fetch|econn|dns|socket|network|enotfound|unreachable/iu.test(message)) return "network_unavailable";
  return "harness_failure";
}

function responseText(message) {
  if (!message || !Array.isArray(message.content)) return "";
  return message.content.filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text).join("\n");
}

function safeAssistantContent(message) {
  if (!message || !Array.isArray(message.content)) return [];
  return message.content.filter((part) => part?.type === "text" || part?.type === "toolCall")
    .map((part) => part.type === "text"
      ? { type: "text", text: part.text }
      : { type: "toolCall", id: part.id, name: part.name, arguments: part.arguments });
}

function bounded(promise, signal) {
  const operation = Promise.resolve(promise);
  operation.catch(() => {});
  if (signal.aborted) return Promise.reject(signal.reason ?? new Error("Evaluation deadline exceeded."));
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason ?? new Error("Evaluation deadline exceeded."));
    signal.addEventListener("abort", abort, { once: true });
    operation.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
  });
}

function signalFor(deadlineSignal, requestSignal) {
  if (!requestSignal) return deadlineSignal;
  if (requestSignal.aborted) return requestSignal;
  if (deadlineSignal.aborted) return deadlineSignal;
  return AbortSignal.any([deadlineSignal, requestSignal]);
}

async function selectModel(pi) {
  const personalAgentDir = pi.getAgentDir();
  const modelRuntime = await pi.ModelRuntime.create({
    authPath: join(personalAgentDir, "auth.json"),
    modelsPath: null,
    allowModelNetwork: false,
    refreshOnCreate: false,
  });
  const signal = AbortSignal.timeout(10_000);
  const credentials = await modelRuntime.listCredentials({ signal });
  if (!credentials.some((credential) => credential.providerId === "openai" && credential.type === "oauth")) {
    throw new Error("openai_oauth_unavailable");
  }
  const available = await modelRuntime.getAvailable("openai", { signal });
  const model = available.find((candidate) => candidate.provider === "openai" && candidate.id === modelId);
  if (!model) throw new Error("requested_model_unavailable");
  return { modelRuntime, model };
}

async function runLifecycle(pi, SQLiteMemoryStore, modelRuntime, model, isolatedRoot, deadlineSignal, runtimeRef, caseId) {
  const fixture = cases[caseId];
  let providerRequests = 0;
  const trace = [];
  const stages = [];
  runtimeRef.report = {
    status: "running",
    case: caseId,
    model: { provider: model.provider, id: model.id },
    providerRequests,
    requestLimit,
    deadlineMs,
    stages,
    trace,
  };

  const agentDir = join(isolatedRoot, "agent");
  const sessionDir = join(isolatedRoot, "sessions");
  const cwd = join(isolatedRoot, "synthetic-project");
  const databasePath = join(agentDir, "distill", "memories.sqlite");
  await Promise.all([
    mkdir(agentDir, { recursive: true }),
    mkdir(sessionDir, { recursive: true }),
    mkdir(cwd, { recursive: true }),
  ]);

  let inFlightRequests = 0;
  let completedRequests = 0;
  let lastRequestCompletion = Date.now();
  let requestLimitHit = false;
  const seenMessageCount = new WeakMap();
  let activeStage = "setup";

  const beginRequest = (kind) => {
    if (deadlineSignal.aborted) throw deadlineSignal.reason ?? new Error("Evaluation deadline exceeded.");
    if (providerRequests >= requestLimit) {
      requestLimitHit = true;
      runtimeRef.requestLimitHit = true;
      throw new Error("Evaluation request budget exceeded.");
    }
    providerRequests += 1;
    runtimeRef.report.providerRequests = providerRequests;
    inFlightRequests += 1;
    return { id: providerRequests, kind, finished: false };
  };
  const finishRequest = (request) => {
    if (request.finished) return;
    request.finished = true;
    inFlightRequests = Math.max(0, inFlightRequests - 1);
    completedRequests += 1;
    lastRequestCompletion = Date.now();
  };

  const originalComplete = modelRuntime.complete.bind(modelRuntime);
  modelRuntime.complete = async (requestedModel, context, options = {}) => {
    const request = beginRequest("consolidation");
    const messages = Array.isArray(context?.messages) ? context.messages : [];
    const previouslySeen = seenMessageCount.get(messages) ?? 0;
    seenMessageCount.set(messages, messages.length);
    const delta = {
      ...(previouslySeen === 0 && typeof context?.systemPrompt === "string" ? { systemPrompt: context.systemPrompt } : {}),
      ...(previouslySeen === 0 && Array.isArray(context?.tools)
        ? { tools: context.tools.map((tool) => ({ name: tool.name, description: tool.description })) }
        : {}),
      messages: messages.slice(previouslySeen),
    };
    trace.push(sanitizeEvaluationTrace({
      stage: activeStage,
      surface: "consolidation",
      type: "request",
      requestId: request.id,
      model: { provider: requestedModel.provider, id: requestedModel.id },
      delta,
    }));
    try {
      const response = await originalComplete(requestedModel, context, {
        ...options,
        signal: signalFor(deadlineSignal, options.signal),
      });
      trace.push(sanitizeEvaluationTrace({
        stage: activeStage,
        surface: "consolidation",
        type: "assistant_response",
        requestId: request.id,
        stopReason: response.stopReason,
        ...(response.errorMessage ? { error: response.errorMessage } : {}),
        content: safeAssistantContent(response),
      }));
      return response;
    } catch (error) {
      trace.push({ stage: activeStage, surface: "consolidation", type: "request_failure", requestId: request.id, failure: failureCode(error, requestLimitHit) });
      throw error;
    } finally {
      finishRequest(request);
    }
  };

  const settingsManager = pi.SettingsManager.inMemory({ compaction: { enabled: false, reserveTokens: 512, keepRecentTokens: 0 } });
  settingsManager.setCompactionEnabled(false);
  settingsManager.setRetryEnabled(false);
  let sessionUnsubscribe;
  const foregroundResults = new Map();
  const observeSession = (session) => {
    sessionUnsubscribe?.();
    sessionUnsubscribe = session.subscribe((event) => {
      if (event.type === "message_end" && event.message?.role === "assistant") {
        const assistantContent = safeAssistantContent(event.message);
        trace.push(sanitizeEvaluationTrace({
          stage: activeStage,
          surface: "foreground",
          type: "assistant_message",
          stopReason: event.message.stopReason,
          ...(event.message.errorMessage ? { error: event.message.errorMessage } : {}),
          content: assistantContent,
        }));
        if (event.message.stopReason === "stop") foregroundResults.set(activeStage, responseText(event.message));
      }
      if (event.type === "tool_execution_start" && ["memory_search", "memory_read"].includes(event.toolName)) {
        trace.push(sanitizeEvaluationTrace({
          stage: activeStage,
          surface: "foreground",
          type: "tool_call",
          name: event.toolName,
          arguments: event.args,
        }));
      }
      if (event.type === "tool_execution_end" && ["memory_search", "memory_read"].includes(event.toolName)) {
        trace.push(sanitizeEvaluationTrace({
          stage: activeStage,
          surface: "foreground",
          type: "tool_result",
          name: event.toolName,
          isError: event.result?.isError === true,
          details: event.result?.details ?? event.result?.content,
        }));
      }
    });

    const originalStream = session.agent.streamFunction;
    session.agent.streamFunction = (requestedModel, context, options = {}) => {
      const request = beginRequest("foreground");
      return trackStreamCompletion(() => originalStream(requestedModel, context, {
        ...options,
        signal: signalFor(deadlineSignal, options.signal),
      }), () => finishRequest(request)).catch((error) => {
        trace.push({ stage: activeStage, surface: "foreground", type: "request_failure", failure: failureCode(error, requestLimitHit) });
        throw error;
      });
    };
  };

  const factory = async (options) => {
    const services = await pi.createAgentSessionServices({
      cwd: options.cwd,
      agentDir,
      settingsManager,
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
      model,
      thinkingLevel: "low",
      noTools: "builtin",
      sessionStartEvent: options.sessionStartEvent,
    });
    if (created.extensionsResult.errors.length > 0) throw new Error("extension_load_failed");
    await created.session.bindExtensions({ mode: "print", onError: () => { throw new Error("extension_runtime_failed"); } });
    const tools = created.session.getActiveToolNames().sort();
    if (tools.length !== 2 || tools[0] !== "memory_read" || tools[1] !== "memory_search") {
      throw new Error("unexpected_tool_exposure");
    }
    observeSession(created.session);
    return { ...created, services, diagnostics: services.diagnostics };
  };

  const runtime = await pi.createAgentSessionRuntime(factory, {
    cwd,
    agentDir,
    sessionManager: pi.SessionManager.create(cwd, sessionDir),
  });
  runtimeRef.runtime = runtime;

  const readState = () => {
    const store = new SQLiteMemoryStore(databasePath);
    try {
      const memories = [];
      let after;
      // Inspect the complete isolated store, including records that omit the
      // project name; a literal search miss cannot establish task deletion.
      for (let page = 0; page < 4; page++) {
        const rows = store.searchBatch({ terms: [fixture.project], match: "any", limit: 128, ...(after === undefined ? {} : { after }) });
        memories.push(...rows.map(({ memory }) => ({ id: memory.id, revision: memory.revision, content: memory.content })));
        if (rows.length < 128) return {
          memories, pendingJobs: store.listConsolidationJobs().length,
          pendingObservations: store.observedSessions().length,
        };
        const next = rows.at(-1).memory.id;
        if (next === after) throw new Error("evaluation_store_cursor_not_advancing");
        after = next;
      }
      throw new Error("evaluation_store_scan_limit");
    } finally {
      store.close();
    }
  };

  const advanceBoundary = async (compact) => {
    const completedBefore = completedRequests;
    if (compact) await bounded(runtime.session.compact(), deadlineSignal);
    else {
      const replacement = await bounded(runtime.newSession(), deadlineSignal);
      if (replacement.cancelled) throw new Error("session_replacement_cancelled");
    }
    return waitForConsolidation({
      readState, signal: deadlineSignal, completedBefore,
      activity: () => ({ inFlightRequests, completedRequests, lastRequestCompletion, requestLimitHit }),
    });
  };

  const closeAndAssess = async (stage, expectedDefaultDays, expectedRevision, previous, compact = false) => {
    activeStage = stage;
    const { state, settled } = await advanceBoundary(compact);
    const assessment = assessCurrentMemories(state.memories, {
      defaultDays: expectedDefaultDays,
      project: fixture.project,
      exceptionDays: fixture.exceptionDays,
      expectedRevision,
      previous,
    });
    if (stage === "correction_close" && !previous) {
      assessment.accepted = false;
      assessment.issues.push("initial_memory_missing");
    }
    const accepted = settled && assessment.accepted;
    const result = {
      name: stage,
      status: accepted ? "passed" : !settled ? "incomplete" : "needs_review",
      settled,
      pendingObservations: state.pendingObservations,
      pendingJobs: state.pendingJobs,
      memories: state.memories,
      assessment,
    };
    stages.push(result);
    return result;
  };

  const promptAndTrace = async (stage, prompt) => {
    activeStage = stage;
    await bounded(runtime.session.prompt(prompt, { expandPromptTemplates: false }), deadlineSignal);
    const response = foregroundResults.get(stage) ?? "";
    const foregroundCalls = trace.filter((item) => item?.stage === stage && item.surface === "foreground" && item.type === "tool_call");
    const foregroundToolResults = trace.filter((item) => item?.stage === stage && item.surface === "foreground" && item.type === "tool_result");
    return { response, foregroundCalls, foregroundToolResults };
  };

  const promptAndAssess = async (stage, prompt, expectedDefaultDays, memory) => {
    const { response, foregroundCalls, foregroundToolResults } = await promptAndTrace(stage, prompt);
    const assessment = assessRecall(response, foregroundToolResults, { memory, project: fixture.project, defaultDays: expectedDefaultDays, exceptionDays: fixture.exceptionDays });
    const result = {
      name: stage,
      status: assessment.accepted ? "passed" : "needs_review",
      prompt,
      response,
      toolCalls: foregroundCalls,
      toolResults: foregroundToolResults,
      signals: assessment.signals,
      evidenceFound: assessment.evidenceFound,
    };
    stages.push(result);
    return result;
  };

  const taskBoundary = async (stage, { cancelled, previous, unchanged = false, compact = false }) => {
    activeStage = stage;
    const { state, settled } = await advanceBoundary(compact);
    const assessment = assessTaskMemories(state.memories, {
      project: fixture.project, cancelled, previous, unchanged, pendingJobs: state.pendingJobs,
    });
    const result = {
      name: stage, status: !settled ? "incomplete" : assessment.accepted ? "passed" : "needs_review",
      ...state, settled, assessment,
    };
    stages.push(result);
    return result;
  };

  const taskRecall = async (stage, prompt, records, cancelled) => {
    const { response, foregroundCalls, foregroundToolResults } = await promptAndTrace(stage, prompt);
    const assessment = assessTaskRecall(response, foregroundToolResults, { records, project: fixture.project, cancelled });
    const result = {
      name: stage, status: assessment.accepted ? "passed" : "needs_review", prompt, response,
      toolCalls: foregroundCalls, toolResults: foregroundToolResults, assessment,
    };
    stages.push(result);
    return result;
  };

  try {
    activeStage = "initial_conversation";
    await bounded(runtime.session.prompt(
      fixture.initial,
      { expandPromptTemplates: false },
    ), deadlineSignal);

    if (fixture.kind === "task") {
      const initial = await taskBoundary("initial_close", { cancelled: false });
      if (initial.status === "incomplete") return { ...runtimeRef.report, status: initial.status };
      // Preserve all completed stages for offline rubric review without another model run.
      await taskRecall("initial_fresh_session_recall", fixture.initialQuestion, initial.memories, false);

      activeStage = "cancellation_conversation";
      await bounded(runtime.session.prompt(fixture.correction, { expandPromptTemplates: false }), deadlineSignal);
      const compact = fixture.trigger === "compact";
      const cancelled = await taskBoundary(compact ? "cancellation_compact" : "cancellation_close", {
        cancelled: true, previous: initial.memories, compact,
      });
      if (cancelled.status === "incomplete") return { ...runtimeRef.report, status: cancelled.status };
      if (compact) {
        const repeated = await taskBoundary("post_compact_close", {
          cancelled: true, previous: cancelled.memories, unchanged: true,
        });
        if (repeated.status === "incomplete") return { ...runtimeRef.report, status: repeated.status };
      }
      await taskRecall("cancelled_fresh_session_recall", fixture.finalQuestion, cancelled.memories, true);
      const expectedStages = compact ? 5 : 4;
      runtimeRef.report.status = stages.length === expectedStages && stages.every((stage) => stage.status === "passed") && !requestLimitHit
        ? "completed" : deadlineSignal.aborted ? "incomplete" : "needs_review";
      return runtimeRef.report;
    }

    const initial = await closeAndAssess("initial_close", fixture.initialDays, 1);
    if (initial.status !== "passed") return { ...runtimeRef.report, status: initial.status };
    const initialMemory = initial.assessment.target;
    await promptAndAssess(
      "initial_fresh_session_recall",
      fixture.initialQuestion,
      fixture.initialDays,
      initialMemory,
    );

    activeStage = "correction_conversation";
    await bounded(runtime.session.prompt(
      fixture.correction,
      { expandPromptTemplates: false },
    ), deadlineSignal);

    const corrected = await closeAndAssess(caseId === "compact" ? "correction_compact" : "correction_close", fixture.correctedDays, 2, initialMemory, caseId === "compact");
    if (corrected.status !== "passed") return { ...runtimeRef.report, status: corrected.status };
    if (caseId === "compact") {
      const repeated = await closeAndAssess("post_compact_close", fixture.correctedDays, 2);
      if (repeated.assessment.target?.id !== initialMemory.id) {
        repeated.status = "needs_review";
        repeated.assessment.issues.push("original_memory_id_changed");
      }
      if (repeated.status !== "passed") return { ...runtimeRef.report, status: repeated.status };
    }
    await promptAndAssess(
      "corrected_fresh_session_recall",
      fixture.finalQuestion,
      fixture.correctedDays,
      corrected.assessment.target,
    );
  } finally {
    sessionUnsubscribe?.();
  }

  const accepted = stages.length === (caseId === "compact" ? 5 : 4) && stages.every((stage) => stage.status === "passed") && !requestLimitHit;
  runtimeRef.report.status = accepted ? "completed" : deadlineSignal.aborted ? "incomplete" : "needs_review";
  return runtimeRef.report;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(`${usage}\n`);
    return;
  }

  const temporaryRoot = await mkdtemp(join(tmpdir(), "pi-distill-lifecycle-eval-"));
  const isolatedRoot = join(temporaryRoot, "runtime");
  const reportPath = join(temporaryRoot, "report.json");
  const pi = await import("@earendil-works/pi-coding-agent");
  const { SQLiteMemoryStore } = await import("../src/memory-store.ts");
  const originalAgentDir = process.env.PI_CODING_AGENT_DIR;
  const originalSessionDir = process.env.PI_CODING_AGENT_SESSION_DIR;
  const originalTelemetry = process.env.PI_TELEMETRY;
  const originalVersionCheck = process.env.PI_SKIP_VERSION_CHECK;
  const deadlineController = new AbortController();
  const deadlineSignal = deadlineController.signal;
  const deadlineTimer = setTimeout(() => deadlineController.abort(new Error("evaluation_deadline_exceeded")), deadlineMs);
  const runtimeRef = {};
  let report = { status: "not_run", case: args.caseId, model: { provider: "openai", id: modelId }, stages: [], trace: [] };
  try {
    const { modelRuntime, model } = await selectModel(pi);
    process.env.PI_CODING_AGENT_DIR = join(isolatedRoot, "agent");
    process.env.PI_CODING_AGENT_SESSION_DIR = join(isolatedRoot, "sessions");
    process.env.PI_TELEMETRY = "0";
    process.env.PI_SKIP_VERSION_CHECK = "1";
    report = await runLifecycle(pi, SQLiteMemoryStore, modelRuntime, model, isolatedRoot, deadlineSignal, runtimeRef, args.caseId);
  } catch (error) {
    const reason = failureCode(error, runtimeRef.requestLimitHit === true);
    const partialReport = runtimeRef.report ?? {};
    report = {
      ...partialReport,
      status: evaluationFailureStatus(reason, partialReport),
      case: args.caseId,
      model: { provider: "openai", id: modelId },
      reason,
      error: sanitizeEvaluationTrace(error instanceof Error ? error.message : String(error)),
      providerRequests: partialReport.providerRequests ?? 0,
      stages: partialReport.stages ?? [],
      trace: partialReport.trace ?? [],
    };
  } finally {
    deadlineController.abort(new Error("evaluation_finished"));
    clearTimeout(deadlineTimer);
    try { await runtimeRef.runtime?.dispose(); } catch { /* The bounded runner records the evaluation before cleanup. */ }
    if (originalAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
    else process.env.PI_CODING_AGENT_DIR = originalAgentDir;
    if (originalSessionDir === undefined) delete process.env.PI_CODING_AGENT_SESSION_DIR;
    else process.env.PI_CODING_AGENT_SESSION_DIR = originalSessionDir;
    if (originalTelemetry === undefined) delete process.env.PI_TELEMETRY;
    else process.env.PI_TELEMETRY = originalTelemetry;
    if (originalVersionCheck === undefined) delete process.env.PI_SKIP_VERSION_CHECK;
    else process.env.PI_SKIP_VERSION_CHECK = originalVersionCheck;
    await rm(isolatedRoot, { recursive: true, force: true });
  }

  report.generatedAt = new Date().toISOString();
  report.node = process.version;
  report.reportPath = reportPath;
  await writeFile(reportPath, `${JSON.stringify(sanitizeEvaluationTrace(report), null, 2)}\n`, { mode: 0o600 });
  const summary = {
    status: report.status,
    case: report.case,
    model: report.model,
    node: report.node,
    providerRequests: report.providerRequests ?? 0,
    stages: (report.stages ?? []).map(({ name, status, pendingJobs, assessment, signals }) => ({
      name,
      status,
      ...(pendingJobs === undefined ? {} : { pendingJobs }),
      ...(assessment === undefined ? {} : { assessment }),
      ...(signals === undefined ? {} : { signals }),
    })),
    reportPath,
  };
  process.stdout.write(`${JSON.stringify(summary)}\n`);
  if (report.status !== "completed") process.exitCode = 1;
}

main().catch((error) => {
  process.stderr.write(`${failureCode(error, false)}\n`);
  process.exitCode = 1;
});
