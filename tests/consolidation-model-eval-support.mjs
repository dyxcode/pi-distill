const forbiddenKey = /(?:usage|tokens?|credential|authorization|headers?|api.?key|thinking|cost)/iu;
const localPath = /(?:\/Users\/[^/\s"']+\/[^\s"']*|\/home\/[^/\s"']+\/[^\s"']*|\/(?:private\/)?(?:tmp|var\/folders)\/[^\s"']*)/gu;

/** Compaction consumes stream.result() without a foreground message_end event. */
export async function trackStreamCompletion(start, finish) {
  try {
    const stream = await start();
    stream.result().then(finish, finish);
    return stream;
  } catch (error) {
    finish();
    throw error;
  }
}

export function evaluationFailureStatus(reason, report = {}) {
  if (reason === "timeout_or_cancelled" || reason === "request_budget_exhausted") return "incomplete";
  const unavailable = [
    "openai_oauth_unavailable", "requested_model_unavailable", "provider_model_unavailable",
    "provider_temporarily_unavailable", "provider_quota_exhausted", "provider_auth_rejected", "network_unavailable",
  ].includes(reason);
  if (!unavailable) return "harness_failure";
  return (report.providerRequests ?? 0) > 0 || (report.stages?.length ?? 0) > 0 ? "incomplete" : "not_run";
}

/** Wait for durable completion, including capture recovery and active requests. */
export async function waitForConsolidation({
  readState, activity, signal, timeoutMs = 45_000, completedBefore = activity().completedRequests,
  now = Date.now, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}) {
  const stopAt = now() + timeoutMs;
  let last;
  let fingerprint;
  let stableSince;
  while (now() < stopAt && !signal.aborted) {
    let readable = false;
    try { last = readState(); readable = true; } catch { /* Opening or a bounded lock can be retried. */ }
    const current = activity();
    const idle = readable && last.pendingJobs === 0 && last.pendingObservations === 0 && current.inFlightRequests === 0;
    if (idle) {
      const next = JSON.stringify(last);
      if (next !== fingerprint || stableSince === undefined) {
        fingerprint = next;
        stableSince = now();
      }
      if (now() - stableSince >= 1_000) return { state: last, settled: true };
    } else {
      fingerprint = undefined;
      stableSince = undefined;
    }
    const failedRequestSettled = current.completedRequests > completedBefore && current.inFlightRequests === 0 &&
      now() - current.lastRequestCompletion >= 1_500;
    if (current.requestLimitHit || (!idle && readable && failedRequestSettled)) break;
    await sleep(200);
  }
  if (signal.aborted) throw signal.reason ?? new Error("Evaluation cancelled.");
  if (!last) throw new Error("evaluation_store_unavailable");
  return { state: last, settled: false };
}

function clauses(value) {
  return value.split(/[.!?;。！？；\n]+/u).map((clause) => clause.trim()).filter(Boolean);
}

function escaped(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function associatedDayValues(parts, defaultCue, incidentCue) {
  const values = new Set();
  const allDayCounts = /(?<!\d)(\d+)\s*[-‐‑]?\s*(?:calendar\s+)?(?:days?\b|天)/giu;
  const cues = [
    { kind: "default", pattern: new RegExp(defaultCue.source, "giu") },
    { kind: "incident", pattern: new RegExp(incidentCue.source, "giu") },
  ];
  for (const part of parts) {
    const foundCues = cues.flatMap(({ kind, pattern }) =>
      [...part.matchAll(pattern)].map((match) => ({ kind, index: match.index ?? 0 })),
    );
    for (const duration of part.matchAll(allDayCounts)) {
      const durationValue = Number(duration[1]);
      const durationIndex = duration.index ?? 0;
      if (/(?:\b(?:previous|earlier|former|old)\s+(?:[a-z-]+\s+){0,2}|之前的?|原来的?)$/iu.test(part.slice(0, durationIndex))) continue;
      const nearest = foundCues
        .map((cue) => ({ ...cue, distance: Math.abs(durationIndex - cue.index) }))
        .sort((left, right) => left.distance - right.distance);
      const first = nearest[0];
      const second = nearest[1];
      if (!first || first.distance > 100 || (second && second.distance === first.distance)) continue;
      const contextStart = Math.max(0, Math.min(first.index, durationIndex) - 32);
      const contextEnd = Math.min(part.length, Math.max(first.index, durationIndex) + duration[0].length + 12);
      if (/\b(?:not|never|no longer|isn't|wasn't|aren't|weren't)\b|不是|不再|并非/iu.test(part.slice(contextStart, contextEnd))) continue;
      values.add(JSON.stringify([first.kind, durationValue]));
    }
  }
  return values;
}

export function policySignals(value, { project = "Cedar-72", defaultDays, exceptionDays } = {}) {
  const content = typeof value === "string" ? value : "";
  const parts = clauses(content);
  const defaultCue = /\b(?:defaults?|standard|normally|normal|usual|routine|ordinary|regular|baseline)\b|默认|通常|常规|一般/u;
  const incidentCue = /\b(?:incident|investigat\w*|incident response)\b|事故|调查/u;
  const values = associatedDayValues(parts, defaultCue, incidentCue);
  const projectCue = project === "Cedar-72"
    ? /\bcedar(?:[-‐‑–—]|\s)72\b/iu
    : new RegExp(`\\b${escaped(project)}\\b`, "iu");
  return {
    namesProject: projectCue.test(content),
    matchesDefault: values.has(JSON.stringify(["default", defaultDays])) &&
      [...values].filter((value) => JSON.parse(value)[0] === "default").length === 1,
    preservesIncidentException: values.has(JSON.stringify(["incident", exceptionDays])) &&
      [...values].filter((value) => JSON.parse(value)[0] === "incident").length === 1,
  };
}

export function assessCurrentMemories(records, { project, defaultDays, exceptionDays, expectedRevision, previous } = {}) {
  const memories = Array.isArray(records) ? records.filter((memory) =>
    memory && typeof memory === "object" && typeof memory.id === "string" && typeof memory.content === "string",
  ) : [];
  const issues = [];
  if (memories.length !== 1) issues.push("expected_one_current_memory");

  const target = previous && memories.find((memory) => memory.id === previous.id) || memories[0];
  if (previous && (!target || target.id !== previous.id)) issues.push("original_memory_id_changed");
  if (previous && memories.some((memory) => memory.id !== previous.id)) issues.push("unexpected_additional_memory");
  if (target && Number.isSafeInteger(expectedRevision) && target.revision !== expectedRevision) {
    issues.push("expected_revision_mismatch");
  }
  if (target && previous && target.revision !== previous.revision + 1) {
    issues.push("original_revision_not_incremented");
  }

  const signals = policySignals(target?.content, { project, defaultDays, exceptionDays });
  if (!signals.namesProject) issues.push("project_scope_missing");
  if (!signals.matchesDefault) issues.push("default_policy_mismatch");
  if (!signals.preservesIncidentException) issues.push("incident_exception_missing");

  return { accepted: issues.length === 0, issues, signals, ...(target ? { target: { id: target.id, revision: target.revision } } : {}) };
}

export function assessRecall(answer, results, { memory, ...policy }) {
  const signals = policySignals(answer, policy);
  const evidenceFound = results.some((result) => {
    const body = result?.details;
    if (result?.isError || body?.status !== "ok" || !memory) return false;
    const candidates = result.name === "memory_search" ? body.candidates ?? []
      : result.name === "memory_read" && body.hasMore === false
        ? [{ id: body.id, revision: body.revision, preview: { text: body.content, omittedBefore: false, omittedAfter: false } }]
        : [];
    return candidates.some((candidate) => {
      if (candidate.id !== memory.id || candidate.revision !== memory.revision ||
          candidate.preview?.omittedBefore !== false || candidate.preview?.omittedAfter !== false) return false;
      const evidence = policySignals(candidate.preview.text, policy);
      return evidence.namesProject && evidence.matchesDefault && evidence.preservesIncidentException;
    });
  });
  return { accepted: signals.matchesDefault && signals.preservesIncidentException && evidenceFound, signals, evidenceFound };
}

function sanitizeString(value) {
  return value
    .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+=*/giu, "Bearer [redacted]")
    .replace(/\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|AIza[A-Za-z0-9_-]{8,})\b/gu, "[redacted]")
    .replace(/https?:\/\/[^\s"'<>]+/giu, "[url]")
    .replace(localPath, "[local-path]")
    .slice(0, 16_000);
}

export function sanitizeEvaluationTrace(value, depth = 0, seen = new WeakSet()) {
  if (typeof value === "string") return sanitizeString(value);
  if (value === null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value !== "object") return undefined;
  if (depth >= 16) return "[depth-limit]";
  if (seen.has(value)) return "[circular]";
  seen.add(value);
  if (Array.isArray(value)) {
    const output = value.map((item) => sanitizeEvaluationTrace(item, depth + 1, seen));
    seen.delete(value);
    return output;
  }

  const output = {};
  for (const [key, item] of Object.entries(value)) {
    if (forbiddenKey.test(key)) continue;
    const sanitized = sanitizeEvaluationTrace(item, depth + 1, seen);
    if (sanitized !== undefined) output[sanitizeString(key)] = sanitized;
  }
  seen.delete(value);
  return output;
}
