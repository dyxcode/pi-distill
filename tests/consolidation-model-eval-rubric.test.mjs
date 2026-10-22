import assert from "node:assert/strict";
import test from "node:test";
import { assessCurrentMemories, assessRecall, policySignals, sanitizeEvaluationTrace } from "./consolidation-model-eval-support.mjs";

test("policy rubric associates full day values with the default and incident exception", () => {
  const result = policySignals(
    "For Cedar-72, retain logs for 14 days by default; during an incident investigation, retain them for 30 days.",
    { defaultDays: 14, exceptionDays: 30 },
  );

  assert.deepEqual(result, {
    namesProject: true,
    matchesDefault: true,
    preservesIncidentException: true,
  });
});

test("a digit inside Cedar-72 cannot satisfy a seven-day rule", () => {
  const result = policySignals(
    "Cedar-72 logs follow the standard retention policy. Incident investigations keep logs for 30 days.",
    { defaultDays: 7, exceptionDays: 30 },
  );

  assert.equal(result.matchesDefault, false);
  assert.equal(result.preservesIncidentException, true);
});

test("held-out bilingual policies keep their own project, default, and exception", () => {
  for (const content of [
    "Maple 的日志默认保存 5 天；事故调查期间保留 21 天。",
    "Maple has a 5-day default. During an incident investigation, the exception is 21 days.",
    "The fictional Maple service retains logs for 5 days under normal conditions. During an incident investigation, retention remains 21 days. The 5-day normal duration replaces the previous 10-day decision.",
  ]) {
    const result = assessCurrentMemories([{ id: "maple-policy", revision: 2, content }], {
      project: "Maple", defaultDays: 5, exceptionDays: 21, expectedRevision: 2,
      previous: { id: "maple-policy", revision: 1 },
    });
    assert.equal(result.accepted, true, JSON.stringify(result));
  }
});

test("trace copies preserve shared evidence while redacting genuine cycles", () => {
  const shared = { content: "Synthetic policy" };
  const circular = {}; circular.self = circular;
  assert.deepEqual(sanitizeEvaluationTrace({ stage: shared, trace: [shared], circular }), {
    stage: shared, trace: [shared], circular: { self: "[circular]" },
  });
});

test("recall requires retrieved evidence at the expected current ID and revision", () => {
  const answer = "Cedar-72 defaults to 7 days. During an incident investigation, retain logs for 30 days.";
  const policy = { defaultDays: 7, exceptionDays: 30, memory: { id: "policy", revision: 2 } };
  const candidate = { id: "policy", revision: 2, preview: { text: answer, omittedBefore: false, omittedAfter: false } };
  const results = [{ name: "memory_search", details: { status: "ok", candidates: [candidate] } }];
  assert.equal(assessRecall(answer, results, policy).accepted, true);
  for (const failed of [
    [],
    [{ name: "memory_search", details: { status: "ok", candidates: [] } }],
    [{ name: "memory_search", details: { status: "unavailable" } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, revision: 1 }] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, id: "other" }] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, preview: { ...candidate.preview, omittedAfter: true } }] } }],
  ]) assert.equal(assessRecall(answer, failed, policy).accepted, false);
  const read = { name: "memory_read", details: { status: "ok", id: "policy", revision: 2, content: answer, hasMore: false } };
  assert.equal(assessRecall(answer, [read], policy).accepted, true);
});

test("bounded evaluation traces retain later events beyond the first hundred", () => {
  const trace = Array.from({ length: 150 }, (_, index) => ({ index }));
  assert.deepEqual(sanitizeEvaluationTrace(trace), trace);
});

test("rubric fails closed on swapped, contradictory, or negated day values", () => {
  const swapped = policySignals(
    "Cedar-72 default retention is 30 days, extended to 7 days during an incident investigation.",
    { defaultDays: 7, exceptionDays: 30 },
  );
  assert.equal(swapped.matchesDefault, false);
  assert.equal(swapped.preservesIncidentException, false);

  const contradictory = policySignals(
    "Cedar-72 default retention is 7 days; the standard period is 14 days. Incident investigations retain logs for 30 days.",
    { defaultDays: 7, exceptionDays: 30 },
  );
  assert.equal(contradictory.matchesDefault, false);

  const negated = policySignals(
    "Cedar-72 default retention is not 7 days. Incident investigations retain logs for **30 days**.",
    { defaultDays: 7, exceptionDays: 30 },
  );
  assert.equal(negated.matchesDefault, false);
  assert.equal(negated.preservesIncidentException, true);
});

test("current-memory rubric requires one updated record with the same ID and preserved exception", () => {
  const previous = { id: "synthetic-policy-id", revision: 1 };
  const accepted = assessCurrentMemories(
    [{ id: previous.id, revision: 2, content: "Cedar-72 standard log retention is 7 days. Incident investigations retain logs for 30 days." }],
    { defaultDays: 7, exceptionDays: 30, expectedRevision: 2, previous },
  );
  assert.equal(accepted.accepted, true);
  assert.deepEqual(accepted.issues, []);

  const duplicatedOldAndNew = assessCurrentMemories(
    [
      { id: previous.id, revision: 1, content: "Cedar-72 logs are kept for 14 days by default; incident investigations keep them for 30 days." },
      { id: "synthetic-new-policy-id", revision: 1, content: "Cedar-72 logs are kept for 7 days by default; incident investigations keep them for 30 days." },
    ],
    { defaultDays: 7, exceptionDays: 30, expectedRevision: 2, previous },
  );
  assert.equal(duplicatedOldAndNew.accepted, false);
  assert.ok(duplicatedOldAndNew.issues.includes("expected_one_current_memory"));
  assert.ok(duplicatedOldAndNew.issues.includes("original_revision_not_incremented"));
  assert.ok(duplicatedOldAndNew.issues.includes("unexpected_additional_memory"));
});

test("evaluation trace sanitizer removes credentials, paths, thinking, and usage fields", () => {
  const sanitized = sanitizeEvaluationTrace({
    request: { prompt: "Cedar-72 policy", path: "/Users/example/private/session.jsonl" },
    response: { thinking: "private reasoning", text: "Bearer secret-value Cedar keeps logs for 7 days." },
    usage: { input: 123, output: 45, totalTokens: 168 },
    credentials: { openai: "secret-value" },
  });

  assert.equal(JSON.stringify(sanitized).includes("secret-value"), false);
  assert.equal(JSON.stringify(sanitized).includes("private/session.jsonl"), false);
  assert.equal(JSON.stringify(sanitized).includes("private reasoning"), false);
  assert.equal(JSON.stringify(sanitized).includes("totalTokens"), false);
  assert.equal(sanitized.response.text.includes("7 days"), true);
});
