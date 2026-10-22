import assert from "node:assert/strict";
import test from "node:test";
import { assessTaskMemories, assessTaskRecall } from "./task-cancellation-eval-support.mjs";

const project = "River";
const initialContent = "River's migration proposal must be prepared. River's weekly log review remains required.";
const cancelledContent = "River's migration proposal has been canceled and is no longer needed. River's weekly log review remains required.";
const weeklyContent = "River's weekly log review remains required.";

function memory(id, revision, content) {
  return { id, revision, content };
}

function evaluate(records, options = {}) {
  return assessTaskMemories(records, { project, cancelled: true, ...options });
}

function searchEvidence(records) {
  return records.map(({ id, revision, content }) => ({
    name: "memory_search",
    details: {
      status: "ok",
      candidates: [{ id, revision, preview: { text: content, omittedBefore: false, omittedAfter: false } }],
    },
  }));
}

test("initial River tasks require both a pending migration proposal and continuing weekly review", () => {
  const result = assessTaskMemories([memory("river-work", 1, initialContent)], { project, cancelled: false });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.deepEqual(result.issues, []);
  assert.deepEqual(result.targets, [{ id: "river-work", revision: 1 }]);
  assert.equal(result.signals.migrationPending, true);
  assert.equal(result.signals.weeklyReviewRequired, true);
});

test("final cancellation updates the migration task while the same-clause weekly review remains required", () => {
  const before = [memory("river-work", 1, initialContent)];
  const after = [memory("river-work", 2, "For River, the migration proposal was canceled, while River's weekly log review remains required.")];
  const result = evaluate(after, { previous: before });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.deepEqual(result.issues, []);
  assert.equal(result.signals.migrationCancelled, true);
  assert.equal(result.signals.weeklyReviewRequired, true);
  assert.deepEqual(result.targets, [{ id: "river-work", revision: 2 }]);
});

test("Chinese memories can express the pending tasks and their later ordinary cancellation", () => {
  const initial = assessTaskMemories(
    [memory("river-zh", 1, "River 的迁移提案仍需准备；River 每周日志复核仍需继续。")],
    { project, cancelled: false },
  );
  assert.equal(initial.accepted, true, JSON.stringify(initial));

  const cancelled = evaluate(
    [memory("river-zh", 2, "River 的迁移提案已取消，不再需要；River 每周日志复核仍需继续。")],
    { previous: [memory("river-zh", 1, "River 的迁移提案仍需准备；River 每周日志复核仍需继续。")] },
  );
  assert.equal(cancelled.accepted, true, JSON.stringify(cancelled));

  const brook = assessTaskMemories(
    [memory("brook-zh", 1, "Brook 的迁移提案已取消，不再需要；每周日志复核仍需继续。")],
    { project: "Brook", cancelled: true },
  );
  assert.equal(brook.accepted, true, JSON.stringify(brook));
});

test("ordinary English task paraphrases and a For-project bullet heading stay in scope", () => {
  const cases = [
    "For the fictional River project, submitting the migration proposal and carrying out the weekly log review are both agreed commitments; submitting the proposal is required.",
    "River requires submitting the migration proposal. The weekly log review is also an agreed commitment.",
    "For the fictional River project, submitting the migration proposal and completing the weekly log review are current commitments.",
    "For the fictional River project, the current commitments are to submit the migration proposal and complete the weekly log review.",
    "River needs the migration proposal submitted and the weekly log review completed.",
    "For the fictional River project, current commitments are to submit the migration proposal and complete the required weekly log review.",
    "For River:\n- Submit a migration proposal.\n- Perform weekly log reviews.",
    "For River:\n- The migration proposal must be submitted.\n- Review the logs weekly.",
    "River:\n- Submit the migration proposal.\n- Continue weekly log reviews.",
  ];
  for (const content of cases) {
    const result = assessTaskMemories([memory("river-paraphrase", 1, content)], { project, cancelled: false });
    assert.equal(result.accepted, true, JSON.stringify({ content, issues: result.issues, signals: result.signals }));
  }
});

test("the proposal may be physically removed while the weekly task remains", () => {
  const before = [
    memory("river-proposal", 1, "River's migration proposal is pending and must be prepared."),
    memory("river-weekly", 1, weeklyContent),
  ];
  const after = [memory("river-weekly", 1, weeklyContent)];
  const result = evaluate(after, { previous: before });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.deepEqual(result.targets, [{ id: "river-weekly", revision: 1 }]);
  assert.equal(result.signals.migrationMentioned, false);
});

test("a valid delete-and-create representation does not require one fixed ID layout", () => {
  const before = [
    memory("river-old-proposal", 1, "River's migration proposal is pending and must be prepared."),
    memory("river-weekly", 1, weeklyContent),
  ];
  const after = [
    memory("river-new-status", 1, "River's migration proposal has been canceled and is no longer needed."),
    memory("river-weekly", 1, weeklyContent),
  ];
  const result = evaluate(after, { previous: before });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.deepEqual(result.targets, [
    { id: "river-new-status", revision: 1 },
    { id: "river-weekly", revision: 1 },
  ]);
});

test("an old pending proposal left beside a cancellation remains a conflict", () => {
  const records = [
    memory("river-old-proposal", 1, "River's migration proposal is still required and pending."),
    memory("river-cancelled-proposal", 1, "River's migration proposal was canceled and is no longer needed."),
    memory("river-weekly", 1, weeklyContent),
  ];
  const result = evaluate(records);

  assert.equal(result.accepted, false);
  assert.ok(result.issues.includes("migration_still_pending"), JSON.stringify(result.issues));
});

test("a cancellation row cannot hide a separate unclassified migration status", () => {
  for (const content of ["River migration proposal is due tomorrow.", "River migration proposal."]) {
    const result = evaluate([
      memory("river-cancelled", 1, cancelledContent),
      memory("river-unknown", 1, content),
    ]);
    assert.equal(result.accepted, false, JSON.stringify(result));
    assert.ok(result.issues.includes("migration_status_unknown"));
  }
});

test("conflicting required and canceled statuses in one current proposal remain unresolved", () => {
  const result = evaluate([
    memory("river-conflict", 1, "River's migration proposal is still required but has been canceled."),
    memory("river-weekly", 1, weeklyContent),
  ]);

  assert.equal(result.accepted, false);
  assert.ok(result.issues.includes("migration_still_pending"));
});

test("cancellation does not pass when the weekly review was lost or canceled", () => {
  for (const content of [
    "River's migration proposal was canceled and is no longer needed.",
    "River's migration proposal was canceled. River's weekly log review is no longer required.",
    "River's migration proposal was canceled. River's weekly log review might stop.",
  ]) {
    const result = evaluate([memory("river-work", 1, content)]);
    assert.equal(result.accepted, false, JSON.stringify({ content, issues: result.issues, signals: result.signals }));
    assert.ok(result.issues.includes("weekly_review_missing"));
  }
});

test("uncertainty that weekly work may stop fails even when another task is canceled", () => {
  const result = evaluate([
    memory("river-work", 1, "River's migration proposal was canceled. River's weekly log review continues for now but may stop."),
  ]);

  assert.equal(result.accepted, false);
  assert.ok(result.issues.includes("weekly_review_uncertain"), JSON.stringify(result.issues));
});

test("possible cancellation is unresolved and cannot be treated as final", () => {
  const uncertain = "River's migration proposal may be canceled. River's weekly log review remains required.";
  const result = evaluate([memory("river-work", 1, uncertain)]);

  assert.equal(result.accepted, false, JSON.stringify(result));
  assert.ok(result.issues.includes("migration_cancellation_uncertain"), JSON.stringify(result.issues));
  assert.equal(result.signals.migrationCancelled, false);

  const negated = evaluate([memory("river-work", 1, "River's migration proposal has not been canceled. River's weekly log review remains required.")]);
  assert.equal(negated.accepted, false, JSON.stringify(negated));
  assert.equal(negated.signals.migrationCancelled, false);
});

test("migration or proposal wording still identifies the cancellation task", () => {
  const cases = [
    {
      content: "River migration may be canceled. River's weekly log review remains required.",
      signal: "migrationCancellationUncertain",
    },
    {
      content: "River proposal is still required. River's weekly log review remains required.",
      signal: "migrationPending",
    },
  ];

  for (const { content, signal } of cases) {
    const result = evaluate([memory("river-split-noun", 1, content)]);
    assert.equal(result.accepted, false, JSON.stringify({ content, issues: result.issues, signals: result.signals }));
    assert.equal(result.signals.migrationMentioned, true);
    assert.equal(result.signals[signal], true);
  }
});

test("weekly continuation uncertainty and conflicting rows cannot pass as required work", () => {
  const uncertain = evaluate([memory(
    "river-weekly-uncertain",
    1,
    "River's migration proposal was canceled. River's weekly log review might continue.",
  )]);
  assert.equal(uncertain.accepted, false, JSON.stringify(uncertain));
  assert.ok(uncertain.issues.includes("weekly_review_uncertain"));

  const conflicting = evaluate([
    memory("river-weekly-required", 1, "River's weekly log review is required."),
    memory("river-weekly-canceled", 1, "River's weekly log review is no longer required."),
    memory("river-migration-canceled", 1, "River's migration proposal was canceled and is no longer needed."),
  ]);
  assert.equal(conflicting.accepted, false, JSON.stringify(conflicting));
  assert.ok(conflicting.issues.includes("weekly_review_cancelled"));
});

test("an explicit proposal cancellation in one clause does not cancel weekly work in that sentence", () => {
  const result = evaluate([
    memory("river-work", 1, "For River, the migration proposal is canceled while River's weekly log review must continue."),
  ]);

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.equal(result.signals.migrationCancelled, true);
  assert.equal(result.signals.weeklyReviewRequired, true);
});

test("task status from Brook cannot satisfy or conflict with River's tasks", () => {
  const records = [
    memory("river-cancelled", 1, "River's migration proposal is canceled and no longer needed."),
    memory("river-weekly", 1, weeklyContent),
    memory("brook-pending", 1, "Brook's migration proposal is still pending and Brook's weekly log review remains required."),
    memory("brook-heading", 1, "For Brook:\n- The migration proposal is pending.\n- Weekly log review is required."),
  ];
  const result = evaluate(records);

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.equal(result.signals.migrationPending, false);
  assert.equal(result.signals.weeklyReviewRequired, true);
});

test("unknown task rows need attribution, while a River heading scopes its bullets", () => {
  const unscoped = evaluate([memory(
    "unscoped-tasks",
    1,
    "The migration proposal is canceled and no longer needed. The weekly log review remains required.",
  )]);
  assert.equal(unscoped.accepted, false);
  assert.ok(unscoped.issues.includes("unscoped_task_memory"));

  const headed = assessTaskMemories(
    [memory("river-headed", 1, "River:\n- migration proposal is required and must be submitted.\n- weekly log review must continue.")],
    { project, cancelled: false },
  );
  assert.equal(headed.accepted, true, JSON.stringify(headed));
});

test("memory IDs must be nonempty and revisions must be positive safe integers", () => {
  const result = evaluate([
    memory("", 1, cancelledContent),
    memory("zero-revision", 0, weeklyContent),
    memory("fractional-revision", 1.5, weeklyContent),
  ]);

  assert.equal(result.accepted, false);
  assert.ok(result.issues.includes("invalid_memory_id"));
  assert.ok(result.issues.includes("invalid_memory_revision"));
});

test("pending consolidation work prevents acceptance", () => {
  const result = evaluate([memory("river-work", 1, cancelledContent)], { pendingJobs: 1 });

  assert.equal(result.accepted, false);
  assert.ok(result.issues.includes("pending_jobs"));
});

test("surviving unchanged content keeps its revision and changed content increments exactly once", () => {
  const before = [memory("river-work", 4, initialContent)];
  const unchanged = assessTaskMemories([memory("river-work", 4, initialContent)], {
    project,
    cancelled: false,
    previous: before,
  });
  assert.equal(unchanged.accepted, true, JSON.stringify(unchanged));

  const changedWithoutRevision = evaluate([memory("river-work", 4, cancelledContent)], { previous: before });
  assert.equal(changedWithoutRevision.accepted, false);
  assert.ok(changedWithoutRevision.issues.includes("changed_revision_not_incremented"));

  const changedByTwo = evaluate([memory("river-work", 6, cancelledContent)], { previous: before });
  assert.equal(changedByTwo.accepted, false);
  assert.ok(changedByTwo.issues.includes("changed_revision_not_incremented"));

  const changedOnce = evaluate([memory("river-work", 5, cancelledContent)], { previous: before });
  assert.equal(changedOnce.accepted, true, JSON.stringify(changedOnce));
});

test("unchanged close and compact snapshots compare exact state independent of row ordering", () => {
  const previous = [
    memory("river-proposal", 1, initialContent),
    memory("river-weekly", 3, weeklyContent),
  ];
  const sameStateReordered = [previous[1], previous[0]];
  const accepted = assessTaskMemories(sameStateReordered, {
    project,
    cancelled: false,
    previous,
    unchanged: true,
  });
  assert.equal(accepted.accepted, true, JSON.stringify(accepted));

  const changedState = assessTaskMemories(
    [memory("river-proposal", 2, cancelledContent), previous[1]],
    { project, cancelled: false, previous, unchanged: true },
  );
  assert.equal(changedState.accepted, false);
  assert.ok(changedState.issues.includes("unchanged_state_changed"));
});

test("recall needs the expected current tasks and complete exact evidence for every relevant row", () => {
  const records = [
    memory("river-status", 2, "River's migration proposal has been canceled and is no longer needed."),
    memory("river-weekly", 1, weeklyContent),
  ];
  const answer = "The migration has been canceled, so there is no need to submit the proposal. The weekly log review must continue.";
  const result = assessTaskRecall(answer, searchEvidence(records), { records, project, cancelled: true });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.equal(result.evidenceFound, true);

  const onlyOneRow = assessTaskRecall(answer, searchEvidence(records.slice(0, 1)), { records, project, cancelled: true });
  assert.equal(onlyOneRow.accepted, false);
  assert.equal(onlyOneRow.evidenceFound, false);
});

test("recall may use a complete exact single-memory read", () => {
  const records = [memory("river-work", 2, cancelledContent)];
  const answer = "River's migration proposal is canceled. River's weekly log review remains required.";
  const read = {
    name: "memory_read",
    details: { status: "ok", id: "river-work", revision: 2, content: cancelledContent, hasMore: false },
  };

  const result = assessTaskRecall(answer, [read], { records, project, cancelled: true });
  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.equal(result.evidenceFound, true);
});

test("recall rejects missing, stale, wrong-ID, partial, failed, or non-exact evidence", () => {
  const records = [memory("river-work", 2, cancelledContent)];
  const answer = "River's migration proposal is canceled. River's weekly log review remains required.";
  const valid = searchEvidence(records)[0];
  const candidate = valid.details.candidates[0];
  const invalidResults = [
    [],
    [{ name: "memory_search", details: { status: "ok", candidates: [] } }],
    [{ name: "memory_search", details: { status: "unavailable", candidates: [candidate] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, revision: 1 }] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, id: "wrong-id" }] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, preview: { ...candidate.preview, text: "partial" } }] } }],
    [{ name: "memory_search", details: { status: "ok", candidates: [{ ...candidate, preview: { ...candidate.preview, omittedAfter: true } }] } }],
    [{ name: "memory_read", details: { status: "ok", id: "river-work", revision: 2, content: cancelledContent.slice(0, -1), hasMore: false } }],
    [{ name: "memory_read", details: { status: "ok", id: "river-work", revision: 2, content: cancelledContent, hasMore: true } }],
    [{ name: "memory_read", isError: true, details: { status: "ok", id: "river-work", revision: 2, content: cancelledContent, hasMore: false } }],
  ];

  for (const results of invalidResults) {
    const result = assessTaskRecall(answer, results, { records, project, cancelled: true });
    assert.equal(result.accepted, false, JSON.stringify({ results, assessment: result }));
    assert.equal(result.evidenceFound, false);
  }
});

test("recall rejects uncertain or pending cancellation claims even when storage evidence is complete", () => {
  const records = [memory("river-work", 2, cancelledContent)];
  const evidence = searchEvidence(records);
  for (const answer of [
    "River's migration proposal may be canceled; River's weekly log review remains required.",
    "River's migration proposal is pending and still required; River's weekly log review remains required.",
  ]) {
    const result = assessTaskRecall(answer, evidence, { records, project, cancelled: true });
    assert.equal(result.accepted, false, JSON.stringify(result));
  }
});

test("initial recall must state both original tasks and cite their current memory", () => {
  const records = [memory("river-work", 1, initialContent)];
  const briefAnswer = "We need to submit the migration proposal. The weekly log review is required.";
  const good = assessTaskRecall(briefAnswer, searchEvidence(records), { records, project, cancelled: false });
  assert.equal(good.accepted, true, JSON.stringify(good));

  const missingProposal = assessTaskRecall(weeklyContent, searchEvidence(records), { records, project, cancelled: false });
  assert.equal(missingProposal.accepted, false);
  assert.equal(missingProposal.evidenceFound, true);
});

test("recall accepts omission of a physically removed proposal when weekly work is evidenced", () => {
  const records = [memory("river-weekly", 1, weeklyContent)];
  const result = assessTaskRecall(weeklyContent, searchEvidence(records), { records, project, cancelled: true });

  assert.equal(result.accepted, true, JSON.stringify(result));
  assert.equal(result.evidenceFound, true);
});
