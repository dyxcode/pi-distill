const migrationWords = /\b(?:migration|migrat(?:e|ed|es|ing))\b|迁移/giu;
const proposalWords = /\bproposal\b|提案|方案/giu;
const weeklyTask = /\bweekly\s+(?:log|logs|logging)\s+reviews?\b|\bweekly\s+reviews?\s+of\s+(?:the\s+)?logs?\b|\b(?:review|check)\s+(?:the\s+)?logs?\s+(?:weekly|every week|each week)\b|每周.{0,20}(?:日志|log).{0,20}(?:复核|审查|检查|review)|(?:日志|log).{0,20}(?:每周|weekly).{0,20}(?:复核|审查|检查|review)/giu;
const cancelWords = /\bcancel(?:led|ed|s|ing)?\b|\bwithdrawn\b|\babandoned\b|\bscrapped\b|\bdropped\b|\bno longer needed\b|\bnot needed (?:any more|anymore)\b|\bno need to (?:submit|prepare|write|draft|create)\s+(?:the\s+)?(?:migration\s+)?proposal\b|已取消|取消|撤回|废弃|不再需要|不需要了/giu;
const uncertainWords = /\b(?:may|might|could|possibly|perhaps|maybe|potentially|considering|uncertain|unclear|unconfirmed|not confirmed|not yet confirmed)\b|可能|也许|或许|考虑|不确定|尚未确认|还没确认|未确认|是否/giu;
const negatedCancel = /\b(?:not|never|isn't|wasn't|hasn't|is not|was not|has not)\s+(?:been\s+)?(?:cancel(?:led|ed)?|withdrawn|abandoned|scrapped|dropped)\b|\bnot\s+(?:yet\s+)?confirmed\b|未取消|尚未取消|没有取消|没有被取消/iu;
const pendingWords = /\bneeds?\s+(?:the\s+)?(?:migration\s+)?proposal\s+(?:submitted|prepared|written|drafted|completed)\b|\b(?:pending|outstanding|open)\s+(?:migration\s+)?proposal\b|\b(?:is|remains?|stays?)\s+(?:still\s+)?(?:pending|required|necessary|needed|open|outstanding)\b|\b(?:is\s+)?still\s+(?:pending|required|necessary|needed|open|outstanding)\b|\b(?:is\s+)?(?:also\s+)?required\b|\b(?:must|needs? to|has to|should|still needs? to)\s+(?:be\s+)?(?:prepare|write|submit|draft|create|develop|complete|finish)\w*\b|\b(?:must|should|needs? to)\s+be\s+(?:prepared|written|submitted|drafted|created|completed)\b|\b(?:prepare|write|submit|draft|create|develop|complete|finish)\w*\s+(?:(?:the|a|an)\s+)?(?:migration\s+)?proposal\b|\bneed to submit\s+(?:(?:the|a|an)\s+)?migration\s+proposal\b|(?:仍需|尚需|待(?:准备|提交|处理)|必须(?:准备|提出|提交|撰写)|需要(?:准备|提出|提交)|仍然?需要(?:准备|提出|提交)?)/giu;
const weeklyRequiredWords = /\brequired(?=\s+weekly)\b|\b(?:continue|continu(?:es|ing)|ongoing|still required|remains?\s+(?:required|a required task|on the schedule)|is required|must continue|should continue|needs? to continue|will continue|must be (?:reviewed|checked)|is also required|perform\w*|complet(?:e|ing)|carry(?:ing)? out|is\s+(?:also\s+)?an?\s+agreed commitment|conduct\w*)\b|(?:仍需|仍然?需要|依然?需要|仍然?是(?:一项)?必需|依旧(?:继续|需要)|继续|照常|必须继续)/giu;
const weeklyAction = /\b(?:review|check)\s+(?:the\s+)?logs?\s+(?:weekly|every week|each week)\b/giu;
const weeklyStopWords = /\bstop\w*|\bend\w*|\bpause\w*|\bcancel(?:led|ed|s|ing)?\b|\bwithdrawn\b|\bno longer required\b|\bnot required\b|\bno longer needed\b|不再|不需要|无需|停止|暂停|取消/giu;
const negativeStatusWords = /\b(?:no longer|no\s+(?:need|requirement)\b|not|never|isn't|doesn't|won't|cannot|can't|stopped|ended|cancel(?:led|ed)?)\b|不再|不需要|无需|不继续|没有|并未|不能|取消|停止/iu;
const statusStatements = /(?:[.!?;。！？；\n]+|,\s*(?:but|however|while|yet)\s+|，\s*(?:但是|但|不过|然而)\s*)/iu;

function escaped(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function expressionMatches(value, expression) {
  if (typeof value !== "string") return [];
  const flags = expression.flags.includes("g") ? expression.flags : `${expression.flags}g`;
  const matcher = new RegExp(expression.source, flags);
  return [...value.matchAll(matcher)].map((match) => ({ index: match.index ?? 0, text: match[0] }));
}

function containsNear(left, right, distance) {
  return left.some((a) => right.some((b) => Math.abs(a.index - b.index) <= distance));
}

function projectPattern(project) {
  return new RegExp(`(?<![\\p{L}\\p{N}])${escaped(project)}(?![\\p{L}\\p{N}])`, "iu");
}

function migrationTaskMatches(statement) {
  const migrations = expressionMatches(statement, migrationWords);
  const proposals = expressionMatches(statement, proposalWords);
  return migrations.length > 0 || proposals.length > 0;
}

function labelsAnotherProject(text, project) {
  const projectKey = typeof project === "string" ? project.toLocaleLowerCase() : "";
  const labels = [
    ...[...text.matchAll(/\b([\p{Lu}][\p{L}\p{N}-]{1,40})(?:'s|’s)\b/gu)].map((match) => match[1]),
    ...[...text.matchAll(/\b(?:for|project)\s+([\p{Lu}][\p{L}\p{N}-]{1,40})\b/gu)].map((match) => match[1]),
  ];
  return projectKey !== "" && labels.some((label) => label.toLocaleLowerCase() !== projectKey);
}

function projectScopedStatements(content, project, projectFallback) {
  if (typeof content !== "string") return [];
  const projectCue = typeof project === "string" && project.length > 0 ? projectPattern(project) : null;
  let activeProjectHeading = false;
  let activeForeignProjectHeading = false;
  const result = [];

  for (const line of content.split(/\r?\n/u)) {
    const trimmed = line.trim().replace(/^(?:[-*•]\s+|#{1,6}\s*)/u, "");
    if (trimmed.length === 0) {
      activeProjectHeading = false;
      activeForeignProjectHeading = false;
      continue;
    }
    const heading = trimmed.match(/^(?:For\s+)?([\p{Lu}][\p{L}\p{N}-]{1,40})\s*[:：]?\s*$/u);
    if (heading) {
      activeProjectHeading = typeof project === "string" && heading[1].toLocaleLowerCase() === project.toLocaleLowerCase();
      activeForeignProjectHeading = typeof project === "string" && !activeProjectHeading;
      continue;
    }

    for (const rawStatement of line.split(statusStatements)) {
      const text = rawStatement.trim().replace(/^(?:[-*•]\s+|\d+[.)]\s*)/u, "");
      if (!text) continue;
      const explicitForeignProject = activeForeignProjectHeading || labelsAnotherProject(text, project);
      const scoped = !explicitForeignProject &&
        (projectFallback || activeProjectHeading || (projectCue !== null && projectCue.test(text)));
      result.push({ text, scoped, explicitForeignProject });
    }
  }
  return result;
}

function hasNearbyNegation(text, statusIndex, statusLength) {
  const start = Math.max(0, statusIndex - 26);
  const end = Math.min(text.length, statusIndex + statusLength + 14);
  return negativeStatusWords.test(text.slice(start, end));
}

function inspectTaskText(content, project, projectFallback = false) {
  const signals = {
    projectAttributed: false,
    unscopedTaskMentioned: false,
    migrationMentioned: false,
    migrationPending: false,
    migrationCancelled: false,
    migrationCancellationUncertain: false,
    migrationStatusUnknown: false,
    weeklyReviewMentioned: false,
    weeklyReviewRequired: false,
    weeklyReviewCancelled: false,
    weeklyReviewUncertain: false,
  };

  for (const { text, scoped, explicitForeignProject } of projectScopedStatements(content, project, projectFallback)) {
    const migrationTask = migrationTaskMatches(text);
    const weekly = expressionMatches(text, weeklyTask);
    if (!migrationTask && weekly.length === 0) continue;
    if (!scoped) {
      if (!explicitForeignProject) signals.unscopedTaskMentioned = true;
      continue;
    }
    signals.projectAttributed = true;

    if (migrationTask) {
      signals.migrationMentioned = true;
      const migrationMatches = expressionMatches(text, migrationWords);
      const proposalMatches = expressionMatches(text, proposalWords);
      const cancelMatches = expressionMatches(text, cancelWords).filter((match) =>
        containsNear(migrationMatches, [match], 40) || containsNear(proposalMatches, [match], 40),
      );
      const uncertaintyMatches = expressionMatches(text, uncertainWords);
      const cancellationUncertain = cancelMatches.some((cancel) =>
        containsNear([cancel], uncertaintyMatches, 30) || negatedCancel.test(text.slice(Math.max(0, cancel.index - 32), cancel.index + cancel.text.length + 24)),
      );
      const cancellationFinal = cancelMatches.some((cancel) =>
        !containsNear([cancel], uncertaintyMatches, 30) &&
        !negatedCancel.test(text.slice(Math.max(0, cancel.index - 32), cancel.index + cancel.text.length + 24)),
      );
      const pendingMatches = expressionMatches(text, pendingWords).filter((match) => containsNear(proposalMatches, [match], 40));
      const pending = pendingMatches.some((match) => !hasNearbyNegation(text, match.index, match.text.length));

      signals.migrationStatusUnknown ||= !pending && !cancellationFinal && !cancellationUncertain;
      signals.migrationCancellationUncertain ||= cancellationUncertain;
      signals.migrationCancelled ||= cancellationFinal;
      // Preserve pending status alongside cancellation so contradictory wording fails closed.
      signals.migrationPending ||= pending;
    }

    if (weekly.length > 0) {
      signals.weeklyReviewMentioned = true;
      const requiredMatches = expressionMatches(text, weeklyRequiredWords)
        .filter((match) => containsNear(weekly, [match], 120));
      const directReviewMatches = expressionMatches(text, weeklyAction);
      const neededReviewMatches = expressionMatches(text, /\bneeds?\b.{0,100}\bweekly\s+logs?\s+review\s+(?:completed|performed|conducted)\b/giu);
      const required = requiredMatches.some((match) => !hasNearbyNegation(text, match.index, match.text.length)) ||
        [...directReviewMatches, ...neededReviewMatches].some((match) => !hasNearbyNegation(text, match.index, match.text.length));
      signals.weeklyReviewRequired ||= required;
      const stopMatches = [...expressionMatches(text, weeklyStopWords), ...expressionMatches(text, negativeStatusWords)];
      const localStops = stopMatches.filter((stop) => containsNear(weekly, [stop], 36) &&
        (stop.index >= weekly[0].index || !migrationTask));
      const uncertaintyMatches = expressionMatches(text, uncertainWords).filter((match) =>
        containsNear(weekly, [match], 80) && (match.index >= weekly[0].index || !migrationTask),
      );
      signals.weeklyReviewCancelled ||= localStops.length > 0;
      signals.weeklyReviewUncertain ||= uncertaintyMatches.length > 0;
    }
  }

  return signals;
}

function mergeSignals(target, incoming) {
  for (const [key, value] of Object.entries(incoming)) target[key] ||= value;
}

function addIssue(issues, issue) {
  if (!issues.includes(issue)) issues.push(issue);
}

function validSnapshot(records, issues, prefix) {
  if (!Array.isArray(records)) {
    addIssue(issues, `${prefix}_invalid`);
    return [];
  }
  const result = [];
  const seen = new Set();
  for (const record of records) {
    if (!record || typeof record !== "object" || Array.isArray(record)) {
      addIssue(issues, `${prefix}_invalid_record`);
      continue;
    }
    const idValid = typeof record.id === "string" && record.id.length > 0 && record.id.trim() === record.id;
    const revisionValid = Number.isSafeInteger(record.revision) && record.revision > 0;
    const contentValid = typeof record.content === "string";
    if (!idValid) addIssue(issues, prefix === "current" ? "invalid_memory_id" : "invalid_previous_id");
    if (!revisionValid) addIssue(issues, prefix === "current" ? "invalid_memory_revision" : "invalid_previous_revision");
    if (!contentValid) addIssue(issues, prefix === "current" ? "invalid_memory_content" : "invalid_previous_content");
    if (typeof record.project !== "undefined" && (typeof record.project !== "string" || record.project.trim() === "")) {
      addIssue(issues, prefix === "current" ? "invalid_project_attribution" : "invalid_previous_project_attribution");
    }
    if (!idValid || !revisionValid || !contentValid) continue;
    if (seen.has(record.id)) addIssue(issues, prefix === "current" ? "duplicate_memory_id" : "duplicate_previous_id");
    seen.add(record.id);
    result.push(record);
  }
  return result;
}

function comparePrevious(current, previous, issues, unchanged) {
  const oldById = new Map(previous.map((record) => [record.id, record]));
  const currentById = new Map(current.map((record) => [record.id, record]));
  for (const record of current) {
    const old = oldById.get(record.id);
    if (!old) continue;
    if (record.content === old.content && record.revision !== old.revision) {
      addIssue(issues, "unchanged_revision_changed");
    } else if (record.content !== old.content && record.revision !== old.revision + 1) {
      addIssue(issues, "changed_revision_not_incremented");
    }
  }
  if (unchanged) {
    const same = previous.length === current.length && previous.every((old) => {
      const record = currentById.get(old.id);
      return record !== undefined && record.revision === old.revision && record.content === old.content;
    });
    if (!same) addIssue(issues, "unchanged_state_changed");
  }
}

function taskSignalsForRecords(records, project) {
  const signals = {
    projectAttributed: false,
    unscopedTaskMentioned: false,
    migrationMentioned: false,
    migrationPending: false,
    migrationCancelled: false,
    migrationCancellationUncertain: false,
    migrationStatusUnknown: false,
    weeklyReviewMentioned: false,
    weeklyReviewRequired: false,
    weeklyReviewCancelled: false,
    weeklyReviewUncertain: false,
  };
  const relevant = [];
  for (const record of records) {
    if (typeof record.project === "string" && record.project !== project) continue;
    const explicitProjectInRecord = typeof record.content === "string" && typeof project === "string" &&
      projectPattern(project).test(record.content) && !labelsAnotherProject(record.content, project);
    const recordSignals = inspectTaskText(record.content, project, record.project === project || explicitProjectInRecord);
    mergeSignals(signals, recordSignals);
    if (recordSignals.projectAttributed) relevant.push(record);
  }
  return { signals, relevant };
}

function stateIssues(signals, cancelled, issues) {
  if (!signals.projectAttributed) addIssue(issues, "project_scope_missing");
  if (signals.unscopedTaskMentioned) addIssue(issues, "unscoped_task_memory");
  if (signals.weeklyReviewCancelled) addIssue(issues, "weekly_review_cancelled");
  if (signals.weeklyReviewUncertain) addIssue(issues, "weekly_review_uncertain");
  if (!signals.weeklyReviewMentioned || !signals.weeklyReviewRequired) addIssue(issues, "weekly_review_missing");
  if (cancelled) {
    if (signals.migrationPending) addIssue(issues, "migration_still_pending");
    if (signals.migrationCancellationUncertain) addIssue(issues, "migration_cancellation_uncertain");
    if (signals.migrationStatusUnknown) {
      addIssue(issues, "migration_status_unknown");
    }
  } else {
    if (!signals.migrationPending) addIssue(issues, "migration_pending_missing");
    if (signals.migrationCancelled) addIssue(issues, "migration_cancellation_unexpected");
  }
}

function orderedTargets(records) {
  return records
    .map(({ id, revision }) => ({ id, revision }))
    .sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0);
}

export function assessTaskMemories(records, {
  project,
  cancelled,
  pendingJobs = 0,
  previous,
  unchanged = false,
} = {}) {
  const issues = [];
  if (typeof project !== "string" || project.trim() === "") addIssue(issues, "invalid_project");
  if (typeof cancelled !== "boolean") addIssue(issues, "invalid_cancelled_status");
  if (!Number.isSafeInteger(pendingJobs) || pendingJobs < 0) addIssue(issues, "invalid_pending_jobs");
  else if (pendingJobs > 0) addIssue(issues, "pending_jobs");
  if (typeof unchanged !== "boolean") addIssue(issues, "invalid_unchanged_flag");

  const current = validSnapshot(records, issues, "current");
  let old = [];
  if (typeof previous !== "undefined") old = validSnapshot(previous, issues, "previous");
  else if (unchanged === true) addIssue(issues, "previous_snapshot_required");
  if (typeof previous !== "undefined") comparePrevious(current, old, issues, unchanged === true);

  const { signals } = taskSignalsForRecords(current, typeof project === "string" ? project : "");
  stateIssues(signals, cancelled === true, issues);

  return {
    accepted: issues.length === 0,
    issues,
    signals,
    targets: orderedTargets(current),
  };
}

function evidenceForRecord(record, results) {
  if (!Array.isArray(results)) return false;
  return results.some((result) => {
    if (!result || typeof result !== "object" || result.isError === true) return false;
    const body = result.details;
    if (!body || typeof body !== "object" || body.status !== "ok") return false;
    if (result.name === "memory_search") {
      if (!Array.isArray(body.candidates)) return false;
      return body.candidates.some((candidate) => candidate &&
        candidate.id === record.id && candidate.revision === record.revision &&
        candidate.preview?.omittedBefore === false && candidate.preview?.omittedAfter === false &&
        candidate.preview?.text === record.content);
    }
    if (result.name === "memory_read") {
      return body.id === record.id && body.revision === record.revision &&
        body.content === record.content && body.hasMore === false;
    }
    return false;
  });
}

export function assessTaskRecall(answer, results, { records, project, cancelled } = {}) {
  const issues = [];
  // The answer is evaluated in the known project context; stored task evidence still needs its own attribution.
  const answerSignals = inspectTaskText(typeof answer === "string" ? answer : "", project, true);
  if (typeof answer !== "string") addIssue(issues, "invalid_answer");

  const currentAssessment = assessTaskMemories(records, { project, cancelled });
  if (!currentAssessment.accepted) addIssue(issues, "current_memories_invalid");
  const current = validSnapshot(records, [], "current");
  const { relevant } = taskSignalsForRecords(current, typeof project === "string" ? project : "");
  const evidenceFound = relevant.length > 0 && relevant.every((record) => evidenceForRecord(record, results));
  if (!evidenceFound) addIssue(issues, "complete_current_task_evidence_missing");
  stateIssues(answerSignals, cancelled === true, issues);

  return {
    accepted: issues.length === 0,
    issues,
    signals: answerSignals,
    evidenceFound,
  };
}
