import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { test } from "node:test";
import { loadSnapshot, planSources, snapshotSession } from "../src/consolidation-source.ts";

const sourceLimit = 24 * 1024;
const contextLimit = 8 * 1024;

function messageEntry(id, parentId, role, text, timestamp = "2026-10-21T00:00:00.000Z") {
  return {
    type: "message",
    id,
    parentId,
    timestamp,
    message: { role, content: [{ type: "text", text }], timestamp: Date.parse(timestamp) },
  };
}

function header(id, cwd = "/synthetic/project", parentSession) {
  return {
    type: "session",
    version: 3,
    id,
    timestamp: "2026-10-21T00:00:00.000Z",
    cwd,
    ...(parentSession ? { parentSession } : {}),
  };
}

async function writeSession(t, rows, name = "session.jsonl") {
  const dir = await mkdtemp(join(tmpdir(), "pi-distill-source-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, name);
  await writeFile(path, rows.map((row) => JSON.stringify(row)).join("\n") + "\n", "utf8");
  return path;
}

function allEvidence(batches) {
  return batches.flatMap((batch) => batch.evidence);
}

test("snapshotSession selects the active parent chain and applies Pi context edits", () => {
  const manager = SessionManager.inMemory("/synthetic/project");
  const sharedId = manager.appendMessage({
    role: "user",
    content: [{ type: "text", text: "Original phrasing that must be replaced." }],
    timestamp: 1,
  });
  const siblingId = manager.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "Sibling branch must stay out." }],
    timestamp: 2,
  });
  manager.branch(sharedId);
  const activeId = manager.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "Active branch answer." }],
    timestamp: 3,
  });
  manager.appendContextEdit(sharedId, {
    content: [{ type: "text", text: "Corrected source phrasing." }],
  });

  const snapshot = snapshotSession(manager);
  const evidence = allEvidence(planSources(snapshot, new Set()));
  const sourceText = evidence.map((part) => part.text).join("\n");

  assert.equal(snapshot.leafId, manager.getLeafId());
  assert.equal(snapshot.sessionId, manager.getSessionId());
  assert.deepEqual(snapshot.entries.map((entry) => entry.id), manager.getBranch().map((entry) => entry.id));
  assert.ok(snapshot.entries.some((entry) => entry.id === activeId));
  assert.ok(!snapshot.entries.some((entry) => entry.id === siblingId));
  assert.ok(sourceText.includes("Corrected source phrasing."));
  assert.ok(!sourceText.includes("Original phrasing"));
  assert.ok(!sourceText.includes("Sibling branch"));
});

test("context edits retain stable entry identity while replacing source evidence", () => {
  const manager = SessionManager.inMemory("/synthetic/project", { id: "edited-lineage-session" });
  const originalId = manager.appendMessage({
    role: "user",
    content: [{ type: "text", text: "Original source phrasing." }],
    timestamp: 1,
  });
  const originalSnapshot = snapshotSession(manager);
  const originalKeys = planSources(originalSnapshot, new Set()).flatMap((batch) => batch.sourceKeys);

  manager.appendContextEdit(originalId, {
    content: [{ type: "text", text: "Corrected source phrasing." }],
  });
  const correctedSnapshot = snapshotSession(manager);
  const currentBatches = planSources(correctedSnapshot, new Set());
  const currentKeys = currentBatches.flatMap((batch) => batch.sourceKeys);
  const evidenceText = allEvidence(currentBatches).map((part) => part.text).join("\n");

  assert.ok(originalKeys.length > 0);
  assert.ok(currentKeys.length > 0);
  assert.ok(correctedSnapshot.entries.some(entry => entry.id === originalId));
  assert.ok(!currentKeys.some((key) => originalKeys.includes(key)));
  assert.ok(evidenceText.includes("Corrected source phrasing."));
  assert.ok(!evidenceText.includes("Original source phrasing."));
});

test("oversized live snapshots stay pending without exposing partial source batches", () => {
  const manager = SessionManager.inMemory("/synthetic/oversized", { id: "oversized-live-session" });
  manager.appendMessage({
    role: "user",
    content: [{ type: "text", text: "x".repeat(16 * 1024 * 1024) }],
    timestamp: 1,
  });

  const snapshot = snapshotSession(manager);
  assert.equal(snapshot.pending, true);
  assert.match(snapshot.pendingReason, /size|limit|bound/i);
  assert.deepEqual(planSources(snapshot, new Set()), []);
});

test("persisted sessions reject a single file that exceeds the aggregate source bound", async (t) => {
  const path = await writeSession(t, [
    header("oversized-file-session"),
    messageEntry("large", null, "user", "x".repeat(16 * 1024 * 1024)),
  ]);

  await assert.rejects(loadSnapshot(path), /size|limit|bound/i);
});

test("fork ancestry shares one byte budget across all source files", async (t) => {
  const parentPath = await writeSession(t, [
    header("large-parent-session"),
    messageEntry("shared-large", null, "user", "p".repeat(8 * 1024 * 1024 + 512)),
  ], "parent.jsonl");
  const childPath = await writeSession(t, [
    header("large-child-session", "/synthetic/project", parentPath),
    messageEntry("child-large", null, "assistant", "c".repeat(8 * 1024 * 1024 + 512)),
  ], "child.jsonl");

  await assert.rejects(loadSnapshot(childPath), /size|limit|bound/i);
});

test("persisted sessions reject ancestry graphs above the entry-count limit", async (t) => {
  const entries = [];
  let parentId = null;
  for (let index = 0; index < 20_001; index += 1) {
    const id = `metadata-${index}`;
    entries.push({
      type: "session_info",
      id,
      parentId,
      timestamp: "2026-10-21T00:00:00.000Z",
      name: "synthetic",
    });
    parentId = id;
  }
  const path = await writeSession(t, [header("entry-limit-session"), ...entries]);

  await assert.rejects(loadSnapshot(path), /entry limit/i);
});

test("many context edits retain a plannable current source without quadratic historical reconstruction", () => {
  const entries = [messageEntry("base", null, "user", "Original context.")];
  let parentId = "base";
  for (let index = 0; index < 10_000; index += 1) {
    const id = `edit-${index}`;
    entries.push({
      type: "context_edit",
      id,
      parentId,
      timestamp: "2026-10-21T00:00:00.000Z",
      targetId: "base",
      replacement: { content: [{ type: "text", text: "Corrected context." }] },
    });
    parentId = id;
  }
  const snapshot = {
    sessionId: "large-edit-lineage",
    sessionFile: null,
    cwd: "/synthetic/project",
    leafId: parentId,
    entries,
  };

  const batches = planSources(snapshot, new Set());
  assert.ok(batches.length > 0);
  assert.equal(allEvidence(batches).map(part => part.text).join(""), "Corrected context.");
});

test("Pi resetLeaf forests validate and expose only the active root branch", async (t) => {
  const manager = SessionManager.inMemory("/synthetic/forest", { id: "forest-session" });
  manager.appendMessage({
    role: "user",
    content: "Abandoned root evidence.",
    timestamp: Date.parse("2026-10-21T00:00:00.000Z"),
  });
  manager.resetLeaf();
  manager.appendMessage({
    role: "user",
    content: "Current root evidence.",
    timestamp: Date.parse("2026-10-21T00:00:01.000Z"),
  });

  const snapshot = snapshotSession(manager);
  const liveEvidence = allEvidence(planSources(snapshot, new Set()));
  const path = await writeSession(t, [manager.getHeader(), ...manager.getEntries()], "forest-session.jsonl");
  const loaded = await loadSnapshot(path, manager.getLeafId());
  const loadedEvidence = allEvidence(planSources(loaded, new Set()));

  assert.deepEqual(liveEvidence.map((part) => part.text), ["Current root evidence."]);
  assert.deepEqual(loadedEvidence.map((part) => part.text), ["Current root evidence."]);
});

test("planning is repeatable, scopes evidence to the selected branch, and keeps prior text as context", () => {
  const sessionId = "branch-session";
  const common = messageEntry("common", null, "user", "Shared setup: use the local synthetic project.");
  const left = messageEntry("left", "common", "assistant", "Sibling conclusion: prefer blue.");
  const right = messageEntry("right", "common", "user", "Current correction: prefer green.");
  const leftSnapshot = {
    sessionId,
    sessionFile: null,
    cwd: "/synthetic/project",
    leafId: "left",
    entries: [common, left],
  };
  const rightSnapshot = { ...leftSnapshot, leafId: "right", entries: [common, right] };

  const leftBatch = planSources(leftSnapshot, new Set());
  const commonLeft = allEvidence(leftBatch).find((part) => part.entryId === "common");
  assert.ok(commonLeft);
  const rightBatch = planSources(rightSnapshot, new Set([commonLeft.key]));
  const commonRightContext = rightBatch.flatMap((batch) => batch.context).find((part) => part.entryId === "common");

  assert.ok(commonRightContext);
  assert.equal(commonLeft.key, commonRightContext.key);
  assert.ok(!allEvidence(rightBatch).some((part) => part.text.includes("blue")));
  assert.ok(allEvidence(rightBatch).some((part) => part.text.includes("green")));
  const completedRightKeys = [commonLeft.key, ...allEvidence(rightBatch).map((part) => part.key)];
  assert.deepEqual(planSources(rightSnapshot, new Set(completedRightKeys)), []);
  assert.ok(rightBatch.every((batch) => batch.sourceKeys.every((key) => batch.evidence.some((part) => part.key === key))));
});

test("fork copies reuse only verified ancestor keys and retain copied context", async (t) => {
  const shared = messageEntry("ancestor-message", null, "user", "In a greenhouse, shade cloth limits summer heat.");
  const parentPath = await writeSession(t, [header("parent-session"), shared]);
  const copied = structuredClone(shared);
  const childOnly = messageEntry("child-message", "ancestor-message", "assistant", "For this branch, test shade cloth before buying more fans.");
  const childPath = await writeSession(t, [header("child-session", "/synthetic/greenhouse", parentPath), copied, childOnly], "child.jsonl");

  const parent = await loadSnapshot(parentPath);
  const child = await loadSnapshot(childPath);
  const liveChild = snapshotSession(SessionManager.open(childPath));
  const parentKey = allEvidence(planSources(parent, new Set())).find((part) => part.entryId === "ancestor-message")?.key;
  const childPlan = planSources(child, parentKey ? new Set([parentKey]) : new Set());
  const childEvidence = allEvidence(childPlan);
  const rehydratedChildKeys = planSources(child, new Set()).flatMap((batch) => batch.sourceKeys);
  const liveChildKeys = planSources(liveChild, new Set()).flatMap((batch) => batch.sourceKeys);

  assert.ok(parentKey);
  assert.ok(!childEvidence.some((part) => part.entryId === "ancestor-message"));
  assert.ok(childEvidence.some((part) => part.entryId === "child-message"));
  assert.ok(childPlan.flatMap((batch) => batch.context).some((part) => part.entryId === "ancestor-message"));
  assert.deepEqual(liveChildKeys, rehydratedChildKeys);

  const changedCopy = { ...copied, message: { ...copied.message, content: [{ type: "text", text: "Changed copied evidence." }] } };
  const changedChildPath = await writeSession(t, [header("unverified-child", "/synthetic/greenhouse", parentPath), changedCopy], "changed.jsonl");
  const changedChild = await loadSnapshot(changedChildPath);
  const changedKey = allEvidence(planSources(changedChild, new Set())).find((part) => part.entryId === "ancestor-message")?.key;
  assert.ok(changedKey);
  assert.notEqual(changedKey, parentKey);
});

test("source keys stay stable as a leaf grows and unrelated entry ID collisions stay separate", async (t) => {
  const first = messageEntry("same-id", null, "user", "Unchanged shared evidence.");
  const onePath = await writeSession(t, [header("one-session"), first], "one.jsonl");
  const extended = messageEntry("later", "same-id", "assistant", "A later leaf.");
  const longerPath = await writeSession(t, [header("one-session"), first, extended], "longer.jsonl");
  const unrelatedPath = await writeSession(t, [header("other-session"), first], "other.jsonl");
  const one = await loadSnapshot(onePath);
  const longer = await loadSnapshot(longerPath);
  const unrelated = await loadSnapshot(unrelatedPath);
  const keyFor = (snapshot) => allEvidence(planSources(snapshot, new Set())).find((part) => part.entryId === "same-id")?.key;

  assert.equal(keyFor(one), keyFor(longer));
  assert.notEqual(keyFor(one), keyFor(unrelated));
});

test("same entry IDs under different parent lineages or roles produce different source keys", () => {
  const parentA = messageEntry("parent-a", null, "user", "First branch context.");
  const childA = messageEntry("colliding-child", "parent-a", "user", "Same child text.");
  const parentB = messageEntry("parent-b", null, "user", "Second branch context.");
  const childB = messageEntry("colliding-child", "parent-b", "assistant", "Same child text.");
  const snapshotA = { sessionId: "lineage-session", sessionFile: null, cwd: "/synthetic", leafId: childA.id, entries: [parentA, childA] };
  const snapshotB = { ...snapshotA, leafId: childB.id, entries: [parentB, childB] };
  const keyForChild = (snapshot) => allEvidence(planSources(snapshot, new Set())).find((part) => part.entryId === "colliding-child")?.key;

  assert.notEqual(keyForChild(snapshotA), keyForChild(snapshotB));
});

test("Pi system messages with tool schemas are accepted but do not change user source identity", () => {
  const plain = messageEntry("user-entry", null, "user", "Preserve this user evidence.");
  const system = {
    type: "message",
    id: "system-entry",
    parentId: null,
    timestamp: "2026-10-21T00:00:00.000Z",
    message: {
      role: "system",
      content: "",
      sections: { systemPrompt: "Synthetic prompt content." },
      toolsAdded: [{ name: "synthetic_tool", description: "Synthetic schema." }],
      timestamp: 1,
    },
  };
  const withSystem = messageEntry("user-entry", "system-entry", "user", "Preserve this user evidence.");
  const plainSnapshot = { sessionId: "system-session", sessionFile: null, cwd: "/synthetic", leafId: plain.id, entries: [plain] };
  const systemSnapshot = { ...plainSnapshot, leafId: withSystem.id, entries: [system, withSystem] };
  const keyForUser = (snapshot) => allEvidence(planSources(snapshot, new Set())).find((part) => part.entryId === "user-entry")?.key;
  const systemEvidence = allEvidence(planSources(systemSnapshot, new Set()));

  assert.equal(keyForUser(plainSnapshot), keyForUser(systemSnapshot));
  assert.equal(systemEvidence.length, 1);
  assert.equal(systemEvidence[0]?.role, "user");
  assert.ok(!JSON.stringify(systemEvidence).includes("Synthetic prompt"));
  assert.ok(!JSON.stringify(systemEvidence).includes("synthetic_tool"));
});

test("Pi branch and compaction summary message roles are accepted as metadata but never source evidence", () => {
  const branchSummary = {
    type: "message",
    id: "branch-summary-entry",
    parentId: null,
    timestamp: "2026-10-21T00:00:00.000Z",
    message: {
      role: "branchSummary",
      summary: "Summary of an abandoned branch.",
      fromId: "earlier-entry",
      timestamp: 1,
    },
  };
  const compactionSummary = {
    type: "message",
    id: "compaction-summary-entry",
    parentId: "branch-summary-entry",
    timestamp: "2026-10-21T00:00:01.000Z",
    message: {
      role: "compactionSummary",
      summary: "A compacted context summary.",
      tokensBefore: 9000,
      timestamp: 2,
    },
  };
  const user = messageEntry("user-after-summaries", "compaction-summary-entry", "user", "Keep this user evidence.");
  const snapshot = {
    sessionId: "summary-role-session",
    sessionFile: null,
    cwd: "/synthetic",
    leafId: user.id,
    entries: [branchSummary, compactionSummary, user],
  };

  const evidence = allEvidence(planSources(snapshot, new Set()));

  assert.deepEqual(evidence.map(({ entryId, role, text }) => ({ entryId, role, text })), [
    { entryId: "user-after-summaries", role: "user", text: "Keep this user evidence." },
  ]);
});

test("long Unicode text splits without loss and each evidence batch stays within serialized byte bounds", () => {
  const text = `${"🧠界a\n".repeat(2400)}end`;
  const entry = messageEntry("long-entry", null, "user", text);
  const snapshot = {
    sessionId: "long-session",
    sessionFile: null,
    cwd: "/synthetic/project",
    leafId: "long-entry",
    entries: [entry],
  };
  const batches = planSources(snapshot, new Set());
  const parts = allEvidence(batches);

  assert.ok(parts.length > 1);
  assert.equal(parts.map((part) => part.text).join(""), text);
  assert.ok(parts.every((part) => Buffer.byteLength(part.text, "utf8") <= 1024));
  assert.ok(parts.every((part) => Buffer.byteLength(JSON.stringify([part]), "utf8") <= 2 * 1024));
  assert.ok(batches.every((batch) => Buffer.byteLength(JSON.stringify(batch.evidence), "utf8") <= sourceLimit));
  assert.ok(batches.every((batch) => Buffer.byteLength(JSON.stringify(batch.context), "utf8") <= contextLimit));
  assert.deepEqual(planSources(snapshot, new Set(parts.map((part) => part.key))), []);
});

test("large and repeated entries form deterministic, bounded batches with prior context separated", () => {
  const entries = [
    messageEntry("opening", null, "user", "Shared condition: " + "A".repeat(5000)),
    messageEntry("new-one", "opening", "assistant", "Evidence one: " + "B".repeat(12000)),
    messageEntry("new-two", "new-one", "user", "Evidence two: " + "C".repeat(12000)),
  ];
  const snapshot = { sessionId: "large-session", sessionFile: null, cwd: "/synthetic", leafId: "new-two", entries };
  const first = planSources(snapshot, new Set());
  const again = planSources(snapshot, new Set());

  assert.deepEqual(first, again);
  assert.ok(first.length > 1);
  assert.ok(first.every((batch) => Buffer.byteLength(JSON.stringify(batch.evidence), "utf8") <= sourceLimit));
  assert.ok(first.every((batch) => Buffer.byteLength(JSON.stringify(batch.context), "utf8") <= contextLimit));
  assert.ok(first.slice(1).some((batch) => batch.context.length > 0));
  assert.equal(new Set(allEvidence(first).map((part) => part.key)).size, allEvidence(first).length);
});

test("hidden thinking is excluded and image data is represented only as an omission marker", () => {
  const imageEntry = {
    type: "message",
    id: "image-message",
    parentId: null,
    timestamp: "2026-10-21T00:00:00.000Z",
    message: {
      role: "user",
      content: [
        { type: "text", text: "Visible text." },
        { type: "image", mimeType: "image/png", data: "BASE64-SECRET" },
      ],
      timestamp: 1,
    },
  };
  const thinkingEntry = {
    type: "message",
    id: "thinking-message",
    parentId: "image-message",
    timestamp: "2026-10-21T00:00:01.000Z",
    message: {
      role: "assistant",
      content: [
        { type: "text", text: "Assistant text." },
        { type: "thinking", thinking: "Private chain of thought." },
      ],
      timestamp: 2,
    },
  };
  const snapshot = { sessionId: "multimodal-session", sessionFile: null, cwd: "/synthetic", leafId: "thinking-message", entries: [imageEntry, thinkingEntry] };
  const text = allEvidence(planSources(snapshot, new Set())).map((part) => part.text).join("");

  assert.ok(text.includes("Visible text."));
  assert.ok(text.includes("[image omitted]"));
  assert.ok(!text.includes("Private chain of thought."));
  assert.ok(!text.includes("BASE64-SECRET"));
  assert.match(text, /image omitted/i);
});

test("null context edits suppress the removed text and create a new edit source", () => {
  const manager = SessionManager.inMemory("/synthetic/project");
  const targetId = manager.appendMessage({
    role: "user",
    content: [{ type: "text", text: "This entry is removed from context." }],
    timestamp: 1,
  });
  manager.appendContextEdit(targetId, null);
  const evidence = allEvidence(planSources(snapshotSession(manager), new Set()));

  assert.ok(!evidence.some((part) => part.text.includes("removed from context")));
  assert.ok(evidence.some((part) => part.role === "context_edit" && part.text.includes("removed from this branch")));
});

test("malformed JSONL, duplicate IDs, missing parents, cycles, and invalid entry schemas fail closed", async (t) => {
  const goodHeader = header("bad-session");
  const good = messageEntry("first", null, "user", "Synthetic text.");
  const cases = [
    ["malformed", `${JSON.stringify(goodHeader)}\n{"type":\n${JSON.stringify(good)}\n`],
    ["duplicate", [goodHeader, good, structuredClone(good)]],
    ["missing-parent", [goodHeader, messageEntry("orphan", "absent", "user", "orphan")]],
    ["cycle", [goodHeader, messageEntry("a", "b", "user", "a"), messageEntry("b", "a", "assistant", "b")]],
    ["bad-schema", [goodHeader, { ...good, message: { role: "alien", content: "not valid" } }]],
  ];

  for (const [name, input] of cases) {
    const contents = typeof input === "string" ? input : input.map((row) => JSON.stringify(row)).join("\n") + "\n";
    const path = await writeSession(t, [], `${name}.jsonl`);
    await writeFile(path, contents, "utf8");
    await assert.rejects(loadSnapshot(path), undefined, name);
  }
});

test("an incomplete trailing line is pending while an empty session has no checkpoint", async (t) => {
  const partialPath = await writeSession(t, [header("partial-session"), messageEntry("complete", null, "user", "Complete prefix.")], "partial.jsonl");
  const headerOnlyPath = await writeSession(t, [header("empty-session")], "empty.jsonl");
  await writeFile(partialPath, `${await readFile(partialPath, "utf8")}{"type":`, "utf8");
  const partial = await loadSnapshot(partialPath);
  const empty = await loadSnapshot(headerOnlyPath);

  assert.ok(partial.entries.length > 0);
  assert.ok(partial.pending);
  assert.equal(planSources(partial, new Set()).length, 0);
  assert.equal(empty.leafId, null);
  assert.equal(empty.pending, false);
});

test("an explicit missing checkpoint is pending and never falls back to another leaf", async (t) => {
  const path = await writeSession(t, [header("checkpoint-session"), messageEntry("known", null, "user", "Known prefix.")]);
  const snapshot = await loadSnapshot(path, "not-present");

  assert.equal(snapshot.leafId, null);
  assert.equal(snapshot.pending, true);
  assert.deepEqual(planSources(snapshot, new Set()), []);
});
