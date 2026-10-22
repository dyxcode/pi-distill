import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { stripVTControlCharacters } from "node:util";
import { after, test } from "node:test";

const root = await mkdtemp(join(tmpdir(), "pi-distill-rendering-"));
process.env.PI_CODING_AGENT_DIR = join(root, "agent");
process.env.PI_CODING_AGENT_SESSION_DIR = join(root, "sessions");
process.env.PI_OFFLINE = "1";
process.env.PI_TELEMETRY = "0";
process.env.PI_SKIP_VERSION_CHECK = "1";
await mkdir(process.env.PI_CODING_AGENT_DIR);
after(() => rm(root, { recursive: true, force: true }));
const { DefaultResourceLoader, ToolExecutionComponent, initTheme } = await import("@earendil-works/pi-coding-agent");
const { visibleWidth } = await import("@earendil-works/pi-tui");
initTheme("dark");
const loader = new DefaultResourceLoader({
  cwd: root, agentDir: process.env.PI_CODING_AGENT_DIR,
  additionalExtensionPaths: [join(dirname(dirname(fileURLToPath(import.meta.url))), "src/index.ts")],
  noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
});
await loader.reload();
assert.deepEqual(loader.getExtensions().errors, []);
const definitions = new Map(loader.getExtensions().extensions.flatMap(extension =>
  [...extension.tools].map(([name, registered]) => [name, registered.definition])));

function row(name, args) {
  return new ToolExecutionComponent(name, "synthetic-call", args, {}, definitions.get(name), { requestRender() {} }, root);
}
function output(component, width = 110) {
  return component.render(width).map(line => stripVTControlCharacters(line).trimEnd()).filter(line => line.trim()).join("\n");
}
function result(details) { return { content: [{ type: "text", text: JSON.stringify(details) }], details, isError: false }; }
const candidate = {
  id: "INTERNAL_MEMORY_ID", revision: 41, matchedTerms: ["River"],
  preview: { text: "River 的迁移已取消。每周日志检查继续。", omittedBefore: false, omittedAfter: false },
};
const search = { status: "ok", candidates: [candidate], hasMore: false };
const read = {
  status: "ok", id: candidate.id, revision: 41, content: candidate.preview.text,
  createdAt: "2020-01-01T00:00:00.000Z", updatedAt: "2026-10-22T04:15:00.000Z", hasMore: false,
};

test("the actual Pi tool row replaces pending output in place with one completed summary", () => {
  const component = row("memory_search", { terms: ["River", "migration"] });
  component.markExecutionStarted();
  const pending = output(component);
  assert.match(pending, /Search memories/);
  assert.doesNotMatch(pending, /River|migration|memories found|Searching/);
  component.updateResult(result(search));
  const done = output(component);
  assert.equal(done.trim(), "Search memories · 1 memory found using 2 keywords");
  component.setExpanded(true);
  const expanded = output(component);
  assert.match(expanded, /Keywords:.*River.*migration/);
  assert.match(expanded, /Matched:.*River/);
  assert.match(expanded, /Search complete/);
  assert.ok(expanded.includes(candidate.preview.text));
  assert.doesNotMatch(expanded, /INTERNAL_MEMORY_ID|revision|nextCursor/iu);
  component.setExpanded(false);
  assert.equal(output(component), done);
});

test("search counts this page and distinguishes partial empty pages and truncated previews", () => {
  const component = row("memory_search", { terms: ["River"] });
  component.updateResult(result({ ...search, candidates: [], hasMore: true }));
  assert.match(output(component), /0 memories found using 1 keyword.*Partial search/);
  component.setExpanded(true);
  assert.match(output(component), /Search incomplete/);
  assert.doesNotMatch(output(component), /Search complete|No memories exist/);
  component.updateResult(result({ ...search, candidates: [{ ...candidate, preview: { ...candidate.preview, omittedAfter: true } }] }));
  assert.match(output(component), /Preview truncated/);
});

test("reads display content and update time only when expanded", () => {
  const component = row("memory_read", { id: candidate.id, expectedRevision: 41 });
  assert.equal(output(component).trim(), "Read a memory");
  component.updateResult(result(read));
  assert.equal(output(component).trim(), "Read a memory · Complete");
  component.setExpanded(true);
  const expanded = output(component);
  assert.ok(expanded.includes(read.content));
  assert.match(expanded, /Updated:/);
  assert.doesNotMatch(expanded, /INTERNAL_MEMORY_ID|revision|created|2020|session|source/iu);
  const historical = output(component);
  component.setExpanded(false);
  component.setExpanded(true);
  assert.equal(output(component), historical, "rendering uses the original result, without another lookup");
});

test("the final chunk is not described as a complete single read", () => {
  const first = row("memory_read", { id: candidate.id, expectedRevision: 41 });
  first.updateResult(result({ ...read, hasMore: true }));
  assert.match(output(first), /Partial read/);
  const last = row("memory_read", { id: candidate.id, expectedRevision: 41, cursor: "INTERNAL_CURSOR" });
  last.updateResult(result(read));
  assert.match(output(last), /Final chunk/);
  assert.doesNotMatch(output(last), /Complete|INTERNAL_CURSOR/);
});

test("errors and legacy or malformed details stay readable without leaking raw payloads", () => {
  const component = row("memory_read", { id: candidate.id, expectedRevision: 41 });
  for (const [status, expected] of [
    ["stale", /changed.*Read it again/], ["not_found", /not found/i],
    ["unavailable", /unavailable/i], ["budget_exhausted", /limit reached/i],
    ["cancelled", /cancelled/i], ["invalid_input", /invalid/i],
  ]) {
    component.updateResult(result({ status, currentRevision: 42, message: "INTERNAL_MEMORY_ID" }));
    assert.match(output(component), expected);
    assert.doesNotMatch(output(component), /INTERNAL_MEMORY_ID|42/);
  }
  component.setExpanded(true);
  component.updateResult({ content: [{ type: "text", text: JSON.stringify(read) }], isError: false });
  assert.ok(output(component).includes(read.content), "old structured text results remain readable");
  for (const details of [null, [], { status: "ok", content: 12 }, { status: "ok", candidates: "bad" }]) {
    component.updateResult(result(details));
    assert.match(output(component), /unavailable/i);
    assert.doesNotMatch(output(component), /INTERNAL_MEMORY_ID/);
  }
});

test("Chinese and long text wrap safely and terminal controls remain inert", () => {
  const component = row("memory_read", { id: candidate.id, expectedRevision: 41 });
  const content = "中文记忆🧠".repeat(50) + "\n\u001b]52;c;ATTACK\u0007\r\u202eend";
  component.updateResult(result({ ...read, content }));
  component.setExpanded(true);
  for (const width of [24, 45, 100]) {
    const lines = component.render(width);
    assert.ok(lines.every(line => visibleWidth(line) <= width));
    assert.ok(!lines.join("\n").includes("\u001b]52"));
    assert.ok(!lines.join("\n").includes("\u202e"));
    assert.match(output(component, width), /中文/);
  }
});
