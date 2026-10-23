import assert from "node:assert/strict";
import { appendFile, mkdtemp, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { distillRetryContext } from "../../src/distill/retry-context.ts";

const stamp = "2026-10-23T00:00:00.000Z";

function header(id, parentSession) {
  return {
    type: "session",
    version: 3,
    id,
    timestamp: stamp,
    cwd: "/synthetic/project",
    ...(parentSession === undefined ? {} : { parentSession }),
  };
}

function model(overrides = {}) {
  return {
    provider: "synthetic-provider",
    id: "synthetic-model",
    api: "openai-completions",
    baseUrl: "https://models.synthetic.invalid/v1",
    contextWindow: 32_000,
    maxTokens: 4_096,
    ...overrides,
  };
}

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-retry-context-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const sessionFile = join(directory, "child.jsonl");
  const parentFile = join(directory, "parent.jsonl");
  return { directory, sessionFile, parentFile };
}

async function writeHeader(path, value) {
  await writeFile(path, `${JSON.stringify(value)}\n`, "utf8");
}

test("source fingerprints are stable and react to root and reachable parent changes", async t => {
  const f = await fixture(t);
  await writeHeader(f.parentFile, header("parent-session"));
  await writeHeader(f.sessionFile, header("child-session", "parent.jsonl"));

  const initial = distillRetryContext(f.sessionFile, model());
  assert.deepEqual(distillRetryContext(f.sessionFile, model()), initial);

  await appendFile(f.parentFile, `${JSON.stringify({ type: "message", id: "parent-body" })}\n`, "utf8");
  const changedParent = distillRetryContext(f.sessionFile, model());
  assert.notEqual(changedParent.sourceVersion, initial.sourceVersion);
  assert.equal(changedParent.modelKey, initial.modelKey);

  await appendFile(f.sessionFile, `${JSON.stringify({ type: "message", id: "child-body" })}\n`, "utf8");
  const changedRoot = distillRetryContext(f.sessionFile, model());
  assert.notEqual(changedRoot.sourceVersion, changedParent.sourceVersion);
  assert.equal(changedRoot.policyVersion, initial.policyVersion);
});

test("missing and malformed source files reactivate source-invalid holds after repair", async t => {
  const f = await fixture(t);
  const absent = distillRetryContext(f.sessionFile, model());
  await writeHeader(f.sessionFile, header("repaired-root"));
  const repairedRoot = distillRetryContext(f.sessionFile, model());
  assert.notEqual(repairedRoot.sourceVersion, absent.sourceVersion);

  const missingParent = join(f.directory, "later-parent.jsonl");
  await writeHeader(f.sessionFile, header("child-with-missing-parent", "later-parent.jsonl"));
  const beforeParentRepair = distillRetryContext(f.sessionFile, model());
  await writeHeader(missingParent, header("later-parent"));
  const afterParentRepair = distillRetryContext(f.sessionFile, model());
  assert.notEqual(afterParentRepair.sourceVersion, beforeParentRepair.sourceVersion);

  await writeFile(f.sessionFile, `${"x".repeat(64 * 1024 + 20)}\n`, "utf8");
  const malformed = distillRetryContext(f.sessionFile, model());
  assert.deepEqual(distillRetryContext(f.sessionFile, model()), malformed);
  await writeHeader(f.sessionFile, header("repaired-malformed-root"));
  assert.notEqual(distillRetryContext(f.sessionFile, model()).sourceVersion, malformed.sourceVersion);
});

test("model identity and declared capabilities change only the model fingerprint", async t => {
  const f = await fixture(t);
  await writeHeader(f.sessionFile, header("model-identity-session"));
  const baselineModel = model();
  const baseline = distillRetryContext(f.sessionFile, baselineModel);

  for (const changed of [
    model({ provider: "other-provider" }),
    model({ id: "other-model" }),
    model({ api: "anthropic-messages" }),
    model({ contextWindow: 64_000 }),
    model({ maxTokens: 8_192 }),
    model({ baseUrl: "https://alternate.synthetic.invalid/v1" }),
  ]) {
    const context = distillRetryContext(f.sessionFile, changed);
    assert.equal(context.sourceVersion, baseline.sourceVersion);
    assert.notEqual(context.modelKey, baseline.modelKey);
  }

  const withoutModel = distillRetryContext(f.sessionFile, undefined);
  assert.notEqual(withoutModel.modelKey, baseline.modelKey);
  assert.equal(withoutModel.sourceVersion, baseline.sourceVersion);
  assert.match(baseline.policyVersion, /.+/);

  const privateEndpoint = "https://user:credential@models.synthetic.invalid/v1?token=synthetic-secret";
  const privateContext = distillRetryContext(f.sessionFile, model({ baseUrl: privateEndpoint }));
  assert.notEqual(privateContext.modelKey, baseline.modelKey);
  assert.equal(JSON.stringify(privateContext).includes("synthetic-secret"), false);
  assert.equal(JSON.stringify(privateContext).includes("credential"), false);
  assert.equal(JSON.stringify(privateContext).includes("models.synthetic.invalid"), false);
});

test("symlink headers stay unknown without following the target, then change when replaced", async t => {
  const f = await fixture(t);
  const target = join(f.directory, "real-parent.jsonl");
  const linkedParent = join(f.directory, "parent.jsonl");
  await writeHeader(target, header("real-parent"));
  await symlink(target, linkedParent);
  await writeHeader(f.sessionFile, header("symlink-child", "parent.jsonl"));

  const throughSymlink = distillRetryContext(f.sessionFile, model());
  await appendFile(target, `${JSON.stringify({ type: "message", id: "target-edit" })}\n`, "utf8");
  assert.deepEqual(distillRetryContext(f.sessionFile, model()), throughSymlink);

  await unlink(linkedParent);
  await writeHeader(linkedParent, header("regular-parent"));
  assert.notEqual(distillRetryContext(f.sessionFile, model()).sourceVersion, throughSymlink.sourceVersion);
});

test("cycles and ancestry beyond the supported depth have stable bounded fingerprints", async t => {
  const f = await fixture(t);
  await writeHeader(f.sessionFile, header("cycle", "child.jsonl"));
  const cycle = distillRetryContext(f.sessionFile, model());
  assert.deepEqual(distillRetryContext(f.sessionFile, model()), cycle);
  await writeHeader(f.sessionFile, header("cycle-repaired"));
  assert.notEqual(distillRetryContext(f.sessionFile, model()).sourceVersion, cycle.sourceVersion);

  const ancestry = [];
  for (let index = 0; index < 130; index++) {
    ancestry.push(join(f.directory, `ancestor-${index}.jsonl`));
  }
  for (let index = 0; index < ancestry.length; index++) {
    const parent = ancestry[index + 1];
    await writeHeader(ancestry[index], header(
      `ancestor-${index}`,
      parent === undefined ? undefined : `ancestor-${index + 1}.jsonl`,
    ));
  }

  const bounded = distillRetryContext(ancestry[0], model());
  assert.deepEqual(distillRetryContext(ancestry[0], model()), bounded);
  await appendFile(ancestry[129], `${JSON.stringify({ type: "message", id: "beyond-bound" })}\n`, "utf8");
  assert.deepEqual(distillRetryContext(ancestry[0], model()), bounded);
  await appendFile(ancestry[128], `${JSON.stringify({ type: "message", id: "at-bound" })}\n`, "utf8");
  assert.notEqual(distillRetryContext(ancestry[0], model()).sourceVersion, bounded.sourceVersion);
});
