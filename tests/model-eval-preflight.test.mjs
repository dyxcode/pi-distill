import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ModelRuntime } from "@earendil-works/pi-coding-agent";
import { selectEvaluationModel } from "./model-eval-preflight.mjs";

test("stored credentials work before the SDK auth snapshot is refreshed", async () => {
  const dir = await mkdtemp(join(tmpdir(), "pi-eval-auth-"));
  try {
    const authPath = join(dir, "auth.json");
    await writeFile(authPath, JSON.stringify({ google: { type: "api_key", key: "synthetic-not-a-real-key" } }));
    const runtime = await ModelRuntime.create({ authPath, modelsPath: null, refreshOnCreate: false });
    assert.equal(runtime.hasConfiguredAuth("google"), false);
    const result = await selectEvaluationModel(runtime, "gemini-3.8-flash");
    assert.equal(result.model.id, "gemini-3.8-flash");
    assert.equal(result.model.provider, "google");
    assert.deepEqual(result.credentialProviders, [{ providerId: "google", type: "api_key" }]);
    assert.equal(JSON.stringify(result).includes("synthetic-not-a-real-key"), false);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("credential read failures propagate instead of looking like missing configuration", async () => {
  const failure = Object.assign(new Error("lock denied"), { code: "EPERM" });
  await assert.rejects(selectEvaluationModel({
    listCredentials: async ({ signal }) => { assert.ok(signal); throw failure; },
  }, "gemini-3.8-flash"), (error) => error === failure);
});

test("an unavailable requested model never falls back to a paid model", async () => {
  const result = await selectEvaluationModel({
    listCredentials: async () => [{ providerId: "google", type: "api_key" }],
    getAvailable: async () => [{ provider: "google", id: "gemini-3.1-pro-preview" }],
  }, "gemini-3.8-flash");
  assert.equal(result.model, undefined);
  assert.equal(result.failure, "requested_model_unavailable");
});
