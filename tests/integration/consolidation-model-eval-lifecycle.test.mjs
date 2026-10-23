import assert from "node:assert/strict";
import test from "node:test";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { countOutstandingDistillTriggers, evaluationFailureStatus, trackStreamCompletion, waitForConsolidation } from "../model/consolidation-model-eval-support.mjs";

test("SDK streams finish request accounting without a foreground message_end event", async () => {
  for (const type of ["done", "error"]) {
    const stream = createAssistantMessageEventStream();
    let completed = 0;
    const tracked = await trackStreamCompletion(() => Promise.resolve(stream), () => { completed++; });
    assert.equal(tracked, stream);
    assert.equal(completed, 0);
    const message = { role: "assistant", content: [], timestamp: Date.now(), stopReason: type === "done" ? "stop" : "error" };
    const event = type === "done" ? { type, reason: "stop", message } : { type, reason: "error", error: message };
    stream.push(event);
    assert.equal(await tracked.result(), message);
    assert.equal(completed, 1);
    assert.deepEqual(await Array.fromAsync(tracked), [event]);
  }
});

test("a rejected or synchronously failed stream start finishes accounting once", async () => {
  for (const start of [() => { throw new Error("start failed"); }, () => Promise.reject(new Error("start failed"))]) {
    let completed = 0;
    await assert.rejects(trackStreamCompletion(start, () => { completed++; }), /start failed/);
    assert.equal(completed, 1);
  }
});

test("provider failure after partial evaluation remains incomplete with recorded progress", () => {
  assert.equal(evaluationFailureStatus("provider_temporarily_unavailable", {}), "not_run");
  assert.equal(evaluationFailureStatus("provider_temporarily_unavailable", { providerRequests: 1 }), "incomplete");
  assert.equal(evaluationFailureStatus("network_unavailable", { stages: [{ status: "passed" }] }), "incomplete");
  assert.equal(evaluationFailureStatus("request_budget_exhausted", {}), "incomplete");
  assert.equal(evaluationFailureStatus("harness_failure", { providerRequests: 1 }), "harness_failure");
});

const empty = { memories: [], pendingJobs: 0, pendingTriggers: 0, pendingObservations: 0 };
const idle = { inFlightRequests: 0, completedRequests: 0, lastRequestCompletion: 0, requestLimitHit: false };

async function runWait(readState, activity = () => idle, timeoutMs = 3_000) {
  let clock = 0;
  const result = await waitForConsolidation({
    readState: () => readState(clock), activity: () => activity(clock),
    signal: new AbortController().signal, timeoutMs,
    now: () => clock, sleep: async (ms) => { clock += ms; },
  });
  return { ...result, elapsed: clock };
}

test("an apparently idle snapshot waits for delayed boundary work and its durable completion", async () => {
  const final = { ...empty, memories: [{ id: "task", revision: 2, content: "cancelled" }] };
  const result = await runWait((now) => now < 200 ? empty
    : now < 600 ? { ...empty, pendingJobs: 1 } : final);
  assert.equal(result.settled, true);
  assert.deepEqual(result.state, final);
  assert.ok(result.elapsed >= 1_600);
});

test("a live session observation is diagnostic and does not block completed work", async () => {
  const observed = await runWait(() => ({ ...empty, pendingObservations: 1 }));
  assert.equal(observed.settled, true);
  assert.equal(observed.state.pendingObservations, 1);
});

test("an unprepared trigger blocks settlement until durable distillation finishes", async () => {
  const result = await runWait((now) => now < 600
    ? { ...empty, pendingTriggers: 1 }
    : { ...empty, memories: [{ id: "task", revision: 1, content: "cancelled" }] });
  assert.equal(result.settled, true);
  assert.equal(result.state.pendingTriggers, 0);
  assert.ok(result.elapsed >= 1_600);
});

test("outstanding trigger count paginates and includes candidates, pending, and prepared work", () => {
  const pages = new Map([
    [undefined, { items: [{ status: "candidate" }, { status: "complete" }], nextCursor: 2 }],
    [2, { items: [{ status: "pending" }, { status: "prepared" }], nextCursor: 4 }],
    [4, { items: [{ status: "complete" }] }],
  ]);
  const cursors = [];
  const count = countOutstandingDistillTriggers(({ cursor }) => {
    cursors.push(cursor);
    const page = pages.get(cursor);
    assert.ok(page, `unexpected cursor ${cursor}`);
    return page;
  });
  assert.equal(count, 3);
  assert.deepEqual(cursors, [undefined, 2, 4]);
});

test("an in-flight model request prevents an unchanged-state pass", async () => {
  const running = await runWait(() => empty, () => ({ ...idle, inFlightRequests: 1 }));
  assert.equal(running.settled, false);
});

test("read errors reset the stable window and never substitute an empty successful state", async () => {
  const result = await runWait((now) => {
    if (now >= 400 && now < 800) throw new Error("synthetic database busy");
    return empty;
  });
  assert.equal(result.settled, true);
  assert.ok(result.elapsed >= 1_800);
  await assert.rejects(runWait(() => { throw new Error("unavailable"); }), /evaluation_store_unavailable/);
});

test("a terminal failed model request returns the durable pending work as incomplete", async () => {
  const result = await runWait(() => ({ ...empty, pendingJobs: 1 }),
    (now) => ({ ...idle, completedRequests: now < 200 ? 0 : 1, lastRequestCompletion: 200 }));
  assert.equal(result.settled, false);
  assert.equal(result.state.pendingJobs, 1);
  assert.ok(result.elapsed < 3_000);
});
