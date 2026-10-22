import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { SQLiteMemoryStore } from "../src/memory-store.ts";

const createdAt = "2026-10-21T00:00:00.000Z";

async function createStore(t) {
  const directory = await mkdtemp(join(tmpdir(), "pi-distill-search-"));
  const databasePath = join(directory, "memories.sqlite");
  let store = new SQLiteMemoryStore(databasePath);
  t.after(async () => {
    store.close();
    await rm(directory, { recursive: true, force: true });
  });
  return {
    get store() {
      return store;
    },
    reopen() {
      store.close();
      store = new SQLiteMemoryStore(databasePath);
      return store;
    },
  };
}

function save(store, id, content) {
  store.save({ id, content, createdAt });
}

test("returns matched terms and current memory metadata for bilingual any/all matches", async (t) => {
  const { store } = await createStore(t);
  save(store, "english", "Review the CUDA environment before training.");
  save(store, "mixed", "Bunya 的调度记录需要复查。");
  save(store, "noise", "A completely unrelated note.");
  save(store, "chinese", "请阅读这条记忆并检查 CUDA 结果。");
  save(store, "complete", "CUDA results were verified.");

  const any = store.searchBatch({
    terms: ["cuda", "调度", "missing"],
    match: "any",
    limit: 10,
  });
  assert.deepEqual(any.map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [
    ["chinese", ["cuda"]],
    ["complete", ["cuda"]],
    ["english", ["cuda"]],
    ["mixed", ["调度"]],
    ["noise", []],
  ]);
  assert.deepEqual(any[0]?.memory, {
    id: "chinese",
    content: "请阅读这条记忆并检查 CUDA 结果。",
    createdAt,
    revision: 1,
    updatedAt: createdAt,
  });

  const all = store.searchBatch({ terms: ["CUDA", "results"], match: "all" });
  const anyForTheSameTerms = store.searchBatch({ terms: ["CUDA", "results"], match: "any" });
  assert.deepEqual(all.map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [
    ["chinese", ["CUDA"]],
    ["complete", ["CUDA", "results"]],
    ["english", ["CUDA"]],
    ["mixed", []],
    ["noise", []],
  ]);
  assert.deepEqual(anyForTheSameTerms, all);
});

test("matches punctuation, SQL metacharacters, quotes, backslashes, and meaningful spaces literally", async (t) => {
  const { store } = await createStore(t);
  save(store, "literal", "  Keep  50%_ O'Brien \\path .* exactly.  ");

  const rows = store.searchBatch({
    terms: ["50%_", "O'Brien", "\\path", ".*", "  Keep  "],
    match: "any",
  });
  assert.deepEqual(rows.map(({ matchedTerms }) => matchedTerms), [[
    "50%_", "O'Brien", "\\path", ".*", "  Keep  ",
  ]]);
  assert.deepEqual(store.searchBatch({ terms: ["50%"], match: "all" })[0]?.matchedTerms, ["50%"]);
});

test("folds ASCII letters only", async (t) => {
  const { store } = await createStore(t);
  save(store, "ascii", "The ASCII letter I is case insensitive.");
  save(store, "accent", "ÉCOLE İSTANBUL");

  const ascii = store.searchBatch({ terms: ["ascii", "i"], match: "any" });
  assert.deepEqual(ascii.map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [
    ["accent", []],
    ["ascii", ["ascii", "i"]],
  ]);
  assert.deepEqual(store.searchBatch({ terms: ["école"], match: "any" })
    .map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [["accent", []], ["ascii", []]]);
  assert.deepEqual(store.searchBatch({ terms: ["İSTANBUL"], match: "any" })
    .map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [["accent", ["İSTANBUL"]], ["ascii", []]]);
});

test("finds and returns content after embedded NUL characters", async (t) => {
  const { store } = await createStore(t);
  const content = "prefix\0 AFTER NUL and café";
  save(store, "nul-content", content);

  const rows = store.searchBatch({ terms: ["after nul", "\0 after"], match: "any" });
  assert.equal(rows.length, 1);
  assert.equal(rows[0]?.memory.content, content);
  assert.deepEqual(rows[0]?.matchedTerms, ["after nul", "\0 after"]);
});

test("uses BINARY ID keyset order across Unicode and NUL IDs", async (t) => {
  const { store } = await createStore(t);
  const ids = ["中", "a\0z", "é", "a", "b", "a\0", "a\0A"];
  for (const id of ids) save(store, id, "needle");

  const seen = [];
  let after;
  while (true) {
    const page = store.searchBatch({
      terms: ["needle"], match: "any", limit: 2,
      ...(after === undefined ? {} : { after }),
    });
    seen.push(...page.map(({ memory }) => memory.id));
    if (page.length < 2) break;
    assert.notEqual(page.at(-1).memory.id, after, "Pagination must advance the cursor rather than repeat a page");
    after = page.at(-1).memory.id;
  }
  assert.deepEqual(seen, ["a", "a\0", "a\0A", "a\0z", "b", "é", "中"]);
  assert.deepEqual(store.searchBatch({ terms: ["needle"], match: "any", after: "a\0", limit: 2 })
    .map(({ memory }) => memory.id), ["a\0A", "a\0z"]);
});

test("bounds each raw scan before evaluating match predicates and defaults to 128 rows", async (t) => {
  const { store } = await createStore(t);
  for (let index = 0; index < 130; index += 1) {
    save(store, `id-${String(index).padStart(3, "0")}`, index === 2 ? "needle" : "noise");
  }

  const first = store.searchBatch({ terms: ["needle"], match: "any", limit: 2 });
  assert.deepEqual(first.map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [
    ["id-000", []],
    ["id-001", []],
  ]);
  const second = store.searchBatch({
    terms: ["needle"], match: "any", after: first.at(-1).memory.id, limit: 2,
  });
  assert.deepEqual(second.map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [
    ["id-002", ["needle"]],
    ["id-003", []],
  ]);
  assert.equal(store.searchBatch({ terms: ["needle"], match: "any" }).length, 128);
});

test("rejects malformed requests and accepts the 128-code-point boundary", async (t) => {
  const { store } = await createStore(t);
  const validTerms = ["a"];
  const invalidRequests = [
    null,
    [],
    { terms: validTerms, match: "any", extra: true },
    { terms: [], match: "any" },
    { terms: "term", match: "any" },
    { terms: [""], match: "any" },
    { terms: [" \t\n"], match: "any" },
    { terms: ["\uD800"], match: "any" },
    { terms: ["😀".repeat(129)], match: "any" },
    { terms: Array(9).fill("a"), match: "any" },
    { terms: validTerms },
    { terms: ["a"], match: "sometimes" },
    { terms: ["a"], match: "any", after: " " },
    { terms: ["a"], match: "any", after: "\uDFFF" },
    { terms: ["a"], match: "any", limit: 0 },
    { terms: ["a"], match: "any", limit: 129 },
    { terms: ["a"], match: "any", limit: 1.5 },
    { terms: ["a"], match: "any", limit: NaN },
    { terms: ["a"], match: "any", limit: undefined },
    { terms: ["a"], match: "any", [Symbol("unknown")]: true },
  ];
  for (const request of invalidRequests) {
    assert.throws(() => store.searchBatch(request), TypeError, `${String(request)}`);
  }

  save(store, "max-term", "😀".repeat(128));
  assert.deepEqual(store.searchBatch({ terms: ["😀".repeat(128)], match: "any" })
    .map(({ memory }) => memory.id), ["max-term"]);
});

test("reflects updates after reopening and excludes forgotten memories", async (t) => {
  const fixture = await createStore(t);
  let store = fixture.store;
  save(store, "mutable", "old wording");
  store.update({
    id: "mutable",
    expectedRevision: 1,
    content: "new wording",
    updatedAt: "2026-10-22T00:00:00.000Z",
  });
  store = fixture.reopen();

  assert.deepEqual(store.searchBatch({ terms: ["old"], match: "any" })
    .map(({ memory, matchedTerms }) => [memory.id, matchedTerms]), [["mutable", []]]);
  assert.deepEqual(store.searchBatch({ terms: ["new"], match: "any" })
    .map(({ memory }) => [memory.revision, memory.updatedAt]), [[2, "2026-10-22T00:00:00.000Z"]]);

  store.forget({ id: "mutable", expectedRevision: 2 });
  store = fixture.reopen();
  assert.deepEqual(store.searchBatch({ terms: ["new"], match: "any" }), []);
});
