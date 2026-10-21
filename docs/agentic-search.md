# Agentic memory search

English | [简体中文](i18n/agentic-search.zh-CN.md)

Pi's current model chooses what to search, reads the results, and decides which memories help with the current task. The extension provides two tools backed by the personal SQLite store:

- `memory_search` finds literal text matches and returns candidate previews.
- `memory_read` retrieves a selected memory's current content.

Memories remain searchable across projects. Their original conditions, uncertainty, and project context help the model judge applicability.

## How search works

```text
Current task
     |
     v
Choose search terms <-------------------+
     |                                  |
     v                                  |
Search SQLite --> Inspect previews      |
                       |                |
                       v                |
                Read selected memories  |
                       |                |
                       v                |
                Assess applicability ---+ Refine query
                       |
                       v
                  Use the evidence
```

Pi searches when earlier preferences, decisions, or task context could help. It generates alternate expressions, examines candidates, and follows up as it learns more.

For a question about where tests belong, it might search `测试`, `单元测试`, `unit test`, and `tests` together. If a result mentions publication rules, the next query might use `GitHub`, `提交`, or `push`.

Use `match: "any"` for alternate expressions and `match: "all"` for terms that are all required in the record. Multiple queries contribute to a candidate set keyed by memory ID. The tool reports the unique candidate count for the current run.

Previews show original text around a match. Pi reads further to understand omitted conditions and exceptions, then applies the relevant evidence under the current user instructions. A short memory shown in full can be used directly.

## `memory_search`

```json
{
  "terms": ["测试", "unit test", "tests"],
  "match": "any",
  "limit": 20
}
```

| Input | Meaning |
| --- | --- |
| `terms` | 1–8 nonempty literal strings, each up to 128 Unicode code points |
| `match` | `any` by default; `all` requires every distinct term in the record |
| `limit` | Requested candidate count: default 20, maximum 50 |
| `cursor` | Continuation from an earlier page of the same query |

Matching folds ASCII letter case and preserves the original content. Equivalent terms are deduplicated. Meaningful spaces, punctuation, `%`, `_`, and regex symbols are matched literally. Pi supplies synonyms, inflections, and alternate languages.

SQLite computes a match flag for each bound term:

```sql
SELECT instr(lower(content), lower(?)) > 0 AS matches_term
```

The tool selects candidates using `any` or `all` and returns them in stable ID order. Each candidate includes its ID, revision, matched terms, and source preview with omission flags.

The response also includes:

| Field | Meaning |
| --- | --- |
| `hasMore`, `nextCursor` | Continue scanning with the returned cursor, including after an empty candidate page |
| `stopReason` | Search exhausted, candidate limit, output limit, or scan deadline |
| `uniqueCandidates` | Number of distinct memory IDs returned during the run |
| `remaining` | Remaining call and response-byte budgets |

Pass `nextCursor` back with the same normalized terms and match mode. The page size can change between calls. When a matching record does not fit the current page, the next page retries that record. Reloading the extension expires its cursors.

A completed search covers the supplied expressions. Pi can expand those expressions to find memories written differently, or add constraints to narrow a noisy result set.

## `memory_read`

```json
{
  "id": "memory-example",
  "expectedRevision": 3
}
```

Read returns the original content, ID, revision, creation time, and update time. Long memories arrive in Unicode-safe chunks. Pass the returned `nextCursor` with the same ID and revision to continue; the chunks reconstruct the original text exactly.

| Result | Next step |
| --- | --- |
| `ok` | Read the content and continue if `hasMore` is true |
| `stale` | Restart the read using `currentRevision`, or search again |
| `not_found` | The memory has been deleted; use other available evidence |

Reads preserve the stored content and metadata. Recreating a deleted ID starts a new record at revision 1; revision checks apply within a record's lifetime.

## Budgets and continuation

| Resource | Limit |
| --- | --- |
| Search candidates | Default 20, maximum 50 per page |
| Serialized tool result | 16 KiB |
| Full-content chunk | Up to 8 KiB, adjusted to fit the result |
| Search/read work calls | 12 per Pi agent run |
| Cumulative work responses | 64 KiB per run |
| SQLite scan batches | Up to 128 examined rows |
| Cooperative scan deadline | 250 ms per search call |

The service measures UTF-8 bytes of the complete serialized tool result, including content, details, and error fields. It reserves 512 bytes for the first exhaustion response. Later calls receive a refusal of at most 256 bytes and perform no database work.

The output budget can produce a page smaller than the requested candidate count. Scans check cancellation and the deadline between SQLite batches, yielding to the event loop. Individual synchronous operations and lock waits can take longer than the cooperative deadline.

| Status | Next step |
| --- | --- |
| `invalid_input` | Correct the arguments |
| `result_too_large` | Stop this operation; its required metadata exceeds the result budget |
| `budget_exhausted`, `unavailable`, `cancelled` | Continue the conversation using the evidence already available |

## Storage and Pi integration

The database lives at `distill/memories.sqlite` under Pi's agent directory, normally `~/.pi/agent/distill/memories.sqlite`. The extension validates custom locations and symlinks to keep runtime storage outside repositories and notes vaults.

Storage opens on the first memory call. Missing storage initializes an empty store; corrupt or inaccessible storage returns `unavailable`. Pi's normal tool UI displays results. Budgets reset at `agent_start`, and `session_shutdown` closes owned resources.

Each batch reads current committed records. Restart a search after concurrent changes to revisit earlier IDs; selected reads check the expected revision.

## Evaluating retrieval

Evaluate candidate recall and the model's final selection separately. Synthetic cases cover alternate wording, Chinese/English queries, targets on later pages, common-word noise, conditional cross-project memories, and unrelated input.

Record the relevant IDs retrieved within budget, which memories informed the answer, inappropriate use of distractors, tool calls, returned bytes, and latency. Classify misses by query wording, literal coverage, early termination, omitted context, or final judgment. These results guide changes to prompts, budgets, and retrieval methods.

## References

- [LangChain MultiQueryRetriever](https://reference.langchain.com/python/langchain-classic/retrievers/multi_query/MultiQueryRetriever): multiple generated queries and the unique union of their results.
- [Anthropic: Effective context engineering for AI agents](https://www.anthropic.com/research/effective-context-engineering-for-ai-agents): just-in-time retrieval and progressive discovery.
- [Anthropic: Writing effective tools for agents](https://www.anthropic.com/research/writing-tools-for-agents): useful tool results, pagination, and evaluation of agent behavior.
- [SQLite core functions](https://www.sqlite.org/lang_corefunc.html): `instr` substring matching and ASCII case folding with `lower`.
