# Agentic memory search：由模型主动检索记忆

[English](../agentic-search.md) | 简体中文

Pi 当前使用的模型决定搜索什么、读取结果，并判断哪些记忆有助于当前任务。扩展通过个人 SQLite memory store 提供两个工具：

- `memory_search` 执行字面匹配，返回候选记忆的预览。
- `memory_read` 读取所选记忆的当前内容。

记忆可以跨项目检索。模型结合记忆原有的条件、不确定性和项目背景，判断它是否适用。

## 搜索如何进行

```text
当前任务
   |
   v
选择搜索词 <---------------------+
   |                            |
   v                            |
搜索 SQLite --> 检查预览         |
                   |            |
                   v            |
               读取所选记忆      |
                   |            |
                   v            |
               判断适用性 -------+ 调整查询
                   |
                   v
                 使用证据
```

当以往的偏好、决策或任务背景可能有帮助时，Pi 会搜索记忆。它生成不同表达，检查候选，并根据新发现继续检索。

例如，回答测试应该放在哪里时，Pi 可以一起搜索 `测试`、`单元测试`、`unit test` 和 `tests`。如果结果提到发布规则，下一次查询可以使用 `GitHub`、`提交` 或 `push`。

用 `match: "any"` 搜索不同表达；用 `match: "all"` 要求同一条记忆包含所有搜索词。多次查询共同形成以 memory ID 去重的候选集。工具会报告本轮运行中返回的不同候选数量。

预览展示匹配位置附近的原文。Pi 可以继续读取，了解被省略的条件和例外，再结合当前用户指令应用相关证据。已完整展示的短记忆可以直接使用。

## `memory_search`

```json
{
  "terms": ["测试", "unit test", "tests"],
  "match": "any",
  "limit": 20
}
```

| 输入 | 含义 |
| --- | --- |
| `terms` | 1–8 个非空字面字符串，每个最多 128 个 Unicode 码点 |
| `match` | 默认为 `any`；`all` 要求记录包含每个不同的搜索词 |
| `limit` | 请求的候选数量，默认 20，最多 50 |
| `cursor` | 用于继续读取同一查询后续页面的游标 |

匹配时忽略 ASCII 字母大小写，原始内容保持不变。等价搜索词会去重。有意义的空格、标点、`%`、`_` 和正则表达式符号均按字面匹配。同义词、词形变化和其他语言的表达由 Pi 提供。

SQLite 为每个绑定参数计算匹配标记：

```sql
SELECT instr(lower(content), lower(?)) > 0 AS matches_term
```

工具根据 `any` 或 `all` 选择候选，并按稳定的 ID 顺序返回。每条候选包含 ID、revision、匹配的搜索词，以及带有省略标记的原文预览。

响应还包含：

| 字段 | 含义 |
| --- | --- |
| `hasMore`、`nextCursor` | 使用返回的游标继续扫描，包括当前页候选为空的情况 |
| `stopReason` | 停止原因：搜索完成、候选数量上限、输出上限或扫描时限 |
| `uniqueCandidates` | 本轮运行中返回的不同 memory ID 数量 |
| `remaining` | 剩余调用次数与响应字节预算 |

继续查询时，传入 `nextCursor`，并保持规范化后的搜索词和匹配模式一致。每次调用可以调整页面大小。若一条匹配记录无法放入当前页，下一页会重新尝试返回它。重新加载扩展会使游标失效。

一次完成的搜索覆盖所提供的表达。Pi 可以扩展这些表达，查找措辞不同的记忆，也可以增加条件，缩小噪声较多的结果集。

## `memory_read`

```json
{
  "id": "memory-example",
  "expectedRevision": 3
}
```

读取结果包含原文、ID、revision、创建时间和更新时间。长记忆分块返回，分块边界保留完整的 Unicode 字符。继续读取时，传入返回的 `nextCursor`，并保持 ID 和 revision 一致；按顺序拼接各块即可还原原文。

| 结果 | 后续操作 |
| --- | --- |
| `ok` | 阅读内容；若 `hasMore` 为 true，则继续读取 |
| `stale` | 使用 `currentRevision` 重新开始读取，或重新搜索 |
| `not_found` | 记忆已被删除，使用其他可用证据 |

读取保持已存储的内容和 metadata 不变。删除后重新使用同一 ID 会创建 revision 为 1 的新记录；revision 检查适用于一条记录的存续期间。

## 预算与继续读取

| 资源 | 上限 |
| --- | --- |
| 搜索候选 | 每页默认 20 条，最多 50 条 |
| 序列化后的工具结果 | 16 KiB |
| 完整内容的分块 | 最多 8 KiB，并根据结果大小调整 |
| 执行搜索或读取的调用 | 每轮 Pi agent 运行 12 次 |
| 上述调用的累计响应 | 每轮运行 64 KiB |
| SQLite 扫描批次 | 每批最多检查 128 行 |
| 协作式扫描时限 | 每次搜索调用 250 ms |

服务按完整序列化工具结果的 UTF-8 字节数计量，包括 content、details 和错误字段。它为首次预算耗尽响应预留 512 字节。后续调用返回最多 256 字节的拒绝响应，并停止数据库操作。

输出预算可能使页面中的候选数量少于请求值。扫描在 SQLite 批次之间检查取消信号和时限，并将执行权交还事件循环。单次同步操作或锁等待可能超过协作式时限。

| 状态 | 后续操作 |
| --- | --- |
| `invalid_input` | 修正参数 |
| `result_too_large` | 停止本次操作，必需的 metadata 已超过结果预算 |
| `budget_exhausted`、`unavailable`、`cancelled` | 使用已有证据继续对话 |

## 存储与 Pi 集成

数据库位于 Pi agent 目录下的 `distill/memories.sqlite`，通常为 `~/.pi/agent/distill/memories.sqlite`。扩展会验证自定义位置和符号链接，使运行时存储位于代码仓库和笔记库之外。

首次调用记忆工具时打开存储。存储不存在时初始化空库；存储损坏或无法访问时返回 `unavailable`。结果显示在 Pi 常规工具界面中。`agent_start` 重置预算，`session_shutdown` 关闭扩展持有的资源。

每个批次读取当前已提交的记录。发生并发修改后，重新搜索可以再次检查之前经过的 ID；读取选中的记忆时会检查预期 revision。

## 评估检索效果

分别评估候选召回率和模型的最终选择。合成案例覆盖不同措辞、中英文查询、后续页面中的目标、常见词噪声、带有适用条件的跨项目记忆，以及无关输入。

记录预算内召回的相关 ID、哪些记忆影响了回答、干扰项被误用的情况、工具调用次数、返回字节数和延迟。将遗漏归因于查询措辞、字面覆盖、提前终止、上下文省略或最终判断。这些结果用于改进提示词、预算和检索方法。

## 参考资料

- [LangChain MultiQueryRetriever](https://reference.langchain.com/python/langchain-classic/retrievers/multi_query/MultiQueryRetriever)：生成多个查询，并对结果取去重后的并集。
- [Anthropic: Effective context engineering for AI agents](https://www.anthropic.com/research/effective-context-engineering-for-ai-agents)：按需检索与逐步发现。
- [Anthropic: Writing effective tools for agents](https://www.anthropic.com/research/writing-tools-for-agents)：有用的工具结果、分页，以及 agent 行为评估。
- [SQLite core functions](https://www.sqlite.org/lang_corefunc.html)：`instr` 子串匹配与 `lower` 的 ASCII 大小写转换。
