# pi-distill

[English](../../README.md) | 简体中文

**为 Pi 提供自适应记忆与上下文。**

pi-distill 是一个 [Pi](https://github.com/earendil-works/pi) 扩展，让记忆融入日常对话。

![从对话中提炼记忆，为之后的 Pi session 提供上下文。](../assets/pi-distill-workflow.svg)

## 产品愿景

开启新 session，自由切换话题和项目。Pi 将此前对话中的有用知识带回当前工作。

pi-distill 的目标是：

- **记住有用的知识：** 从对话中提炼决策、经验、偏好和任务状态，也涵盖经过上下文压缩（compaction）的历史。
- **带回相关上下文：** 根据当前消息和近期对话，带回有帮助的记忆。
- **让记忆保持准确：** 接纳修正、替换过时结论、合并重复内容，并区分已完成、暂缓和待解决的工作。
- **让记忆可以理解：** 解释本次使用了哪些记忆、为什么选中，以及何时提炼或更新；允许用户查看、纠正和删除记忆。
- **让聊天历史可以删除：** 保留提炼后的记忆及管理它们所需的 metadata，同时删除聊天历史。

```text
Conversations --> Distill --> Memories
      |                         |
      |                         v
      |       New input --> Select context --> Pi responds
      |                         |
      |                         v
      |                    Memory inspector
      |                    - What was used?
      |                    - Why was it selected?
      v
Delete chat history
```

### 保留知识，放下聊天

删除原始对话后，提炼出的记忆继续保留有用的知识。聊天清理遵循你的保留策略，在处理完成、session 不再活跃后，移除原始聊天及其管理副本。

### 设计原则

- 在本地存储和管理记忆。
- 结合当前请求应用记忆。
- 保留条件、不确定性和不同对话分支。

## 当前能力

- 通过 Pi 自带的扩展机制加载和 reload。
- 本地 SQLite memory store，支持保存、读取、修正和删除记忆。
- 通过 `memory_search` 和 `memory_read` 进行 agentic memory search。

### Agentic memory search：由模型主动检索记忆

Pi 当前使用的模型可以选择关键词、尝试不同措辞和语言、检查候选，并在读取当前内容后判断适用性。SQLite 在个人 memory store 中执行字面匹配。记忆可以跨项目检索，并保留原有背景，帮助模型判断适用性。

搜索结果包含原文片段，模型可以通过分页和完整读取继续探索。工具接口与检索行为见 [Agentic memory search](agentic-search.zh-CN.md)。
