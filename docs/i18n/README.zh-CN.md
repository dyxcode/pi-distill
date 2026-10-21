# pi-distill

[English](../../README.md) | 简体中文

**为 Pi 提供自适应记忆与上下文。**

pi-distill 是一个规划中的 [Pi](https://github.com/earendil-works/pi) 扩展，旨在让有用的知识跨对话延续。开启新 session，自然地交流，继续工作时就能获得相关背景。

![pi-distill 工作流程：从对话中提炼记忆，为 Pi 提供相关上下文；完成处理并满足删除策略后，可以删除聊天历史。](../assets/pi-distill-workflow.svg)

## 预期体验

一段对话可以涉及多个话题。你无需为 session 命名、按项目整理对话，也无需主动要求搜索记忆。

pi-distill 的目标是：

- **记住有用的知识：** 从对话中提炼决策、经验、偏好和任务状态，也涵盖经过上下文压缩（compaction）的历史。
- **带回相关上下文：** 根据当前消息和近期对话，在 Pi 回答前选出有帮助的记忆，同时控制上下文大小和检索耗时。
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
(after durable processing,
 inactivity checks, and explicit policy)
```

## 保留记忆，无需归档聊天

目标是在删除原始对话后，仍保留其中有用的知识。删除聊天时，也应移除系统管理的原文副本、摘录和完整对话摘要。

删除必须以处理结果已可靠保存为前提，并遵循明确的策略，保护仍在使用中的 session。原始聊天删除后便无法用于核查，因此查看和纠正记忆是必要能力。

## 设计原则

- 在本地存储和管理记忆。
- 当前用户指令优先于记忆中的偏好。
- 保留不确定性，区分不同对话分支。
- 检索或记忆整理失败时，Pi 的普通对话仍应正常进行。
