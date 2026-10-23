# pi-distill

[English](../../README.md) | 简体中文

**为 Pi 提供自适应记忆与上下文。**

pi-distill 是一个 [Pi](https://github.com/earendil-works/pi) 扩展，让记忆融入日常对话。

## 产品愿景

![从对话中提炼记忆，为之后的 Pi session 提供上下文。](../assets/pi-distill-workflow.zh-CN.svg)

*产品愿景，包含规划中的记忆查看器。*

打开一个新 session，继续你的工作。pi-distill 希望让相关记忆随着对话自然延续。

Session 是临时的对话空间。你可以自由切换话题和项目，关闭 Pi 后重新开始，无需给对话命名，也无需寻找当初做出某个决定的 session。多个 terminals 可以承载不同的工作片段，共同使用个人 memory store。

扩展通过普通对话维护记忆，根据当前请求找回相关的决定、偏好、适用条件和未完成事项。你决定现在做什么，pi-distill 负责记住有用的内容，并清理旧聊天。

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

原始对话消失后，有用的知识继续保存在 memories 中。记忆处理完成、session 不再使用时，旧聊天便可以自动清理。新 session 根据相关记忆延续工作，连续性来自已经学到的内容，而非重新打开某个特定聊天。

### 设计原则

- 在本地存储和管理记忆。
- 结合当前请求应用记忆。
- 保留条件、不确定性和不同对话分支。

## 当前能力

### 自动提炼与清理

Session 结束或压缩成功后，pi-distill 会记录提炼任务。运行中的 Pi 使用模型在后台提炼知识，并根据对话证据创建、修正或删除记忆。退出时保存待办任务；最后一个 Pi 进程结束后，工作留到下次启动继续。

Cleanup 独立执行本地检查，自动删除处理完成、无人使用且依赖已解决的受管理聊天文件，保留记忆。刚结束的对话需要等待提炼提交后，相关知识才可检索。两个队列的处理时机与图示见[记忆提炼与聊天清理](distillation-and-cleanup.zh-CN.md)。

### Agentic memory search：由模型主动检索记忆

Pi 当前使用的模型通过 `memory_search` 选择关键词、尝试不同措辞和语言，再通过 `memory_read` 读取候选并判断适用性。SQLite 执行字面匹配；记忆可以跨项目检索，并保留原有条件和背景。

搜索和读取结果显示在 Pi 的工具卡片中。展开后可以查看搜索词、匹配预览、读取内容及记忆更新时间。工具接口与检索行为见 [Agentic memory search](agentic-search.zh-CN.md)。

### 本地存储与恢复

记忆和任务队列保存在本地 SQLite 中。扩展支持 Pi 常规加载与 reload，协调共享记忆库的多个 Pi 进程，并在重启后恢复已持久化的待办任务。提炼使用领取任务的 Pi 当前配置的模型，相关证据会发送给该模型服务商。

## 推荐用法

通过 `pi` 或 `/new` 开始新 session，让相关记忆延续工作。自动清理运行期间，将旧 session 文件交给扩展管理。[操作清单](distillation-and-cleanup.zh-CN.md#自动清理期间应避免的操作)列出应避免的 Pi 旧 session 命令和界面操作，并说明原因及当前 session 的不同边界。处理时机与图示见[记忆提炼与聊天清理](distillation-and-cleanup.zh-CN.md)。
