# 安装与本地数据

[English](../installation.md) | 简体中文

pi-distill 是一个 Pi 扩展。通过 `pi` 或 `/new` 开始新 session，让相关记忆在不同对话间延续知识。使用自动清理前，请阅读[旧 session 操作清单](distillation-and-cleanup.zh-CN.md#自动清理期间应避免的操作)，尤其是在多个终端同时运行 Pi 时。

## 环境要求

- Node.js **22.19.0 或更新版本**。
- Pi **1.3.0**，本包已验证的集成版本。
- 在 Pi 中配置可用于对话和后台提炼的模型。

检查已安装的工具：

```sh
node --version
pi --version
```

Pi 通过 `pi install`、`pi list` 和 `pi remove` 管理扩展包。更多包管理选项见 Pi 的[包文档](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/packages.md)。

## 安装最新发布版

```sh
pi install git:github.com/dyxcode/pi-distill@v0.1.0
pi list
pi
```

打开新 session 后，使用 Pi 常规的模型选择和认证方式。

提炼使用领取任务的 Pi runtime 当前配置的模型，相关对话证据和检索到的记忆会发送给该模型服务商。搜索、存储和清理在本地执行。

## 安装本地 checkout

开发时，克隆仓库并注册它的目录：

```sh
git clone https://github.com/dyxcode/pi-distill.git
cd pi-distill
npm ci
pi install "$PWD"
pi list
pi
```

Pi 从该路径加载本地包；注册期间请保留 checkout。若 Pi 已打开，可通过 `/reload` 发现新注册的包。代码变化会在 reload 或重启后加载；切换版本时，请遵循下方的升级步骤。

为 pi-distill 保留一个安装来源。在 checkout 和发布版之间切换时，先移除 `pi list` 中的旧来源，再安装新来源。对于 checkout，向 `pi remove` 传入其绝对路径。这可以避免不同来源分别加载同一扩展。

## 多终端升级

1. 完成当前轮次，并退出所有使用同一个 agent 目录的 Pi 进程。退出会记录待办工作，交给之后的 runtime 处理。
2. 更新安装来源。
3. 重新启动 Pi。已持久化的记忆和待办任务仍保存在 agent 目录中。

升级时，使用 [Releases](https://github.com/dyxcode/pi-distill/releases) 中最新发布版的 tag：

```sh
pi install git:github.com/dyxcode/pi-distill@RELEASE_TAG
```

运行前替换 `RELEASE_TAG`。Pi 通过仓库识别 Git 包，因此这会替换已配置的 ref。`pi update --extensions` 会按已配置的 ref 同步安装的包。

对于 checkout，在这些 Pi 进程退出后更新：

```sh
git pull --ff-only
npm ci
```

## 本地数据

Pi 的 agent 目录默认为 `~/.pi/agent/`。设置 `PI_CODING_AGENT_DIR` 可以选择其他 agent 目录及独立的记忆库。

| Agent 目录下的位置 | 用途 |
| --- | --- |
| `distill/memories.sqlite` | 记忆、待办工作、证据身份及协调状态 |
| `distill/diagnostics.sqlite` | 有大小限制的本地诊断记录；记录问题时创建 |
| `sessions/` | Pi 默认的聊天历史位置；可通过 Pi 的 session 目录设置更改 |

请将 agent 目录放在 Git 仓库和 Obsidian vault 之外。pi-distill 会检查数据库路径是否满足这些边界。

Session 结束或压缩成功后会记录提炼任务，由运行中的 Pi 处理；最后一个 Pi 退出后，剩余任务在之后启动时继续。Cleanup 在处理和使用检查通过后删除符合条件的受管理源聊天，保留记忆及所需 metadata。处理时机、依赖关系和完整操作清单见[记忆提炼与聊天清理](distillation-and-cleanup.zh-CN.md)。

### 诊断日志

诊断日志自动运行，保存在本机。它记录操作失败、因限制而无法推进的工作，以及已记录问题的恢复。

记录范围为操作元数据：时间、扩展/Pi/Node 版本、操作阶段、固定错误类别与错误码，以及可获得的耗时或重试提示。任务、来源和模型标识使用带密钥的哈希。

最多保留 2,048 条记录，并在后续活动中清理七天前的记录。SQLite 数据库上限为 8 MiB，另有大小受限的回滚日志。同一问题在 30 秒内的重复记录会被抑制；该问题下一次被记录或恢复时，会包含当前 runtime 累计抑制的次数。Pi 退出时，这些内存中的计数可能丢失。

诊断存储忙碌或不可用时会跳过写入，应用继续运行。日志反映成功记录下来的故障与恢复信息；问题关联也受保留限制约束。

## 移除扩展

使用 `pi list` 中显示的完整来源。例如：

```sh
pi remove git:github.com/dyxcode/pi-distill@v0.1.0
```

退出并重新启动运行中的 Pi，以卸载它们的 worker。移除包会保留 agent 目录中的记忆、待办任务、诊断日志和剩余 sessions。重新安装扩展并使用相同 agent 目录启动 Pi 后，pi-distill 的待办工作可以继续。

## 许可证

pi-distill 使用 [MIT 许可证](../../LICENSE)。

返回[产品概览](README.zh-CN.md)。
