# pi-distill

English | [简体中文](docs/i18n/README.zh-CN.md)

**Keep the knowledge. Start fresh.**

pi-distill gives [Pi](https://github.com/earendil-works/pi) memory across conversations. It distills useful knowledge from your chats, retrieves relevant memories for later work, and cleans up processed chat history.

Use a session for whatever you are working on: several topics, several projects, or a quick question. Open a fresh session whenever you want. Decisions, preferences, lessons, and unfinished work can carry forward through memory.

## Install

Requires Node.js **22.19.0+** and Pi. Integration is verified with **Pi 1.3.0**.

```sh
pi install git:github.com/dyxcode/pi-distill@v0.1.0
pi
```

Choose your configured model in Pi and start talking. In an already-open Pi, `/reload` loads the newly installed extension.

For checkout installation, upgrades, and removal, see the [installation guide](docs/installation.md).

## Everyday use

Work with Pi as usual. For example, you might tell it:

> For Cedar, keep logs for 14 days normally and 30 days during an incident investigation.

After that conversation has been distilled, a fresh session can retrieve the policy when you work on Cedar again. If the policy changes, explain the change in conversation:

> Cedar now keeps logs for 21 days normally. The incident rule stays the same.

That correction becomes evidence for updating the memory. Memories keep the conditions that make a fact useful, and the model judges their relevance across projects and topics.

Use `/new` to start another conversation, or exit Pi and run `pi` again. Several terminals can share the same personal memory store.

**Fresh sessions are the intended workflow.** Automatic cleanup can overlap Pi's native access to old session files. Follow the [old-session operation checklist](docs/distillation-and-cleanup.md#operations-to-avoid-with-automatic-cleanup) when working with saved chats.

## How memory works

1. **Distill.** Ending a session or successfully compacting it records work for background processing. A running Pi uses its configured model to extract useful knowledge and create, update, or remove memories.
2. **Recall.** During later conversations, Pi's model chooses keywords with `memory_search`, reads candidates with `memory_read`, and decides which memories apply. Search and read results appear in Pi's tool cards.
3. **Clean up.** Once a managed chat is fully processed, no longer in use, and free of unresolved dependencies, cleanup deletes its source file. The distilled memories remain available.

New knowledge becomes searchable after distillation saves it. Exit records pending work quickly; another running Pi can continue it, or processing resumes when you next start Pi.

## Your data

Memories and pending work live in local SQLite storage, normally under `~/.pi/agent/distill/`. A fresh session uses that shared store. Background processing also resumes from it after a restart.

Distillation sends relevant conversation evidence and retrieved memories to **the model provider configured in Pi**. Search and cleanup run locally. Diagnostic records of failures, blocked work, and recovery stay on your machine; their contents and retention limits are described in the [local data guide](docs/installation.md#local-data).

## Product vision

Sessions are temporary workspaces; useful knowledge should remain available as your work moves between them. The aim is memory maintained through ordinary conversation, with its context and uncertainty intact.

![Product vision: conversations become memories that supply context to future Pi sessions.](docs/assets/pi-distill-workflow.svg)

*Product vision. The memory inspector shown here is planned: it would explain which memories were used, where they came from, and why they were selected.*

## Read more

- [Installation and local data](docs/installation.md) — setup, updates, removal, and diagnostics.
- [Distillation and cleanup](docs/distillation-and-cleanup.md) — processing timing, multiple terminals, recovery, and session handling.
- [Agentic memory search](docs/agentic-search.md) — keyword selection, literal matching, and reading evidence.

## License

[MIT](LICENSE).
