# pi-distill

English | [简体中文](docs/i18n/README.zh-CN.md)

**Adaptive memory and context for Pi.**

pi-distill is an extension for [Pi](https://github.com/earendil-works/pi) that brings memory into everyday conversations.

![Conversations become memories that supply context to future Pi sessions.](docs/assets/pi-distill-workflow.svg)

## Vision

Start a fresh session and move freely between topics and projects. Pi brings useful knowledge from earlier conversations into the work at hand.

pi-distill aims to:

- **Remember useful knowledge:** distill decisions, lessons, preferences, and task state from conversations, including history that has been compacted.
- **Bring back relevant context:** use the current message and recent conversation to bring back helpful memories.
- **Keep memory current:** incorporate corrections, replace outdated conclusions, resolve duplicates, and distinguish completed, paused, and unresolved work.
- **Make memory understandable:** explain which memories were used, why they were selected, and when they were extracted or updated. Let users inspect, correct, and forget them.
- **Let chats go:** delete chat history while retaining distilled memories and the metadata needed to manage them.

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

### Keep the knowledge, let the chats go

Distilled memories carry useful knowledge after the original conversations are deleted. Chat cleanup follows your retention policy, removing the original chats and managed copies after processing is complete and the sessions are inactive.

### Design principles

- Store and manage memory locally.
- Apply memories in the context of the current request.
- Preserve conditions, uncertainty, and alternative conversation branches.

## Available today

- Pi extension loading and reload through Pi's normal extension system.
- A local SQLite store for saving, reading, correcting, and forgetting memories.
- Agentic memory search through `memory_search` and `memory_read`.

### Agentic memory search

Pi's current model can choose search terms, try alternate expressions and languages, inspect candidates, and read their current content before judging applicability. SQLite performs literal matching across the personal memory store. Memories remain searchable across projects, with their original context available for interpretation.

Search results include original text previews, with pagination and full reads for further exploration. See [Agentic memory search](docs/agentic-search.md) for the tool contracts and retrieval behavior.
