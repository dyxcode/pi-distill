# pi-distill

English | [简体中文](docs/i18n/README.zh-CN.md)

**Adaptive memory and context for Pi.**

pi-distill is a planned extension for [Pi](https://github.com/earendil-works/pi), designed to carry useful knowledge across conversations. Start a fresh session, talk naturally, and continue with relevant background already available.

![pi-distill workflow: conversations become memories, which supply relevant context to Pi; chat history can be deleted after processing and policy checks.](docs/assets/pi-distill-workflow.svg)

## Intended experience

Conversations can cover multiple topics. There is no need to name sessions, organize them by project, or manually request a memory search.

pi-distill aims to:

- **Remember useful knowledge:** distill decisions, lessons, preferences, and task state from conversations, including history that has been compacted.
- **Bring back relevant context:** use the current message and recent conversation to select helpful memories before Pi answers, within context and latency limits.
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
(after durable processing,
 inactivity checks, and explicit policy)
```

## Memory without a chat archive

The goal is to retain useful knowledge after removing the conversations it came from. Chat deletion should also remove managed raw copies, excerpts, and full-conversation summaries.

Deletion requires durable processing and an explicit policy that protects sessions still in use. After deletion, the original chats are unavailable for verification, making memory inspection and correction essential.

## Design principles

- Keep memory storage and management local.
- Give current user instructions priority over remembered preferences.
- Preserve uncertainty and distinguish alternative conversation branches.
- Keep ordinary Pi conversation usable when memory retrieval or consolidation fails.
