# Installation and local data

English | [简体中文](i18n/installation.zh-CN.md)

pi-distill is a Pi extension. Start a fresh session with `pi` or `/new`; relevant memories carry knowledge between conversations. Read the [old-session operation checklist](distillation-and-cleanup.md#operations-to-avoid-with-automatic-cleanup) before using automatic cleanup, especially when running multiple terminals.

## Requirements

- Node.js **22.19.0 or later**.
- Pi **1.3.0**, the integration version verified for this package.
- A model configured in Pi for conversation and background distillation.

Check the installed tools:

```sh
node --version
pi --version
```

Pi manages extension packages through `pi install`, `pi list`, and `pi remove`. See Pi's [package documentation](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/packages.md) for more package options.

## Install the latest release

```sh
pi install git:github.com/dyxcode/pi-distill@v0.1.0
pi list
pi
```

Start with an empty session and use Pi's usual model selection and authentication.

Distillation uses the current model of the Pi runtime claiming the task. Relevant conversation evidence and retrieved memories are sent to that configured model provider. Search, storage, and cleanup operate locally.

## Install a checkout

For development, clone the repository and register its directory:

```sh
git clone https://github.com/dyxcode/pi-distill.git
cd pi-distill
npm ci
pi install "$PWD"
pi list
pi
```

Pi loads a local package from that path. Keep the checkout there while it is registered. If Pi is already open, `/reload` discovers a newly registered package. Source changes are picked up on reload or restart; the upgrade procedure below applies when changing versions.

Use one installation source for pi-distill. When switching between a checkout and a release, remove the old source shown by `pi list` before installing the new one. For a checkout, use its absolute path with `pi remove`. This prevents duplicate extension instances from different source types.

## Update across terminals

1. Finish the current turns and exit every Pi process using the same agent directory. Exit captures pending work for a later runtime.
2. Update the installation source.
3. Start Pi again. Persisted memories and pending tasks remain in the agent directory.

To upgrade, use the latest published tag from [Releases](https://github.com/dyxcode/pi-distill/releases):

```sh
pi install git:github.com/dyxcode/pi-distill@RELEASE_TAG
```

Replace `RELEASE_TAG` before running the command. Pi identifies Git packages by repository, so this replaces the configured ref. `pi update --extensions` reconciles installed packages at their configured refs.

For a checkout, update it while those Pi processes are closed:

```sh
git pull --ff-only
npm ci
```

## Local data

Pi's agent directory defaults to `~/.pi/agent/`. Setting `PI_CODING_AGENT_DIR` selects a different agent directory and a separate memory store.

| Location under the agent directory | Purpose |
| --- | --- |
| `distill/memories.sqlite` | Memories, pending work, evidence identities, and coordination state |
| `distill/diagnostics.sqlite` | Bounded local diagnostic records, created when an issue is recorded |
| `sessions/` | Pi's default chat history location; Pi's session-directory settings can override it |

Keep the agent directory outside Git repositories and Obsidian vaults. pi-distill validates its database locations against those boundaries.

Session closure and successful compaction record pending distillation work. A live Pi processes it; when the last Pi exits, remaining work resumes at a later startup. Cleanup deletes eligible managed source chats after processing and usage checks. It retains memories and their required metadata. See [Distillation and cleanup](distillation-and-cleanup.md) for timing, dependency handling, and the complete operation checklist.

### Diagnostics

Diagnostic logging runs automatically and stays on your machine. It records operation failures, work blocked by limits, and recovery from previously recorded issues.

Records are limited to operational metadata: time, package/Pi/Node versions, the operation stage, fixed error categories and codes, and elapsed time or retry hints where available. Task, source, and model identifiers use keyed hashes.

The recorder keeps up to 2,048 records and removes records older than seven days during later activity. The SQLite database is capped at 8 MiB, with a separate bounded rollback journal. Repeated issues are suppressed for 30 seconds per matching issue; the next recorded occurrence or recovery includes that runtime's suppressed count. Those in-memory counts may be lost when Pi exits.

When diagnostic storage is busy or unavailable, the recorder skips the write and the application continues. Logs describe the failures and recoveries that were successfully recorded; retention limits also apply to issue correlations.

## Remove the extension

Use the exact source shown by `pi list`. For example:

```sh
pi remove git:github.com/dyxcode/pi-distill@v0.1.0
```

Exit and restart running Pi instances to unload their workers. Removing the package leaves memories, pending tasks, diagnostics, and remaining sessions in the agent directory. Pending pi-distill work resumes if you reinstall the extension and start Pi with the same agent directory.

## License

pi-distill uses the [MIT License](../LICENSE).

Return to the [product overview](../README.md).
