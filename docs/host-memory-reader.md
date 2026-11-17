# Read memories from a local host

English | [简体中文](i18n/host-memory-reader.zh-CN.md)

A host such as Dwell can search and read Distill memories through `pi-distill/host-storage` without starting Pi or calling a model. Distill owns the matching rules, cursors, versions and database access. The host owns user authentication and presentation.

## Open the reader

Build the package with `npm run build:package`, then import `openMemoryReaderV1` from `pi-distill/host-storage`. Pass the absolute path of an existing Distill database. The returned reader has `version: 1`, `search`, `read` and `close` operations. Its connection is read-only: opening does not create a store, migrate a schema, schedule work or change memories. A compatible existing schema is required; ordinary Distill store initialization owns upgrades.

The existing `openHostStorageV1` API continues to coordinate session-source reads and use by live processes. A host that needs both capabilities opens each against the same store and closes both on shutdown.

## Search and read

`search({query, limit?, cursor?})` treats the trimmed query as one literal substring. Matching follows the existing SQLite search behavior: ASCII case-insensitive matching, with other characters matched literally. The query accepts up to 128 Unicode code points. `limit` is 1–20 and defaults to 20.

A search page returns `items`, each with an `id`, `revision` and a matching `preview` of up to 500 Unicode code points. It scans at most 128 stored rows in ID order. When more rows remain, `nextCursor` continues from the last examined row. A page can contain no matches and still have a cursor: continue before reporting that the entire search found nothing. Pages observe current committed data; an edit or insertion behind the cursor appears in a new search.

`read({id, expectedRevision, cursor?})` returns `id`, `revision`, `text` and an optional `nextCursor`. Each page contains up to 8,000 Unicode code points. Use the result's revision from search for every page. A changed memory reports `memory_changed`; a removed memory reports `memory_not_found`. Refresh search to select the current version rather than joining pages from different versions.

Cursors are opaque and bound to the operation, query or memory revision, and reader instance. Reopening the reader requires a new search/read. Keep the query unchanged when continuing. Invalid or mismatched cursors report `invalid_cursor`; malformed requests report `invalid_input`. Store access failures report `memory_unavailable`. IDs are nonempty Unicode text up to 512 code points without NUL. Calling `close` repeatedly is safe.

## Host integration

Expose these operations only to authenticated users of the local host. Render memory text as content, and keep browsing separate from context injection. A human opening a memory does not perform a Pi recall or add the text to a conversation. The same reader can observe updates committed by ordinary Pi sessions while those sessions continue working.
