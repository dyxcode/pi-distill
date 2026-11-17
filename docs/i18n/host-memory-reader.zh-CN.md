# 从本地 Host 读取 Memory

[English](../host-memory-reader.md) | 简体中文

Dwell 等 Host 可以通过 `pi-distill/host-storage` 搜索和读取 Distill Memory，无需启动 Pi 或调用模型。Distill 负责匹配规则、cursor、版本和数据库访问；Host 负责用户认证和界面展示。

## 打开 Reader

运行 `npm run build:package` 构建后，从 `pi-distill/host-storage` 导入 `openMemoryReaderV1`，传入现有 Distill 数据库的绝对路径。返回对象提供 `version: 1`、`search`、`read` 和 `close`。连接为只读：打开时不创建数据库、不迁移 schema、不调度工作，也不修改 Memory。数据库需要使用兼容的现有 schema，升级由普通 Distill store 初始化负责。

现有 `openHostStorageV1` 继续协调 session source 的读取和运行中进程的使用。需要两种能力的 Host 将它们指向同一数据库，并在关闭时释放两个连接。

## 搜索与读取

`search({query, limit?, cursor?})` 将去掉首尾空白后的 query 视为一个完整的字面子串。匹配沿用现有 SQLite 行为：ASCII 字符不区分大小写，其他字符按字面匹配。query 最多 128 个 Unicode code points；`limit` 为 1–20，默认 20。

搜索页返回 `items`，每项包含 `id`、`revision` 和最多 500 个 Unicode code points 的匹配 `preview`。每次按 ID 顺序检查最多 128 条记录。还有数据时，`nextCursor` 从最后检查的位置继续。一页可以没有匹配但仍有 cursor，此时应继续搜索再判断是否完全没有结果。各页读取当前已提交的数据；cursor 之前的修改或新增记录会在重新搜索时出现。

`read({id, expectedRevision, cursor?})` 返回 `id`、`revision`、`text` 和可选的 `nextCursor`。每页最多 8,000 个 Unicode code points。所有页都使用搜索结果中的 revision；Memory 更新后返回 `memory_changed`，删除后返回 `memory_not_found`。重新搜索选择当前版本，避免拼接不同版本的正文。

Cursor 是绑定操作、query 或 Memory revision，以及当前 reader 实例的不透明值。重新打开 reader 后重新开始搜索或读取；续页时保持 query 不变。无效或不匹配的 cursor 返回 `invalid_cursor`，格式错误的请求返回 `invalid_input`，数据库访问失败返回 `memory_unavailable`。ID 是非空、无 NUL、最多 512 个 Unicode code points 的文本。`close` 可以重复调用。

## Host 接入

Host 只向经过认证的用户开放读取操作，将 Memory 作为内容展示，并将浏览与上下文注入分开。用户打开 Memory 不会触发 Pi recall，也不会把正文加入会话。同一个 reader 可以在普通 Pi session 继续工作时读到它们提交的更新。
