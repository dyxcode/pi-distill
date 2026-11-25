import { createHash, randomUUID } from "node:crypto";
import type {
  CleanTask,
  CleanTaskInput,
  CleanTaskPageOptions,
  TaskPage,
  WaitReason
} from "../distill/tasks.ts";
import type { CompletedDeletionReceiptV1, HostChildIdentityV1, HostSourceRefV1, HostUseRecordV1, HostUseRecoveryV1, HostUseSourceRefV1, HostWorkspaceHoldV1, ReleaseWorkspaceHoldOptionsV1, SessionDeletion } from '../retention/types.ts';
import { cleanTaskColumns, deletionColumns, hostUseColumns, hostWorkspaceHoldColumns, jobColumns, sessionUsageColumns, textColumn } from './columns.ts';
import type { StorageConnection } from './connection.ts';
import type { DistillQueue } from './distill.ts';
import { parsePersistedJobRow } from './evidence-validation.ts';
import { maxTaskPageSize } from './limits.ts';
import type { PersistedCleanTask } from './records.ts';
import { parseStoredSessionUsage, processHasExited } from './session-validation.ts';
import { parseHostChildIdentity, parseHostSourceRef, parseHostUseRecovery, parseHostUseSourceRef, parseReleaseWorkspaceHoldOptions, parseStoredHostUse, parseStoredHostWorkspaceHold } from './host-use-validation.ts';
import type { SessionRecords } from './sessions.ts';
import { SourceRegistry } from "./source-registry.ts";
import { cleanTaskId, parseCleanTaskInput, parsePersistedCleanTask, parseTaskPageOptions } from './task-validation.ts';
import { fields, parseNullableText, validateAbsolutePath, validateLeaseDuration, validateNow, validateTaskCursor, validateText } from './validation.ts';
interface Dependencies {
  readonly distill: Pick<DistillQueue, "jobTriggerIds" | "enqueueDistillTriggerInTransaction">;
  readonly sources: SourceRegistry;
  readonly sessions: Pick<SessionRecords, "assertSourceEnqueueAllowed">;
}

export class RetentionTasks {
  private readonly connection: StorageConnection;
  private readonly dependencies: Dependencies;
  constructor(connection: StorageConnection, dependencies: Dependencies) { this.connection = connection; this.dependencies = dependencies; }

  enqueueCleanTask(input: CleanTaskInput): CleanTask {
    this.connection.ensureOpen();
    const task = parseCleanTaskInput(input);
    return this.connection.transaction(() => this.enqueueCleanTaskInTransaction(task));
  }

  maintenanceBoundary(kind: "clean" | "legacy"): number {
    this.connection.ensureOpen();
    if (kind !== "clean" && kind !== "legacy") throw new TypeError("Invalid maintenance boundary kind.");
    const table = kind === "clean" ? "clean_tasks" : "consolidation_jobs";
    const value = this.connection.database.prepare(`SELECT COALESCE(MAX(rowid),0) AS value FROM ${table}`).get()?.["value"];
    validateNow(value, "Maintenance sweep boundary"); return value;
  }

  listCleanTasks(options: CleanTaskPageOptions): TaskPage<CleanTask> {
    this.connection.ensureOpen();
    const page = parseTaskPageOptions(options, true);
    const cursor = page.cursor ?? 0;
    const clauses = ["rowid > ?", "rowid <= ?", "NOT EXISTS (SELECT 1 FROM session_deletions d WHERE d.session_file = json_extract(clean_tasks.task_json, '$.sessionFile') AND d.state = 'complete')"];
    const params: (number | string)[] = [cursor, page.throughCursor ?? this.maintenanceBoundary("clean")];
    if (page.now !== undefined) {
      clauses.push("(retry_at IS NULL OR retry_at <= ?)");
      params.push(page.now);
    }
    params.push(page.limit + 1);
    const rows = this.connection.database.prepare(
      `SELECT rowid AS cursorRow, ${cleanTaskColumns} FROM clean_tasks ` +
      `WHERE ${clauses.join(" AND ")} ORDER BY rowid LIMIT ?`,
    ).all(...params);
    const hasMore = rows.length > page.limit;
    const visible = rows.slice(0, page.limit);
    const items = visible.map(raw => {
      const decoded = this.connection.decodeRow(raw);
      const { cursorRow, ...taskRow } = decoded;
      validateNow(cursorRow, "Stored clean task cursor");
      return this.cleanTaskItem(parsePersistedCleanTask(taskRow));
    });
    const last = visible.at(-1);
    const nextCursor = hasMore && last !== undefined ? this.connection.decodeRow(last)["cursorRow"] : undefined;
    if (nextCursor !== undefined) validateNow(nextCursor, "Next clean task cursor");
    return { items, ...(nextCursor === undefined ? {} : { nextCursor }) };
  }

  /**
     * `owner` is a fresh, single-attempt fencing token, not a reusable worker ID.
     * The caller must use a new token after release, expiry, or intent replacement.
     */
  claimCleanTask(id: string, owner: string, now: number, leaseMs: number, force = false): boolean {
    this.connection.ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task claim owner");
    validateNow(now, "Clean task claim time");
    validateLeaseDuration(leaseMs, now, "Clean task lease duration");
    if (typeof force !== "boolean") throw new TypeError("Clean task force flag must be boolean.");
    return this.connection.transaction(() => {
      const row = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = parsePersistedCleanTask(this.connection.decodeRow(row));
      if (current.status !== "pending" && current.status !== "waiting") return false;
      if (this.getDeletion(current.task.sessionFile)?.state === "complete") return false;
      if (!force && current.retryAt !== null && current.retryAt > now) return false;
      if (current.leaseOwner !== null && current.leaseUntil !== null && current.leaseUntil > now) return false;
      this.connection.database.prepare(
        "UPDATE clean_tasks SET lease_owner = ?, lease_until = ? WHERE id = ?",
      ).run(owner, now + leaseMs, id);
      return true;
    });
  }

  releaseCleanTask(id: string, owner: string): boolean {
    this.connection.ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task lease owner");
    return this.connection.transaction(() => {
      const changed = this.connection.database.prepare(
        "UPDATE clean_tasks SET lease_owner = NULL, lease_until = NULL " +
        "WHERE id = ? AND lease_owner = ?",
      ).run(id, owner);
      return changed.changes === 1;
    });
  }

  recordCleanAssessment(id: string, owner: string, now: number, reason: WaitReason, retryAt: number): boolean {
    this.connection.ensureOpen();
    validateText(id, "clean task ID");
    validateText(owner, "clean task lease owner");
    validateNow(now, "Clean assessment time");
    validateNow(retryAt, "Clean assessment retry time");
    if (
      reason !== "host_coordination" && reason !== "unprocessed" && reason !== "usage" &&
      reason !== "dependencies" && reason !== "unavailable"
    ) throw new TypeError("Clean assessment reason is invalid.");
    return this.connection.transaction(() => {
      const row = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (row === undefined) return false;
      const current = parsePersistedCleanTask(this.connection.decodeRow(row));
      if (
        current.leaseOwner !== owner || current.leaseUntil === null || current.leaseUntil <= now
      ) return false;
      this.connection.database.prepare(
        "UPDATE clean_tasks SET status = 'waiting', wait_reason = ?, retry_category = ?, retry_at = ?, " +
        "lease_owner = NULL, lease_until = NULL WHERE id = ? AND lease_owner = ?",
      ).run(reason, reason === "unavailable" ? "unavailable" : "transient", retryAt, id, owner);
      return true;
    });
  }

  reconcileLegacyCleanTasks(limit = 32, cursor?: number, throughCursor?: number): { readonly created: number; readonly nextCursor?: number; } {
    this.connection.ensureOpen();
    if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > maxTaskPageSize) {
      throw new TypeError("Legacy cleanup reconciliation limit must be from 1 through " + maxTaskPageSize + ".");
    }
    const after = validateTaskCursor(cursor, "Legacy cleanup reconciliation cursor") ?? 0;
    const end = throughCursor ?? this.maintenanceBoundary("legacy");
    validateNow(end, "Legacy reconciliation boundary");
    return this.connection.transaction(() => {
      const rows = this.connection.database.prepare(
        `SELECT rowid AS cursorRow, ${jobColumns} FROM consolidation_jobs ` +
        "WHERE rowid > ? AND rowid <= ? AND status = 'complete' ORDER BY rowid LIMIT ?",
      ).all(after, end, limit + 1);
      const hasMore = rows.length > limit;
      const visible = rows.slice(0, limit);
      let created = 0;
      for (const raw of visible) {
        const decoded = this.connection.decodeRow(raw);
        const { cursorRow, ...jobRow } = decoded;
        validateNow(cursorRow, "Legacy job cursor");
        const record = parsePersistedJobRow(jobRow);
        if (this.dependencies.distill.jobTriggerIds(record.job.id).length > 0 || record.job.sessionFile === null) continue;
        const cleanId = cleanTaskId(record.job.sessionFile);
        const cleanRow = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(cleanId);
        if (cleanRow !== undefined) {
          parsePersistedCleanTask(this.connection.decodeRow(cleanRow));
          continue;
        }
        this.enqueueCleanTaskInTransaction({
          sessionId: record.job.sessionId,
          sessionFile: record.job.sessionFile,
          leafId: record.job.leafId,
          cwd: record.job.cwd,
          reason: "legacy",
          intentId: "legacy-job:" + record.job.id,
        });
        created += 1;
      }
      const last = visible.at(-1);
      const nextCursor = hasMore && last !== undefined ? this.connection.decodeRow(last)["cursorRow"] : undefined;
      if (nextCursor !== undefined) validateNow(nextCursor, "Next legacy job cursor");
      return { created, ...(nextCursor === undefined ? {} : { nextCursor }) };
    });
  }

  hasUncapturedObservation(scope: readonly string[]): boolean {
    const filter = this.scopeFilter(scope);
    return this.connection.database.prepare(`SELECT 1 FROM session_observations WHERE captured = 0 AND ${filter.sql("session_file")} LIMIT 1`).get(...filter.values) !== undefined;
  }

  /** Distinguishes a path reserved by a fresh empty session from a captured source. */
  hasSessionSourceWork(path: string): boolean {
    validateAbsolutePath(path, "Source work path");
    return this.connection.database.prepare(
      "SELECT 1 FROM session_observations WHERE session_file = ? " +
      "UNION ALL SELECT 1 FROM consolidation_jobs WHERE json_extract(job_json, '$.sessionFile') = ? " +
      "UNION ALL SELECT 1 FROM distill_triggers WHERE json_extract(trigger_json, '$.sessionFile') = ? " +
      "UNION ALL SELECT 1 FROM clean_tasks WHERE json_extract(task_json, '$.sessionFile') = ? LIMIT 1",
    ).get(path, path, path, path) !== undefined;
  }

  /** Coordinates per-session reads (shared) and deletion (exclusive). Never held during LLM work. */
  acquireSourceAccess(sessionFile: string, mode: "read" | "delete" = "delete"): string | undefined {
    validateAbsolutePath(sessionFile, "Source access path");
    if (mode !== "read" && mode !== "delete") throw new TypeError("Source access mode must be read or delete.");
    return this.connection.transaction(() => {
      const rows = this.connection.database.prepare(
        `SELECT ${textColumn("token")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("mode")}, owner_pid FROM source_access WHERE session_file = ?`,
      ).all(sessionFile);
      let hasLiveRead = false;
      let hasLiveDelete = false;
      for (const raw of rows) {
        const parsed = this.parseSourceAccessRow(this.connection.decodeRow(raw));
        if (processHasExited(parsed.pid)) {
          this.connection.database.prepare("DELETE FROM source_access WHERE token = ?").run(parsed.token);
          continue;
        }
        if (parsed.mode === "delete") hasLiveDelete = true;
        else hasLiveRead = true;
      }
      if (hasLiveDelete || (mode === "delete" && hasLiveRead)) return undefined;
      const token = randomUUID();
      this.connection.database.prepare(
        "INSERT INTO source_access (token, session_file, mode, owner_pid) VALUES (?, ?, ?, ?)",
      ).run(token, sessionFile, mode, process.pid);
      return token;
    });
  }

  hasLiveSourceAccess(sessionFile: string): boolean {
    validateAbsolutePath(sessionFile, "Source access path");
    const rows = this.connection.database.prepare(
      `SELECT ${textColumn("token")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("mode")}, owner_pid FROM source_access WHERE session_file = ?`,
    ).all(sessionFile);
    for (const raw of rows) {
      const parsed = this.parseSourceAccessRow(this.connection.decodeRow(raw));
      if (!processHasExited(parsed.pid)) return true;
    }
    return false;
  }

  releaseSourceAccess(token: string): boolean {
    validateText(token, "Source access token");
    return this.connection.transaction(() => this.connection.database.prepare("DELETE FROM source_access WHERE token = ?").run(token).changes === 1);
  }

  /** Persist a host's pre-read/pre-spawn reservation under the cleanup mutex. */
  beginHostUse(input: HostUseSourceRefV1): HostUseRecordV1 | undefined {
    this.connection.ensureOpen();
    const source = parseHostUseSourceRef(input);
    const access = source.sessionFile === undefined ? undefined : this.acquireSourceAccess(source.sessionFile, "read");
    if (source.sessionFile !== undefined && !access) return undefined;
    try {
      return this.connection.transaction(() => {
        const registered = source.sessionFile === undefined
          ? undefined
          : this.dependencies.sources.get(source.sessionFile, source.sessionId);
        if (registered && registered.cwd !== source.cwd) throw new Error("Host source working directory does not match its registered source.");
        if (registered && source.sourceGeneration !== undefined && source.sourceGeneration !== registered.generation) {
          throw new Error("Host source generation does not match its registered source.");
        }
        const sourceGeneration = source.sourceGeneration ?? registered?.generation ?? null;
        const id = randomUUID();
        const ownerPid = process.pid;
        const createdAt = Date.now();
        this.connection.database.prepare(
          "INSERT INTO host_session_uses " +
          "(id, session_id, session_file, cwd, source_generation, owner_pid, created_at, state, child_pid, child_token) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, 'opening', NULL, NULL)",
        ).run(id, source.sessionId, source.sessionFile ?? null, source.cwd, sourceGeneration, ownerPid, createdAt);
        return { id, sessionId: source.sessionId, sessionFile: source.sessionFile ?? null, cwd: source.cwd, sourceGeneration, ownerPid, createdAt, state: "opening", child: null };
      });
    } finally {
      if (access) this.releaseSourceAccess(access);
    }
  }

  attachHostUseChild(id: string, pid: number): HostChildIdentityV1 {
    this.connection.ensureOpen();
    validateText(id, "host use ID");
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new TypeError("Host child PID must be a positive safe integer.");
    const child = { pid, token: randomUUID() };
    this.connection.transaction(() => {
      const changed = this.connection.database.prepare(
        "UPDATE host_session_uses SET state = 'attached', child_pid = ?, child_token = ? " +
        "WHERE id = ? AND state = 'opening'",
      ).run(child.pid, child.token, id).changes;
      if (changed !== 1) throw new Error("Host use is no longer awaiting a child.");
    });
    return child;
  }

  releaseHostUseAfterClose(id: string, childInput: HostChildIdentityV1): boolean {
    this.connection.ensureOpen();
    validateText(id, "host use ID");
    const child = parseHostChildIdentity(childInput);
    return this.connection.transaction(() => this.connection.database.prepare(
      "DELETE FROM host_session_uses WHERE id = ? AND state = 'attached' AND child_pid = ? AND child_token = ?",
    ).run(id, child.pid, child.token).changes === 1);
  }

  holdHostWorkspaceSession(input: HostUseSourceRefV1): HostWorkspaceHoldV1 | undefined {
    this.connection.ensureOpen();
    const source = parseHostUseSourceRef(input);
    const access = source.sessionFile === undefined ? undefined : this.acquireSourceAccess(source.sessionFile, "read");
    if (source.sessionFile !== undefined && !access) return undefined;
    try {
      return this.connection.transaction(() => {
        if (source.sessionFile !== undefined) {
          const deletion = this.getDeletion(source.sessionFile);
          if (deletion) return undefined;
        }
        const registered = source.sessionFile === undefined
          ? undefined
          : this.dependencies.sources.get(source.sessionFile, source.sessionId);
        if (registered && registered.cwd !== source.cwd) {
          throw new Error("Host workspace hold working directory does not match its registered source.");
        }
        if (registered && source.sourceGeneration !== undefined && source.sourceGeneration !== registered.generation) {
          throw new Error("Host workspace hold source generation does not match its registered source.");
        }
        const existingRow = this.connection.database.prepare(
          `SELECT ${hostWorkspaceHoldColumns} FROM host_workspace_holds WHERE session_id = ?`,
        ).get(source.sessionId);
        const sourceGeneration = source.sourceGeneration ?? registered?.generation ?? null;
        if (existingRow !== undefined) {
          const existing = parseStoredHostWorkspaceHold(this.connection.decodeRow(existingRow));
          const nextFile = source.sessionFile ?? existing.sessionFile;
          const nextGeneration = sourceGeneration ?? existing.sourceGeneration;
          if (existing.sessionFile !== nextFile || existing.sourceGeneration !== nextGeneration || existing.cwd !== source.cwd) {
            this.connection.database.prepare(
              "UPDATE host_workspace_holds SET session_file = ?, cwd = ?, source_generation = ? WHERE session_id = ?",
            ).run(nextFile, source.cwd, nextGeneration, source.sessionId);
          }
          return {
            sessionId: existing.sessionId,
            sessionFile: nextFile,
            cwd: source.cwd,
            sourceGeneration: nextGeneration,
            createdAt: existing.createdAt,
          };
        }
        const createdAt = Date.now();
        this.connection.database.prepare(
          "INSERT INTO host_workspace_holds (session_id, session_file, cwd, source_generation, created_at) VALUES (?, ?, ?, ?, ?)",
        ).run(source.sessionId, source.sessionFile ?? null, source.cwd, sourceGeneration, createdAt);
        return {
          sessionId: source.sessionId,
          sessionFile: source.sessionFile ?? null,
          cwd: source.cwd,
          sourceGeneration,
          createdAt,
        };
      });
    } finally {
      if (access) this.releaseSourceAccess(access);
    }
  }

  releaseHostWorkspaceHold(input: HostUseSourceRefV1, optionsInput?: ReleaseWorkspaceHoldOptionsV1): boolean {
    this.connection.ensureOpen();
    const source = parseHostUseSourceRef(input);
    const options = parseReleaseWorkspaceHoldOptions(optionsInput);
    return this.connection.transaction(() => {
      const existingRow = this.connection.database.prepare(
        `SELECT ${hostWorkspaceHoldColumns} FROM host_workspace_holds WHERE session_id = ?`,
      ).get(source.sessionId);
      if (existingRow === undefined) return false;
      const existing = parseStoredHostWorkspaceHold(this.connection.decodeRow(existingRow));
      const deleted = this.connection.database.prepare(
        "DELETE FROM host_workspace_holds WHERE session_id = ?",
      ).run(source.sessionId).changes === 1;
      const sessionFile = source.sessionFile ?? existing.sessionFile;
      if (deleted && options.enqueueDistill === true && sessionFile !== null) {
        // An explicit workspace close supersedes interruptions, but never a live owner.
        const owners = this.connection.database.prepare(
          `SELECT ${sessionUsageColumns} FROM session_usages WHERE session_id = ? AND session_file = ? AND state <> 'released'`,
        ).all(source.sessionId, sessionFile);
        const releaseOwner = this.connection.database.prepare(
          "UPDATE session_usages SET state = 'released' WHERE owner_id = ?",
        );
        for (const row of owners) {
          const owner = parseStoredSessionUsage(this.connection.decodeRow(row));
          if (processHasExited(owner.ownerPid)) releaseOwner.run(owner.ownerId);
        }
        const leafId = options.leafId ?? null;
        const triggerId = createHash("sha256").update(JSON.stringify([
          "pi-distill-close-trigger-v1", source.sessionId, sessionFile, leafId,
        ])).digest("hex");
        this.dependencies.distill.enqueueDistillTriggerInTransaction({
          id: triggerId,
          reason: "close",
          sessionId: source.sessionId,
          sessionFile,
          cwd: source.cwd,
          leafId,
        });
      }
      return deleted;
    });
  }

  listHostWorkspaceHolds(): HostWorkspaceHoldV1[] {
    this.connection.ensureOpen();
    const rows = this.connection.database.prepare(
      `SELECT ${hostWorkspaceHoldColumns} FROM host_workspace_holds ORDER BY created_at, session_id LIMIT 1025`,
    ).all().map(raw => parseStoredHostWorkspaceHold(this.connection.decodeRow(raw)));
    if (rows.length > 1024) throw new RangeError("Host workspace holds exceed the reconciliation limit.");
    return rows;
  }

  hasHostWorkspaceHold(sessionId: string, sessionFile?: string | null): boolean {
    this.connection.ensureOpen();
    validateText(sessionId, "session ID");
    if (sessionFile !== undefined && sessionFile !== null) {
      validateAbsolutePath(sessionFile, "session file");
      return this.connection.database.prepare(
        "SELECT 1 FROM host_workspace_holds WHERE session_id = ? OR session_file = ? LIMIT 1",
      ).get(sessionId, sessionFile) !== undefined;
    }
    return this.connection.database.prepare(
      "SELECT 1 FROM host_workspace_holds WHERE session_id = ? LIMIT 1",
    ).get(sessionId) !== undefined;
  }

  listPendingHostUses(): HostUseRecordV1[] {
    this.connection.ensureOpen();
    const rows = this.connection.database.prepare(
      `SELECT ${hostUseColumns} FROM host_session_uses ORDER BY created_at, id LIMIT 1025`,
    ).all().map(raw => parseStoredHostUse(this.connection.decodeRow(raw)));
    if (rows.length > 1024) throw new RangeError("Pending host uses exceed the reconciliation limit.");
    return rows;
  }

  hostSourceGeneration(input: HostSourceRefV1): string | null | undefined {
    this.connection.ensureOpen();
    const sourceRef = parseHostSourceRef(input);
    return this.connection.transaction(() => {
      const source = this.dependencies.sources.get(sourceRef.sessionFile, sourceRef.sessionId);
      if (source && (source.cwd !== sourceRef.cwd ||
        (sourceRef.sourceGeneration !== undefined && sourceRef.sourceGeneration !== source.generation))) return undefined;
      return source?.generation ?? sourceRef.sourceGeneration ?? null;
    }, "DEFERRED");
  }

  recoverHostUse(id: string, evidenceInput: HostUseRecoveryV1): boolean {
    this.connection.ensureOpen();
    validateText(id, "host use ID");
    const evidence = parseHostUseRecovery(evidenceInput);
    if (evidence.kind === "opening-abandoned") {
      return this.connection.transaction(() => this.connection.database.prepare(
        "DELETE FROM host_session_uses WHERE id = ? AND state = 'opening'",
      ).run(id).changes === 1);
    }
    if (!processHasExited(evidence.child.pid)) return false;
    return this.connection.transaction(() => this.connection.database.prepare(
      "DELETE FROM host_session_uses WHERE id = ? AND state = 'attached' AND child_pid = ? AND child_token = ?",
    ).run(id, evidence.child.pid, evidence.child.token).changes === 1);
  }

  private parseSourceAccessRow(value: unknown): { readonly token: string; readonly sessionFile: string; readonly mode: "read" | "delete"; readonly pid: number; } {
    const row = fields(value, ["token", "sessionFile", "mode", "owner_pid"]);
    validateText(row["token"], "Stored source token");
    validateAbsolutePath(row["sessionFile"], "Stored source access path");
    if (row["mode"] !== "read" && row["mode"] !== "delete") throw new TypeError("Invalid source access mode.");
    const pid = row["owner_pid"];
    if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) throw new TypeError("Invalid source owner PID.");
    return { token: row["token"], sessionFile: row["sessionFile"], mode: row["mode"], pid };
  }

  private hasDeleteAccess(sessionFile: string, token: string): boolean {
    const raw = this.connection.database.prepare(
      `SELECT ${textColumn("token")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("mode")}, owner_pid FROM source_access WHERE token = ?`,
    ).get(token);
    if (!raw) return false;
    const parsed = this.parseSourceAccessRow(this.connection.decodeRow(raw));
    return parsed.sessionFile === sessionFile && parsed.mode === "delete" && !processHasExited(parsed.pid);
  }

  sourceAccess(): { token: string; pid: number; } | undefined {
    const schemaVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    if (typeof schemaVersion === "number" && schemaVersion < 15) {
      const raw = this.connection.database.prepare(`SELECT ${textColumn("token")}, owner_pid FROM source_access`).get();
      if (!raw) return undefined;
      const row = this.connection.decodeRow(raw);
      validateText(row["token"], "Stored source token");
      const pid = row["owner_pid"];
      if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) throw new TypeError("Invalid source owner PID.");
      return { token: row["token"], pid };
    }
    let first: { token: string; pid: number; } | undefined;
    for (const raw of this.connection.database.prepare(
      `SELECT ${textColumn("token")}, ${textColumn("session_file", "sessionFile")}, ${textColumn("mode")}, owner_pid FROM source_access`,
    ).iterate()) {
      const parsed = this.parseSourceAccessRow(this.connection.decodeRow(raw));
      first ??= { token: parsed.token, pid: parsed.pid };
    }
    return first;
  }

  scopeFilter(scope?: readonly string[]): { values: string[]; sql: (column: string) => string; } {
    if (scope === undefined) return { values: [], sql: () => "1" };
    if (!Array.isArray(scope) || scope.length > 1024) throw new TypeError("Invalid source scope.");
    for (const path of scope) validateAbsolutePath(path, "Source scope path");
    const requested = [...new Set(scope)].sort();
    const placeholders = requested.map(() => "?").join(",");
    const related = requested.length === 0 ? [] : this.connection.database.prepare(
      "SELECT DISTINCT session_file AS path FROM managed_sources WHERE deleted = 0 AND (" +
      `session_id IN (SELECT session_id FROM managed_sources WHERE session_file IN (${placeholders})) OR ` +
      `physical_identity IN (SELECT physical_identity FROM managed_sources WHERE session_file IN (${placeholders}))) LIMIT 1025`,
    ).all(...requested, ...requested);
    if (related.length > 1024) throw new RangeError("Source aliases exceed the assessment budget.");
    const values = [...new Set([...requested, ...related.map(raw => {
      const path = this.connection.decodeRow(raw)["path"]; validateAbsolutePath(path, "Source alias path"); return path;
    })])].sort();
    return { values, sql: column => values.length ? `${column} IN (${values.map(() => "?").join(",")})` : "0" };
  }

  /** Compare on this connection only; includes other connections and local mutations. */
  sourceRetentionState(path: string, sessionId: string): { pending: boolean; released: boolean; hostUse: boolean; } {
    validateAbsolutePath(path, "source path"); validateText(sessionId, "session ID");
    const pending = this.connection.database.prepare("SELECT 1 FROM consolidation_jobs WHERE status='pending' AND (json_extract(job_json,'$.sessionFile')=? OR json_extract(job_json,'$.sessionId')=?) LIMIT 1").get(path, sessionId) !== undefined ||
      this.connection.database.prepare("SELECT 1 FROM distill_triggers WHERE status<>'complete' AND (json_extract(trigger_json,'$.sessionFile')=? OR json_extract(trigger_json,'$.sessionId')=?) LIMIT 1").get(path, sessionId) !== undefined;
    const known = this.connection.database.prepare("SELECT 1 FROM session_usages WHERE session_file=? OR session_id=? LIMIT 1").get(path, sessionId) !== undefined;
    const outstanding = this.connection.database.prepare("SELECT 1 FROM session_usages WHERE (session_file=? OR session_id=?) AND state<>'released' LIMIT 1").get(path, sessionId) !== undefined;
    const hostUse = this.connection.database.prepare("SELECT 1 FROM host_session_uses WHERE session_file=? OR session_id=? LIMIT 1").get(path, sessionId) !== undefined ||
      this.connection.database.prepare("SELECT 1 FROM host_workspace_holds WHERE session_file=? OR session_id=? LIMIT 1").get(path, sessionId) !== undefined;
    return { pending, released: known && !outstanding && !hostUse, hostUse };
  }

  retentionVersion(scope?: readonly string[]): string {
    this.connection.ensureOpen();
    if (scope !== undefined) {
      const filter = this.scopeFilter(scope);
      const digest = createHash("sha256").update(String(this.dependencies.sources.topologyVersion()));
      for (const path of filter.values) {
        const row = this.connection.database.prepare("SELECT revision FROM source_fences WHERE path=?").get(path);
        const revision = row?.["revision"] ?? 0;
        if (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 0) throw new TypeError("Invalid source fence.");
        digest.update(JSON.stringify([path, revision]));
      }
      return digest.digest("hex");
    }
    return `${this.connection.database.prepare("PRAGMA data_version").get()?.["data_version"]}:${this.connection.database.prepare("SELECT total_changes() AS n").get()?.["n"]}`;
  }

  reconcileExitedSessionOwners(scope?: readonly string[]): void {
    this.connection.transaction(() => {
      const filter = this.scopeFilter(scope);
      const rows = this.connection.database.prepare(`SELECT ${sessionUsageColumns} FROM session_usages WHERE state IN ('active', 'closing') AND ${filter.sql("session_file")} ORDER BY rowid LIMIT 128`).all(...filter.values);
      for (const raw of rows) {
        const owner = parseStoredSessionUsage(this.connection.decodeRow(raw));
        if (!processHasExited(owner.ownerPid)) continue;
        // Only a graceful user close (closing) releases a session; an active PID dying is an unexpected crash/interruption.
        const nextState = owner.state === "closing" ? "released" : "interrupted";
        this.connection.database.prepare("UPDATE session_usages SET state = ? WHERE owner_id = ?").run(nextState, owner.ownerId);
      }
    });
  }

  getDeletion(path: string): SessionDeletion | undefined {
    validateAbsolutePath(path, "Deletion path");
    const schemaVersion = this.connection.database.prepare("PRAGMA user_version").get()?.["user_version"];
    const columns = typeof schemaVersion === "number" && schemaVersion >= 7
      ? deletionColumns
      : [textColumn("session_file", "sessionFile"), textColumn("session_id", "sessionId"),
      textColumn("intent_id", "intentId"), textColumn("file_version", "fileVersion"),
        "NULL AS sourceGeneration", textColumn("state")].join(", ");
    const raw = this.connection.database.prepare(`SELECT ${columns} FROM session_deletions WHERE session_file = ?`).get(path);
    if (!raw) return undefined;
    return this.parseDeletionRow(this.connection.decodeRow(raw));
  }

  getCompletedDeletionFor(input: HostSourceRefV1): CompletedDeletionReceiptV1 | undefined {
    this.connection.ensureOpen();
    const sourceRef = parseHostSourceRef(input);
    return this.connection.transaction(() => {
      const source = this.dependencies.sources.get(sourceRef.sessionFile, sourceRef.sessionId);
      let expectedGeneration = sourceRef.sourceGeneration;
      if (source) {
        if (!source.deleted || source.cwd !== sourceRef.cwd ||
          (expectedGeneration !== undefined && expectedGeneration !== source.generation)) return undefined;
        expectedGeneration = source.generation;
      }
      if (expectedGeneration === undefined) return undefined;
      const receipt = this.getDeletion(sourceRef.sessionFile);
      if (receipt?.state !== "complete" || receipt.sessionId !== sourceRef.sessionId ||
        receipt.sourceGeneration === null || receipt.sourceGeneration !== expectedGeneration) return undefined;
      return { ...receipt, sourceGeneration: receipt.sourceGeneration, state: "complete" };
    }, "DEFERRED");
  }

  private parseDeletionRow(value: unknown): SessionDeletion {
    const row = fields(value, ["sessionFile", "sessionId", "intentId", "fileVersion", "sourceGeneration", "state"]);
    validateAbsolutePath(row["sessionFile"], "Stored deletion path");
    validateText(row["sessionId"], "Stored deletion session ID");
    validateText(row["intentId"], "Stored deletion intent ID");
    validateText(row["fileVersion"], "Stored deletion file version");
    if (!/^\d+:\d+:\d+:\d+:\d+$/.test(row["fileVersion"])) throw new TypeError("Invalid deletion file version.");
    const sourceGeneration = parseNullableText(row["sourceGeneration"], "Stored deletion source generation");
    if (row["state"] !== "deleting" && row["state"] !== "complete") throw new TypeError("Invalid deletion state.");
    return {
      sessionFile: row["sessionFile"],
      sessionId: row["sessionId"],
      intentId: row["intentId"],
      fileVersion: row["fileVersion"],
      sourceGeneration,
      state: row["state"],
    };
  }

  private deletionTask(id: string, owner: string, access: string, version: string, scope?: readonly string[]): CleanTaskInput | undefined {
    if (this.retentionVersion(scope) !== version) return undefined;
    const raw = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (!raw) return undefined;
    const record = parsePersistedCleanTask(this.connection.decodeRow(raw));
    if (record.leaseOwner !== owner || record.leaseUntil === null || record.leaseUntil <= Date.now()) return undefined;
    if (!this.hasDeleteAccess(record.task.sessionFile, access)) return undefined;
    return record.task;
  }

  prepareDeletion(id: string, owner: string, access: string, version: string, fileVersion: string, scope?: readonly string[]): boolean {
    if (typeof fileVersion !== "string" || !/^\d+:\d+:\d+:\d+:\d+$/.test(fileVersion)) throw new TypeError("Invalid deletion file version.");
    return this.connection.transaction(() => {
      const task = this.deletionTask(id, owner, access, version, scope);
      if (!task) return false;
      const source = this.dependencies.sources.get(task.sessionFile, task.sessionId);
      if (!source || source.deleted) return false;
      const previous = this.getDeletion(task.sessionFile);
      if (previous && (previous.state === "complete" || previous.sessionId !== task.sessionId || previous.fileVersion.split(":").slice(0, 2).join(":") !== fileVersion.split(":").slice(0, 2).join(":"))) return false;
      this.connection.database.prepare(
        "INSERT INTO session_deletions (session_file, session_id, intent_id, file_version, source_generation, state) " +
        "VALUES (?, ?, ?, ?, ?, 'deleting') ON CONFLICT(session_file) DO UPDATE SET " +
        "intent_id = excluded.intent_id, file_version = excluded.file_version, source_generation = excluded.source_generation",
      ).run(task.sessionFile, task.sessionId, task.intentId, fileVersion, source.generation);
      return true;
    });
  }

  /** Callback performs only bounded synchronous identity checks/unlink, under the writer reservation. */
  finishDeletion(id: string, owner: string, access: string, version: string, mutate: () => boolean, scope?: readonly string[]): boolean {
    return this.connection.transaction(() => {
      const task = this.deletionTask(id, owner, access, version, scope);
      if (!task) return false;
      const receipt = this.getDeletion(task.sessionFile);
      const source = this.dependencies.sources.get(task.sessionFile, task.sessionId);
      if (!receipt || receipt.state !== "deleting" || receipt.sessionId !== task.sessionId ||
        receipt.intentId !== task.intentId || !source || source.deleted || receipt.sourceGeneration !== source.generation) return false;
      if (mutate() !== true) return false;
      this.connection.database.prepare("UPDATE session_deletions SET state = 'complete' WHERE session_file = ?").run(task.sessionFile);
      this.dependencies.sources.markDeleted(task.sessionFile, task.sessionId);
      this.connection.database.prepare("UPDATE clean_tasks SET lease_owner = NULL, lease_until = NULL WHERE id = ?").run(id);
      return true;
    });
  }

  enqueueCleanTaskInTransaction(task: CleanTaskInput): CleanTask {
    const record = parseCleanTaskInput(task);
    if (record.sessionFile !== null) this.dependencies.sessions.assertSourceEnqueueAllowed(record.sessionId, record.sessionFile, record.cwd);
    const id = cleanTaskId(record.sessionFile);
    const serialized = JSON.stringify(record);
    const row = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (row === undefined) {
      this.connection.database.prepare(
        "INSERT INTO clean_tasks " +
        "(id, task_json, status, wait_reason, retry_category, retry_at, lease_owner, lease_until) " +
        "VALUES (?, ?, 'pending', NULL, NULL, NULL, NULL, NULL)",
      ).run(id, serialized);
      const inserted = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
      if (inserted === undefined) throw new Error("Clean task insert was not persisted.");
      return this.cleanTaskItem(parsePersistedCleanTask(this.connection.decodeRow(inserted)));
    }
    const existing = parsePersistedCleanTask(this.connection.decodeRow(row));
    if (JSON.stringify(existing.task) === serialized) return this.cleanTaskItem(existing);
    this.connection.database.prepare(
      "UPDATE clean_tasks SET task_json = ?, status = 'pending', wait_reason = NULL, retry_category = NULL, " +
      "retry_at = NULL, lease_owner = NULL, lease_until = NULL WHERE id = ?",
    ).run(serialized, id);
    const refreshed = this.connection.database.prepare(`SELECT ${cleanTaskColumns} FROM clean_tasks WHERE id = ?`).get(id);
    if (refreshed === undefined) throw new Error("Clean task refresh was not persisted.");
    return this.cleanTaskItem(parsePersistedCleanTask(this.connection.decodeRow(refreshed)));
  }

  private cleanTaskItem(record: PersistedCleanTask): CleanTask {
    return {
      ...record.task,
      id: cleanTaskId(record.task.sessionFile),
      status: record.status,
      ...(record.waitReason === null ? {} : { waitReason: record.waitReason }),
      ...(record.retryCategory === null ? {} : { retryCategory: record.retryCategory }),
      ...(record.retryAt === null ? {} : { retryAt: record.retryAt }),
    };
  }
}
