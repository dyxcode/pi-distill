import type { PagedSourceView } from "./source-view.ts";
import { createHash } from "node:crypto";
import { closeSync, openSync, readSync } from "node:fs";
import { lstat, open } from "node:fs/promises";
import { TextDecoder } from "node:util";
import { dirname, resolve } from "node:path";
import {
  buildSessionProjection,
  type SessionEntry,
  type SessionHeader,
  type SessionProjection,
} from "@earendil-works/pi-coding-agent";
import type { SessionObservation, SourceBatch, SourcePart } from "./consolidation-types.js";
import { DistillFailure } from "./distill-failure.ts";

const MAX_PART_TEXT_BYTES = 1024;
const MAX_PART_JSON_BYTES = 2 * 1024;
const MAX_SOURCE_JSON_BYTES = 24 * 1024;
const MAX_CONTEXT_JSON_BYTES = 8 * 1024;
const MAX_ANCESTRY_JSON_BYTES = 16 * 1024 * 1024;
const MAX_ANCESTRY_ENTRIES = 20_000;
const MAX_SESSION_ID_LENGTH = 512;
const MAX_ENTRY_ID_LENGTH = 512;
const MAX_ANCESTRY_DEPTH = 128;
const MAX_JSON_DEPTH = 256;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * The installed Pi 1.3.0 top-level declaration omits its internal
 * ReadonlySessionManager alias. Keep only the public, read-only methods this
 * adapter needs and let the real exported SessionManager satisfy the shape.
 */
export interface ReadonlySessionManager {
  getCwd(): string;
  getSessionId(): string;
  getSessionFile(): string | undefined;
  getLeafId(): string | null;
  getHeader(): SessionHeader | null;
  getBranch(): SessionEntry[];
  getEntries(): SessionEntry[];
  buildSessionProjection(): SessionProjection;
}

export interface SourceSnapshot {
  readonly indexed?: PagedSourceView;
  readonly sessionId: string;
  readonly sessionFile: string | null;
  readonly cwd: string;
  readonly leafId: string | null;
  readonly entries: readonly unknown[];
  readonly pending?: boolean;
  readonly pendingReason?: string;
  readonly parentSession?: string;
  readonly ancestry?: SnapshotAncestry;
}

interface SourceOrigin {
  readonly sessionId: string;
  readonly entryId: string;
}

interface SnapshotAncestry {
  readonly origins: ReadonlyMap<string, SourceOrigin>;
  readonly signatures: ReadonlyMap<string, string>;
  readonly unresolved?: boolean;
}

interface ParsedSession {
  readonly header: SessionHeader;
  readonly entries: readonly SessionEntry[];
  readonly pending: boolean;
}

interface SessionGraph {
  readonly header: SessionHeader;
  readonly entries: readonly SessionEntry[];
  readonly entriesById: Map<string, SessionEntry>;
  readonly signatures: ReadonlyMap<string, string>;
  readonly origins: ReadonlyMap<string, SourceOrigin>;
  readonly pending: boolean;
}

interface AggregateBudget {
  bytes: number;
  entries: number;
}

interface EffectiveContent {
  readonly content: unknown;
  readonly omitted: boolean;
}

interface ProjectionContent {
  readonly messages: readonly unknown[];
}

const projections = new WeakMap<SourceSnapshot, SessionProjection>();

function invalidSource(message: string): DistillFailure {
  return new DistillFailure("source_invalid", message);
}

export function snapshotSession(manager: ReadonlySessionManager): SourceSnapshot {
  const sessionId = manager.getSessionId();
  const header = manager.getHeader();
  const sessionFile = manager.getSessionFile() ?? null;
  const cwd = manager.getCwd();
  const allEntries = manager.getEntries();
  const leafId = manager.getLeafId();
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  if (!consumeAggregateBudget(budget, [sessionId, sessionFile, cwd, header, allEntries], allEntries.length)) {
    return pendingSnapshot(sessionId, sessionFile, cwd, leafId,
      "Session snapshot exceeds the aggregate ancestry size or entry limit.");
  }

  validateSessionHeader({ ...header, id: sessionId, cwd });
  validateSessionEntries(allEntries);

  const branch = manager.getBranch();
  if (leafId === null && branch.length > 0) {
    throw invalidSource("Pi returned session entries without a leaf checkpoint");
  }
  if (leafId !== null) validateBranch(branch, leafId);

  // Pi's projection is the public implementation of context_edit semantics.
  // It also gives us effective content for message entries still in model
  // context; older compacted messages are handled by their append-only edits.
  const projection = manager.buildSessionProjection();
  const ancestry = syncAncestry(sessionId, allEntries, sessionFile ?? undefined, header?.parentSession, budget);
  const pendingReason = ancestry.unresolved
    ? "Fork ancestry could not be verified; reload the original session asynchronously before planning."
    : validateContextEdits(branch).reason;

  const snapshot: SourceSnapshot = {
    sessionId,
    sessionFile,
    cwd,
    leafId,
    entries: branch,
    ...(header?.parentSession ? { parentSession: header.parentSession } : {}),
    ancestry,
    ...(pendingReason ? { pending: true, pendingReason } : { pending: false }),
  };
  projections.set(snapshot, projection);
  return snapshot;
}

/** Read and validate a persisted Pi session without changing the source file. */
export async function loadSnapshot(sessionFile: string, leafId?: string | null): Promise<SourceSnapshot> {
  if (typeof sessionFile !== "string" || sessionFile.trim().length === 0) {
    throw invalidSource("Session file path must be a nonempty string.");
  }
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  const graph = await loadSessionGraph(resolve(sessionFile), new Set(), 0, budget);
  return snapshotFromGraph(graph, sessionFile, leafId);
}

/** Recover a saved compaction across every branch, including an inactive branch. */
export async function findCompactionRecord(
  sessionFile: string,
  sessionId: string,
  cwd: string,
  leafId: string | null,
): Promise<string | undefined> {
  if (typeof sessionFile !== "string" || sessionFile.trim().length === 0 ||
    !validNonEmptyString(sessionId, MAX_SESSION_ID_LENGTH) || typeof cwd !== "string" ||
    (leafId !== null && !validNonEmptyString(leafId, MAX_ENTRY_ID_LENGTH))) {
    throw invalidSource("Compaction recovery source identity is invalid.");
  }
  const graph = await loadSessionGraph(resolve(sessionFile), new Set(), 0, { bytes: 0, entries: 0 });
  if (graph.header.id !== sessionId || graph.header.cwd !== cwd || graph.pending || graph.parentSessionPending) {
    throw invalidSource("Compaction recovery requires the complete original session and ancestry.");
  }
  return graph.entries.find(entry => entry.type === "compaction" && entry.parentId === leafId)?.id;
}

function snapshotFromGraph(
  graph: SessionGraph & { readonly parentSessionPending: boolean },
  sessionFile: string,
  leafId?: string | null,
  budget?: AggregateBudget,
): SourceSnapshot {
  const selectedLeaf = leafId === undefined ? graph.entries.at(-1)?.id ?? null : leafId;
  if (selectedLeaf !== null && !graph.entriesById.has(selectedLeaf)) {
    return {
      sessionId: graph.header.id,
      sessionFile: resolve(sessionFile),
      cwd: graph.header.cwd,
      leafId: null,
      entries: [],
      pending: true,
      pendingReason: "Requested source checkpoint is not present in the complete session prefix.",
      ...(graph.header.parentSession ? { parentSession: graph.header.parentSession } : {}),
      ancestry: { origins: graph.origins, signatures: graph.signatures },
    };
  }

  const branch = selectedLeaf === null ? [] : branchForLeaf(graph.entriesById, selectedLeaf);
  if (budget && !consumeAggregateBudget(budget,
    [graph.header.id, resolve(sessionFile), graph.header.cwd, branch], branch.length)) {
    throw invalidSource("Session branch coverage exceeds the aggregate size or entry limit.");
  }
  const editStatus = validateContextEdits(branch);
  const projection = buildSessionProjection(branch, selectedLeaf, graph.entriesById);
  const pendingReason = graph.pending
    ? "Session file has an incomplete trailing JSONL record."
    : graph.parentSessionPending
      ? "Fork ancestry could not be verified completely."
      : editStatus.reason;

  const snapshot: SourceSnapshot = {
    sessionId: graph.header.id,
    sessionFile: resolve(sessionFile),
    cwd: graph.header.cwd,
    leafId: selectedLeaf,
    entries: branch,
    ...(graph.header.parentSession ? { parentSession: graph.header.parentSession } : {}),
    ancestry: { origins: graph.origins, signatures: graph.signatures, ...(graph.parentSessionPending ? { unresolved: true } : {}) },
    ...(pendingReason ? { pending: true, pendingReason } : { pending: false }),
  };
  projections.set(snapshot, projection);
  return snapshot;
}

/** Content-free processing coverage for terminal branches and observed checkpoints. */
export interface SessionCoverage {
  readonly sessionId: string;
  readonly sourceKeys: readonly string[];
  /** Includes the session itself and every ancestor read to resolve its source identities. */
  readonly fileVersions: ReadonlyMap<string, string>;
}

export async function loadSessionCoverage(
  sessionFile: string,
  observations: readonly SessionObservation[] = [],
  budget: AggregateBudget = { bytes: 0, entries: 0 },
): Promise<SessionCoverage> {
  const path = resolve(sessionFile);
  const fileVersions = new Map<string, string>();
  const graph = await loadSessionGraph(path, new Set(), 0, budget, fileVersions);
  const parents = new Set(graph.entries.map(entry => entry.parentId));
  const leaves: readonly (string | null)[] = graph.entries.length === 0
    ? [null]
    : graph.entries.filter(entry => !parents.has(entry.id)).map(entry => entry.id);
  const checkpoints = observations.filter(observation =>
    observation.sessionId === graph.header.id || resolve(observation.sessionFile) === path);
  if (checkpoints.some(observation => observation.sessionId !== graph.header.id)) {
    throw invalidSource("Observed session identity does not match its current file.");
  }
  const keys = new Set<string>();
  for (const leaf of new Set([...leaves, ...checkpoints.map(observation => observation.leafId)])) {
    // Shared prefixes count again; the caller can share this budget across files.
    const snapshot = snapshotFromGraph(graph, path, leaf, budget);
    if (snapshot.pending) throw invalidSource(snapshot.pendingReason ?? "Incomplete session branch.");
    for (const batch of planSources(snapshot, new Set())) {
      for (const key of batch.sourceKeys) keys.add(key);
    }
  }
  return { sessionId: graph.header.id, sourceKeys: [...keys], fileVersions };
}

/** Recheck the read versions; this is not a lock or permission to delete. */
export async function sessionCoverageUnchanged(coverage: SessionCoverage): Promise<boolean> {
  try {
    for (const [path, version] of coverage.fileVersions) {
      if (await sessionFileVersion(path) !== version) return false;
    }
    return true;
  } catch {
    return false;
  }
}

async function sessionFileVersion(path: string): Promise<string> {
  const info = await lstat(path, { bigint: true });
  if (!info.isFile()) throw invalidSource("Session coverage requires a regular file.");
  return [info.dev, info.ino, info.size, info.mtimeNs, info.ctimeNs].join(":");
}

/** Establish whether an empty plan is valid rather than a failed source parse. */
export function isValidSourceSnapshot(snapshot: SourceSnapshot): boolean {
  if (snapshot.pending || snapshot.ancestry?.unresolved) return false;
  if (!validNonEmptyString(snapshot.sessionId, MAX_SESSION_ID_LENGTH) || typeof snapshot.cwd !== "string" ||
    !Array.isArray(snapshot.entries)) return false;
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  if (!consumeAggregateBudget(budget, [snapshot.sessionId, snapshot.sessionFile, snapshot.cwd, snapshot.entries], snapshot.entries.length)) return false;
  if (snapshot.leafId === null) return snapshot.entries.length === 0;
  try {
    const branch = validateBranch(snapshot.entries, snapshot.leafId);
    if (validateContextEdits(branch).reason) return false;
    getProjection(snapshot);
    return true;
  } catch { return false; }
}

export function planSources(snapshot: SourceSnapshot, processed: ReadonlySet<string>): readonly SourceBatch[] {
  if (snapshot.pending || snapshot.ancestry?.unresolved) return [];
  if (!validNonEmptyString(snapshot.sessionId, MAX_SESSION_ID_LENGTH) || typeof snapshot.cwd !== "string") return [];
  if (snapshot.leafId === null || snapshot.entries.length === 0) return [];
  const budget: AggregateBudget = { bytes: 0, entries: 0 };
  if (!consumeAggregateBudget(budget, [snapshot.sessionId, snapshot.sessionFile, snapshot.cwd, snapshot.entries], snapshot.entries.length)) return [];

  let branch: SessionEntry[];
  try {
    branch = validateBranch(snapshot.entries, snapshot.leafId);
  } catch {
    return [];
  }

  const editStatus = validateContextEdits(branch);
  if (editStatus.reason) return [];
  const effectiveContents = applyContextEdits(branch);
  const projection = getProjection(snapshot);
  const projected = projectionContentByEntry(projection);
  const allParts: SourcePart[] = [];
  const lineage = lineageFingerprints(branch, snapshot, effectiveContents, projected);

  for (const entry of branch) {
    const entryParts = sourcePartsForEntry(
      entry,
      effectiveContents.get(entry.id),
      projected.get(entry.id),
      originFor(snapshot, entry),
      lineage.get(entry.id) ?? "",
    );
    if (entryParts.length === 0) continue;
    allParts.push(...entryParts);
  }

  const pendingParts = allParts.filter((part) => !processed.has(part.key));
  if (pendingParts.length === 0) return [];

  const batches: SourceBatch[] = [];
  let cursor = 0;
  while (cursor < pendingParts.length) {
    const evidence: SourcePart[] = [];
    let next = pendingParts[cursor];
    while (next) {
      const candidate = [...evidence, next];
      if (serializedBytes(candidate) > MAX_SOURCE_JSON_BYTES) {
        if (evidence.length === 0) return [];
        break;
      }
      evidence.push(next);
      cursor++;
      next = pendingParts[cursor];
    }

    const firstEvidenceIndex = allParts.findIndex((part) => part.key === evidence[0]?.key);
    const context = selectContext(allParts.slice(0, Math.max(0, firstEvidenceIndex)), evidence[0]);
    const sourceKeys = evidence.map((part) => part.key);
    const branchFingerprint = hash(JSON.stringify([
      "pi-distill-branch-context-v1",
      snapshot.sessionId,
      evidence[0]?.entryId ?? "",
      context.map((part) => part.key),
    ]));
    const batchId = hash(JSON.stringify([
      "pi-distill-source-batch-v1",
      snapshot.sessionId,
      snapshot.leafId,
      branchFingerprint,
      sourceKeys,
    ]));
    batches.push({
      id: batchId,
      sessionId: snapshot.sessionId,
      sessionFile: snapshot.sessionFile,
      leafId: snapshot.leafId,
      cwd: snapshot.cwd,
      sourceKeys,
      evidence,
      context,
    });

  }
  return batches;
}

function getProjection(snapshot: SourceSnapshot): SessionProjection | undefined {
  return projections.get(snapshot);
}

function projectionContentByEntry(projection: SessionProjection | undefined): ReadonlyMap<string, ProjectionContent> {
  const result = new Map<string, ProjectionContent>();
  if (!projection) return result;
  for (const projected of projection.entries) {
    result.set(projected.sourceEntry.id, { messages: projected.messages });
  }
  return result;
}

export function sourcePartsForEntry(
  entry: SessionEntry,
  effective: EffectiveContent | undefined,
  projected: ProjectionContent | undefined,
  origin: SourceOrigin,
  entryIdentity: string,
): SourcePart[] {
  if (entry.type === "context_edit") {
    if (entry.replacement !== null && contentTextSegments(entry.replacement.content).length > 0) return [];
    const text = entry.replacement === null
      ? "[Earlier message content was removed from this branch.]"
      : "[Earlier message content was cleared in this branch.]";
    return [{ key: sourceKey(origin, entryIdentity, "context_edit", 0, text), entryId: origin.entryId, role: "context_edit", text }];
  }
  const sourceContent = sourceContentForEntry(entry, effective, projected);
  if (!sourceContent) return [];
  const { role, segments } = sourceContent;
  const parts: SourcePart[] = [];
  let ordinal = 0;
  for (const segment of segments) {
    for (const text of splitText(segment, origin.entryId, role)) {
      const key = sourceKey(origin, entryIdentity, role, ordinal, text);
      parts.push({ key, entryId: origin.entryId, role, text });
      ordinal++;
    }
  }
  return parts;
}

export function sourceContentForEntry(
  entry: SessionEntry,
  effective: EffectiveContent | undefined,
  projected: ProjectionContent | undefined,
): { readonly role: string; readonly segments: readonly string[] } | undefined {
  if (entry.type !== "message") return undefined;
  const role = entry.message.role;
  if (role !== "user" && role !== "assistant" && role !== "toolResult") return undefined;
  if (effective?.omitted) return undefined;

  let content: unknown = effective?.content ?? entry.message.content;
  if (projected) {
    if (projected.messages.length === 0) return undefined;
    const projectedMessage = projected.messages.find((message) => isRecord(message) && message.role === role);
    if (isRecord(projectedMessage) && "content" in projectedMessage) content = projectedMessage.content;
  }
  return { role, segments: contentTextSegments(content) };
}

function contentTextSegments(content: unknown): string[] {
  if (typeof content === "string") return content.length > 0 ? [content] : [];
  if (!Array.isArray(content)) return [];
  const segments: string[] = [];
  for (const block of content) {
    if (!isRecord(block) || typeof block.type !== "string") continue;
    if (block.type === "text" && typeof block.text === "string" && block.text.length > 0) {
      segments.push(block.text);
    } else if (block.type === "image") {
      segments.push("[image omitted]");
    }
    // Thinking, tool-call arguments, audio and unknown binary blocks never
    // enter evidence; in particular no image/audio payload is serialized.
  }
  return segments;
}

function splitText(text: string, entryId: string, role: string): string[] {
  if (text.length === 0) return [];
  const emptyPartBytes = Buffer.byteLength(JSON.stringify({ key: "0".repeat(64), entryId, role, text: "" }), "utf8");
  const maxEncodedTextBytes = Math.min(MAX_PART_JSON_BYTES, MAX_CONTEXT_JSON_BYTES - 8) - emptyPartBytes;
  if (maxEncodedTextBytes <= 0) throw invalidSource("Session entry metadata is too large for a bounded source part.");
  const chunks: string[] = [];
  let current = "";
  let rawBytes = 0;
  let encodedTextBytes = 0;
  for (const codePoint of text) {
    const codePointBytes = Buffer.byteLength(codePoint, "utf8");
    const jsonPointBytes = Buffer.byteLength(JSON.stringify(codePoint), "utf8") - 2;
    if (
      current.length > 0 &&
      (rawBytes + codePointBytes > MAX_PART_TEXT_BYTES ||
        encodedTextBytes + jsonPointBytes > maxEncodedTextBytes)
    ) {
      chunks.push(current);
      current = "";
      rawBytes = 0;
      encodedTextBytes = 0;
    }
    current += codePoint;
    rawBytes += codePointBytes;
    encodedTextBytes += jsonPointBytes;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

function selectContext(prior: readonly SourcePart[], firstEvidence: SourcePart | undefined): SourcePart[] {
  if (!firstEvidence) return [];
  const before = prior.filter((part) => part.entryId !== firstEvidence.entryId || part.key !== firstEvidence.key);
  const selected: SourcePart[] = [];
  for (let index = before.length - 1; index >= 0; index--) {
    const candidate = [before[index]!, ...selected];
    if (serializedBytes(candidate) <= MAX_CONTEXT_JSON_BYTES) selected.unshift(before[index]!);
  }
  return selected;
}

function serializedBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

function sourceKey(origin: SourceOrigin, entryIdentity: string, role: string, ordinal: number, text: string): string {
  return hash(JSON.stringify(["pi-distill-source-v1", origin.sessionId, origin.entryId, entryIdentity, role, ordinal, text]));
}

function originFor(snapshot: SourceSnapshot, entry: SessionEntry): SourceOrigin {
  return snapshot.ancestry?.origins.get(entry.id) ?? { sessionId: snapshot.sessionId, entryId: entry.id };
}

function lineageFingerprints(
  entries: readonly SessionEntry[],
  snapshot: SourceSnapshot,
  effectiveContents: ReadonlyMap<string, EffectiveContent>,
  projected: ReadonlyMap<string, ProjectionContent>,
): ReadonlyMap<string, string> {
  const result = new Map<string, string>();
  let conversationFingerprint = hash("pi-distill-source-root-v1");
  for (const entry of entries) {
    const origin = originFor(snapshot, entry);
    const step = sourceEntryIdentity(entry, origin, conversationFingerprint, effectiveContents.get(entry.id), projected.get(entry.id));
    result.set(entry.id, step.identity);
    conversationFingerprint = step.nextFingerprint;
  }
  return result;
}

export function sourceEntryIdentity(entry: SessionEntry, origin: SourceOrigin, conversationFingerprint: string,
  effective?: EffectiveContent, projected?: ProjectionContent): { identity: string; nextFingerprint: string } {
  const content = sourceContentForEntry(entry, effective, projected);
  const event = entry.type === "context_edit" ? hash(stableJson(entry.replacement))
    : content && content.segments.length > 0 ? hash(JSON.stringify([content.role, content.segments])) : "";
  const identity = hash(JSON.stringify(["pi-distill-entry-lineage-v1", origin.sessionId, origin.entryId,
    conversationFingerprint, entry.type, entry.timestamp, event]));
  const nextFingerprint = (content && content.segments.length > 0) || entry.type === "context_edit"
    ? hash(JSON.stringify(["pi-distill-conversation-branch-v1", conversationFingerprint, origin.sessionId, origin.entryId, identity, event]))
    : conversationFingerprint;
  return { identity, nextFingerprint };
}

function hash(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function validateContextEdits(entries: readonly SessionEntry[]): { readonly reason?: string } {
  const seen = new Map<string, SessionEntry>();
  for (const entry of entries) {
    if (entry.type !== "context_edit") {
      seen.set(entry.id, entry);
      continue;
    }
    const target = seen.get(entry.targetId);
    if (!target || !isEditableEntry(target)) {
      return { reason: "A context_edit target is not an earlier supported message entry." };
    }
    if (entry.replacement !== null && !isContextEditableContent(target.message.role, entry.replacement.content)) {
      return { reason: "A context_edit replacement has unsupported content." };
    }
    seen.set(entry.id, entry);
  }
  return {};
}

function isEditableEntry(entry: SessionEntry): entry is Extract<SessionEntry, { type: "message" }> {
  if (entry.type !== "message") return false;
  return entry.message.role === "user" || entry.message.role === "assistant" || entry.message.role === "toolResult";
}

function isContent(value: unknown): boolean {
  if (typeof value === "string") return true;
  if (!Array.isArray(value)) return false;
  return value.every((block) => isTextContentBlock(block) || isThinkingContentBlock(block) || isImageContentBlock(block) || isToolCallBlock(block));
}

export function isContextEditableContent(role: string, value: unknown): boolean {
  switch (role) {
    case "user":
      return isUserContent(value);
    case "assistant":
      return isAssistantContent(value);
    case "toolResult":
      return isImageTextContentArray(value);
    default:
      return false;
  }
}

function pendingSnapshot(
  sessionId: string,
  sessionFile: string | null,
  cwd: string,
  leafId: string | null,
  pendingReason: string,
): SourceSnapshot {
  return {
    sessionId,
    sessionFile,
    cwd,
    leafId,
    entries: [],
    pending: true,
    pendingReason,
    ancestry: { origins: new Map(), signatures: new Map(), unresolved: true },
  };
}

function consumeAggregateBudget(budget: AggregateBudget, value: unknown, entryCount: number): boolean {
  if (!consumeEntryBudget(budget, entryCount)) return false;
  const bytes = boundedJsonBytes(value, MAX_ANCESTRY_JSON_BYTES - budget.bytes);
  if (bytes === undefined) {
    budget.entries -= entryCount;
    return false;
  }
  budget.bytes += bytes;
  return true;
}

function consumeEntryBudget(budget: AggregateBudget, entryCount: number): boolean {
  if (!Number.isSafeInteger(entryCount) || entryCount < 0 || entryCount > MAX_ANCESTRY_ENTRIES - budget.entries) return false;
  budget.entries += entryCount;
  return true;
}

function consumeRawByteBudget(budget: AggregateBudget, byteCount: number): boolean {
  if (!Number.isSafeInteger(byteCount) || byteCount < 0 || byteCount > MAX_ANCESTRY_JSON_BYTES - budget.bytes) return false;
  budget.bytes += byteCount;
  return true;
}

export function boundedJsonBytes(value: unknown, limit: number): number | undefined {
  if (!Number.isSafeInteger(limit) || limit < 0) return undefined;
  const active = new WeakSet<object>();
  const count = (current: unknown, remaining: number, depth: number, inArray: boolean): number | undefined => {
    if (remaining < 0 || depth > MAX_JSON_DEPTH) return undefined;
    if (current === null) return remaining >= 4 ? 4 : undefined;
    switch (typeof current) {
      case "string":
        return jsonStringBytes(current, remaining);
      case "boolean": {
        const bytes = current ? 4 : 5;
        return remaining >= bytes ? bytes : undefined;
      }
      case "number": {
        const bytes = Number.isFinite(current) ? String(current).length : 4;
        return remaining >= bytes ? bytes : undefined;
      }
      case "undefined":
      case "function":
      case "symbol":
        if (inArray) return remaining >= 4 ? 4 : undefined;
        return 0;
      case "bigint":
        return undefined;
      case "object":
        break;
      default:
        return undefined;
    }

    const object = current;
    if (active.has(object)) return undefined;
    active.add(object);
    try {
      if (Array.isArray(object)) {
        let total = 2;
        if (total > remaining) return undefined;
        for (let index = 0; index < object.length; index += 1) {
          if (index > 0) total += 1;
          const child = count(object[index], remaining - total, depth + 1, true);
          if (child === undefined) return undefined;
          total += child;
        }
        return total;
      }

      let total = 2;
      if (total > remaining) return undefined;
      let first = true;
      for (const key in object) {
        if (!Object.prototype.hasOwnProperty.call(object, key)) continue;
        let childValue: unknown;
        try {
          childValue = Reflect.get(object, key);
        } catch {
          return undefined;
        }
        if (childValue === undefined || typeof childValue === "function" || typeof childValue === "symbol") continue;
        if (!first) total += 1;
        first = false;
        const keyBytes = jsonStringBytes(key, remaining - total);
        if (keyBytes === undefined) return undefined;
        total += keyBytes + 1;
        const childBytes = count(childValue, remaining - total, depth + 1, false);
        if (childBytes === undefined) return undefined;
        total += childBytes;
        if (total > remaining) return undefined;
      }
      return total;
    } finally {
      active.delete(object);
    }
  };

  return count(value, limit, 0, false);
}

function jsonStringBytes(value: string, limit: number): number | undefined {
  let total = 2;
  if (total > limit) return undefined;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    let bytes: number;
    if (code === 0x22 || code === 0x5c || code === 0x08 || code === 0x09 || code === 0x0a || code === 0x0c || code === 0x0d) {
      bytes = 2;
    } else if (code < 0x20) {
      bytes = 6;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes = 4;
        index += 1;
      } else {
        bytes = 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      bytes = 6;
    } else if (code <= 0x7f) {
      bytes = 1;
    } else if (code <= 0x7ff) {
      bytes = 2;
    } else {
      bytes = 3;
    }
    total += bytes;
    if (total > limit) return undefined;
  }
  return total;
}

async function readBoundedUtf8(path: string, budget: AggregateBudget): Promise<string> {
  const remaining = MAX_ANCESTRY_JSON_BYTES - budget.bytes;
  if (remaining < 0) throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
  const buffer = Buffer.allocUnsafe(remaining + 1);
  const file = await open(path, "r");
  let total = 0;
  try {
    while (total < buffer.length) {
      const { bytesRead } = await file.read(buffer, total, buffer.length - total, null);
      if (bytesRead === 0) break;
      total += bytesRead;
      if (total > remaining) {
        // Failed reads still consume a shared assessment budget.
        budget.bytes = MAX_ANCESTRY_JSON_BYTES;
        throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
      }
    }
  } finally {
    await file.close();
  }
  if (!consumeRawByteBudget(budget, total)) throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
  try {
    return utf8Decoder.decode(buffer.subarray(0, total));
  } catch {
    throw invalidSource("Session file is not valid UTF-8.");
  }
}

function readBoundedUtf8Sync(path: string, budget: AggregateBudget): string {
  const remaining = MAX_ANCESTRY_JSON_BYTES - budget.bytes;
  if (remaining < 0) throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
  const buffer = Buffer.allocUnsafe(remaining + 1);
  const descriptor = openSync(path, "r");
  let total = 0;
  try {
    while (total < buffer.length) {
      const bytesRead = readSync(descriptor, buffer, total, buffer.length - total, null);
      if (bytesRead === 0) break;
      total += bytesRead;
      if (total > remaining) throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
    }
  } finally {
    closeSync(descriptor);
  }
  if (!consumeRawByteBudget(budget, total)) throw invalidSource("Session ancestry exceeds the aggregate byte limit.");
  try {
    return utf8Decoder.decode(buffer.subarray(0, total));
  } catch {
    throw invalidSource("Session file is not valid UTF-8.");
  }
}

function applyContextEdits(entries: readonly SessionEntry[]): ReadonlyMap<string, EffectiveContent> {
  const result = new Map<string, EffectiveContent>();
  for (const entry of entries) {
    if (entry.type !== "context_edit") continue;
    const previous = result.get(entry.targetId);
    if (entry.replacement === null) {
      result.set(entry.targetId, { content: previous?.content, omitted: true });
    } else {
      result.set(entry.targetId, { content: entry.replacement.content, omitted: false });
    }
  }
  return result;
}

export function validateSessionHeader(value: unknown): asserts value is SessionHeader {
  if (!isRecord(value) || value.type !== "session") throw invalidSource("Session JSONL must start with a session header.");
  if (!validNonEmptyString(value.id, MAX_SESSION_ID_LENGTH)) throw invalidSource("Session header has an invalid id.");
  if (typeof value.timestamp !== "string" || value.timestamp.length === 0) throw invalidSource("Session header has an invalid timestamp.");
  if (typeof value.cwd !== "string") throw invalidSource("Session header has an invalid cwd.");
  if (value.version !== undefined) {
    if (typeof value.version !== "number" || !Number.isInteger(value.version) || value.version < 1) {
      throw invalidSource("Session header has an invalid version.");
    }
  }
  if (value.parentSession !== undefined && !validNonEmptyString(value.parentSession, 16_384)) {
    throw invalidSource("Session header has an invalid parentSession path.");
  }
}

function parseSessionText(text: string, budget: AggregateBudget): ParsedSession {
  if (text.length === 0) throw invalidSource("Session file is empty.");
  const hasFinalNewline = text.endsWith("\n");
  const lines = text.split("\n");
  if (hasFinalNewline) lines.pop();
  let pending = false;
  if (lines.length === 0 || lines[0]?.length === 0) throw invalidSource("Session JSONL is missing its header.");

  let headerValue: unknown;
  try {
    headerValue = JSON.parse(lines[0]!);
  } catch {
    throw invalidSource("Session JSONL header is malformed.");
  }
  validateSessionHeader(headerValue);

  const entries: SessionEntry[] = [];
  for (let index = 1; index < lines.length; index++) {
    const line = lines[index]!;
    if (line.length === 0) throw invalidSource(`Session JSONL contains an empty record at line ${index + 1}.`);
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      if (!hasFinalNewline && index === lines.length - 1) {
        pending = true;
        break;
      }
      throw invalidSource(`Session JSONL record at line ${index + 1} is malformed.`);
    }
    try {
      validateEntryShape(value, index + 1);
    } catch (error) {
      if (!hasFinalNewline && index === lines.length - 1) {
        pending = true;
        break;
      }
      throw error;
    }
    if (!consumeEntryBudget(budget, 1)) {
      throw invalidSource("Session ancestry exceeds the aggregate entry limit.");
    }
    entries.push(value);
  }
  validateSessionEntries(entries);
  return { header: headerValue, entries, pending };
}

export function validateEntryShape(value: unknown, lineNumber: number): asserts value is SessionEntry {
  const fail = (): never => {
    throw invalidSource(`Session JSONL record at line ${lineNumber} has an invalid ${isRecord(value) ? String(value.type) : "entry"} shape.`);
  };
  if (!isRecord(value) || !validNonEmptyString(value.id, MAX_ENTRY_ID_LENGTH)) return fail();
  if (value.parentId !== null && !validNonEmptyString(value.parentId, MAX_ENTRY_ID_LENGTH)) return fail();
  if (typeof value.timestamp !== "string" || value.timestamp.length === 0) return fail();

  switch (value.type) {
    case "message":
      if (!isRecord(value.message) || !isKnownMessageRole(value.message.role)) return fail();
      if (!validStoredMessage(value.message)) return fail();
      break;
    case "thinking_level_change":
      if (typeof value.thinkingLevel !== "string") return fail();
      break;
    case "model_change":
      if (typeof value.provider !== "string" || typeof value.modelId !== "string") return fail();
      break;
    case "usage":
      if (typeof value.kind !== "string" || typeof value.provider !== "string" || typeof value.model !== "string") return fail();
      break;
    case "compaction":
      if (typeof value.summary !== "string" || (typeof value.firstKeptEntryId !== "string" && value.firstKeptEntryId !== null) || typeof value.tokensBefore !== "number") return fail();
      break;
    case "branch_summary":
      if (!validNonEmptyString(value.fromId, MAX_ENTRY_ID_LENGTH) || typeof value.summary !== "string") return fail();
      break;
    case "custom":
      if (typeof value.customType !== "string") return fail();
      break;
    case "custom_message":
      if (typeof value.customType !== "string" || !isUserContent(value.content) || typeof value.display !== "boolean") return fail();
      break;
    case "context_edit":
      if (!validNonEmptyString(value.targetId, MAX_ENTRY_ID_LENGTH)) return fail();
      if (value.replacement !== null && (!isRecord(value.replacement) || !isContent(value.replacement.content))) return fail();
      break;
    case "label":
      if (!validNonEmptyString(value.targetId, MAX_ENTRY_ID_LENGTH) || (value.label !== undefined && typeof value.label !== "string")) return fail();
      break;
    case "session_info":
      if (value.name !== undefined && typeof value.name !== "string") return fail();
      break;
    default:
      return fail();
  }
}

function validateSessionEntries(entries: readonly SessionEntry[]): void {
  const byId = new Map<string, SessionEntry>();
  const indexById = new Map<string, number>();
  for (const [index, value] of entries.entries()) {
    validateEntryShape(value, index + 2);
    if (byId.has(value.id)) throw invalidSource(`Session contains duplicate entry id ${value.id}.`);
    byId.set(value.id, value);
    indexById.set(value.id, index);
  }

  for (const [index, entry] of entries.entries()) {
    if (entry.parentId === null) {
      continue;
    }
    const parentIndex = indexById.get(entry.parentId);
    if (parentIndex === undefined) throw invalidSource(`Session entry ${entry.id} has missing parent ${entry.parentId}.`);
    if (parentIndex >= index) throw invalidSource(`Session entry ${entry.id} points to a later entry ${entry.parentId}.`);
  }
  const state = new Map<string, "visiting" | "visited">();
  const visit = (id: string): void => {
    const current = state.get(id);
    if (current === "visiting") throw invalidSource(`Session entry graph contains a parent cycle at ${id}.`);
    if (current === "visited") return;
    state.set(id, "visiting");
    const entry = byId.get(id);
    if (entry?.parentId !== null && entry?.parentId !== undefined) visit(entry.parentId);
    state.set(id, "visited");
  };
  for (const entry of entries) visit(entry.id);
}

function validateBranch(entries: readonly unknown[], leafId: string): SessionEntry[] {
  if (!validNonEmptyString(leafId, MAX_ENTRY_ID_LENGTH)) throw invalidSource("Session checkpoint has an invalid leaf id.");
  const typed: SessionEntry[] = [];
  const byId = new Map<string, SessionEntry>();
  for (const [index, value] of entries.entries()) {
    validateEntryShape(value, index + 2);
    if (byId.has(value.id)) throw invalidSource(`Snapshot contains duplicate entry id ${value.id}.`);
    byId.set(value.id, value);
    typed.push(value);
  }
  const leaf = byId.get(leafId);
  if (!leaf) throw invalidSource(`Session checkpoint ${leafId} is absent from the snapshot.`);
  const reverse: SessionEntry[] = [];
  const seen = new Set<string>();
  let current: SessionEntry | undefined = leaf;
  while (current) {
    if (seen.has(current.id)) throw invalidSource(`Session branch contains a parent cycle at ${current.id}.`);
    seen.add(current.id);
    reverse.push(current);
    if (current.parentId === null) break;
    const parent = byId.get(current.parentId);
    if (!parent) throw invalidSource(`Session branch has missing parent ${current.parentId}.`);
    current = parent;
  }
  reverse.reverse();
  for (let index = 1; index < reverse.length; index++) {
    if (reverse[index]!.parentId !== reverse[index - 1]!.id) throw invalidSource("Session branch is not a contiguous parent chain.");
  }
  return reverse;
}

function branchForLeaf(entriesById: ReadonlyMap<string, SessionEntry>, leafId: string): SessionEntry[] {
  const reverse: SessionEntry[] = [];
  const seen = new Set<string>();
  let current = entriesById.get(leafId);
  if (!current) throw invalidSource(`Session checkpoint ${leafId} is missing.`);
  while (current) {
    if (seen.has(current.id)) throw invalidSource(`Session branch contains a parent cycle at ${current.id}.`);
    seen.add(current.id);
    reverse.push(current);
    current = current.parentId === null ? undefined : entriesById.get(current.parentId);
    if (reverse.at(-1)?.parentId !== null && current === undefined) throw invalidSource("Session branch has a missing parent.");
  }
  return reverse.reverse();
}

function syncAncestry(
  sessionId: string,
  entries: readonly SessionEntry[],
  sessionFile: string | undefined,
  parentSession: string | undefined,
  budget: AggregateBudget,
): SnapshotAncestry {
  const ownEntries = entryMaps(entries, sessionId);
  if (!parentSession) return { origins: ownEntries.origins, signatures: ownEntries.signatures };
  if (!sessionFile) return { origins: ownEntries.origins, signatures: ownEntries.signatures, unresolved: true };
  try {
    const parent = loadSessionGraphSync(resolve(dirname(resolve(sessionFile)), parentSession), new Set(), 0, budget);
    const origins = new Map<string, SourceOrigin>();
    const signatures = new Map<string, string>();
    for (const entry of entries) {
      const signature = stableJson(entry);
      signatures.set(entry.id, signature);
      const parentSignature = parent.signatures.get(entry.id);
      const parentOrigin = parent.origins.get(entry.id);
      origins.set(entry.id, parentSignature === signature && parentOrigin
        ? parentOrigin
        : { sessionId, entryId: entry.id });
    }
    return {
      origins,
      signatures,
      ...(parent.pending || parent.parentSessionPending ? { unresolved: true } : {}),
    };
  } catch {
    return { origins: ownEntries.origins, signatures: ownEntries.signatures, unresolved: true };
  }
}

async function loadSessionGraph(
  path: string,
  ancestors: Set<string>,
  depth: number,
  budget: AggregateBudget,
  fileVersions?: Map<string, string>,
): Promise<SessionGraph & { readonly parentSessionPending: boolean }> {
  if (depth > MAX_ANCESTRY_DEPTH) throw invalidSource("Session parentSession ancestry exceeds the supported depth.");
  const resolvedPath = resolve(path);
  if (ancestors.has(resolvedPath)) throw invalidSource("Session parentSession ancestry contains a file cycle.");
  const nextAncestors = new Set(ancestors);
  nextAncestors.add(resolvedPath);
  const before = fileVersions ? await sessionFileVersion(resolvedPath) : undefined;
  const parsed = parseSessionText(await readBoundedUtf8(resolvedPath, budget), budget);
  if (fileVersions && before !== undefined) {
    if (await sessionFileVersion(resolvedPath) !== before) {
      throw new DistillFailure("conflict", "Session changed while reading.");
    }
    fileVersions.set(resolvedPath, before);
  }
  const local = entryMaps(parsed.entries, parsed.header.id);
  if (!parsed.header.parentSession) {
    return { header: parsed.header, entries: parsed.entries, entriesById: local.entriesById, signatures: local.signatures, origins: local.origins, pending: parsed.pending, parentSessionPending: false };
  }
  const parentPath = resolve(dirname(resolvedPath), parsed.header.parentSession);
  const parent = await loadSessionGraph(parentPath, nextAncestors, depth + 1, budget, fileVersions);
  if (parent.header.id === parsed.header.id) throw invalidSource("Forked sessions must have distinct session ids.");
  const origins = new Map<string, SourceOrigin>();
  const signatures = new Map<string, string>();
  for (const entry of parsed.entries) {
    const signature = stableJson(entry);
    signatures.set(entry.id, signature);
    const parentSignature = parent.signatures.get(entry.id);
    const parentOrigin = parent.origins.get(entry.id);
    origins.set(entry.id, parentSignature === signature && parentOrigin
      ? parentOrigin
      : { sessionId: parsed.header.id, entryId: entry.id });
  }
  return {
    header: parsed.header,
    entries: parsed.entries,
    entriesById: local.entriesById,
    signatures,
    origins,
    pending: parsed.pending,
    parentSessionPending: parent.pending || parent.parentSessionPending,
  };
}

function loadSessionGraphSync(
  path: string,
  ancestors: Set<string>,
  depth: number,
  budget: AggregateBudget,
): SessionGraph & { readonly parentSessionPending: boolean } {
  if (depth > MAX_ANCESTRY_DEPTH) throw invalidSource("Session parentSession ancestry exceeds the supported depth.");
  const resolvedPath = resolve(path);
  if (ancestors.has(resolvedPath)) throw invalidSource("Session parentSession ancestry contains a file cycle.");
  const nextAncestors = new Set(ancestors);
  nextAncestors.add(resolvedPath);
  const parsed = parseSessionText(readBoundedUtf8Sync(resolvedPath, budget), budget);
  const local = entryMaps(parsed.entries, parsed.header.id);
  if (!parsed.header.parentSession) {
    return { header: parsed.header, entries: parsed.entries, entriesById: local.entriesById, signatures: local.signatures, origins: local.origins, pending: parsed.pending, parentSessionPending: false };
  }
  const parentPath = resolve(dirname(resolvedPath), parsed.header.parentSession);
  const parent = loadSessionGraphSync(parentPath, nextAncestors, depth + 1, budget);
  if (parent.header.id === parsed.header.id) throw invalidSource("Forked sessions must have distinct session ids.");
  const origins = new Map<string, SourceOrigin>();
  const signatures = new Map<string, string>();
  for (const entry of parsed.entries) {
    const signature = stableJson(entry);
    signatures.set(entry.id, signature);
    const parentSignature = parent.signatures.get(entry.id);
    const parentOrigin = parent.origins.get(entry.id);
    origins.set(entry.id, parentSignature === signature && parentOrigin
      ? parentOrigin
      : { sessionId: parsed.header.id, entryId: entry.id });
  }
  return {
    header: parsed.header,
    entries: parsed.entries,
    entriesById: local.entriesById,
    signatures,
    origins,
    pending: parsed.pending,
    parentSessionPending: parent.pending || parent.parentSessionPending,
  };
}

function entryMaps(entries: readonly SessionEntry[], sessionId: string): SessionGraph {
  const entriesById = new Map<string, SessionEntry>();
  const signatures = new Map<string, string>();
  const origins = new Map<string, SourceOrigin>();
  for (const entry of entries) {
    entriesById.set(entry.id, entry);
    signatures.set(entry.id, stableJson(entry));
    origins.set(entry.id, { sessionId, entryId: entry.id });
  }
  // These placeholder fields are only used when the map portion of a graph is
  // needed for origin comparison in snapshotSession.
  return { header: { type: "session", id: sessionId, cwd: "", timestamp: "" }, entries, entriesById, signatures, origins, pending: false };
}

export function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map((item) => stableJson(item)).join(",")}]`;
  if (isRecord(value)) {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function validNonEmptyString(value: unknown, maxLength: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength;
}

function isKnownMessageRole(value: unknown): value is string {
  return value === "system" || value === "user" || value === "assistant" || value === "toolResult" ||
    value === "bashExecution" || value === "custom" || value === "branchSummary" || value === "compactionSummary";
}

function validStoredMessage(message: Record<string, unknown>): boolean {
  switch (message.role) {
    case "system":
      return validTimestamp(message.timestamp) && isSystemContent(message.content) &&
        (message.sections === undefined || isSystemSections(message.sections)) &&
        (message.toolsAdded === undefined || isRecordArray(message.toolsAdded)) &&
        (message.toolsRemoved === undefined || isRecordArray(message.toolsRemoved));
    case "user":
      return validTimestamp(message.timestamp) && isUserContent(message.content);
    case "assistant":
      return validTimestamp(message.timestamp) && isAssistantContent(message.content);
    case "toolResult":
      return validTimestamp(message.timestamp) && isImageTextContentArray(message.content) &&
        validNonEmptyString(message.toolCallId, 512) && validNonEmptyString(message.toolName, 512) && typeof message.isError === "boolean";
    case "bashExecution":
      return validTimestamp(message.timestamp) && typeof message.command === "string" && typeof message.output === "string" &&
        (message.exitCode === undefined || typeof message.exitCode === "number") && typeof message.cancelled === "boolean" &&
        typeof message.truncated === "boolean";
    case "custom":
      return validTimestamp(message.timestamp) && validNonEmptyString(message.customType, 512) &&
        isUserContent(message.content) && typeof message.display === "boolean";
    case "branchSummary":
      return validTimestamp(message.timestamp) && typeof message.summary === "string" &&
        (message.fromId === null || validNonEmptyString(message.fromId, MAX_ENTRY_ID_LENGTH));
    case "compactionSummary":
      return validTimestamp(message.timestamp) && typeof message.summary === "string" &&
        typeof message.tokensBefore === "number" && Number.isFinite(message.tokensBefore);
    default:
      return false;
  }
}

function validTimestamp(value: unknown): boolean {
  return typeof value === "number" && Number.isFinite(value);
}

function isSystemContent(value: unknown): boolean {
  if (typeof value === "string") return true;
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block));
}

function isSystemSections(value: unknown): boolean {
  return isRecord(value) && Object.values(value).every((section) => typeof section === "string" || section === null);
}

function isUserContent(value: unknown): boolean {
  return typeof value === "string" || isImageTextContentArray(value);
}

function isAssistantContent(value: unknown): boolean {
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block) || isThinkingContentBlock(block) || isToolCallBlock(block));
}

function isImageTextContentArray(value: unknown): boolean {
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block) || isImageContentBlock(block));
}

function isTextContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "text" && typeof value.text === "string";
}

function isThinkingContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "thinking" && typeof value.thinking === "string";
}

function isImageContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "image" && typeof value.data === "string" && typeof value.mimeType === "string";
}

function isToolCallBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "toolCall" && validNonEmptyString(value.id, 512) &&
    validNonEmptyString(value.name, 512) && isRecord(value.arguments);
}

function isRecordArray(value: unknown): boolean {
  return Array.isArray(value) && value.every(isRecord);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
