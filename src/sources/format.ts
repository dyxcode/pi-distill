import {
  type SessionEntry,
  type SessionHeader
} from "@earendil-works/pi-coding-agent";
import { DistillFailure } from "../distill/failure.ts";

export const MAX_SESSION_ID_LENGTH = 512;

export const MAX_ENTRY_ID_LENGTH = 512;

export function invalidSource(message: string): DistillFailure {
  return new DistillFailure("source_invalid", message);
}

export function validateContextEdits(entries: readonly SessionEntry[]): { readonly reason?: string; } {
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

export function isEditableEntry(entry: SessionEntry): entry is Extract<SessionEntry, { type: "message"; }> {
  if (entry.type !== "message") return false;
  return entry.message.role === "user" || entry.message.role === "assistant" || entry.message.role === "toolResult";
}

export function isContent(value: unknown): boolean {
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

export function validateSessionEntries(entries: readonly SessionEntry[]): void {
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

export function validateBranch(entries: readonly unknown[], leafId: string): SessionEntry[] {
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

export function branchForLeaf(entriesById: ReadonlyMap<string, SessionEntry>, leafId: string): SessionEntry[] {
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

export function validNonEmptyString(value: unknown, maxLength: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength;
}

export function isKnownMessageRole(value: unknown): value is string {
  return value === "system" || value === "user" || value === "assistant" || value === "toolResult" ||
    value === "bashExecution" || value === "custom" || value === "branchSummary" || value === "compactionSummary";
}

export function validStoredMessage(message: Record<string, unknown>): boolean {
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

export function validTimestamp(value: unknown): boolean {
  return typeof value === "number" && Number.isFinite(value);
}

export function isSystemContent(value: unknown): boolean {
  if (typeof value === "string") return true;
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block));
}

export function isSystemSections(value: unknown): boolean {
  return isRecord(value) && Object.values(value).every((section) => typeof section === "string" || section === null);
}

export function isUserContent(value: unknown): boolean {
  return typeof value === "string" || isImageTextContentArray(value);
}

export function isAssistantContent(value: unknown): boolean {
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block) || isThinkingContentBlock(block) || isToolCallBlock(block));
}

export function isImageTextContentArray(value: unknown): boolean {
  return Array.isArray(value) && value.every((block) => isTextContentBlock(block) || isImageContentBlock(block));
}

export function isTextContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "text" && typeof value.text === "string";
}

export function isThinkingContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "thinking" && typeof value.thinking === "string";
}

export function isImageContentBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "image" && typeof value.data === "string" && typeof value.mimeType === "string";
}

export function isToolCallBlock(value: unknown): boolean {
  return isRecord(value) && value.type === "toolCall" && validNonEmptyString(value.id, 512) &&
    validNonEmptyString(value.name, 512) && isRecord(value.arguments);
}

export function isRecordArray(value: unknown): boolean {
  return Array.isArray(value) && value.every(isRecord);
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
