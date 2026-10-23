import {
  type SessionEntry
} from "@earendil-works/pi-coding-agent";
import { closeSync, openSync, readSync } from "node:fs";
import { open } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { TextDecoder } from "node:util";
import { consumeEntryBudget, consumeRawByteBudget, MAX_ANCESTRY_JSON_BYTES } from './budget.ts';
import { invalidSource, validateEntryShape, validateSessionEntries, validateSessionHeader } from './format.ts';
import { stableJson } from './identity.ts';
import type { AggregateBudget, ParsedSession, SessionGraph, SnapshotAncestry, SourceOrigin } from './types.ts';
export const MAX_ANCESTRY_DEPTH = 128;

export const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

export async function readBoundedUtf8(path: string, budget: AggregateBudget): Promise<string> {
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

export function readBoundedUtf8Sync(path: string, budget: AggregateBudget): string {
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

export function parseSessionText(text: string, budget: AggregateBudget): ParsedSession {
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

export function syncAncestry(
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

export async function loadSessionGraph(
  path: string,
  ancestors: Set<string>,
  depth: number,
  budget: AggregateBudget,
): Promise<SessionGraph & { readonly parentSessionPending: boolean; }> {
  if (depth > MAX_ANCESTRY_DEPTH) throw invalidSource("Session parentSession ancestry exceeds the supported depth.");
  const resolvedPath = resolve(path);
  if (ancestors.has(resolvedPath)) throw invalidSource("Session parentSession ancestry contains a file cycle.");
  const nextAncestors = new Set(ancestors);
  nextAncestors.add(resolvedPath);
  const parsed = parseSessionText(await readBoundedUtf8(resolvedPath, budget), budget);
  const local = entryMaps(parsed.entries, parsed.header.id);
  if (!parsed.header.parentSession) {
    return { header: parsed.header, entries: parsed.entries, entriesById: local.entriesById, signatures: local.signatures, origins: local.origins, pending: parsed.pending, parentSessionPending: false };
  }
  const parentPath = resolve(dirname(resolvedPath), parsed.header.parentSession);
  const parent = await loadSessionGraph(parentPath, nextAncestors, depth + 1, budget);
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

export function loadSessionGraphSync(
  path: string,
  ancestors: Set<string>,
  depth: number,
  budget: AggregateBudget,
): SessionGraph & { readonly parentSessionPending: boolean; } {
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

export function entryMaps(entries: readonly SessionEntry[], sessionId: string): SessionGraph {
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
