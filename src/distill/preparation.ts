import {
  type SessionEntry
} from "@earendil-works/pi-coding-agent";
import { consumeAggregateBudget } from '../sources/budget.ts';
import { contentTextSegments, sourceContentForEntry } from '../sources/content.ts';
import { invalidSource, MAX_SESSION_ID_LENGTH, validateBranch, validateContextEdits, validNonEmptyString } from '../sources/format.ts';
import { hash, lineageFingerprints, originFor, sourceKey } from '../sources/identity.ts';
import { applyContextEdits, getProjection, projectionContentByEntry } from '../sources/projection.ts';
import type { AggregateBudget, EffectiveContent, ProjectionContent, SourceOrigin, SourceSnapshot } from '../sources/types.ts';
import type { SourceBatch, SourcePart } from "./types.ts";
export const MAX_PART_TEXT_BYTES = 1024;

export const MAX_PART_JSON_BYTES = 2 * 1024;

export const MAX_SOURCE_JSON_BYTES = 24 * 1024;

export const MAX_CONTEXT_JSON_BYTES = 8 * 1024;

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

export function splitText(text: string, entryId: string, role: string): string[] {
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

export function selectContext(prior: readonly SourcePart[], firstEvidence: SourcePart | undefined): SourcePart[] {
  if (!firstEvidence) return [];
  const before = prior.filter((part) => part.entryId !== firstEvidence.entryId || part.key !== firstEvidence.key);
  const selected: SourcePart[] = [];
  for (let index = before.length - 1; index >= 0; index--) {
    const candidate = [before[index]!, ...selected];
    if (serializedBytes(candidate) <= MAX_CONTEXT_JSON_BYTES) selected.unshift(before[index]!);
  }
  return selected;
}

export function serializedBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}
