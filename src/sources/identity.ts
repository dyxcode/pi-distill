import {
  type SessionEntry
} from "@earendil-works/pi-coding-agent";
import { createHash } from "node:crypto";
import { sourceContentForEntry } from './content.ts';
import { isRecord } from './format.ts';
import type { EffectiveContent, ProjectionContent, SourceOrigin, SourceSnapshot } from './types.ts';
export function sourceKey(origin: SourceOrigin, entryIdentity: string, role: string, ordinal: number, text: string): string {
  return hash(JSON.stringify(["pi-distill-source-v1", origin.sessionId, origin.entryId, entryIdentity, role, ordinal, text]));
}

export function originFor(snapshot: SourceSnapshot, entry: SessionEntry): SourceOrigin {
  return snapshot.ancestry?.origins.get(entry.id) ?? { sessionId: snapshot.sessionId, entryId: entry.id };
}

export function lineageFingerprints(
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
  effective?: EffectiveContent, projected?: ProjectionContent): { identity: string; nextFingerprint: string; } {
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

export function hash(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

export function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map((item) => stableJson(item)).join(",")}]`;
  if (isRecord(value)) {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
