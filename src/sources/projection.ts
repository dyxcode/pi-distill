import {
  type SessionEntry,
  type SessionProjection
} from "@earendil-works/pi-coding-agent";
import type { EffectiveContent, ProjectionContent, SourceSnapshot } from './types.ts';
export const projections = new WeakMap<SourceSnapshot, SessionProjection>();

export function getProjection(snapshot: SourceSnapshot): SessionProjection | undefined {
  return projections.get(snapshot);
}

export function projectionContentByEntry(projection: SessionProjection | undefined): ReadonlyMap<string, ProjectionContent> {
  const result = new Map<string, ProjectionContent>();
  if (!projection) return result;
  for (const projected of projection.entries) {
    result.set(projected.sourceEntry.id, { messages: projected.messages });
  }
  return result;
}

export function applyContextEdits(entries: readonly SessionEntry[]): ReadonlyMap<string, EffectiveContent> {
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
