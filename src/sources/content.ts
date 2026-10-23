import {
  type SessionEntry
} from "@earendil-works/pi-coding-agent";
import { isRecord } from './format.ts';
import type { EffectiveContent, ProjectionContent } from './types.ts';
export function sourceContentForEntry(
  entry: SessionEntry,
  effective: EffectiveContent | undefined,
  projected: ProjectionContent | undefined,
): { readonly role: string; readonly segments: readonly string[]; } | undefined {
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

export function contentTextSegments(content: unknown): string[] {
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
