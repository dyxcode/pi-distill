import type { ToolRenderers } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";

type Kind = "search" | "read";
type Fields = Record<string, unknown>;
interface CandidateView {
  text: string;
  matched: string[];
  truncated: boolean;
}

function fields(value: unknown): Fields | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  return Object.fromEntries(Object.entries(value));
}

function strings(value: unknown): string[] | undefined {
  if (!Array.isArray(value) || !value.every((item: unknown) => typeof item === "string")) return undefined;
  return value.map((item: string) => item);
}

/** Render stored text literally; terminal control sequences must never execute. */
function literal(value: string): string {
  return value.replace(/\r\n/g, "\n")
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/gu,
      character => `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`);
}

function words(values: string[]): string {
  return values.map(value => literal(JSON.stringify(value))).join(", ");
}

function body(result: { details?: unknown; content: readonly { type: string; text?: string; }[]; }): Fields | undefined {
  if (result.details !== undefined) return fields(result.details);
  // Older session results may have only the structured JSON text payload.
  const text = result.content.find(part => part.type === "text")?.text;
  if (text === undefined) return undefined;
  try { return fields(JSON.parse(text)); } catch { return undefined; }
}

function candidates(value: unknown): CandidateView[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const views: CandidateView[] = [];
  for (const item of value) {
    const row = fields(item);
    const preview = fields(row?.["preview"]);
    const matched = strings(row?.["matchedTerms"]);
    if (!preview || !matched || typeof preview["text"] !== "string" ||
      typeof preview["omittedBefore"] !== "boolean" || typeof preview["omittedAfter"] !== "boolean") return undefined;
    views.push({ text: preview["text"], matched, truncated: preview["omittedBefore"] || preview["omittedAfter"] });
  }
  return views;
}

function failure(status: unknown): string {
  switch (status) {
    case "stale": return "This memory has changed. Read it again.";
    case "not_found": return "Memory not found.";
    case "unavailable": return "Memory storage is unavailable.";
    case "budget_exhausted": return "Memory retrieval limit reached.";
    case "cancelled": return "Cancelled.";
    case "invalid_input": return "Invalid memory request.";
    case "result_too_large": return "Memory result is too large to return.";
    default: return "Memory result unavailable.";
  }
}

function updated(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const date = new Date(value);
  if (!Number.isFinite(date.getTime()) || date.toISOString() !== value) return undefined;
  return new Intl.DateTimeFormat("en", {
    year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZoneName: "short",
  }).format(date);
}

function searchView(data: Fields, args: unknown, expanded: boolean): string | undefined {
  const rows = candidates(data["candidates"]);
  const terms = strings(fields(args)?.["terms"]);
  if (!rows || !terms || typeof data["hasMore"] !== "boolean") return undefined;
  const partial = data["hasMore"];
  const count = rows.length;
  const summary = `${count} ${count === 1 ? "memory" : "memories"} found using ${terms.length} ${terms.length === 1 ? "keyword" : "keywords"}`;
  const lines = [`Search memories · ${summary}${partial ? " · Partial search" : ""}`];
  if (expanded) {
    lines.push(`Keywords: ${words(terms)}`, partial ? "Search incomplete · More results may be available" : "Search complete");
    for (const row of rows) {
      lines.push("", `• ${literal(row.text)}`, `  Matched: ${words(row.matched)}`);
      if (row.truncated) lines.push("  Preview truncated");
    }
  }
  return lines.join("\n");
}

function readView(data: Fields, args: unknown, expanded: boolean): string | undefined {
  if (typeof data["content"] !== "string" || typeof data["hasMore"] !== "boolean") return undefined;
  const continued = typeof fields(args)?.["cursor"] === "string";
  const status = data["hasMore"] ? "Partial read" : continued ? "Final chunk" : "Complete";
  const lines = [`Read a memory · ${status}`];
  if (expanded) {
    lines.push("", literal(data["content"]));
    const time = updated(data["updatedAt"]);
    if (time) lines.push("", `Updated: ${time}`);
    if (data["hasMore"]) lines.push("More content remains");
  }
  return lines.join("\n");
}

function renderers(kind: Kind): ToolRenderers {
  const title = kind === "search" ? "Search memories" : "Read a memory";
  return {
    // Pi owns the pending shell and replaces this row's children on completion.
    // The result supplies the final title, keeping one summary in the same row.
    renderCall(_args, theme, context) {
      return new Text(context.isPartial ? theme.fg("toolTitle", title) : "", 0, 0);
    },
    renderResult(result, options, theme, context) {
      if (options.isPartial) return new Text("", 0, 0);
      const data = body(result);
      const success = !context.isError && data?.["status"] === "ok";
      const view = success && data
        ? (kind === "search" ? searchView(data, context.args, options.expanded) : readView(data, context.args, options.expanded))
        : undefined;
      return new Text(theme.fg(view === undefined ? "warning" : "toolOutput",
        view ?? `${title} · ${failure(success ? undefined : data?.["status"])}`), 0, 0);
    },
  };
}

export const memorySearchRenderers = renderers("search");
export const memoryReadRenderers = renderers("read");
