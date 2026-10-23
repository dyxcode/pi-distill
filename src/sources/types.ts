import {
  type SessionEntry,
  type SessionHeader,
  type SessionProjection
} from "@earendil-works/pi-coding-agent";
import type { PagedSourceView } from "../storage/source-view.ts";

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

export interface SourceOrigin {
  readonly sessionId: string;
  readonly entryId: string;
}

export interface SnapshotAncestry {
  readonly origins: ReadonlyMap<string, SourceOrigin>;
  readonly signatures: ReadonlyMap<string, string>;
  readonly unresolved?: boolean;
}

export interface ParsedSession {
  readonly header: SessionHeader;
  readonly entries: readonly SessionEntry[];
  readonly pending: boolean;
}

export interface SessionGraph {
  readonly header: SessionHeader;
  readonly entries: readonly SessionEntry[];
  readonly entriesById: Map<string, SessionEntry>;
  readonly signatures: ReadonlyMap<string, string>;
  readonly origins: ReadonlyMap<string, SourceOrigin>;
  readonly pending: boolean;
}

export interface AggregateBudget {
  bytes: number;
  entries: number;
}

export interface EffectiveContent {
  readonly content: unknown;
  readonly omitted: boolean;
}

export interface ProjectionContent {
  readonly messages: readonly unknown[];
}
