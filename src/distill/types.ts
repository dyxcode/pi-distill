import type { MemoryForget, MemoryInput, MemoryUpdate } from '../memory/types.ts';

/** Durable jobs contain source references only; evidence stays in original sessions. */
export interface ConsolidationJob {
  readonly id: string;
  readonly sessionId: string;
  readonly sessionFile: string | null;
  readonly leafId: string | null;
  readonly cwd: string;
  readonly sourceKeys: readonly string[];
  readonly lineageSessionIds?: readonly string[];
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

export interface SessionObservation {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
  readonly leafId: string | null;
  readonly ownerPid: number;
}

export interface SourcePart {
  readonly key: string;
  readonly entryId: string;
  readonly role: string;
  readonly text: string;
}

export interface SourceBatch extends ConsolidationJob {
  readonly evidence: readonly SourcePart[];
  readonly context: readonly SourcePart[];
  readonly lineageKeys?: readonly string[];
  readonly lineageAnchors?: readonly string[];
}

export interface ConsolidationChanges {
  readonly creates: readonly MemoryInput[];
  readonly updates: readonly MemoryUpdate[];
  readonly deletes: readonly MemoryForget[];
}

/** Source boundary attached to one successfully committed evidence prefix. */
export interface ConsolidationEvidenceBoundary {
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

export interface ConsolidationCommit extends ConsolidationChanges {
  readonly jobId: string;
  readonly owner: string;
  readonly now: number;
  readonly sourceKeys?: readonly string[];
  readonly evidence?: ConsolidationEvidenceBoundary;
}

export class ConsolidationConflictError extends Error {
  readonly resourceId: string;

  constructor(resourceId: string, reason = "conflicts with current consolidation state") {
    super("Consolidation record \"" + resourceId + "\" " + reason + ".");
    this.name = "ConsolidationConflictError";
    this.resourceId = resourceId;
  }
}
