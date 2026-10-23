import type { ConsolidationJob } from "./consolidation-types.js";
import type { DistillFailureInput, DistillFailureRecord, DistillRetryContext, DistillTaskKind } from "./distill-retry.js";

export type DistillTriggerReason = "close" | "compact";
export type DistillTriggerStatus = "candidate" | "pending" | "prepared" | "complete";
export type CleanTaskReason = DistillTriggerReason | "legacy";
export type CleanTaskStatus = "pending" | "waiting";
export type WaitReason = "host_coordination" | "unprocessed" | "usage" | "dependencies" | "unavailable";
export type RetryCategory = "transient" | "unavailable" | "invalid";

/** Durable source reference only. Conversation text stays in the original session file. */
export interface DistillTriggerInput {
  readonly id: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly leafId: string | null;
  readonly cwd: string;
  readonly reason: DistillTriggerReason;
}

export interface DistillTrigger extends DistillTriggerInput {
  readonly status: DistillTriggerStatus;
  readonly compactionRecordId: string | null;
  readonly jobIds: readonly string[];
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
}

export interface TaskPageOptions {
  readonly limit: number;
  readonly cursor?: number;
}

export interface TaskPage<T> {
  readonly items: readonly T[];
  readonly nextCursor?: number;
}

export interface TaskSweepPageOptions extends TaskPageOptions {
  readonly throughCursor?: number;
}

export interface TaskSweepPage<T> extends TaskPage<T> {
  readonly throughCursor: number;
}

export interface DistillClaimInput {
  readonly workerId: string;
  readonly now: number;
  readonly leaseMs: number;
  readonly retryContext?: DistillRetryContext;
}

export interface DistillTriggerClaimInput extends DistillClaimInput {
  /** Restrict this claim to one source so its version can be checked accurately. */
  readonly id?: string;
  /** Allow recovery code to claim an unconfirmed compaction candidate by ID. */
  readonly allowCandidate?: boolean;
}

export interface DistillTriggerClaim {
  readonly trigger: DistillTrigger;
  /** Unique fencing token; use as the consolidation commit owner. */
  readonly claimToken: string;
  readonly leaseUntil: number;
}

export interface DistillJobClaim {
  readonly job: ConsolidationJob;
  /** Unique fencing token; pass as ConsolidationCommit.owner. */
  readonly claimToken: string;
  readonly leaseUntil: number;
}

export interface RetryRequest {
  readonly category: RetryCategory;
  readonly retryAt: number;
}

export interface ReleaseDistillClaimInput {
  readonly claimToken: string;
  readonly now: number;
  readonly retry?: RetryRequest;
  readonly failure?: DistillFailureInput;
}

export type DistillFailure = DistillFailureRecord;
export type DistillFailureKind = DistillTaskKind;

export interface PrepareDistillTriggerInput {
  readonly id: string;
  readonly claimToken: string;
  readonly now: number;
  readonly jobs: readonly ConsolidationJob[];
}

export interface CleanTaskInput {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly leafId: string | null;
  readonly cwd: string;
  readonly reason: CleanTaskReason;
  /** Identifies the newly completed source range and fences older assessments. */
  readonly intentId: string;
}

export interface CleanTask extends CleanTaskInput {
  readonly id: string;
  readonly status: CleanTaskStatus;
  readonly waitReason?: WaitReason;
  readonly retryCategory?: RetryCategory;
  readonly retryAt?: number;
}

export interface CleanTaskPageOptions {
  readonly throughCursor?: number;
  readonly limit: number;
  readonly cursor?: number;
  /** When omitted, return future retry tasks so an explicit wake can recheck them. */
  readonly now?: number;
}
