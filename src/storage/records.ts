import type {
  CleanTask,
  CleanTaskInput,
  DistillTrigger,
  DistillTriggerInput,
  RetryCategory,
  WaitReason
} from "../distill/tasks.ts";
import type {
  ConsolidationJob
} from "../distill/types.ts";

export interface PersistedDistillTrigger {
  readonly trigger: DistillTriggerInput;
  readonly status: DistillTrigger["status"];
  readonly compactionRecordId: string | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
  readonly jobIds: readonly string[];
}

export interface PersistedCleanTask {
  readonly task: CleanTaskInput;
  readonly status: CleanTask["status"];
  readonly waitReason: WaitReason | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
  readonly leaseOwner: string | null;
  readonly leaseUntil: number | null;
}

export interface PersistedDistillLease {
  readonly claimToken: string | null;
  readonly workerId: string | null;
  readonly taskKind: "trigger" | "job" | null;
  readonly taskId: string | null;
  readonly leaseUntil: number | null;
}

export interface PersistedConsolidationJob {
  readonly job: ConsolidationJob;
  readonly status: "pending" | "complete";
  readonly leaseOwner: string | null;
  readonly leaseUntil: number | null;
  readonly retryCategory: RetryCategory | null;
  readonly retryAt: number | null;
}

export interface MemoryEvidence {
  readonly evidenceId: string;
  readonly sessionId: string;
  readonly sessionFile: string | null;
  readonly sourceGeneration: string | null;
  readonly cwd: string;
  readonly leafId: string | null;
  readonly sourceKeys: readonly string[];
  readonly lineageSessionIds?: readonly string[];
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

export interface SchemaColumn {
  readonly name: string;
  readonly type: "TEXT" | "INTEGER";
  readonly notNull: 0 | 1;
  readonly primaryKey: 0 | 1 | 2;
}
