import type { SessionObservation } from "../distill/types.ts";

/** Metadata registered by one Pi runtime while it uses a session file. */
export interface SessionUsage extends SessionObservation {
  readonly ownerId: string;
  readonly processToken: string;
  readonly state: "active" | "closing" | "released";
}


export interface SessionDeletion {
  readonly sessionFile: string;
  readonly sessionId: string;
  readonly intentId: string;
  readonly fileVersion: string;
  readonly sourceGeneration: string | null;
  readonly state: "deleting" | "complete";
}

export type {
  HostSourceRefV1,
  HostChildIdentityV1,
  HostUseRecordV1,
  HostUseRecoveryV1,
  CompletedDeletionReceiptV1,
} from "../host-storage.ts";
