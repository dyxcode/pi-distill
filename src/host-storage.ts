import { parseHostSourceRef } from "./storage/host-use-validation.ts";
import { SQLiteMemoryStore } from "./storage/store.ts";

export interface HostSourceRefV1 {
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
  /** Bind receipt lookup to a generation cached before registry metadata was collected. */
  readonly sourceGeneration?: string;
}

export interface HostUseSourceIdentityV1 {
  readonly sessionId: string;
  readonly cwd: string;
}

export type HostUseSourceRefV1 = HostUseSourceIdentityV1 &
  (
    | { readonly sessionFile?: never; readonly sourceGeneration?: never }
    | { readonly sessionFile: string; readonly sourceGeneration?: string }
  );

export interface HostChildIdentityV1 {
  readonly pid: number;
  readonly token: string;
}

export interface HostUseRecordV1 extends HostUseSourceIdentityV1 {
  readonly id: string;
  readonly sessionFile: string | null;
  readonly sourceGeneration: string | null;
  readonly ownerPid: number;
  readonly createdAt: number;
  readonly state: "opening" | "attached";
  readonly child: HostChildIdentityV1 | null;
}

export type HostUseRecoveryV1 =
  | { readonly kind: "opening-abandoned" }
  | { readonly kind: "child-exited"; readonly child: HostChildIdentityV1 };

export interface CompletedDeletionReceiptV1 {
  readonly sessionFile: string;
  readonly sessionId: string;
  readonly intentId: string;
  readonly fileVersion: string;
  readonly sourceGeneration: string;
  readonly state: "complete";
}

export interface SourceReadLeaseV1 {
  readonly sourceGeneration: string | null;
  /** Idempotently release the short source-read reservation. */
  release(): void;
}

export interface HostUseReservationV1 {
  readonly id: string;
  /** Generation observed when this reservation began, or null for an unmanaged source. */
  readonly sourceGeneration: string | null;
  /** Bind the durable opening reservation to the spawned child exactly once. */
  attachChild(pid: number): HostChildIdentityV1;
  /** Release only after this exact child's ChildProcess `close` event. */
  releaseAfterClose(child: HostChildIdentityV1): boolean;
}

export interface HostStorageV1 {
  readonly version: 1;
  /** Returns undefined when cleanup currently holds an exclusive delete lock on this session file. */
  acquireSourceRead(source: HostSourceRefV1): SourceReadLeaseV1 | undefined;
  /** Call before reading or spawning. A new session may have no Pi session file yet. */
  beginHostUse(source: HostUseSourceRefV1): HostUseReservationV1 | undefined;
  listPendingHostUses(): readonly HostUseRecordV1[];
  /** Recover only after verifying the stated no-child or exact-child-exited condition. */
  recoverHostUse(id: string, evidence: HostUseRecoveryV1): boolean;
  /** Requires the exact current registry generation, or a caller-supplied cached generation after registry GC. */
  getCompletedDeletion(source: HostSourceRefV1): CompletedDeletionReceiptV1 | undefined;
  /** Close the local connection. Durable host-use rows intentionally survive. */
  close(): void;
}

class HostStorage implements HostStorageV1 {
  readonly version = 1 as const;
  readonly #store: SQLiteMemoryStore;
  #readLeases = 0;
  #closed = false;

  constructor(databasePath: string) {
    this.#store = new SQLiteMemoryStore(databasePath);
  }

  acquireSourceRead(sourceInput: HostSourceRefV1): SourceReadLeaseV1 | undefined {
    this.#ensureOpen();
    const sourceRef = parseHostSourceRef(sourceInput);
    const token = this.#store.acquireSourceAccess(sourceRef.sessionFile, "read");
    if (!token) return undefined;
    let sourceGeneration: string | null | undefined;
    try {
      sourceGeneration = this.#store.hostSourceGeneration(sourceInput);
    } catch (error) {
      this.#store.releaseSourceAccess(token);
      throw error;
    }
    if (sourceGeneration === undefined) {
      this.#store.releaseSourceAccess(token);
      return undefined;
    }
    this.#readLeases += 1;
    let released = false;
    return {
      sourceGeneration,
      release: () => {
        if (released) return;
        this.#store.releaseSourceAccess(token);
        released = true;
        this.#readLeases -= 1;
      },
    };
  }

  beginHostUse(source: HostUseSourceRefV1): HostUseReservationV1 | undefined {
    this.#ensureOpen();
    const record = this.#store.beginHostUse(source);
    if (!record) return undefined;
    let child: HostChildIdentityV1 | undefined;
    return {
      id: record.id,
      sourceGeneration: record.sourceGeneration,
      attachChild: pid => {
        if (child) throw new Error("Host use already has a child.");
        child = this.#store.attachHostUseChild(record.id, pid);
        return child;
      },
      releaseAfterClose: identity => this.#store.releaseHostUseAfterClose(record.id, identity),
    };
  }

  listPendingHostUses(): readonly HostUseRecordV1[] {
    this.#ensureOpen();
    return this.#store.listPendingHostUses();
  }

  recoverHostUse(id: string, evidence: HostUseRecoveryV1): boolean {
    this.#ensureOpen();
    return this.#store.recoverHostUse(id, evidence);
  }

  getCompletedDeletion(source: HostSourceRefV1): CompletedDeletionReceiptV1 | undefined {
    this.#ensureOpen();
    return this.#store.getCompletedDeletionFor(source);
  }

  close(): void {
    if (this.#closed) return;
    if (this.#readLeases !== 0) throw new Error("Release source-read leases before closing host storage.");
    this.#store.close();
    this.#closed = true;
  }

  #ensureOpen(): void {
    if (this.#closed) throw new Error("Host storage is closed.");
  }
}

/** Open Distill's local storage coordination API without importing Pi. */
export function openHostStorageV1(databasePath: string): HostStorageV1 {
  return new HostStorage(databasePath);
}

export { openMemoryReaderV1, HostMemoryError, type HostMemoryReaderV1, type HostMemorySearchRequestV1, type HostMemorySearchResponseV1, type HostMemoryReadRequestV1, type HostMemoryReadResponseV1 } from "./memory/host-reader.ts";
