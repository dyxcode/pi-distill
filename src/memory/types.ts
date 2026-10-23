export interface MemoryInput {
  readonly id: string;
  readonly content: string;
  readonly createdAt: string;
}

export interface Memory extends MemoryInput {
  readonly revision: number;
  readonly updatedAt: string;
}

export interface MemoryOrigin {
  readonly sessionId: string;
  readonly sourceKeys: readonly string[];
  readonly lineageSessionIds?: readonly string[];
  readonly sourceTime?: string;
  readonly branchAnchor?: string;
}

export interface MemoryUpdate {
  readonly id: string;
  readonly expectedRevision: number;
  readonly content: string;
  readonly updatedAt: string;
}

export interface MemoryForget {
  readonly id: string;
  readonly expectedRevision: number;
}

export interface MemorySearchRequest {
  readonly terms: readonly string[];
  readonly match: "any" | "all";
  readonly after?: string;
  readonly limit?: number;
}

export interface MemorySearchRow {
  readonly memory: Memory;
  readonly matchedTerms: readonly string[];
}

export class MemoryConflictError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`A different memory already exists for ID "${memoryId}".`);
    this.name = "MemoryConflictError";
    this.memoryId = memoryId;
  }
}

export class MemoryNotFoundError extends Error {
  readonly memoryId: string;

  constructor(memoryId: string) {
    super(`No memory exists for ID "${memoryId}".`);
    this.name = "MemoryNotFoundError";
    this.memoryId = memoryId;
  }
}

/** Read capabilities consumed by retrieval; no queue or storage implementation dependency. */
export interface MemoryReader {
  get(id: string): Memory | undefined;
  searchBatch(request: MemorySearchRequest): readonly MemorySearchRow[];
  close(): void;
}

export interface MemoryEvidenceReader extends MemoryReader {
  memoryOrigin(id: string): MemoryOrigin | undefined;
}
