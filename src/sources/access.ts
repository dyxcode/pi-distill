import { setTimeout as delay } from "node:timers/promises";
import type { SQLiteMemoryStore } from '../storage/store.ts';

/** A separate connection keeps protection until the actual read ends, even after runtime shutdown. */
export async function withSourceAccess<T>(
  openStore: () => Pick<SQLiteMemoryStore, "acquireSourceAccess" | "releaseSourceAccess" | "close">,
  operation: () => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  const store = openStore();
  let token: string | undefined;
  try {
    const deadline = Date.now() + 1_000;
    while (!(token = store.acquireSourceAccess())) {
      signal?.throwIfAborted();
      if (Date.now() >= deadline) throw new Error("Source access is busy; retry the queued task.");
      await delay(10, undefined, signal ? { signal } : undefined);
    }
    signal?.throwIfAborted();
    return await operation();
  } finally {
    try { if (token) store.releaseSourceAccess(token); }
    finally { store.close(); }
  }
}
