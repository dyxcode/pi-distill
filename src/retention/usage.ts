import { randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

import type { SQLiteMemoryStore } from '../storage/store.ts';
import type { SessionUsage } from "./types.ts";

interface Context {
  readonly cwd: string;
  readonly sessionManager: {
    getSessionFile(): string | undefined;
    getSessionId(): string;
    getLeafId(): string | null;
    getHeader(): { readonly parentSession?: string; } | null;
  };
}
interface Start { readonly reason: "startup" | "new" | "resume" | "fork" | "reload"; readonly previousSessionFile?: string; }
interface Shutdown { readonly reason: "quit" | "new" | "resume" | "fork" | "reload"; readonly targetSessionFile?: string; }
interface Binding {
  readonly namespace: string;
  readonly manager: WeakRef<object>;
  record: SessionUsage;
  shutdown?: Shutdown;
}

export interface UsageProcessState {
  readonly version: 1;
  readonly pid: number;
  readonly token: string;
  readonly managers: WeakMap<object, Set<Binding>>;
  readonly owners: Map<string, Binding>;
  readonly uncertain: Set<string>;
}

/** Process-local owner state shared by every extension reload/runtime. */
export function createUsageProcessState(): UsageProcessState {
  return { version: 1, pid: process.pid, token: randomUUID(), managers: new WeakMap(), owners: new Map(), uncertain: new Set() };
}

function isProcessState(value: unknown): value is UsageProcessState {
  return typeof value === "object" && value !== null && "version" in value && value.version === 1 &&
    "pid" in value && value.pid === process.pid && "token" in value && typeof value.token === "string" &&
    "managers" in value && value.managers instanceof WeakMap && "owners" in value && value.owners instanceof Map &&
    "uncertain" in value && value.uncertain instanceof Set;
}

function sharedProcessState(): UsageProcessState {
  const key = Symbol.for("pi-distill.session-usage.v1");
  const existing: unknown = Reflect.get(process, key);
  if (isProcessState(existing)) return existing;
  const state = createUsageProcessState();
  Object.defineProperty(process, key, { value: state });
  return state;
}

interface Options {
  readonly namespace: string;
  readonly openStore: () => SQLiteMemoryStore;
  readonly processState?: UsageProcessState;
}

/** Tracks attributed Pi use and observed session boundaries for one runtime. */
export class SessionUsageCoordinator {
  private readonly state: UsageProcessState;
  private readonly options: Options;
  private binding: Binding | undefined;
  private store: SQLiteMemoryStore | undefined;

  constructor(options: Options) {
    this.options = options;
    this.state = options.processState ?? sharedProcessState();
  }

  private withStore<T>(operation: (store: SQLiteMemoryStore) => T): T {
    this.store ??= this.options.openStore();
    return operation(this.store);
  }

  private save(binding: Binding, record: SessionUsage): void {
    this.withStore(store => store.saveSessionUsage(record));
    binding.record = record;
  }

  private register(ctx: Context): Binding | undefined {
    const sessionFile = ctx.sessionManager.getSessionFile();
    if (!sessionFile) return undefined;
    const binding: Binding = {
      namespace: this.options.namespace, manager: new WeakRef(ctx.sessionManager), record: {
        ownerId: randomUUID(), processToken: this.state.token, state: "active",
        sessionId: ctx.sessionManager.getSessionId(), sessionFile: resolve(sessionFile),
        cwd: ctx.cwd, leafId: ctx.sessionManager.getLeafId(), ownerPid: process.pid,
      }
    };
    this.save(binding, binding.record);
    this.state.owners.set(binding.record.ownerId, binding);
    const bindings = this.state.managers.get(ctx.sessionManager) ?? new Set<Binding>();
    bindings.add(binding);
    this.state.managers.set(ctx.sessionManager, bindings);
    this.binding = binding;
    if (!existsSync(sessionFile)) {
      const header = ctx.sessionManager.getHeader();
      this.withStore(store => store.reserveSessionSource({
        sessionId: binding.record.sessionId,
        sessionFile: binding.record.sessionFile,
        cwd: binding.record.cwd,
        ...(header?.parentSession === undefined ? {} : { parentSession: header.parentSession }),
      }));
    }
    return binding;
  }

  start(event: Start, ctx: Context): void {
    try {
      // Duplicate delivery to the same runtime must not create a second owner.
      if (this.binding) return;
      let previous: Binding | undefined;
      if (!this.state.uncertain.has(this.options.namespace)) {
        if (event.reason === "reload") {
          const candidates = [...(this.state.managers.get(ctx.sessionManager) ?? [])].filter(binding =>
            binding.namespace === this.options.namespace && binding.record.state !== "released");
          const candidate = candidates.length === 1 ? candidates[0] : undefined;
          if (candidate?.namespace === this.options.namespace && candidate.shutdown?.reason === "reload" &&
            candidate.record.state === "closing") previous = candidate;
        } else if (event.previousSessionFile && ["new", "resume", "fork"].includes(event.reason)) {
          const previousFile = resolve(event.previousSessionFile);
          const candidates = [...this.state.owners.values()].filter(binding =>
            binding.namespace === this.options.namespace && binding.record.sessionFile === previousFile);
          const candidate = candidates.length === 1 ? candidates[0] : undefined;
          if (candidate?.record.state === "closing" && candidate.shutdown?.reason === event.reason &&
            (!candidate.shutdown.targetSessionFile || resolve(candidate.shutdown.targetSessionFile) === ctx.sessionManager.getSessionFile())) previous = candidate;
        }
      }
      const current = this.register(ctx);
      // A new session_start follows old runtime disposal in Pi 1.3.0. Only
      // release the attributable old owner after the replacement is protected.
      if (current && previous) {
        this.save(previous, { ...previous.record, state: "released" });
        this.state.owners.delete(previous.record.ownerId);
        const manager = previous.manager.deref();
        if (manager) this.state.managers.get(manager)?.delete(previous);
      }
    } catch { this.state.uncertain.add(this.options.namespace); }
  }

  observe(ctx: Context): void {
    try {
      if (!this.binding) { this.register(ctx); return; }
      const binding = this.binding;
      if (binding.record.state !== "active") return;
      if (ctx.sessionManager.getSessionFile() !== binding.record.sessionFile || ctx.sessionManager.getSessionId() !== binding.record.sessionId) {
        this.save(binding, { ...binding.record, state: "closing" });
        this.state.uncertain.add(this.options.namespace);
        this.register(ctx);
        return;
      }
      const leafId = ctx.sessionManager.getLeafId();
      if (leafId !== binding.record.leafId) this.save(binding, { ...binding.record, leafId });
    } catch { this.state.uncertain.add(this.options.namespace); }
  }

  shutdown(event: Shutdown, ctx: Context): void {
    this.observe(ctx);
    const binding = this.binding;
    try {
      if (binding?.record.state === "active") {
        this.save(binding, { ...binding.record, state: "closing" });
        binding.shutdown = event;
      }
    } catch { this.state.uncertain.add(this.options.namespace); }
    finally {
      try { this.store?.close(); } catch { this.state.uncertain.add(this.options.namespace); }
      this.store = undefined;
    }
  }
}
