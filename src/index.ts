import { getAgentDir, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { existsSync } from "node:fs";
import { ConsolidationCoordinator } from "./distill/coordinator.ts";
import { MemoryTools } from "./memory/retrieval.ts";
import { registerLifecycle } from './pi/lifecycle.ts';
import { registerMemoryTools } from './pi/tools.ts';
import { CleanupCoordinator } from "./retention/coordinator.ts";
import { SessionUsageCoordinator } from "./retention/usage.ts";
import { openMemoryStore, resolveMemoryDatabasePath } from "./storage/path.ts";

export default function piDistill(pi: ExtensionAPI): void {
  const memoryTools = new MemoryTools(() => openMemoryStore(getAgentDir()));
  const cleanup = new CleanupCoordinator({
    openStore: () => openMemoryStore(getAgentDir()),
    hasStore: () => existsSync(resolveMemoryDatabasePath(getAgentDir())),
  });
  const consolidation = new ConsolidationCoordinator({
    openStore: () => openMemoryStore(getAgentDir()),
    hasStore: () => existsSync(resolveMemoryDatabasePath(getAgentDir())),
    onProgress: () => cleanup.wake(),
  });
  const usage = new SessionUsageCoordinator({
    namespace: getAgentDir(),
    openStore: () => openMemoryStore(getAgentDir()),
  });

  registerMemoryTools(pi, memoryTools);
  registerLifecycle(pi, memoryTools, consolidation, usage, cleanup);
}
