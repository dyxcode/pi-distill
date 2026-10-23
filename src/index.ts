import { LocalDiagnostics } from "./diagnostics/recorder.ts";
import { getAgentDir, VERSION, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { existsSync, readFileSync } from "node:fs";
import { ConsolidationCoordinator } from "./distill/coordinator.ts";
import { MemoryTools } from "./memory/retrieval.ts";
import { registerLifecycle } from './pi/lifecycle.ts';
import { registerMemoryTools } from './pi/tools.ts';
import { CleanupCoordinator } from "./retention/coordinator.ts";
import { SessionUsageCoordinator } from "./retention/usage.ts";
import { openMemoryStore, resolveMemoryDatabasePath } from "./storage/path.ts";

export default function piDistill(pi: ExtensionAPI): void {
  const diagnostics = new LocalDiagnostics({agentDir:getAgentDir(),piVersion:VERSION,extensionVersion:extensionVersion()});
  const memoryTools = new MemoryTools(() => openMemoryStore(getAgentDir()), diagnostics);
  const cleanup = new CleanupCoordinator({
    diagnostics,
    openStore: () => openMemoryStore(getAgentDir()),
    hasStore: () => existsSync(resolveMemoryDatabasePath(getAgentDir())),
  });
  const consolidation = new ConsolidationCoordinator({
    diagnostics,
    openStore: () => openMemoryStore(getAgentDir()),
    hasStore: () => existsSync(resolveMemoryDatabasePath(getAgentDir())),
    onProgress: () => cleanup.wake(),
  });
  const usage = new SessionUsageCoordinator({
    diagnostics,
    namespace: getAgentDir(),
    openStore: () => openMemoryStore(getAgentDir()),
  });

  registerMemoryTools(pi, memoryTools);
  registerLifecycle(pi, memoryTools, consolidation, usage, cleanup, diagnostics);
}

function extensionVersion(): string {
  try {
    const manifest: unknown = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
    return typeof manifest === 'object' && manifest !== null && 'version' in manifest && typeof manifest.version === 'string'
      ? manifest.version : 'unknown';
  } catch { return 'unknown'; }
}
