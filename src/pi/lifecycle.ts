import { diagnose, type DiagnosticSink } from '../diagnostics/types.ts';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import type { ConsolidationCoordinator } from '../distill/coordinator.ts';
import type { MemoryTools } from '../memory/retrieval.ts';
import type { CleanupCoordinator } from '../retention/coordinator.ts';
import type { SessionUsageCoordinator } from '../retention/usage.ts';

/** Pi owns event ordering; coordinators own durable work and resource lifetimes. */
export function registerLifecycle(pi: ExtensionAPI, memoryTools: MemoryTools, consolidation: ConsolidationCoordinator,
  usage: SessionUsageCoordinator, cleanup: CleanupCoordinator, diagnostics?: DiagnosticSink & {close(): void}): void {
  pi.on("agent_start", () => { memoryTools.reset(); consolidation.setForegroundBusy(true); });
  pi.on("agent_settled", (_event, ctx) => {
    consolidation.observe(ctx);
    consolidation.setForegroundBusy(false);
    cleanup.wake();
  });
  pi.on("session_start", (event, ctx) => {
    usage.start(event, ctx);
    consolidation.start(ctx);
    cleanup.wake();
  });
  pi.on("model_select", (_event, ctx) => consolidation.start(ctx));
  pi.on("message_end", (_event, ctx) => { usage.observe(ctx); consolidation.observe(ctx); });
  pi.on("agent_end", (_event, ctx) => { usage.observe(ctx); consolidation.observe(ctx); });
  pi.on("session_tree", (_event, ctx) => { usage.observe(ctx); consolidation.observe(ctx); cleanup.wake(); });
  pi.on("session_before_compact", (_event, ctx) => consolidation.beforeCompact(ctx));
  pi.on("session_compact", (event, ctx) => {
    usage.observe(ctx);
    consolidation.afterCompact(ctx, event.compactionEntry.id);
    cleanup.wake();
  });
  pi.on("session_compact_failed", () => consolidation.compactFailed());
  pi.on("session_shutdown", async (event, ctx) => {
    try {
      cleanup.close();
      await consolidation.shutdown(ctx, event.reason);
      usage.shutdown(event, ctx);
      memoryTools.close();
    } catch (error) {
      diagnose(diagnostics, {operation:"runtime.stop",outcome:"failed",reason:event.reason,error});
      throw error;
    } finally {
      try { diagnostics?.close(); } catch { /* Logging cannot delay shutdown. */ }
    }
  });
}
