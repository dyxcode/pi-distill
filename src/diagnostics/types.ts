/** Closed vocabulary: diagnostic records never accept free-form messages. */
export const operations = [
  'runtime.stop', 'capture.observe', 'capture.compact', 'capture.close',
  'usage.start', 'usage.observe', 'usage.stop', 'distill.pass', 'distill.prepare',
  'distill.model', 'distill.commit', 'distill.release', 'distill.recover',
  'cleanup.pass', 'cleanup.assess', 'cleanup.delete', 'memory.search', 'memory.read',
] as const;
export const reasons = [
  'source_invalid', 'model_unavailable', 'model_invalid', 'model_budget', 'transient', 'conflict',
  'reload', 'quit', 'new', 'resume', 'fork',
] as const;
export interface DiagnosticEvent {
  readonly operation: typeof operations[number];
  /** ok resolves a known issue; it is never a routine activity record. */
  readonly outcome: 'ok' | 'failed' | 'blocked';
  readonly reason?: string;
  readonly task?: string;
  readonly source?: string;
  readonly model?: string;
  readonly durationMs?: number;
  readonly retryAfterMs?: number;
  readonly error?: unknown;
}
export interface DiagnosticSink { record(event: DiagnosticEvent): void; }

/** Instrumentation, including a failing injected recorder, cannot change application behavior. */
export function diagnose(sink: DiagnosticSink | undefined, event: DiagnosticEvent): void {
  try { sink?.record(event); } catch { /* Diagnostics are disposable. */ }
}
