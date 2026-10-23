import { appendFileSync } from 'node:fs';
import { fauxAssistantMessage, fauxProvider } from '@earendil-works/pi-ai';

const logPath = process.env.PI_TASK_TEST_PROVIDER_LOG;
const distillDelayMs = Number(process.env.PI_TASK_TEST_DISTILL_DELAY_MS ?? 0);

function record(event) {
  if (!logPath) return;
  appendFileSync(logPath, `${JSON.stringify({ pid: process.pid, ...event })}\n`);
}

function textOf(context) {
  return context.messages.flatMap((message) => typeof message.content === 'string'
    ? [message.content]
    : message.content?.filter((block) => block.type === 'text').map((block) => block.text) ?? []).join('\n');
}

const provider = fauxProvider({
  provider: 'task-test',
  models: [{ id: 'synthetic', contextWindow: 32_768, maxTokens: 8_192 }],
  tokensPerSecond: 100_000,
});
const response = (context, options) => {
  const key = textOf(context).match(/"key"\s*:\s*"([^"\n]+)"/u)?.[1];
  const distill = key !== undefined;
  record({ event: 'start', distill });
  return new Promise((resolve) => setTimeout(() => {
    record({ event: 'finish', distill, aborted: options?.signal?.aborted ?? false });
    resolve(fauxAssistantMessage(distill
      ? JSON.stringify({ operations: [{ action: 'create', content: 'For fictional Cedar, keep test rules local.', sourceKeys: [key] }] })
      : 'Synthetic response.'));
  }, distill ? distillDelayMs : 0));
};
provider.setResponses(Array.from({ length: 100 }, () => response));

export default function taskPiCliProvider(pi) {
  pi.registerProvider(provider.provider);
}
