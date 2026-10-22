import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';
import { SQLiteMemoryStore } from '../src/memory-store.ts';

const repository = resolve('.');
const cli = resolve('node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js');
const providerExtension = resolve('tests/task-pi-cli-provider.mjs');
const distillExtension = resolve('src/index.ts');
const ptyRelay = `import errno, os, pty, select, signal, sys
pid, master = pty.fork()
if pid == 0:
    os.execv(sys.argv[1], sys.argv[1:])
def stop(signum, _frame):
    try: os.kill(pid, signal.SIGTERM)
    except ProcessLookupError: pass
    try: os.waitpid(pid, 0)
    except ChildProcessError: pass
    os._exit(128 + signum)
signal.signal(signal.SIGTERM, stop)
input_fd, output_fd = sys.stdin.fileno(), sys.stdout.fileno()
open_input, status = True, None
while status is None:
    ready, _, _ = select.select([master] + ([input_fd] if open_input else []), [], [], 0.05)
    if master in ready:
        try: data = os.read(master, 4096)
        except OSError as error:
            if error.errno != errno.EIO: raise
            data = b''
        if data: os.write(output_fd, data)
    if open_input and input_fd in ready:
        data = os.read(input_fd, 4096)
        if data: os.write(master, data)
        else: open_input = False
    waited, child_status = os.waitpid(pid, os.WNOHANG)
    if waited: status = child_status
try:
    while True:
        data = os.read(master, 4096)
        if not data: break
        os.write(output_fd, data)
except OSError as error:
    if error.errno != errno.EIO: raise
os.close(master)
if os.WIFEXITED(status): sys.exit(os.WEXITSTATUS(status))
sys.exit(128 + os.WTERMSIG(status))`;

const pause = (ms) => new Promise((resolvePause) => setTimeout(resolvePause, ms));

async function readEvents(path) {
  try {
    return (await readFile(path, 'utf8')).split('\n').filter(Boolean).map((line) => JSON.parse(line));
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  }
}

async function waitForEvents(path, predicate, child, timeoutMs = 8_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const events = await readEvents(path);
    if (predicate(events)) return events;
    if (child.exitCode !== null) throw new Error(`Pi exited before provider evidence arrived (status ${child.exitCode}). ${child.output}`);
    await pause(25);
  }
  throw new Error(`Timed out waiting for synthetic provider events in ${path}.`);
}

function startInteractivePi(root, logPath, { sessionFile, delayMs }) {
  const env = {
    ...process.env,
    PI_CODING_AGENT_DIR: join(root, 'agent'),
    PI_CODING_AGENT_SESSION_DIR: join(root, 'sessions'),
    PI_OFFLINE: '1',
    PI_SKIP_VERSION_CHECK: '1',
    PI_TELEMETRY: '0',
    PI_TASK_TEST_PROVIDER_LOG: logPath,
    PI_TASK_TEST_DISTILL_DELAY_MS: String(delayMs),
  };
  const args = [
    process.execPath, cli,
    '--extension', distillExtension,
    '--extension', providerExtension,
    '--model', 'task-test/synthetic',
    '--session-dir', join(root, 'sessions'),
    '--no-mcp', '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--no-approve',
    ...(sessionFile ? ['--session', sessionFile] : ['For synthetic Cedar, keep the test rule local.']),
  ];
  const child = spawn(process.env.PI_TEST_PYTHON ?? 'python3', ['-u', '-c', ptyRelay, ...args], { cwd: repository, env, stdio: ['pipe', 'pipe', 'pipe'] });
  child.output = '';
  for (const stream of [child.stdout, child.stderr]) {
    stream.on('data', (chunk) => { child.output = (child.output + chunk.toString()).slice(-8_000); });
  }
  child.exitPromise = new Promise((resolveExit, rejectExit) => {
    child.once('error', rejectExit);
    child.once('exit', (code, signal) => resolveExit({ code, signal }));
  });
  return child;
}

async function quitWithCtrlD(child, timeoutMs = 5_000) {
  const start = Date.now();
  child.stdin.write('\u0004');
  const timer = setTimeout(() => child.kill('SIGTERM'), timeoutMs);
  try {
    const result = await child.exitPromise;
    assert.equal(result.code, 0, `Pi CLI should exit cleanly (signal ${result.signal ?? 'none'}). ${child.output}`);
    assert.ok(Date.now() - start < 1_200, `Ctrl+D waited ${Date.now() - start}ms during runtime shutdown.`);
  } finally {
    clearTimeout(timer);
    child.stdin.destroy();
  }
}

async function stopPi(child) {
  if (!child || child.exitCode !== null) return;
  try { child.stdin.write('\u0004'); } catch { return; }
  const graceful = await Promise.race([child.exitPromise.then(() => true), pause(2_000).then(() => false)]);
  if (graceful) return;
  child.kill('SIGTERM');
  const terminated = await Promise.race([child.exitPromise.then(() => true), pause(2_000).then(() => false)]);
  if (!terminated) child.kill('SIGKILL');
}

test('Pi CLI Ctrl+D queues the source without a shutdown model call, then startup resumes it from the same session', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-cli-queue-'));
  const logPath = join(root, 'provider-events.jsonl');
  await writeFile(logPath, '');

  let first;
  let second;
  t.after(async () => {
    try {
      await stopPi(second);
      await stopPi(first);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
  first = startInteractivePi(root, logPath, { delayMs: 2_500 });
  const firstEvents = await waitForEvents(logPath,
    (events) => events.some((event) => event.event === 'finish' && !event.distill), first);
  await pause(250);
  await quitWithCtrlD(first);

  const afterQuit = await readEvents(logPath);
  assert.ok(firstEvents.some((event) => event.event === 'start' && !event.distill));
  assert.equal(afterQuit.some((event) => event.event === 'start' && event.distill), false,
    'Ctrl+D must persist the source trigger without starting consolidation in the closing runtime');

  const memoryPath = join(root, 'agent', 'distill', 'memories.sqlite');
  const queuedStore = new SQLiteMemoryStore(memoryPath);
  const queued = queuedStore.listDistillTriggers({ limit: 100 }).items;
  assert.equal(queued.length, 1);
  assert.equal(queued[0].reason, 'close');
  assert.equal(queued[0].status, 'pending');
  assert.deepEqual(queued[0].jobIds, []);
  queuedStore.close();

  second = startInteractivePi(root, logPath, { sessionFile: queued[0].sessionFile, delayMs: 0 });
  await waitForEvents(logPath,
    (events) => events.some((event) => event.event === 'finish' && event.distill), second);

  let recoveredStore;
  let memory;
  const deadline = Date.now() + 8_000;
  while (!memory && Date.now() < deadline) {
    recoveredStore = new SQLiteMemoryStore(memoryPath);
    memory = recoveredStore.searchBatch({ terms: ['Cedar'], match: 'any' }).find((row) => row.matchedTerms.length)?.memory;
    recoveredStore.close();
    if (!memory) await pause(25);
  }
  recoveredStore = new SQLiteMemoryStore(memoryPath);
  try {
    memory = recoveredStore.searchBatch({ terms: ['Cedar'], match: 'any' }).find((row) => row.matchedTerms.length)?.memory;
    assert.ok(memory, 'the restarted CLI should distill the queued source while reopening the same session');
    assert.equal(recoveredStore.listConsolidationJobs().length, 0);
  } finally {
    recoveredStore.close();
  }
  await quitWithCtrlD(second);
});
