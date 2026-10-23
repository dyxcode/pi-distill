import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { test } from 'node:test';

test('actual Pi recalls and cleans a short closed session while another source remains partially distilled', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-distill-scheduling-sdk-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const result = spawnSync(process.execPath, [resolve('tests/helpers/distill-scheduling-pi-child.mjs'), root], {
    cwd: resolve('.'), env: { PATH: process.env.PATH, TMPDIR: root, PI_OFFLINE: '1', PI_SKIP_VERSION_CHECK: '1', PI_TELEMETRY: '0' },
    encoding: 'utf8', timeout: 20000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.deepEqual(JSON.parse(result.stdout.trim().split('\n').at(-1)), { passed: true, recalled: true, shortDeleted: true, longRetained: true });
});
