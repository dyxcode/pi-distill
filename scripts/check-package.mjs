import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const temporary = await mkdtemp(join(tmpdir(), 'pi-distill-package-'));
try {
  execFileSync('npm', ['run', 'build:package'], { cwd: root, stdio: 'inherit' });
  const [packed] = JSON.parse(execFileSync('npm', [
    'pack', '--json', '--ignore-scripts', '--cache', join(temporary, 'cache'), '--pack-destination', temporary,
  ], { cwd: root, encoding: 'utf8' }));
  for (const file of packed.files) assert.doesNotMatch(file.path, /(?:^|\/)(?:agent-docs|\.local|node_modules)\//);
  const installed = join(temporary, 'node_modules', 'pi-distill');
  await mkdir(installed, { recursive: true });
  execFileSync('tar', ['-xzf', join(temporary, packed.filename), '--strip-components=1', '-C', installed]);
  const env = { ...process.env };
  delete env.NODE_OPTIONS;
  execFileSync(process.execPath, ['--input-type=module', '-e', `
    import assert from 'node:assert/strict';
    import { join } from 'node:path';
    import { openHostStorageV1 } from 'pi-distill/host-storage';
    const storage = openHostStorageV1(join(process.cwd(), 'memories.sqlite'));
    assert.equal(storage.version, 1);
    const ref = { sessionId: 'synthetic', sessionFile: join(process.cwd(), 'session.jsonl'), cwd: process.cwd() };
    const read = storage.acquireSourceRead(ref);
    assert.ok(read);
    assert.equal(read.sourceGeneration, null);
    read.release(); read.release();
    const use = storage.beginHostUse(ref);
    assert.ok(use);
    assert.equal(storage.listPendingHostUses()[0].state, 'opening');
    assert.equal(storage.recoverHostUse(use.id, { kind: 'opening-abandoned' }), true);
    storage.close();
  `], { cwd: temporary, env, stdio: 'inherit' });
  await writeFile(join(temporary, 'consumer.mts'), `
    import { openHostStorageV1, type HostSourceRefV1, type CompletedDeletionReceiptV1 } from 'pi-distill/host-storage';
    const source: HostSourceRefV1 = { sessionId: 'synthetic', sessionFile: '/tmp/session.jsonl', cwd: '/tmp' };
    const storage = openHostStorageV1('/tmp/memories.sqlite');
    const receipt: CompletedDeletionReceiptV1 | undefined = storage.getCompletedDeletion(source);
    void receipt;
  `);
  await writeFile(join(temporary, 'tsconfig.json'), JSON.stringify({
    compilerOptions: { target: 'ES2022', module: 'NodeNext', moduleResolution: 'NodeNext', strict: true, noEmit: true, types: [] },
    files: ['consumer.mts'],
  }));
  execFileSync(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '-p', join(temporary, 'tsconfig.json')], { stdio: 'inherit' });
  const manifest = JSON.parse(await readFile(join(installed, 'package.json'), 'utf8'));
  assert.equal(manifest.exports['./host-storage'].import, './dist/host-storage.js');
  console.log('Packed host-storage API passed standalone Node and TypeScript checks.');
} finally {
  await rm(temporary, { recursive: true, force: true });
}
