import { existsSync } from "node:fs";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, sep } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  createAgentSession,
  SessionManager,
} from "@earendil-works/pi-coding-agent";

const repoRoot = await realpath(join(dirname(fileURLToPath(import.meta.url)), "../.."));
const piExecutable = join(repoRoot, "node_modules", ".bin", "pi");

async function fixture(t) {
  const temporaryRoot = await mkdtemp(join(tmpdir(), "pi-distill-loading-"));
  let session;
  t.after(async () => {
    session?.dispose();
    await rm(temporaryRoot, { recursive: true, force: true });
  });
  const root = await realpath(temporaryRoot);
  const cwd = join(root, "unrelated-project");
  const agentDir = join(root, "personal-agent");
  const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = agentDir;
  t.after(() => { if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = previousAgentDir; });
  await mkdir(cwd);
  await mkdir(agentDir);
  await writeFile(join(agentDir, "settings.json"), JSON.stringify({
    enableInstallTelemetry: false,
    enableAnalytics: false,
  }));

  function pi(...args) {
    const result = spawnSync(piExecutable, args, {
      cwd,
      env: {
        ...process.env,
        PI_CODING_AGENT_DIR: agentDir,
        PI_CODING_AGENT_SESSION_DIR: join(root, "sessions"),
        PI_OFFLINE: "1",
        PI_SKIP_VERSION_CHECK: "1",
        PI_TELEMETRY: "0",
      },
      encoding: "utf8",
      timeout: 30_000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr || result.stdout);
  }

  async function start() {
    const result = await createAgentSession({
      cwd,
      agentDir,
      sessionManager: SessionManager.inMemory(cwd),
      noTools: "all",
    });
    session = result.session;
    return session;
  }

  return { cwd, agentDir, pi, start };
}

function packageExtensions(session) {
  const result = session.resourceLoader.getExtensions();
  assert.deepEqual(result.errors, [], "Pi must report no extension load errors");
  assert.deepEqual(result.warnings ?? [], [], "Pi must report no extension warnings");
  return result.extensions.filter((extension) => {
    const path = relative(repoRoot, extension.resolvedPath);
    return path !== ".." && !path.startsWith(`..${sep}`) && !path.startsWith(sep);
  });
}

function assertLoadedOnce(session) {
  const extensions = packageExtensions(session);
  assert.equal(extensions.length, 1, "Pi must load exactly one extension from this package");
  const [extension] = extensions;
  assert.ok(extension);
  assert.equal(extension.sourceInfo.scope, "user", "Registration must be personal/global");
  assert.equal(extension.sourceInfo.origin, "package");
}

test("personal registration loads the local package from an unrelated directory", async (t) => {
  const f = await fixture(t);
  f.pi("install", repoRoot);
  const session = await f.start();
  assertLoadedOnce(session);
  assert.equal(session.sessionManager.getSessionFile(), undefined);
});

test("reload discovers a newly registered package without duplicate loading", async (t) => {
  const f = await fixture(t);
  const session = await f.start();
  assert.equal(packageExtensions(session).length, 0);
  f.pi("install", repoRoot);
  await session.reload();
  assertLoadedOnce(session);
  await session.reload();
  assertLoadedOnce(session);
});

test("removing registration unloads the package on reload and subsequent startup", async (t) => {
  const f = await fixture(t);
  f.pi("install", repoRoot);
  const session = await f.start();
  assertLoadedOnce(session);
  f.pi("remove", repoRoot);
  await session.reload();
  assert.equal(packageExtensions(session).length, 0);
  session.dispose();
  const restarted = await f.start();
  assert.equal(packageExtensions(restarted).length, 0);
});

test("real Pi normal startup and reload create neither diagnostic history nor memory storage", async t => {
  const f=await fixture(t);f.pi("install",repoRoot);const session=await f.start();
  const path=join(f.agentDir,'distill','diagnostics.sqlite');
  assert.equal(existsSync(path),false);
  assert.equal(existsSync(join(f.agentDir,'distill','memories.sqlite')),false);
  await session.reload();
  assert.equal(existsSync(path),false);
  assert.equal(existsSync(join(f.agentDir,'distill','memories.sqlite')),false);
});
