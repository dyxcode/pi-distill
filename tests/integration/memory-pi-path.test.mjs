import assert from "node:assert/strict";
import { access, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { openMemoryStore, resolveMemoryDatabasePath } from "../../src/storage/path.ts";

const repoRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));

async function temporaryRoot(t) {
  const root = await mkdtemp(join(tmpdir(), "pi-distill-memory-path-"));
  t.after(async () => rm(root, { recursive: true, force: true }));
  return root;
}

test("the personal database path stays below an external agent directory", async (t) => {
  const root = await temporaryRoot(t);
  const agentDir = join(root, "agent");
  const canonicalRoot = await realpath(root);
  assert.equal(
    resolveMemoryDatabasePath(agentDir),
    join(canonicalRoot, "agent", "distill", "memories.sqlite"),
  );
});

test("repository paths and symlinks into the repository are rejected", async (t) => {
  const root = await temporaryRoot(t);
  const repositoryLink = join(root, "repository-link");
  await symlink(repoRoot, repositoryLink, "dir");

  assert.throws(() => resolveMemoryDatabasePath(repoRoot), /outside the repository and notes vault/);
  assert.throws(
    () => resolveMemoryDatabasePath(join(repositoryLink, "agent")),
    /outside the repository and notes vault/,
  );
});

test("a custom agent directory inside another Git repository is rejected", async (t) => {
  const root = await temporaryRoot(t);
  const repository = join(root, "unrelated-repository");
  await mkdir(join(repository, ".git"), { recursive: true });
  const worktree = join(root, "unrelated-worktree");
  await mkdir(worktree, { recursive: true });
  await writeFile(join(worktree, ".git"), "gitdir: ../repository/.git\n");

  assert.throws(
    () => resolveMemoryDatabasePath(join(repository, "personal-agent")),
    /outside the repository and notes vault/,
  );
  assert.throws(
    () => resolveMemoryDatabasePath(join(worktree, "personal-agent")),
    /outside the repository and notes vault/,
  );
});

test("a custom agent directory inside an Obsidian vault is rejected through symlinks", async (t) => {
  const root = await temporaryRoot(t);
  const vault = join(root, "notes");
  await mkdir(join(vault, ".obsidian"), { recursive: true });
  const vaultLink = join(root, "agent-link");
  await symlink(vault, vaultLink, "dir");

  assert.throws(
    () => resolveMemoryDatabasePath(vaultLink),
    /outside the repository and notes vault/,
  );
});

test("a dangling agent-directory symlink targeting a vault fails before creating its target", async (t) => {
  const root = await temporaryRoot(t);
  const vault = join(root, "notes");
  await mkdir(join(vault, ".obsidian"), { recursive: true });
  const danglingAgentDir = join(vault, "not-created", "personal-agent");
  await symlink(danglingAgentDir, join(root, "agent-link"), "dir");

  assert.throws(() => openMemoryStore(join(root, "agent-link")));
  assert.equal(
    await access(dirname(danglingAgentDir)).then(() => true, () => false),
    false,
    "validation must fail before mkdir follows a dangling agent-directory symlink",
  );
});

test("a dangling database symlink into a vault is rejected before SQLite writes", async (t) => {
  const root = await temporaryRoot(t);
  const vault = join(root, "notes");
  await mkdir(join(vault, ".obsidian"), { recursive: true });
  const agentDir = join(root, "personal-agent");
  const databaseDirectory = join(agentDir, "distill");
  await mkdir(databaseDirectory, { recursive: true });
  const vaultTarget = join(vault, "would-be-database.sqlite");
  await symlink(vaultTarget, join(databaseDirectory, "memories.sqlite"), "file");

  assert.throws(() => openMemoryStore(agentDir));
  assert.equal(
    await access(vaultTarget).then(() => true, () => false),
    false,
    "path validation must run before SQLite can follow the dangling symlink",
  );
});

test("relative agent directories are rejected", () => {
  assert.throws(() => resolveMemoryDatabasePath(".pi/agent"), /absolute path/);
});
