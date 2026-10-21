import { lstatSync, mkdirSync, realpathSync, statSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { SQLiteMemoryStore } from "./memory-store.ts";

const repositoryRoot = realpathSync(resolve(dirname(fileURLToPath(import.meta.url)), ".."));

function inside(parent: string, child: string): boolean {
  const pathFromParent = relative(parent, child);
  return pathFromParent === "" || (
    pathFromParent !== ".." &&
    !pathFromParent.startsWith(`..${sep}`) &&
    !isAbsolute(pathFromParent)
  );
}

function canonicalPath(path: string): string {
  let candidate = resolve(path);
  const missing: string[] = [];

  while (true) {
    try {
      lstatSync(candidate);
    } catch (error) {
      if (
        typeof error !== "object" || error === null ||
        !("code" in error) || error.code !== "ENOENT"
      ) throw new Error("Cannot validate the memory storage path.");

      const parent = dirname(candidate);
      if (parent === candidate) throw new Error("Cannot validate the memory storage path.");
      missing.unshift(candidate.slice(parent.length + (parent.endsWith(sep) ? 0 : 1)));
      candidate = parent;
      continue;
    }

    // Once lstat finds an existing component, realpath must resolve it fully.
    // In particular, a dangling symlink is not a missing suffix: failing closed
    // here prevents SQLite from following it during database creation.
    try {
      return resolve(realpathSync(candidate), ...missing);
    } catch {
      throw new Error("Cannot validate the memory storage path.");
    }
  }
}

function hasMarker(path: string, markerName: string): boolean {
  try {
    const marker = statSync(join(path, markerName));
    return marker.isDirectory() || (markerName === ".git" && marker.isFile());
  } catch (error) {
    if (
      typeof error === "object" && error !== null &&
      "code" in error && error.code === "ENOENT"
    ) return false;
    throw new Error("Cannot validate the memory storage path.");
  }
}

function hasProtectedAncestor(path: string, markerName: string): boolean {
  let candidate = dirname(path);
  while (true) {
    if (hasMarker(candidate, markerName)) return true;
    const parent = dirname(candidate);
    if (parent === candidate) return false;
    candidate = parent;
  }
}

/** Resolve and validate Pi's personal memory database without creating it. */
export function resolveMemoryDatabasePath(agentDir: string): string {
  if (typeof agentDir !== "string" || !isAbsolute(agentDir)) {
    throw new TypeError("Pi's agent directory must be an absolute path.");
  }

  const databasePath = canonicalPath(join(agentDir, "distill", "memories.sqlite"));
  if (
    inside(repositoryRoot, databasePath) ||
    hasProtectedAncestor(databasePath, ".git") ||
    hasProtectedAncestor(databasePath, ".obsidian")
  ) {
    throw new Error("Memory storage must be outside the repository and notes vault.");
  }
  return databasePath;
}

/** Lazily create the personal storage directory and open its SQLite database. */
export function openMemoryStore(agentDir: string): SQLiteMemoryStore {
  const databasePath = resolveMemoryDatabasePath(agentDir);
  mkdirSync(dirname(databasePath), { recursive: true });
  const verifiedPath = resolveMemoryDatabasePath(agentDir);
  if (verifiedPath !== databasePath) {
    throw new Error("Memory storage path changed during initialization.");
  }
  return new SQLiteMemoryStore(verifiedPath);
}
