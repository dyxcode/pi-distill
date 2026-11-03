import { resolve } from "node:path";
import type { HostChildIdentityV1, HostSourceRefV1, HostUseRecordV1, HostUseRecoveryV1 } from "../retention/types.ts";
import { fields, validateAbsolutePath, validateText, validateNow } from "./validation.ts";

const validSourceGeneration = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function parseHostSourceRef(value: unknown): HostSourceRefV1 {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError("Host source reference must be an object.");
  const hasGeneration = Object.hasOwn(value, "sourceGeneration");
  const row = fields(value, hasGeneration ? ["sessionId", "sessionFile", "cwd", "sourceGeneration"] : ["sessionId", "sessionFile", "cwd"]);
  validateText(row["sessionId"], "host source session ID");
  validateAbsolutePath(row["sessionFile"], "host source session file");
  validateAbsolutePath(row["cwd"], "host source working directory");
  if (resolve(row["sessionFile"]) !== row["sessionFile"] || resolve(row["cwd"]) !== row["cwd"]) {
    throw new TypeError("Host source paths must be normalized.");
  }
  const result: HostSourceRefV1 = {
    sessionId: row["sessionId"],
    sessionFile: row["sessionFile"],
    cwd: row["cwd"],
  };
  if (hasGeneration) {
    validateText(row["sourceGeneration"], "host source generation");
    if (!validSourceGeneration.test(row["sourceGeneration"])) throw new TypeError("Host source generation is invalid.");
    return { ...result, sourceGeneration: row["sourceGeneration"] };
  }
  return result;
}

export function parseHostChildIdentity(value: unknown): HostChildIdentityV1 {
  const row = fields(value, ["pid", "token"]);
  const pid = row["pid"];
  if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0) {
    throw new TypeError("Host child PID must be a positive safe integer.");
  }
  validateText(row["token"], "host child token");
  return { pid, token: row["token"] };
}

export function parseHostUseRecovery(value: unknown): HostUseRecoveryV1 {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Host use recovery evidence must be an object.");
  }
  const kind = Reflect.get(value, "kind");
  if (kind === "opening-abandoned") {
    fields(value, ["kind"]);
    return { kind };
  }
  if (kind === "child-exited") {
    const row = fields(value, ["kind", "child"]);
    return { kind, child: parseHostChildIdentity(row["child"]) };
  }
  throw new TypeError("Host use recovery evidence kind is invalid.");
}

export function parseStoredHostUse(value: unknown): HostUseRecordV1 {
  const row = fields(value, [
    "id", "sessionId", "sessionFile", "cwd", "sourceGeneration", "ownerPid", "createdAt", "state", "childPid", "childToken",
  ]);
  validateText(row["id"], "stored host use ID");
  const source = parseHostSourceRef({
    sessionId: row["sessionId"],
    sessionFile: row["sessionFile"],
    cwd: row["cwd"],
  });
  const sourceGeneration = row["sourceGeneration"];
  if (sourceGeneration !== null) {
    validateText(sourceGeneration, "stored host source generation");
    if (!validSourceGeneration.test(sourceGeneration)) throw new TypeError("Stored host source generation is invalid.");
  }
  const ownerPid = row["ownerPid"];
  if (typeof ownerPid !== "number" || !Number.isSafeInteger(ownerPid) || ownerPid <= 0) {
    throw new TypeError("Stored host owner PID is invalid.");
  }
  validateNow(row["createdAt"], "Stored host use creation time");
  const state = row["state"];
  if (state !== "opening" && state !== "attached") throw new TypeError("Stored host use state is invalid.");
  let child: HostChildIdentityV1 | null;
  if (state === "opening") {
    if (row["childPid"] !== null || row["childToken"] !== null) throw new TypeError("Opening host use has child identity.");
    child = null;
  } else {
    child = parseHostChildIdentity({ pid: row["childPid"], token: row["childToken"] });
  }
  return { id: row["id"], ...source, sourceGeneration, ownerPid, createdAt: row["createdAt"], state, child };
}
