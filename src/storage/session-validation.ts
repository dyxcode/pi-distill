import type {
  SessionObservation
} from "../distill/types.ts";
import type { SessionUsage } from "../retention/types.ts";
import { fields, parseNullableText, validateAbsolutePath, validateText } from './validation.ts';
/** ESRCH establishes exit; permission failures and PID reuse remain protected. */
export function processHasExited(pid: number): boolean {
  try { process.kill(pid, 0); return false; }
  catch (error) { return error instanceof Error && "code" in error && error.code === "ESRCH"; }
}

export function parseSessionObservation(value: unknown): SessionObservation {
  const { sessionId, sessionFile, cwd, leafId, ownerPid } = fields(
    value, ["sessionId", "sessionFile", "cwd", "leafId", "ownerPid"],
  );
  validateText(sessionId, "session ID");
  validateAbsolutePath(sessionFile, "session file");
  validateAbsolutePath(cwd, "session working directory");
  const parsedLeafId = parseNullableText(leafId, "session leaf ID");
  if (typeof ownerPid !== "number" || !Number.isSafeInteger(ownerPid) || ownerPid < 1) {
    throw new TypeError("Session owner PID must be a positive safe integer.");
  }
  return { sessionId, sessionFile, cwd, leafId: parsedLeafId, ownerPid };
}

export function parseSessionUsage(value: unknown): SessionUsage {
  const { ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state } = fields(
    value,
    ["sessionId", "sessionFile", "cwd", "leafId", "ownerPid", "ownerId", "processToken", "state"],
  );
  validateText(ownerId, "session usage owner ID");
  validateText(processToken, "session usage process token");
  if (state !== "active" && state !== "closing" && state !== "released") {
    throw new TypeError("Session usage state must be active, closing, or released.");
  }
  return {
    ...parseSessionObservation({ sessionId, sessionFile, cwd, leafId, ownerPid }),
    ownerId,
    processToken,
    state,
  };
}

export function parseStoredSessionObservation(value: unknown): {
  readonly observation: SessionObservation;
  readonly captured: boolean;
} {
  const { session_id, session_file, cwd, leaf_id, owner_pid, captured } = fields(
    value, ["session_id", "session_file", "cwd", "leaf_id", "owner_pid", "captured"],
  );
  if (captured !== 0 && captured !== 1) {
    throw new TypeError("Stored session observation capture state is invalid.");
  }
  return {
    observation: parseSessionObservation({
      sessionId: session_id,
      sessionFile: session_file,
      cwd,
      leafId: leaf_id,
      ownerPid: owner_pid,
    }),
    captured: captured === 1,
  };
}

export function parseStoredSessionUsage(value: unknown): SessionUsage {
  const { ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state } = fields(
    value,
    ["ownerId", "processToken", "sessionId", "sessionFile", "cwd", "leafId", "ownerPid", "state"],
  );
  return parseSessionUsage({ ownerId, processToken, sessionId, sessionFile, cwd, leafId, ownerPid, state });
}

export function sameSessionUsageIdentity(first: SessionUsage, second: SessionUsage): boolean {
  return first.ownerId === second.ownerId &&
    first.processToken === second.processToken &&
    first.sessionId === second.sessionId &&
    first.sessionFile === second.sessionFile &&
    first.cwd === second.cwd &&
    first.ownerPid === second.ownerPid;
}
