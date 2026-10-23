import { closeSync, fstatSync, lstatSync, openSync, readSync } from "node:fs";
import { TextDecoder } from "node:util";

const MAX_HEADER_BYTES = 64 * 1024;
const MAX_SESSION_ID_LENGTH = 512;
const MAX_PARENT_SESSION_LENGTH = 16_384;
const MAX_CWD_LENGTH = 32_768;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

export type SessionHeaderRead =
  | {
      readonly state: "known";
      readonly sessionId: string;
      readonly parentSession?: string;
      readonly fileVersion: string;
    }
  | {
      readonly state: "unknown";
      readonly fileVersion?: string;
    };

/**
 * Reads only the first bounded JSONL record. Body damage does not hide a valid
 * parent edge, while an unreadable or malformed header remains unknown.
 */
export function readSessionHeader(path: string): SessionHeaderRead {
  let descriptor: number | undefined;
  let observedVersion: string | undefined;
  try {
    const before = lstatSync(path, { bigint: true });
    observedVersion = versionOf(before);
    if (!before.isFile()) return { state: "unknown", fileVersion: observedVersion };

    descriptor = openSync(path, "r");
    const opened = fstatSync(descriptor, { bigint: true });
    if (!opened.isFile() || versionOf(opened) !== observedVersion) {
      return { state: "unknown" };
    }

    const buffer = Buffer.allocUnsafe(MAX_HEADER_BYTES + 1);
    let bytesRead = 0;
    let lineEnd = -1;
    while (bytesRead < buffer.length) {
      const start = bytesRead;
      const amount = readSync(descriptor, buffer, start, buffer.length - start, start);
      if (amount === 0) break;
      bytesRead += amount;
      for (let index = start; index < bytesRead; index++) {
        if (buffer[index] === 0x0a) {
          lineEnd = index;
          break;
        }
      }
      if (lineEnd >= 0) break;
    }
    const afterRead = fstatSync(descriptor, { bigint: true });
    const afterPath = lstatSync(path, { bigint: true });
    if (versionOf(afterRead) !== observedVersion || versionOf(afterPath) !== observedVersion) {
      return { state: "unknown" };
    }

    if (lineEnd < 0 || lineEnd > MAX_HEADER_BYTES || lineEnd === 0 || lineEnd >= bytesRead) {
      return { state: "unknown", fileVersion: observedVersion };
    }

    let decoded: unknown;
    try {
      decoded = JSON.parse(utf8Decoder.decode(buffer.subarray(0, lineEnd)));
    } catch {
      return { state: "unknown", fileVersion: observedVersion };
    }
    if (!isRecord(decoded) || decoded.type !== "session" ||
      !validNonEmptyString(decoded.id, MAX_SESSION_ID_LENGTH) ||
      !validTimestamp(decoded.timestamp) ||
      !validNonEmptyString(decoded.cwd, MAX_CWD_LENGTH) ||
      (decoded.version !== undefined &&
        (typeof decoded.version !== "number" || !Number.isInteger(decoded.version) || decoded.version < 1)) ||
      (decoded.parentSession !== undefined &&
        (!validNonEmptyString(decoded.parentSession, MAX_PARENT_SESSION_LENGTH) || decoded.parentSession.includes("\0")))) {
      return { state: "unknown", fileVersion: observedVersion };
    }

    return {
      state: "known",
      sessionId: decoded.id,
      ...(decoded.parentSession === undefined ? {} : { parentSession: decoded.parentSession }),
      fileVersion: observedVersion,
    };
  } catch {
    return observedVersion === undefined
      ? { state: "unknown" }
      : { state: "unknown", fileVersion: observedVersion };
  } finally {
    if (descriptor !== undefined) {
      try { closeSync(descriptor); } catch { /* Read failure already leaves the header unknown. */ }
    }
  }
}

function versionOf(info: {
  readonly dev: bigint;
  readonly ino: bigint;
  readonly size: bigint;
  readonly mtimeNs: bigint;
  readonly ctimeNs: bigint;
}): string {
  return [info.dev, info.ino, info.size, info.mtimeNs, info.ctimeNs].join(":");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validNonEmptyString(value: unknown, maxLength: number): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength &&
    !/[\uD800-\uDFFF]/u.test(value);
}

function validTimestamp(value: unknown): value is string {
  return typeof value === "string" && value.length <= 64 &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    Number.isFinite(Date.parse(value));
}
