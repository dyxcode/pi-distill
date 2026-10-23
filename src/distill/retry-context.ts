import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { readSessionHeader } from "../sources/header.ts";
import type { DistillRetryContext } from "./retry.ts";
import type { ModelRuntime } from "./runtime.ts";

const POLICY_VERSION = "distill-retry-context-v4";
const MAX_ANCESTRY_DEPTH = 128;
const MAX_MODEL_TEXT_LENGTH = 4_096;
const MAX_BASE_URL_LENGTH = 16_384;
const MAX_SESSION_PATH_LENGTH = 32_768;

/**
 * Build the bounded identity used to decide whether a failed distillation may
 * be retried. Session reads stop at each file's bounded first-line header;
 * transcript bodies are identified through the header reader's file version.
 */
export function distillRetryContext(
  sessionFile: string,
  model: ModelRuntime["model"],
): DistillRetryContext {
  return {
    sourceVersion: sourceVersion(sessionFile),
    modelKey: modelKey(model),
    policyVersion: POLICY_VERSION,
  };
}

function sourceVersion(sessionFile: string): string {
  if (typeof sessionFile !== "string") {
    return fingerprint(["distill-source-v1", "invalid-session-path", typeof sessionFile]);
  }
  if (sessionFile.length === 0 || sessionFile.length > MAX_SESSION_PATH_LENGTH || sessionFile.includes("\0")) {
    return invalidSessionPath(sessionFile);
  }

  let currentPath: string;
  try {
    currentPath = resolve(sessionFile);
  } catch {
    return invalidSessionPath(sessionFile);
  }

  const observations: unknown[][] = [];
  const seen = new Set<string>();
  for (let depth = 0; depth <= MAX_ANCESTRY_DEPTH; depth++) {
    if (seen.has(currentPath)) {
      observations.push(["cycle", currentPath]);
      break;
    }
    seen.add(currentPath);

    const header = readSessionHeader(currentPath);
    observations.push([
      currentPath,
      header.state,
      header.fileVersion ?? null,
      header.state === "known" ? header.sessionId : null,
      header.state === "known" ? header.parentSession ?? null : null,
    ]);

    if (header.state !== "known" || header.parentSession === undefined) break;

    let parentPath: string;
    try {
      parentPath = resolve(dirname(currentPath), header.parentSession);
    } catch {
      observations.push(["invalid-parent-path"]);
      break;
    }

    if (seen.has(parentPath)) {
      observations.push(["cycle", parentPath]);
      break;
    }
    if (depth === MAX_ANCESTRY_DEPTH) {
      observations.push(["ancestry-depth-limit", parentPath]);
      break;
    }
    currentPath = parentPath;
  }

  return fingerprint(["distill-source-v1", observations]);
}

function invalidSessionPath(sessionFile: string): string {
  return fingerprint(["distill-source-v1", "invalid-session-path", fingerprint(["session-path-v1", sessionFile])]);
}

function modelKey(model: ModelRuntime["model"]): string {
  if (model === undefined) return fingerprint(["pi-model-v1", "unavailable"]);

  const baseUrlKey = typeof model.baseUrl === "string" && model.baseUrl.length <= MAX_BASE_URL_LENGTH
    ? fingerprint(["model-endpoint-v1", model.baseUrl])
    : "invalid";
  return fingerprint([
    "pi-model-v1",
    modelText(model.provider) ? model.provider : "invalid-provider",
    modelText(model.id) ? model.id : "invalid-model-id",
    modelText(model.api) ? model.api : "invalid-api",
    modelLimit(model.contextWindow) ? model.contextWindow : "invalid-context-window",
    modelLimit(model.maxTokens) ? model.maxTokens : "invalid-max-tokens",
    baseUrlKey,
  ]);
}

function modelText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0 && value.length <= MAX_MODEL_TEXT_LENGTH &&
    !value.includes("\0") && !/[\uD800-\uDFFF]/u.test(value);
}

function modelLimit(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function fingerprint(value: readonly unknown[]): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}
