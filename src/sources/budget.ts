import type { AggregateBudget } from './types.ts';
export const MAX_ANCESTRY_JSON_BYTES = 16 * 1024 * 1024;

export const MAX_ANCESTRY_ENTRIES = 20_000;

export const MAX_JSON_DEPTH = 256;

export function consumeAggregateBudget(budget: AggregateBudget, value: unknown, entryCount: number): boolean {
  if (!consumeEntryBudget(budget, entryCount)) return false;
  const bytes = boundedJsonBytes(value, MAX_ANCESTRY_JSON_BYTES - budget.bytes);
  if (bytes === undefined) {
    budget.entries -= entryCount;
    return false;
  }
  budget.bytes += bytes;
  return true;
}

export function consumeEntryBudget(budget: AggregateBudget, entryCount: number): boolean {
  if (!Number.isSafeInteger(entryCount) || entryCount < 0 || entryCount > MAX_ANCESTRY_ENTRIES - budget.entries) return false;
  budget.entries += entryCount;
  return true;
}

export function consumeRawByteBudget(budget: AggregateBudget, byteCount: number): boolean {
  if (!Number.isSafeInteger(byteCount) || byteCount < 0 || byteCount > MAX_ANCESTRY_JSON_BYTES - budget.bytes) return false;
  budget.bytes += byteCount;
  return true;
}

export function boundedJsonBytes(value: unknown, limit: number): number | undefined {
  if (!Number.isSafeInteger(limit) || limit < 0) return undefined;
  const active = new WeakSet<object>();
  const count = (current: unknown, remaining: number, depth: number, inArray: boolean): number | undefined => {
    if (remaining < 0 || depth > MAX_JSON_DEPTH) return undefined;
    if (current === null) return remaining >= 4 ? 4 : undefined;
    switch (typeof current) {
      case "string":
        return jsonStringBytes(current, remaining);
      case "boolean": {
        const bytes = current ? 4 : 5;
        return remaining >= bytes ? bytes : undefined;
      }
      case "number": {
        const bytes = Number.isFinite(current) ? String(current).length : 4;
        return remaining >= bytes ? bytes : undefined;
      }
      case "undefined":
      case "function":
      case "symbol":
        if (inArray) return remaining >= 4 ? 4 : undefined;
        return 0;
      case "bigint":
        return undefined;
      case "object":
        break;
      default:
        return undefined;
    }

    const object = current;
    if (active.has(object)) return undefined;
    active.add(object);
    try {
      if (Array.isArray(object)) {
        let total = 2;
        if (total > remaining) return undefined;
        for (let index = 0; index < object.length; index += 1) {
          if (index > 0) total += 1;
          const child = count(object[index], remaining - total, depth + 1, true);
          if (child === undefined) return undefined;
          total += child;
        }
        return total;
      }

      let total = 2;
      if (total > remaining) return undefined;
      let first = true;
      for (const key in object) {
        if (!Object.prototype.hasOwnProperty.call(object, key)) continue;
        let childValue: unknown;
        try {
          childValue = Reflect.get(object, key);
        } catch {
          return undefined;
        }
        if (childValue === undefined || typeof childValue === "function" || typeof childValue === "symbol") continue;
        if (!first) total += 1;
        first = false;
        const keyBytes = jsonStringBytes(key, remaining - total);
        if (keyBytes === undefined) return undefined;
        total += keyBytes + 1;
        const childBytes = count(childValue, remaining - total, depth + 1, false);
        if (childBytes === undefined) return undefined;
        total += childBytes;
        if (total > remaining) return undefined;
      }
      return total;
    } finally {
      active.delete(object);
    }
  };

  return count(value, limit, 0, false);
}

export function jsonStringBytes(value: string, limit: number): number | undefined {
  let total = 2;
  if (total > limit) return undefined;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    let bytes: number;
    if (code === 0x22 || code === 0x5c || code === 0x08 || code === 0x09 || code === 0x0a || code === 0x0c || code === 0x0d) {
      bytes = 2;
    } else if (code < 0x20) {
      bytes = 6;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes = 4;
        index += 1;
      } else {
        bytes = 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      bytes = 6;
    } else if (code <= 0x7f) {
      bytes = 1;
    } else if (code <= 0x7ff) {
      bytes = 2;
    } else {
      bytes = 3;
    }
    total += bytes;
    if (total > limit) return undefined;
  }
  return total;
}
