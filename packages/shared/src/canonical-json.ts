// Deterministic JSON serialisation (docs/plan/04-services.md).
//
// `JSON.stringify` is key-order dependent, so a retry built from a re-parsed
// object would hash differently and be rejected as a conflicting duplicate
// (DEVIATIONS.md D7). `canonicalJson` always emits the same bytes for the same
// value: object keys sorted, no whitespace, `undefined` object values dropped,
// `undefined` array elements serialised as `null` (same as JSON.stringify).
//
// Throws on values JSON cannot represent (functions, symbols, bigints,
// non-finite numbers) — props are validated against the zod schema before this
// is ever called on them, so this is a pure safety net.

type Json =
  | string
  | number
  | boolean
  | null
  | readonly Json[]
  | { readonly [key: string]: Json };

function serialize(value: unknown): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "boolean":
      return value ? "true" : "false";
    case "number":
      if (!Number.isFinite(value)) {
        throw new TypeError(
          `canonicalJson: non-finite number ${String(value)} is not serialisable`,
        );
      }
      return JSON.stringify(value);
    case "object": {
      if (Array.isArray(value)) {
        // undefined array elements serialise as null, like JSON.stringify
        const items = (value as unknown[])
          .map((item) => (item === undefined ? "null" : serialize(item)))
          .join(",");
        return `[${items}]`;
      }
      const source = value as { [key: string]: unknown };
      const parts: string[] = [];
      for (const key of Object.keys(source).sort()) {
        const member = source[key];
        if (member === undefined) continue; // dropped, like JSON.stringify
        parts.push(`${JSON.stringify(key)}:${serialize(member)}`);
      }
      return `{${parts.join(",")}}`;
    }
    default:
      throw new TypeError(
        `canonicalJson: value of type ${typeof value} is not serialisable`,
      );
  }
}

export function canonicalJson(value: unknown): string {
  return serialize(value);
}
