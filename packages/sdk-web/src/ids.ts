// UUIDv7: time-ordered client-minted IDs. The server never trusts the
// embedded timestamp — it is only for local sort order (02-data-model.md).

/** Monotonic-ish UUIDv7 from a millisecond timestamp + random bits. */
export function uuidv7(nowMs: number, randomBytes: Uint8Array): string {
  // 48-bit unix ms → first 6 bytes
  const ts = BigInt(Math.max(0, Math.floor(nowMs))) & 0xffffffffffffn;
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 6; i++) {
    bytes[i] = Number((ts >> BigInt(8 * (5 - i))) & 0xffn);
  }
  // version 7: 0111 low nibble of byte 6
  bytes[6] = (randomBytes[0]! & 0x0f) | 0x70;
  bytes[7] = randomBytes[1]!;
  bytes[8] = (randomBytes[2]! & 0x3f) | 0x80; // variant 10
  bytes[9] = randomBytes[3]!;
  for (let i = 10; i < 16; i++) bytes[i] = randomBytes[i - 6]!;

  const hex = [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Browser randomness for uuidv7. */
export function randomBytes(): Uint8Array {
  const b = new Uint8Array(10);
  if (typeof crypto !== "undefined" && crypto.getRandomValues) {
    crypto.getRandomValues(b);
  } else {
    for (let i = 0; i < 10; i++) b[i] = Math.floor(Math.random() * 256);
  }
  return b;
}
