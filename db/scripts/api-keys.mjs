#!/usr/bin/env node
// API-key provisioning and rotation (P4-3; 02-data-model.md "api_keys").
//
// The key policy lives here, not in DDL: at most TWO active ingest-family keys
// (ingest + server share the rotation budget) and ONE dashboard key per tenant.
// Two ingest keys is what makes zero-downtime rotation possible —
//
//   1. `create` the new key (now 2 active),
//   2. deploy it to the producer,
//   3. `revoke` the old fingerprint once the new key is confirmed in use.
//
// Revocation is effective within the dashboard/ingest auth cache TTL (60 s)
// across instances. The raw key is printed exactly once and never stored: the
// table holds sha256 only, and this script itself only ever prints fingerprints.
//
// Usage (DATABASE_URL or SA_DB, like the migration runner):
//   node db/scripts/api-keys.mjs list   --tenant <uuid>
//   node db/scripts/api-keys.mjs create --tenant <uuid> --kind ingest|server|dashboard
//                                       [--origin <hint>]
//   node db/scripts/api-keys.mjs revoke --tenant <uuid> --fingerprint <hex prefix>
//
// Exit codes: 0 ok, 1 usage/DB error, 2 key-policy violation.

import { createHash, randomBytes } from "node:crypto";
import { pathToFileURL } from "node:url";
import pg from "pg";

/** Active-key budget per tenant. `ingest` and `server` share one family. */
export const KEY_POLICY = { ingest: 2, server: 2, dashboard: 1 };
const INGEST_FAMILY = ["ingest", "server"];
const KINDS = ["ingest", "server", "dashboard"];
const FINGERPRINT_HEX = 12;

export class KeyPolicyError extends Error {
  constructor(message) {
    super(message);
    this.name = "KeyPolicyError";
  }
}

export function hashKey(rawKey) {
  return createHash("sha256").update(rawKey, "utf8").digest();
}

/** `sk_<kind>_<32 bytes base64url>`: high entropy, recognisable, kind-scoped. */
export function mintKey(kind) {
  return `sk_${kind}_${randomBytes(32).toString("base64url")}`;
}

/** Short, non-reversible handle used in listings, logs and runbooks. */
export function fingerprint(keyHash) {
  return keyHash.toString("hex").slice(0, FINGERPRINT_HEX);
}

function assertKind(kind) {
  if (!KINDS.includes(kind)) {
    throw new KeyPolicyError(`kind must be one of ${KINDS.join(", ")} (got ${kind})`);
  }
}

/**
 * Create a key, enforcing the policy under a per-tenant advisory lock so two
 * concurrent operators cannot both see "1 active" and mint a third key.
 * Returns the raw key ONCE plus its fingerprint.
 */
export async function createKey(
  pool,
  { tenantId, kind, originHint = null, rawKey = mintKey(kind) },
) {
  assertKind(kind);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [tenantId]);
    const family = kind === "dashboard" ? ["dashboard"] : INGEST_FAMILY;
    const { rows } = await client.query(
      `SELECT count(*)::int AS n
         FROM api_keys
        WHERE tenant_id = $1 AND kind = ANY($2) AND revoked_at IS NULL`,
      [tenantId, family],
    );
    const active = rows[0].n;
    const budget = kind === "dashboard" ? KEY_POLICY.dashboard : KEY_POLICY.ingest;
    if (active >= budget) {
      throw new KeyPolicyError(
        `tenant ${tenantId} already has ${active} active ${family.join("/")} key(s); ` +
          `revoke one first (rotate: create new, deploy, then revoke the old)`,
      );
    }
    const keyHash = hashKey(rawKey);
    await client.query(
      `INSERT INTO api_keys (key_hash, tenant_id, kind, origin_hint)
       VALUES ($1, $2, $3, $4)`,
      [keyHash, tenantId, kind, originHint],
    );
    await client.query("COMMIT");
    return { rawKey, fingerprint: fingerprint(keyHash), kind, originHint };
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Revoke by fingerprint prefix. Only keys of `tenantId` are considered.
 * Candidates are matched first, so an ambiguous prefix revokes nothing.
 */
export async function revokeKey(pool, { tenantId, fingerprint: prefix }) {
  if (!/^[0-9a-f]{6,64}$/i.test(prefix)) {
    throw new KeyPolicyError("fingerprint must be 6–64 hex characters");
  }
  const candidates = await pool.query(
    `SELECT encode(key_hash, 'hex') AS hex, kind
       FROM api_keys
      WHERE tenant_id = $1
        AND revoked_at IS NULL
        AND encode(key_hash, 'hex') LIKE $2`,
    [tenantId, `${prefix.toLowerCase()}%`],
  );
  if (candidates.rows.length === 0) {
    throw new KeyPolicyError(`no active key matching ${prefix} for tenant ${tenantId}`);
  }
  if (candidates.rows.length > 1) {
    throw new KeyPolicyError(
      `fingerprint ${prefix} is ambiguous (${candidates.rows.length} keys); use more characters`,
    );
  }
  const target = candidates.rows[0];
  await pool.query(
    `UPDATE api_keys SET revoked_at = now()
      WHERE tenant_id = $1 AND key_hash = decode($2, 'hex') AND revoked_at IS NULL`,
    [tenantId, target.hex],
  );
  return { fingerprint: target.hex.slice(0, FINGERPRINT_HEX), kind: target.kind };
}

/** Never returns raw keys: only what is safe to show in a shell or a ticket. */
export async function listKeys(pool, tenantId) {
  const { rows } = await pool.query(
    `SELECT encode(key_hash, 'hex') AS hex, kind, origin_hint, created_at, revoked_at
       FROM api_keys
      WHERE tenant_id = $1
      ORDER BY created_at DESC`,
    [tenantId],
  );
  return rows.map((r) => ({
    fingerprint: r.hex.slice(0, FINGERPRINT_HEX),
    kind: r.kind,
    originHint: r.origin_hint,
    createdAt: r.created_at.toISOString(),
    revoked: r.revoked_at !== null,
  }));
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

export function parseArgs(argv) {
  const [command, ...rest] = argv;
  const flags = {};
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i];
    if (!arg.startsWith("--")) throw new KeyPolicyError(`unexpected argument: ${arg}`);
    const name = arg.slice(2);
    const value = rest[i + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new KeyPolicyError(`--${name} needs a value`);
    }
    flags[name] = value;
    i++;
  }
  if (!["list", "create", "revoke"].includes(command)) {
    throw new KeyPolicyError(
      "usage: api-keys.mjs list|create|revoke --tenant <uuid> [...]",
    );
  }
  if (!flags.tenant) throw new KeyPolicyError("--tenant <uuid> is required");
  if (command === "create" && !flags.kind) throw new KeyPolicyError("--kind is required");
  if (command === "revoke" && !flags.fingerprint) {
    throw new KeyPolicyError("--fingerprint is required");
  }
  return { command, flags };
}

async function main(argv) {
  const { command, flags } = parseArgs(argv);
  const connectionString =
    process.env.DATABASE_URL ??
    (process.env.SA_DB
      ? `postgresql://postgres:postgres@127.0.0.1:5432/${process.env.SA_DB}`
      : "postgresql://postgres:postgres@127.0.0.1:5432/postgres");
  const pool = new pg.Pool({ connectionString, max: 1 });
  try {
    if (command === "list") {
      const keys = await listKeys(pool, flags.tenant);
      if (keys.length === 0) console.log("no keys");
      for (const k of keys) {
        const state = k.revoked ? "revoked" : "active";
        console.log(
          `${k.fingerprint}  ${k.kind.padEnd(9)} ${state.padEnd(7)} ` +
            `${k.originHint ?? "-"}  created ${k.createdAt}`,
        );
      }
      return 0;
    }
    if (command === "create") {
      const created = await createKey(pool, {
        tenantId: flags.tenant,
        kind: flags.kind,
        originHint: flags.origin ?? null,
      });
      // The only time the raw key exists outside the producer's secret store.
      console.log(created.rawKey);
      console.error(
        `created ${created.kind} key ${created.fingerprint} — store it now, it is not shown again`,
      );
      return 0;
    }
    const revoked = await revokeKey(pool, {
      tenantId: flags.tenant,
      fingerprint: flags.fingerprint,
    });
    console.error(`revoked ${revoked.kind} key ${revoked.fingerprint}`);
    return 0;
  } finally {
    await pool.end();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2))
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error(`api-keys: ${err.message}`);
      process.exit(err instanceof KeyPolicyError ? 2 : 1);
    });
}
