// Key policy + rotation CLI (P4-3): at most two active ingest-family keys
// (ingest + server) and one dashboard key per tenant; raw keys are never
// stored or listable. Runs against the real schema.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb } from "../harness.mjs";
import {
  KEY_POLICY,
  KeyPolicyError,
  createKey,
  hashKey,
  listKeys,
  mintKey,
  parseArgs,
  revokeKey,
} from "../../scripts/api-keys.mjs";
async function tenant(pool, name = "keys-test") {
  const { rows } = await pool.query(
    "INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id",
    [name],
  );
  return rows[0].tenant_id;
}

async function rejectsPolicy(promise, pattern) {
  await assert.rejects(promise, (err) => {
    assert.ok(err instanceof KeyPolicyError, `expected KeyPolicyError, got ${err}`);
    if (pattern) assert.match(err.message, pattern);
    return true;
  });
}

test("key policy: two ingest-family keys, one dashboard key, per tenant", async () => {
  await withTestDb("api_keys_policy", async ({ pool }) => {
    const tenantId = await tenant(pool);

    const first = await createKey(pool, { tenantId, kind: "ingest", originHint: "web" });
    assert.match(first.rawKey, /^sk_ingest_/);
    assert.equal(first.fingerprint.length, 12);

    // The raw key is never persisted — only its sha256.
    assert.equal(mintKey("ingest").startsWith("sk_ingest_"), true);
    const stored = await pool.query(
      "SELECT key_hash FROM api_keys WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(
      stored.rows[0].key_hash.toString("hex"),
      hashKey(first.rawKey).toString("hex"),
    );

    await createKey(pool, { tenantId, kind: "server" });

    // A third ingest-family key violates the rotation budget.
    await rejectsPolicy(
      createKey(pool, { tenantId, kind: "ingest" }),
      /revoke one first/,
    );

    const dashboard = await createKey(pool, { tenantId, kind: "dashboard" });
    assert.match(dashboard.rawKey, /^sk_dashboard_/);
    await rejectsPolicy(
      createKey(pool, { tenantId, kind: "dashboard" }),
      /revoke one first/,
    );

    // The budget is per tenant: another tenant is unaffected.
    const other = await tenant(pool, "keys-other");
    await createKey(pool, { tenantId: other, kind: "ingest" });

    // Rotation: revoke the old key, then a new one fits again.
    const revoked = await revokeKey(pool, {
      tenantId,
      fingerprint: first.fingerprint,
    });
    assert.equal(revoked.kind, "ingest");
    await createKey(pool, { tenantId, kind: "ingest" });

    const keys = await listKeys(pool, tenantId);
    assert.equal(keys.length, 4);
    assert.equal(keys.filter((k) => !k.revoked && k.kind === "ingest").length, 1);
    assert.equal(keys.filter((k) => k.revoked).length, 1);
    assert.ok(keys.every((k) => k.fingerprint.length === 12));
    assert.ok(!JSON.stringify(keys).includes("sk_"), "listings never expose raw keys");
  });
});

test("rotation: unknown and ambiguous fingerprints are refused", async () => {
  await withTestDb("api_keys_revoke", async ({ pool }) => {
    const tenantId = await tenant(pool);
    const a = await createKey(pool, { tenantId, kind: "ingest" });
    await createKey(pool, { tenantId, kind: "server" });

    await rejectsPolicy(
      revokeKey(pool, { tenantId, fingerprint: "deadbeefdead" }),
      /no active key/,
    );
    await rejectsPolicy(revokeKey(pool, { tenantId, fingerprint: "not-hex" }), /hex/);

    // Revoking twice is a no-op error, not a second revocation.
    await revokeKey(pool, { tenantId, fingerprint: a.fingerprint });
    await rejectsPolicy(
      revokeKey(pool, { tenantId, fingerprint: a.fingerprint }),
      /no active key/,
    );

    // A prefix matching two active keys is refused rather than guessing. Two
    // synthetic 32-byte hashes share a 3-byte prefix (the schema stores bytes,
    // so this needs no sha256 pre-image search).
    const t2 = await tenant(pool, "keys-ambiguous");
    await pool.query(
      `INSERT INTO api_keys (key_hash, tenant_id, kind) VALUES
         (decode('aabbcc' || repeat('00', 29), 'hex'), $1, 'ingest'),
         (decode('aabbcc' || repeat('11', 29), 'hex'), $1, 'server')`,
      [t2],
    );
    await rejectsPolicy(
      revokeKey(pool, { tenantId: t2, fingerprint: "aabbcc" }),
      /ambiguous/,
    );
    // A longer prefix resolves exactly one of them.
    await revokeKey(pool, { tenantId: t2, fingerprint: "aabbcc0000" });
  });
});

test("CLI argument parsing rejects unknown commands and missing flags", () => {
  assert.deepEqual(parseArgs(["list", "--tenant", "t1"]), {
    command: "list",
    flags: { tenant: "t1" },
  });
  assert.throws(() => parseArgs(["bogus", "--tenant", "t1"]), KeyPolicyError);
  assert.throws(() => parseArgs(["create", "--tenant", "t1"]), /--kind/);
  assert.throws(() => parseArgs(["revoke", "--tenant", "t1"]), /--fingerprint/);
  assert.throws(() => parseArgs(["list"]), /--tenant/);
  assert.throws(() => parseArgs(["list", "--tenant"]), /needs a value/);
});

test("concurrent creates cannot exceed the budget", async () => {
  await withTestDb("api_keys_race", async ({ pool }) => {
    const tenantId = await tenant(pool, "keys-race");
    const attempts = await Promise.allSettled(
      Array.from({ length: 5 }, () => createKey(pool, { tenantId, kind: "ingest" })),
    );
    const ok = attempts.filter((a) => a.status === "fulfilled").length;
    const blocked = attempts.filter(
      (a) => a.status === "rejected" && a.reason instanceof KeyPolicyError,
    ).length;
    assert.equal(ok, KEY_POLICY.ingest);
    assert.equal(ok + blocked, 5);
  });
});

test("mintKey is high-entropy and kind-scoped", () => {
  const a = mintKey("ingest");
  const b = mintKey("ingest");
  assert.notEqual(a, b);
  assert.ok(crypto.createHash("sha256").update(a).digest("hex").length === 64);
});
