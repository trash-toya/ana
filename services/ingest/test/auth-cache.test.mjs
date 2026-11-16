import test from "node:test";
import assert from "node:assert/strict";
import { withTestDb } from "../../../db/test/harness.mjs";
import { ApiKeyAuth } from "../dist/auth.js";
import { seedTenantWithKeys } from "./helpers.mjs";

function makeAuth(pool, { ttlMs = 60_000, clock = { t: 1_000 } } = {}) {
  const calls = [];
  const query = async (text, values) => {
    calls.push({ text, values });
    return pool.query(text, values);
  };
  const auth = new ApiKeyAuth(pool, { ttlMs, now: () => clock.t, query });
  return { auth, calls, clock };
}

test("cache: repeated authentications hit the cache, not the DB", async () => {
  await withTestDb("ingest_authcache", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const { auth, calls } = makeAuth(pool);

    const first = await auth.authenticate(keys.ingestKey);
    assert.equal(first.ok, true);
    assert.equal(first.tenantId, keys.tenantId);
    assert.equal(calls.length, 1);

    const second = await auth.authenticate(keys.ingestKey);
    assert.equal(second.ok, true);
    assert.equal(second.tenantId, keys.tenantId);
    assert.equal(calls.length, 1); // served from cache

    assert.equal((await auth.authenticate(keys.dashboardKey)).ok, false);
    assert.equal(calls.length, 2);
  });
});

test("invalidate() forces a re-lookup on the same instance", async () => {
  await withTestDb("ingest_authinval", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const { auth, calls } = makeAuth(pool);

    await auth.authenticate(keys.ingestKey);
    assert.equal(calls.length, 1);

    auth.invalidate(keys.ingestKey);
    const again = await auth.authenticate(keys.ingestKey);
    assert.equal(again.ok, true);
    assert.equal(calls.length, 2);

    // unknown keys are never cached
    auth.invalidate("sk_never_cached");
  });
});

test("TTL expiry re-queries and sees a revocation within ttlMs", async () => {
  await withTestDb("ingest_authttl", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const { auth, calls, clock } = makeAuth(pool, { ttlMs: 1_000 });

    assert.equal((await auth.authenticate(keys.ingestKey)).ok, true);
    assert.equal(calls.length, 1);

    // revoke in the DB; the cache still answers ok inside the TTL
    await pool.query(
      "UPDATE api_keys SET revoked_at = now() WHERE tenant_id = $1 AND key_hash = digest($2, 'sha256')",
      [keys.tenantId, keys.ingestKey],
    ).catch(async () => {
      // digest() may be unavailable without pgcrypto; fall back to computing the hash in JS
      const { createHash } = await import("node:crypto");
      const hash = createHash("sha256").update(keys.ingestKey, "utf8").digest();
      await pool.query(
        "UPDATE api_keys SET revoked_at = now() WHERE tenant_id = $1 AND key_hash = $2",
        [keys.tenantId, hash],
      );
    });
    clock.t += 500;
    assert.equal((await auth.authenticate(keys.ingestKey)).ok, true);
    assert.equal(calls.length, 1);

    // past the TTL the revoked state is observed: 403
    clock.t += 2_000;
    const expired = await auth.authenticate(keys.ingestKey);
    assert.equal(expired.ok, false);
    if (!expired.ok) {
      assert.equal(expired.status, 403);
      assert.equal(expired.reason, "key_revoked");
    }
    assert.equal(calls.length, 2);
  });
});

test("expired cache entries do not resurrect (expiry, not refresh)", async () => {
  await withTestDb("ingest_authexp", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const { auth, calls, clock } = makeAuth(pool, { ttlMs: 1_000 });
    await auth.authenticate(keys.ingestKey);
    clock.t += 1_001;
    await auth.authenticate(keys.ingestKey);
    assert.equal(calls.length, 2);
  });
});
