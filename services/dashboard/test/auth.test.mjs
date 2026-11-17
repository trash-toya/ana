// Auth for the dashboard service: ONLY kind='dashboard' keys authenticate.
// The ingest service's ApiKeyAuth deliberately excludes dashboard keys and
// this one excludes ingest keys — a leaked key cannot cross services
// (04-services.md). Cache: 60 s TTL keyed on the digest; revocation takes
// effect within the TTL across instances, immediately with invalidate().
import { test } from "vitest";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { DashboardKeyAuth } from "../dist/auth.js";
import { seedTenantWithKeys } from "./helpers.mjs";

test("missing key → 401 missing_key", async () => {
  await withTestDb("dash_auth_missing", async ({ pool }) => {
    await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const res = await request(app).get("/v1/live");
    assert.equal(res.status, 401);
    assert.equal(res.body.error, "missing_key");
  });
});

test("unknown key → 401 invalid_key", async () => {
  await withTestDb("dash_auth_invalid", async ({ pool }) => {
    await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const res = await request(app).get("/v1/live").set("X-API-Key", "dk_nope");
    assert.equal(res.status, 401);
    assert.equal(res.body.error, "invalid_key");
  });
});

test("ingest-kind key → 401 wrong_key_kind (cross-service key rejected)", async () => {
  await withTestDb("dash_auth_wrongkind", async ({ pool }) => {
    const { ingestKey } = await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const res = await request(app).get("/v1/live").set("X-API-Key", ingestKey);
    assert.equal(res.status, 401);
    assert.equal(res.body.error, "wrong_key_kind");
  });
});

test("revoked dashboard key → 403 key_revoked", async () => {
  await withTestDb("dash_auth_revoked", async ({ pool }) => {
    const { revokedKey } = await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const res = await request(app).get("/v1/live").set("X-API-Key", revokedKey);
    assert.equal(res.status, 403);
    assert.equal(res.body.error, "key_revoked");
  });
});

test("valid dashboard key authenticates; healthz/readyz need no key", async () => {
  await withTestDb("dash_auth_ok", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const health = await request(app).get("/healthz");
    assert.equal(health.status, 200);
    const res = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    assert.equal(res.body.live, true);
  });
});

test("cache: reads within TTL reuse the cached tenant; revocation surfaces at expiry", async () => {
  await withTestDb("dash_auth_cache", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    let now = 1_000_000;
    const auth = new DashboardKeyAuth(pool, { ttlMs: 60_000, now: () => now });
    const app = createApp({ pool, auth });

    const first = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(first.status, 200);

    // Revoke directly in the DB (no invalidate()): still OK within the TTL.
    await pool.query(
      `UPDATE api_keys SET revoked_at = now()
        WHERE tenant_id = $1 AND kind = 'dashboard' AND revoked_at IS NULL`,
      [(await pool.query("SELECT tenant_id FROM tenants")).rows[0].tenant_id],
    );
    const cached = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(cached.status, 200, "cache served past revocation within TTL");

    // Past the TTL the lookup runs again and the revocation lands.
    now += 61_000;
    const expired = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(expired.status, 403);
  });
});

test("invalidate() makes same-instance revocation immediate", async () => {
  await withTestDb("dash_auth_inval", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    const auth = new DashboardKeyAuth(pool, {});
    const app = createApp({ pool, auth });

    const before = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(before.status, 200);

    await pool.query(
      `UPDATE api_keys SET revoked_at = now()
        WHERE key_hash = $1`,
      [createHash("sha256").update(dashboardKey, "utf8").digest()],
    );
    auth.invalidate(dashboardKey);
    const after = await request(app).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(after.status, 403, "invalidate() did not drop the cached entry");
  });
});
