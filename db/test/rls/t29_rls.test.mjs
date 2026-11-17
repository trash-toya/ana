// t29 (I9, P4-1): cross-tenant isolation under row level security.
//
// Behavioral proof, not just catalog flags:
//   1. I9 — as app_dashboard with tenant A bound, EVERY granted surface
//      returns 0 tenant-B rows; tenant A's own rows still come through
//      (the checks are not vacuous); the raw tables that cannot carry RLS
//      (events/sessions_history, compressed hypertables — 0017 header) are
//      only sanctioned through the tenant-bound views.
//   2. Fail-closed — an unset app.tenant_id raises, it does not match all.
//   3. app_ingest holds ZERO table privileges (writes go through the
//      SECURITY DEFINER ingest_batch; keys resolve via resolve_api_key).
//   4. app_dashboard is SELECT-only and denied on the write-path internals
//      (api_keys, guard, dedupe, bootstrap, identity_map, tenants) and on
//      the CAGG views themselves (only the *_tenant wrappers are granted).
//   5. WITH CHECK — a scratch role that holds INSERT sees its cross-tenant
//      insert rejected by the RLS policy, and its own tenant's accepted.
//   6. resolve_api_key from app_ingest: right tenant, revoked flag, no row
//      for an unknown hash (cross-tenant BY DESIGN).
import { test } from "vitest";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import pg from "pg";

import { event, ingest, withTestDb, recentBase } from "../harness.mjs";

const base = recentBase(2);
const iso = (t) => new Date(t).toISOString();

/** Run fn as a given role with a transaction-local tenant binding. */
async function asTenant(pool, role, tenantId, fn) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    if (role) await c.query(`SET LOCAL ROLE ${role}`);
    if (tenantId) {
      await c.query("SELECT set_config('app.tenant_id', $1, true)", [tenantId]);
    }
    return await fn(c);
  } finally {
    await c.query("ROLLBACK");
    c.release();
  }
}

/** Expect the given SQL to throw (asserts the message mentions `needle`). */
async function expectError(pool, role, tenantId, sql, needle) {
  await assert.rejects(
    () => asTenant(pool, role, tenantId, (c) => c.query(sql)),
    (err) => {
      assert.match(err.message, new RegExp(needle), `expected error: ${needle}`);
      return true;
    },
  );
}

test("t29: RLS tenant isolation (I9), role privilege audit and WITH CHECK", async () => {
  await withTestDb("t29_rls", async ({ pool }) => {
    // -- Seed two tenants with distinct, non-overlapping data --------------
    const tenantA = randomUUID();
    const tenantB = randomUUID();
    await pool.query(
      "INSERT INTO tenants (tenant_id, name) VALUES ($1, 'a'), ($2, 'b')",
      [tenantA, tenantB],
    );
    for (const tenantId of [tenantA, tenantB]) {
      for (let i = 0; i < 3; i++) {
        const sid = randomUUID();
        const r = await ingest(
          pool,
          Array.from({ length: 2 }, (_, k) =>
            event({
              tenantId,
              sessionId: sid,
              eventId: randomUUID(),
              seq: k + 1,
              time: iso(base + k * 60_000),
              anonId: tenantId === tenantA ? "anon-a" : "anon-b",
            }),
          ),
        );
        assert.equal(r.stored.length, 2);
      }
    }
    // Keys for resolve_api_key (A valid ingest key, B revoked one).
    const keyA = randomUUID();
    const keyB = randomUUID();
    const hash = (k) => createHash("sha256").update(k).digest();
    await pool.query(
      "INSERT INTO api_keys (key_hash, tenant_id, kind) VALUES ($1, $2, 'ingest')",
      [hash(keyA), tenantA],
    );
    await pool.query(
      "INSERT INTO api_keys (key_hash, tenant_id, kind, revoked_at) VALUES ($1, $2, 'ingest', now())",
      [hash(keyB), tenantB],
    );

    // -- 1. I9: every granted surface, bound to tenant A, has 0 tenant-B rows
    const surfaces = [
      "events_tenant",
      "history_tenant",
      "events_15m_tenant",
      "identity_15m_tenant",
      "sessions",
      "sessions_active",
      "session_stats_15m",
      "session_stats_daily",
      "identity_daily",
      "tenant_settings",
    ];
    for (const surface of surfaces) {
      const n = await asTenant(pool, "app_dashboard", tenantA, (c) =>
        c.query(`SELECT count(*) AS n FROM ${surface} WHERE tenant_id <> $1`, [tenantA]),
      );
      assert.equal(Number(n.rows[0].n), 0, `${surface} leaked tenant-B rows`);
    }
    // Not vacuous: tenant A's own data is visible through the tenant-bound
    // views and the RLS-protected tables.
    const own = await asTenant(pool, "app_dashboard", tenantA, (c) =>
      c.query("SELECT count(*) AS n FROM events_tenant WHERE tenant_id = $1", [tenantA]),
    );
    assert.equal(Number(own.rows[0].n), 6, "tenant A rows visible via events_tenant");
    const ownRls = await asTenant(pool, "app_dashboard", tenantA, (c) =>
      c.query("SELECT count(*) AS n FROM sessions_active"),
    );
    assert.equal(Number(ownRls.rows[0].n), 3, "tenant A rows visible via RLS table");

    // Raw tables (no RLS possible on the pinned image): direct read shows
    // both tenants' rows — the documented D37 gap, sanctioned only through
    // the tenant-bound views (0017).
    const raw = await asTenant(pool, "app_dashboard", tenantA, (c) =>
      c.query("SELECT count(DISTINCT tenant_id) AS n FROM events"),
    );
    assert.equal(
      Number(raw.rows[0].n),
      2,
      "raw events readable (D37), view is the boundary",
    );

    // -- 2. Fail-closed: unset binding raises --------------------------------
    await expectError(
      pool,
      "app_dashboard",
      null,
      "SELECT count(*) FROM events_tenant",
      "uuid",
    );
    await expectError(
      pool,
      "app_dashboard",
      null,
      "SELECT count(*) FROM session_stats_15m",
      "uuid",
    );

    // -- 3. app_ingest: zero table privileges in public ----------------------
    const privs = await pool.query(
      `SELECT table_name, privilege_type
         FROM information_schema.role_table_grants
        WHERE grantee = 'app_ingest' AND table_schema = 'public'`,
    );
    assert.deepEqual(privs.rows, [], "app_ingest must hold no table privileges at all");
    // It still authenticates through the definer function.
    const res = await asTenant(pool, "app_ingest", null, (c) =>
      c.query("SELECT tenant_id, kind, revoked FROM resolve_api_key($1)", [hash(keyA)]),
    );
    assert.equal(res.rows[0].tenant_id, tenantA);
    assert.equal(res.rows[0].kind, "ingest");
    assert.equal(res.rows[0].revoked, false);

    // -- 4. app_dashboard: SELECT-only + write-path/CAGG denied --------------
    for (const denied of [
      "api_keys",
      "session_guard",
      "event_dedupe",
      "session_bootstrap",
      "identity_map",
      "tenants",
      "events_15m",
      "identity_15m",
    ]) {
      await expectError(
        pool,
        "app_dashboard",
        tenantA,
        `SELECT count(*) FROM ${denied}`,
        "permission denied",
      );
    }
    const writes = await pool.query(
      `SELECT table_name, privilege_type
         FROM information_schema.role_table_grants
        WHERE grantee = 'app_dashboard' AND table_schema = 'public'
          AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')`,
    );
    assert.deepEqual(writes.rows, [], "app_dashboard must be SELECT-only");

    // -- 5. WITH CHECK: cross-tenant insert rejected, own-tenant accepted ----
    await pool.query("DROP ROLE IF EXISTS t29_scratch");
    await pool.query("CREATE ROLE t29_scratch NOLOGIN");
    await pool.query(
      "GRANT SELECT, INSERT ON tenant_settings, session_stats_15m TO t29_scratch",
    );
    await expectError(
      pool,
      "t29_scratch",
      tenantA,
      `INSERT INTO tenant_settings (tenant_id, tz) VALUES ('${tenantB}', 'UTC')`,
      "row-level security",
    );
    await asTenant(pool, "t29_scratch", tenantA, (c) =>
      c.query("INSERT INTO tenant_settings (tenant_id, tz) VALUES ($1, 'UTC')", [
        tenantA,
      ]),
    );
    // Scratch role owns nothing — only its grants must be revoked first.
    await pool.query(
      "REVOKE SELECT, INSERT ON tenant_settings, session_stats_15m FROM t29_scratch",
    );
    await pool.query("DROP ROLE t29_scratch");

    // -- 6. resolve_api_key semantics ----------------------------------------
    const revoked = await asTenant(pool, "app_ingest", null, (c) =>
      c.query("SELECT tenant_id, revoked FROM resolve_api_key($1)", [hash(keyB)]),
    );
    assert.equal(revoked.rows[0].tenant_id, tenantB);
    assert.equal(revoked.rows[0].revoked, true);
    const unknown = await asTenant(pool, "app_ingest", null, (c) =>
      c.query("SELECT * FROM resolve_api_key($1)", [hash(randomUUID())]),
    );
    assert.equal(unknown.rows.length, 0, "unknown hash resolves to no row");
    // Both roles hold EXECUTE.
    await asTenant(pool, "app_dashboard", null, (c) =>
      c.query("SELECT * FROM resolve_api_key($1)", [hash(keyA)]),
    );
  });
});

test("t29: RLS catalog flags match the migration contract", async () => {
  await withTestDb("t29_flags", async ({ pool }) => {
    const expected = [
      "sessions_active",
      "session_guard",
      "event_dedupe",
      "session_stats_15m",
      "identity_map",
      "tenants",
      "tenant_settings",
      "session_stats_daily",
      "identity_daily",
      "session_bootstrap",
    ];
    const flags = await pool.query(
      `SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
         FROM pg_class c
        WHERE c.relname = ANY($1)
        ORDER BY 1`,
      [expected],
    );
    assert.equal(flags.rows.length, expected.length);
    for (const row of flags.rows) {
      assert.equal(row.relrowsecurity, true, `${row.relname} ENABLE RLS`);
      assert.equal(row.relforcerowsecurity, true, `${row.relname} FORCE RLS`);
    }
    // The two policy pairs exist per table.
    const policies = await pool.query(
      `SELECT tablename, count(*) AS n FROM pg_policies
        WHERE schemaname = 'public' AND policyname IN ('tenant_isolation', 'owner_maintenance')
        GROUP BY 1 ORDER BY 1`,
    );
    const nByTable = Object.fromEntries(
      policies.rows.map((r) => [r.tablename, Number(r.n)]),
    );
    for (const t of expected) {
      assert.equal(nByTable[t], 2, `${t} has both policies`);
    }
    // The CAGG surfaces the wrappers cannot protect at the RLS layer are
    // instead closed by grants: no SELECT for app_dashboard on the CAGG views
    // (nor on their materialization hypertables), and the wrappers exist.
    for (const [rel, grantable] of [
      ["events_15m", false],
      ["identity_15m", false],
      ["events_15m_tenant", true],
      ["identity_15m_tenant", true],
    ]) {
      const g = await pool.query(
        `SELECT has_table_privilege('app_dashboard', $1, 'SELECT') AS g`,
        [rel],
      );
      assert.equal(
        g.rows[0].g,
        grantable,
        `${rel}: app_dashboard SELECT grant must be ${grantable}`,
      );
    }
    const wrappers = await pool.query(
      `SELECT count(*) AS n FROM information_schema.views
        WHERE table_schema = 'public'
          AND table_name IN ('events_15m_tenant', 'identity_15m_tenant')`,
    );
    assert.equal(Number(wrappers.rows[0].n), 2);
  });
});
