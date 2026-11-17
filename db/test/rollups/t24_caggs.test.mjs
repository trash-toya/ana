// T24 — the P3-1 continuous aggregates (db/migrations/0012_caggs.sql).
// Done-when (08-execution.md): "Refresh under 5 min; toolkit HLL works in the
// CAGG on the pinned version". The pinned-image spike (Timescale 2.32.0 /
// toolkit 1.26.0) settled the design: events_15m materializes an EXACT
// count(DISTINCT session_id); identity_15m materializes HLL STATE and
// finalizes on read (`distinct_count(rollup(...))` cannot nest in a CAGG).
//
// Tests never run the background scheduler: the harness deletes policy_* jobs,
// so refreshes here are explicit `CALL refresh_continuous_aggregate` calls.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { assertInvariant, event, ingest, recentBase, withTestDb } from "../harness.mjs";
import { applyMigrations, BOOTSTRAP_URL } from "../scripts/migrate.mjs";
import { removePolicyJobs } from "../harness.mjs";
import pg from "pg";

const randomUuid = () => crypto.randomUUID();

/**
 * A fixture of ~50 events over six 15-minute buckets, two tenants, three
 * sessions, a handful of anon ids and users (including NULL users).
 * Times are anchored to `recentBase` so I8's 3-day window holds.
 */
function fixture(tenantA, tenantB, sessionIdA1, sessionIdA2, sessionIdB1) {
  const base = recentBase(2); // 2 hours ago
  const at = (m) => new Date(base + m * 60_000).toISOString();
  const evs = [];
  let ord = 0;
  const push = (tenantId, sessionId, m, name, cat, opts = {}) =>
    evs.push(
      event({
        tenantId,
        sessionId,
        eventId: randomUuid(),
        seq: opts.seq ?? null,
        time: at(m),
        name,
        cat,
        anonId: opts.anon,
        userId: opts.user ?? null,
        ord: ord++,
      }),
    );
  // Tenant A, session 1: five events, anon a1, user u1 attached mid-session.
  push(tenantA, sessionIdA1, 0, "session_start", "l", { seq: 1, anon: "a1" });
  push(tenantA, sessionIdA1, 1, "page_view", "e", { seq: 2, anon: "a1" });
  push(tenantA, sessionIdA1, 2, "page_view", "e", { seq: 3, anon: "a1", user: "u1" });
  push(tenantA, sessionIdA1, 3, "click", "e", { seq: 4, anon: "a1", user: "u1" });
  push(tenantA, sessionIdA1, 20, "click", "e", { seq: 5, anon: "a1", user: "u1" });
  // Tenant A, session 2: one bucket later, anon a2, no user ever.
  push(tenantA, sessionIdA2, 30, "session_start", "l", { seq: 1, anon: "a2" });
  push(tenantA, sessionIdA2, 31, "page_view", "e", { seq: 2, anon: "a2" });
  // Tenant A: a lifecycle-only stray event in a third bucket.
  push(tenantA, sessionIdA1, 70, "page_hide", "l", { seq: 6, anon: "a1", user: "u1" });
  // Tenant B: two anons over two sessions, one user u9 in the first bucket.
  push(tenantB, sessionIdB1, 5, "session_start", "l", { seq: 1, anon: "b1", user: "u9" });
  push(tenantB, sessionIdB1, 6, "page_view", "e", { seq: 2, anon: "b1", user: "u9" });
  push(tenantB, sessionIdB1, 45, "heartbeat", "l", { seq: 3, anon: "b2" });
  return evs;
}

async function ingestFixture(pool) {
  const tenantA = randomUuid();
  const tenantB = randomUuid();
  const sessions = { A1: randomUuid(), A2: randomUuid(), B1: randomUuid() };
  const events = fixture(tenantA, tenantB, sessions.A1, sessions.A2, sessions.B1);
  const c = await pool.connect();
  try {
    const r = await ingest(c, events);
    assert.equal(r.capped.length + r.conflicts.length, 0, "all fixture events stored");
    assert.equal(r.stored.length, events.length, "every event stored");
  } finally {
    c.release();
  }
  return { tenantA, tenantB, sessions, events };
}

async function refresh(pool, cagg) {
  const c = await pool.connect();
  try {
    const t0 = Date.now();
    await c.query(`CALL refresh_continuous_aggregate('${cagg}', NULL, NULL)`);
    return Date.now() - t0;
  } finally {
    c.release();
  }
}

const ms = (n) => `${n} ms`;

test("T24a: events_15m equals the raw aggregate exactly (I5-style, both directions)", async () => {
  await withTestDb("t24a", async ({ pool }) => {
    await ingestFixture(pool);
    const took = await refresh(pool, "events_15m");
    assert.ok(took < 5 * 60_000, `refresh took ${ms(took)} (done-when: under 5 min)`);

    const c = await pool.connect();
    try {
      const res = await c.query(`
        SELECT count(*) AS drift FROM (
          (SELECT tenant_id, bucket, events, engaged_events, sessions FROM events_15m
            EXCEPT ALL
           SELECT tenant_id, time_bucket('15 minutes', time) AS bucket,
                  count(*) AS events,
                  count(*) FILTER (WHERE cat = 'e') AS engaged_events,
                  count(DISTINCT session_id) AS sessions
             FROM events GROUP BY 1, 2)
          UNION ALL
          (SELECT tenant_id, time_bucket('15 minutes', time) AS bucket,
                  count(*) AS events,
                  count(*) FILTER (WHERE cat = 'e') AS engaged_events,
                  count(DISTINCT session_id) AS sessions
             FROM events GROUP BY 1, 2
            EXCEPT ALL
           SELECT tenant_id, bucket, events, engaged_events, sessions FROM events_15m)
        ) d`);
      assert.equal(Number(res.rows[0].drift), 0, "events_15m drifted from raw");
    } finally {
      c.release();
    }
  });
});

test("T24b: identity_15m HLL finalization matches exact distinct counts", async () => {
  await withTestDb("t24b", async ({ pool }) => {
    const { tenantA } = await ingestFixture(pool);
    await refresh(pool, "identity_15m");

    const c = await pool.connect();
    try {
      // Small cardinalities are deterministic and exact in the toolkit HLL;
      // the ~1.6% label applies at scale (approximate:true in the API).
      const res = await c.query(
        `SELECT time_bucket('15 minutes', e.time) AS bucket,
                count(DISTINCT e.anon_id) AS anon_exact,
                count(DISTINCT e.user_id) AS users_exact
           FROM events e
          WHERE e.tenant_id = $1
          GROUP BY 1 ORDER BY 1`,
        [tenantA],
      );
      const hll = await c.query(
        `SELECT bucket,
                distinct_count(rollup(anon_hll)) AS anon_hll_count,
                distinct_count(rollup(users_hll)) AS users_hll_count
           FROM identity_15m
          WHERE tenant_id = $1
          GROUP BY bucket ORDER BY bucket`,
        [tenantA],
      );
      assert.equal(hll.rows.length, res.rows.length, "same bucket set");
      for (let i = 0; i < res.rows.length; i++) {
        const exact = res.rows[i];
        const approx = hll.rows[i];
        assert.equal(String(approx.bucket), String(exact.bucket), `bucket ${i} aligned`);
        assert.equal(
          Number(approx.anon_hll_count),
          Number(exact.anon_exact),
          `anon@${i}`,
        );
        // The users sketch filters NULLs (the spike verified NULL poisoning);
        // exact distinct includes only non-NULL user_ids too.
        assert.equal(
          Number(approx.users_hll_count),
          Number(exact.users_exact),
          `users@${i}`,
        );
      }
    } finally {
      c.release();
    }
  });
});

test("T24c: HLL rollup across buckets equals the overall distinct count", async () => {
  // The property P3-2's identity_daily relies on: combining per-bucket sketches
  // must equal one sketch over the whole range.
  await withTestDb("t24c", async ({ pool }) => {
    const { tenantA } = await ingestFixture(pool);
    await refresh(pool, "identity_15m");

    const c = await pool.connect();
    try {
      const res = await c.query(
        `SELECT distinct_count(rollup(anon_hll)) AS anon,
                distinct_count(rollup(users_hll)) AS users
           FROM identity_15m WHERE tenant_id = $1`,
        [tenantA],
      );
      const exact = await c.query(
        `SELECT count(DISTINCT anon_id) AS anon,
                count(DISTINCT user_id) AS users
           FROM events WHERE tenant_id = $1`,
        [tenantA],
      );
      assert.equal(Number(res.rows[0].anon), Number(exact.rows[0].anon));
      assert.equal(Number(res.rows[0].users), Number(exact.rows[0].users));
    } finally {
      c.release();
    }
  });
});

test("T24d: real-time layer serves never-refreshed buckets; late inserts wait for refresh", async () => {
  await withTestDb("t24d", async ({ pool }) => {
    const { tenantA, sessions } = await ingestFixture(pool);
    // Refresh ONLY up to 30 minutes ago: everything newer stays unmaterialized.
    const c = await pool.connect();
    try {
      await c.query(
        "CALL refresh_continuous_aggregate('events_15m', NULL, now() - INTERVAL '30 minutes')",
      );

      // 1) The live bucket (never refreshed) is served by the real-time union.
      const fresh = event({
        tenantId: tenantA,
        sessionId: sessions.A1,
        eventId: randomUuid(),
        seq: 98,
        time: new Date(Date.now()).toISOString(),
        name: "live_click",
        cat: "e",
        anonId: "a1",
        userId: "u1",
      });
      const r = await ingest(c, [fresh]);
      assert.equal(r.stored.length, 1);
      const live = await c.query(
        `SELECT events FROM events_15m WHERE tenant_id = $1
           AND bucket = time_bucket('15 minutes', now())`,
        [tenantA],
      );
      assert.equal(
        Number(live.rows[0]?.events ?? 0),
        1,
        "live bucket not served real-time",
      );

      // 2) A late insert into an ALREADY-MATERIALIZED bucket is invisible until
      //    the next refresh (watermark semantics): the policy closes the gap
      //    within the 15-minute cadence.
      const before = await c.query(
        `SELECT events FROM events_15m WHERE tenant_id = $1
           AND bucket = time_bucket('15 minutes', $2::timestamptz)`,
        [tenantA, new Date(recentBase(2)).toISOString()],
      );
      assert.equal(before.rows.length, 1);
      const late = event({
        tenantId: tenantA,
        sessionId: sessions.A1,
        eventId: randomUuid(),
        seq: 99,
        time: new Date(recentBase(2)).toISOString(),
        name: "late_click",
        cat: "e",
        anonId: "a1",
        userId: "u1",
      });
      const r2 = await ingest(c, [late]);
      assert.equal(r2.stored.length, 1);
      const stale = await c.query(
        `SELECT events FROM events_15m WHERE tenant_id = $1
           AND bucket = time_bucket('15 minutes', $2::timestamptz)`,
        [tenantA, new Date(recentBase(2)).toISOString()],
      );
      assert.equal(
        Number(stale.rows[0].events),
        Number(before.rows[0].events),
        "late insert below the watermark leaked into the unrefreshed read",
      );
      await c.query(
        `CALL refresh_continuous_aggregate('events_15m',
             time_bucket('15 minutes', $1::timestamptz),
             time_bucket('15 minutes', $1::timestamptz) + INTERVAL '15 minutes')`,
        [new Date(recentBase(2)).toISOString()],
      );
      const after = await c.query(
        `SELECT events FROM events_15m WHERE tenant_id = $1
           AND bucket = time_bucket('15 minutes', $2::timestamptz)`,
        [tenantA, new Date(recentBase(2)).toISOString()],
      );
      assert.equal(
        Number(after.rows[0].events),
        Number(before.rows[0].events) + 1,
        "late insert missing after refresh",
      );
    } finally {
      c.release();
    }
  });
});

test("T24e: refresh policies are registered with the documented cadence", async () => {
  // The hermetic harness deletes policy_* jobs AFTER migration; this test reads
  // them in that window — manual DB lifecycle instead of withTestDb.
  const dbName = `sa_test_t24e_${randomUuid().replaceAll("-", "").slice(0, 12)}`;
  const client0 = new pg.Client({ connectionString: BOOTSTRAP_URL });
  await client0.connect();
  try {
    await client0.query(`CREATE DATABASE ${dbName}`);
  } finally {
    await client0.end();
  }
  const url = await applyMigrations(dbName);
  const client = new pg.Client({ connectionString: url });
  try {
    await client.connect();
    const jobs = await client.query(
      `SELECT config, schedule_interval FROM timescaledb_information.jobs
         WHERE proc_name = 'policy_refresh_continuous_aggregate'
         ORDER BY job_id`,
    );
    assert.equal(jobs.rows.length, 2, "one refresh policy per CAGG");
    for (const row of jobs.rows) {
      assert.equal(row.schedule_interval.minutes, 15, "15-minute cadence");
      assert.equal(row.schedule_interval.hours ?? 0, 0, "15-minute cadence (not hourly)");
      assert.equal(row.config.end_offset, "00:15:00", "live bucket stays out");
      assert.equal(
        row.config.start_offset,
        "5 days",
        "window covers the rollup days 2–4",
      );
    }
    const aggs = await client.query(
      `SELECT view_name, materialized_only FROM timescaledb_information.continuous_aggregates
         WHERE view_name IN ('events_15m', 'identity_15m') ORDER BY 1`,
    );
    assert.deepEqual(
      aggs.rows.map((r) => [r.view_name, r.materialized_only]),
      [
        ["events_15m", false],
        ["identity_15m", false],
      ],
      "both CAGGs real-time (materialized_only=false)",
    );
    for (const cagg of ["events_15m", "identity_15m"]) {
      const pol = await client.query(
        `SELECT 1 FROM timescaledb_information.jobs
           WHERE hypertable_name = $1 AND proc_name LIKE 'policy_retention%'`,
        [cagg],
      );
      assert.equal(pol.rowCount, 1, `${cagg} has a 90-day retention policy`);
    }
  } finally {
    await client.end();
    await removePolicyJobs(url); // keep the DB hermetic for whatever remains
    const dropper = new pg.Client({ connectionString: BOOTSTRAP_URL });
    try {
      await dropper.connect();
      await dropper.query(`DROP DATABASE ${dbName}`);
    } finally {
      await dropper.end();
    }
  }
});

test("T24f: invariants still hold with the CAGGs in place", async () => {
  await withTestDb("t24f", async ({ pool }) => {
    await ingestFixture(pool);
    await refresh(pool, "events_15m");
    await refresh(pool, "identity_15m");
    const c = await pool.connect();
    try {
      for (const inv of [
        "i1_no_duplicate_events",
        "i6_dedupe_ledger",
        "i8_no_orphan_events",
      ]) {
        await assertInvariant(c, inv);
      }
    } finally {
      c.release();
    }
  });
});
