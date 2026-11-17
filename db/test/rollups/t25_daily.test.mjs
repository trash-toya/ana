// T25 — the P3-2 daily rollup (db/migrations/0013_daily.sql).
// Done-when (08-execution.md): "Partial local days never written; UTC+5:30 and
// UTC+5:45 correct". rollup_daily rewrites local days [current_date-4,
// current_date-1) every night for three nights; yesterday/today stay out
// because their local day is still open (the live slice serves them).
//
// Two strategies, per the harness conventions (no fake clock):
//   * A: synthetic session_stats_15m rows — the tz/day-boundary unit test.
//   * B: a real ingest anchored 3 days back (guards pre-created so I3 holds),
//     force-archive, CAGG refresh, then rollup — the identity_daily equality.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { event, forceArchive, ingest, withTestDb } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const DAY = 86_400_000;

/** Anchor: 3 days ago at 18:14 UTC (the bucket pair below straddles both tzs). */
function anchor3d() {
  const d = new Date(Date.now() - 3 * DAY);
  d.setUTCHours(18, 14, 0, 0);
  return d.getTime();
}

test("T25a: local-day assignment is correct for UTC+5:30 and UTC+5:45", async () => {
  await withTestDb("t25a", async ({ pool }) => {
    const kolkata = randomUuid(); // +5:30 — local midnight at 18:30 UTC
    const kathmandu = randomUuid(); // +5:45 — local midnight at 18:15 UTC
    const c = await pool.connect();
    try {
      await c.query(
        `INSERT INTO tenants (tenant_id, name) VALUES ($1, 'kolkata'), ($2, 'kathmandu')`,
        [kolkata, kathmandu],
      );
      await c.query(
        `INSERT INTO tenant_settings (tenant_id, tz) VALUES ($1, 'Asia/Kolkata'), ($2, 'Asia/Kathmandu')`,
        [kolkata, kathmandu],
      );

      // Synthetic 15m buckets around the two local midnights:
      //   18:14 UTC — Kolkata 23:44 (same local day), Kathmandu 23:59 (same day)
      //   18:15 UTC — Kolkata 23:45 (same),          Kathmandu 00:00 (NEXT day)
      //   18:30 UTC — Kolkata 00:00 (NEXT day),      Kathmandu 00:15 (NEXT day)
      //   18:45 UTC — Kolkata 00:15 (NEXT),          Kathmandu 00:30 (NEXT)
      // A "day" for both tenants is [D, D+1); the rollup window covers
      // [current_date-4, current_date-1), and anchor is current_date-3 — every
      // mapped day (D-3 or D-2) is inside the window regardless of run time.
      const t0 = anchor3d();
      const buckets = [0, 1, 16, 31].map((m) => new Date(t0 + m * 60_000).toISOString());
      const vals = [];
      for (const tenant of [kolkata, kathmandu]) {
        for (const b of buckets) {
          // One session per bucket, one engagement event: ec_1 = 1 → bounced = 1;
          // duration 30 s; one heartbeat; one event.
          vals.push(`('${tenant}', '${b}'::timestamptz, 1, 0, 1, 0, 0, 30, 1, 1)`);
        }
      }
      await c.query(
        `INSERT INTO session_stats_15m
             (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
              total_duration_s, heartbeat_s, events)
         VALUES ${vals.join(",")}`,
      );

      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);

      const daily = await c.query(
        `SELECT tenant_id, local_day::text AS local_day, sessions, bounced,
               total_duration_s, heartbeat_s, events
           FROM session_stats_daily ORDER BY tenant_id, local_day`,
      );
      // Expected: every tenant has exactly TWO local days — D-3 (buckets
      // before its local midnight) and D-2 (buckets after).
      assert.equal(
        daily.rows.length,
        4,
        `want 4 rows, got ${JSON.stringify(daily.rows)}`,
      );
      const byTenant = new Map();
      for (const r of daily.rows) {
        if (!byTenant.has(r.tenant_id)) byTenant.set(r.tenant_id, []);
        byTenant.get(r.tenant_id).push(r);
      }
      const daysOf = (tenant) =>
        byTenant
          .get(tenant)
          .map((r) => String(r.local_day))
          .sort();
      const d3 = new Date(t0).toISOString().slice(0, 10); // anchor's UTC date = current_date-3
      const d2 = new Date(t0 + DAY).toISOString().slice(0, 10); // current_date-2
      assert.deepEqual(
        daysOf(kolkata),
        [d3, d2].sort(),
        `Kolkata days: got ${daysOf(kolkata)}, want [${d3}, ${d2}]`,
      );
      assert.deepEqual(
        daysOf(kathmandu),
        [d3, d2].sort(),
        `Kathmandu days: got ${daysOf(kathmandu)}, want [${d3}, ${d2}]`,
      );

      // The split POINT differs: Kolkata puts the 18:15 bucket in d3 (23:45
      // local), Kathmandu puts it in d2 (00:00 local).
      const split = (tenant) => {
        const row = byTenant.get(tenant).find((r) => String(r.local_day) === d3);
        return Number(row.sessions);
      };
      assert.equal(split(kolkata), 2, "Kolkata d-3 should hold 18:14+18:15");
      assert.equal(split(kathmandu), 1, "Kathmandu d-3 should hold only 18:14");

      // Totals: two sessions of one engagement event each per tenant.
      const totals = byTenant.get(kolkata).reduce((a, r) => a + Number(r.sessions), 0);
      assert.equal(totals, 4, "no bucket lost between the two local days");
      const bounceTotal = byTenant
        .get(kathmandu)
        .reduce((a, r) => a + Number(r.bounced), 0);
      assert.equal(bounceTotal, 4, "ec_0 + ec_1 = bounced, all four bounced");
      const dur = byTenant
        .get(kolkata)
        .reduce((a, r) => a + Number(r.total_duration_s), 0);
      assert.equal(dur, 120, "duration summed exactly (4 x 30 s, D2 precision)");
    } finally {
      c.release();
    }
  });
});

test("T25b: partial local days are never written; a late bucket rewrites its day", async () => {
  await withTestDb("t25b", async ({ pool }) => {
    const tenant = randomUuid();
    const c = await pool.connect();
    try {
      await c.query(`INSERT INTO tenants (tenant_id, name) VALUES ($1, 't')`, [tenant]);
      // No tenant_settings row: the COALESCE defaults to UTC.

      // Buckets in the rollup window AND today/yesterday (partial days).
      const t0 = anchor3d();
      const inWindow = new Date(t0 + 60_000).toISOString();
      const today = new Date().toISOString();
      const yesterday = new Date(Date.now() - DAY).toISOString();
      await c.query(
        `INSERT INTO session_stats_15m
             (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
              total_duration_s, heartbeat_s, events)
         VALUES ('${tenant}', '${inWindow}'::timestamptz, 1, 0, 0, 0, 0, 10, 0, 1),
                ('${tenant}', '${yesterday}'::timestamptz, 1, 0, 0, 0, 0, 10, 0, 1),
                ('${tenant}', '${today}'::timestamptz, 1, 0, 0, 0, 0, 10, 0, 1)`,
      );

      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);

      const daily = await c.query(
        `SELECT local_day::text AS local_day, sessions FROM session_stats_daily WHERE tenant_id = $1 ORDER BY 1`,
        [tenant],
      );
      const todayStr = today.slice(0, 10);
      const ydayStr = yesterday.slice(0, 10);
      assert.equal(
        daily.rows.some((r) => [todayStr, ydayStr].includes(String(r.local_day))),
        false,
        "a partial local day (yesterday/today) was written",
      );
      assert.equal(
        daily.rows.length,
        1,
        "only the in-window bucket produced a daily row",
      );

      // Idempotent rewrite: same sources → same table. Then a late bucket
      // arriving into the window is picked up by the next night's rewrite.
      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);
      const again = await c.query(
        `SELECT local_day::text AS local_day, sessions FROM session_stats_daily WHERE tenant_id = $1`,
        [tenant],
      );
      assert.deepEqual(again.rows, daily.rows, "second run changed the table");

      await c.query(
        `INSERT INTO session_stats_15m
             (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
              total_duration_s, heartbeat_s, events)
         VALUES ('${tenant}', '${inWindow}'::timestamptz, 1, 0, 0, 0, 0, 5, 0, 1)
         ON CONFLICT (tenant_id, bucket) DO UPDATE
           SET sessions = session_stats_15m.sessions + 1,
               total_duration_s = session_stats_15m.total_duration_s + 5,
               events = session_stats_15m.events + 1`,
      );
      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);
      const after = await c.query(
        `SELECT local_day::text AS local_day, sessions FROM session_stats_daily WHERE tenant_id = $1`,
        [tenant],
      );
      assert.equal(
        Number(after.rows[0].sessions),
        2,
        "a late 15m bucket was not merged by the rewrite",
      );
    } finally {
      c.release();
    }
  });
});

test("T25c: identity_daily rolls HLL states; distinct counts compose", async () => {
  await withTestDb("t25c", async ({ pool }) => {
    const tenant = randomUuid();
    const sessionA = randomUuid();
    const sessionB = randomUuid();
    const c = await pool.connect();
    try {
      // Pre-create guards 3 days back so ingest at that time keeps I3 intact
      // (events must be >= guard.created_at - 24h).
      await c.query(`INSERT INTO tenants (tenant_id, name) VALUES ($1, 't')`, [tenant]);
      const started = new Date(anchor3d());
      await c.query(
        `INSERT INTO session_guard (tenant_id, session_id, home_day, total_events, created_at)
         VALUES ($1, $2, ($3::timestamptz AT TIME ZONE 'UTC')::date, 0, $3),
                ($1, $4, ($3::timestamptz AT TIME ZONE 'UTC')::date, 0, $3)`,
        [tenant, sessionA, started.toISOString(), sessionB],
      );

      // Session A: anon a1 + user u1, in the anchor bucket. Session B: anon a2,
      // no user, in the NEXT UTC bucket (18:31 — next local day for Kolkata,
      // still D-3 for Kathmandu-less UTC default... tenant has no settings row → UTC).
      const evs = [
        event({
          tenantId: tenant,
          sessionId: sessionA,
          eventId: randomUuid(),
          seq: 1,
          time: new Date(anchor3d()).toISOString(),
          name: "page_view",
          cat: "e",
          anonId: "a1",
          userId: "u1",
        }),
        event({
          tenantId: tenant,
          sessionId: sessionA,
          eventId: randomUuid(),
          seq: 2,
          time: new Date(anchor3d() + 30_000).toISOString(),
          name: "click",
          cat: "e",
          anonId: "a1",
          userId: "u1",
        }),
        event({
          tenantId: tenant,
          sessionId: sessionB,
          eventId: randomUuid(),
          seq: 1,
          time: new Date(anchor3d() + 17 * 60_000).toISOString(),
          name: "page_view",
          cat: "e",
          anonId: "a2",
        }),
      ];
      const r = await ingest(c, evs);
      assert.equal(r.capped.length + r.conflicts.length, 0, "all stored");
      await forceArchive(pool);

      // Materialize the CAGGs over the consumed range, then roll up.
      await c.query(
        `CALL refresh_continuous_aggregate('identity_15m',
             (current_date - 4)::timestamp AT TIME ZONE 'UTC' - INTERVAL '14 hours',
             (current_date - 1)::timestamp AT TIME ZONE 'UTC' + INTERVAL '14 hours')`,
      );
      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);

      // The daily sketch must equal the per-day rollup of identity_15m...
      const dailyVs15m = await c.query(
        `SELECT count(*) AS drift FROM (
            (SELECT tenant_id, local_day, distinct_count(rollup(anon_hll)) AS anon,
                    distinct_count(rollup(users_hll)) AS users
               FROM identity_daily WHERE tenant_id = $1 GROUP BY 1, 2
              EXCEPT ALL
             SELECT tenant_id, (time_bucket(INTERVAL '1 day', bucket, 'UTC'))::date,
                    distinct_count(rollup(anon_hll)), distinct_count(rollup(users_hll))
               FROM identity_15m WHERE tenant_id = $1 GROUP BY 1, 2)
          UNION ALL
            (SELECT tenant_id, (time_bucket(INTERVAL '1 day', bucket, 'UTC'))::date,
                    distinct_count(rollup(anon_hll)), distinct_count(rollup(users_hll))
               FROM identity_15m WHERE tenant_id = $1 GROUP BY 1, 2
              EXCEPT ALL
             SELECT tenant_id, local_day, distinct_count(rollup(anon_hll)),
                    distinct_count(rollup(users_hll))
               FROM identity_daily WHERE tenant_id = $1 GROUP BY 1, 2)) d`,
        [tenant],
      );
      assert.equal(Number(dailyVs15m.rows[0].drift), 0, "identity_daily drifted");

      // ...and equal the exact counts over the raw events.
      const exact = await c.query(
        `SELECT (time_bucket(INTERVAL '1 day', time, 'UTC')
                  AT TIME ZONE 'UTC')::date::text AS day,
                count(DISTINCT anon_id) AS anon, count(DISTINCT user_id) AS users
           FROM events WHERE tenant_id = $1 GROUP BY 1 ORDER BY 1`,
        [tenant],
      );
      const daily = await c.query(
        `SELECT local_day::text AS local_day, distinct_count(rollup(anon_hll)) AS anon,
                distinct_count(rollup(users_hll)) AS users
           FROM identity_daily WHERE tenant_id = $1 GROUP BY local_day ORDER BY local_day`,
        [tenant],
      );
      assert.equal(daily.rows.length, exact.rows.length);
      for (let i = 0; i < exact.rows.length; i++) {
        assert.equal(String(daily.rows[i].local_day), String(exact.rows[i].day));
        assert.equal(Number(daily.rows[i].anon), Number(exact.rows[i].anon));
        assert.equal(Number(daily.rows[i].users ?? 0), Number(exact.rows[i].users));
      }
    } finally {
      c.release();
    }
  });
});
