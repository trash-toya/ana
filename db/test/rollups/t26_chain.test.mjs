// T26a/b — P3-5 done-when part 1: "nightly reconcile zero drift". The nightly
// safety net (reconcile_session_stats, 26–50 h) must stay silent on the P3
// chain, and the daily rollup must equal an independent recompute from
// sessions_history — zero drift at every layer (stats → CAGG → daily).
//
// Two anchors (no fake clock, per the harness conventions):
//   * T26a: a late partial at now-30 h — always inside the reconcile window;
//     the daily assertion is conditional because [now-30h] may be "yesterday"
//     (local), which rollup_daily never writes.
//   * T26b: an anchor at now-60 h — always inside the rollup window
//     [current_date-4, current_date-1), so the daily rows must exist and match.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { event, forceArchive, ingest, withTestDb } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const MIN = 60_000;
const HOUR = 3_600_000;
const iso = (t) => new Date(t).toISOString();

function collectWarnings(client) {
  const warnings = [];
  client.on("notice", (n) => {
    if (n.severity === "WARNING") warnings.push(n.message);
  });
  return warnings;
}

/** Seed one tenant, guards at the anchor for three sessions, real events. */
async function seedChain(pool, anchorMs) {
  const tenant = randomUuid();
  const s1 = randomUuid();
  const s2 = randomUuid();
  const s3 = randomUuid();
  const c = await pool.connect();
  try {
    await c.query(
      `INSERT INTO session_guard (tenant_id, session_id, home_day, total_events, created_at)
       VALUES ($1, $2, ($3::timestamptz AT TIME ZONE 'UTC')::date, 0, $3),
              ($1, $4, ($3::timestamptz AT TIME ZONE 'UTC')::date, 0, $3),
              ($1, $5, ($3::timestamptz AT TIME ZONE 'UTC')::date, 0, $3)`,
      [tenant, s1, iso(anchorMs), s2, s3],
    );
    const evs = [
      // s1: two engagement events (not bounced), user attached.
      event({
        tenantId: tenant,
        sessionId: s1,
        eventId: randomUuid(),
        seq: 1,
        time: iso(anchorMs),
        name: "page_view",
        anonId: "a1",
        userId: "u1",
      }),
      event({
        tenantId: tenant,
        sessionId: s1,
        eventId: randomUuid(),
        seq: 2,
        time: iso(anchorMs + 5 * MIN),
        name: "click",
        anonId: "a1",
        userId: "u1",
      }),
      // s2: one engagement event (bounced), no user.
      event({
        tenantId: tenant,
        sessionId: s2,
        eventId: randomUuid(),
        seq: 1,
        time: iso(anchorMs + 2 * MIN),
        name: "page_view",
        anonId: "a2",
      }),
    ];
    const r = await ingest(c, evs);
    assert.equal(r.stored.length, 3, "all chain events stored");
  } finally {
    c.release();
  }
  return { tenant, s1, s2, s3 };
}

/** Late partial: one more event for an already-archived session, then re-archive. */
test("T26b: daily rollup equals an independent recompute from sessions_history", async () => {
  await withTestDb("t26b", async ({ pool }) => {
    const anchor = Date.now() - 60 * HOUR; // always inside [cd-4, cd-1)
    const { tenant, s3 } = await seedChain(pool, anchor);
    await forceArchive(pool);
    // s3's first (and only) event lands via the merge path: guard pre-created,
    // event arrives after the first archive (the late-partial lifecycle).
    const c = await pool.connect();
    try {
      const r = await ingest(c, [
        event({
          tenantId: tenant,
          sessionId: s3,
          eventId: randomUuid(),
          seq: 9,
          time: iso(anchor + 20 * MIN),
          name: "click",
          anonId: "a3",
          userId: "u3",
        }),
      ]);
      assert.equal(r.stored.length, 1);
      await forceArchive(pool);

      // 1) Nightly reconcile: silent on the freshly maintained chain.
      const warnings = collectWarnings(c);
      await c.query("CALL reconcile_session_stats(NULL, NULL)");
      assert.equal(
        warnings.length,
        0,
        `reconcile must be silent: ${JSON.stringify(warnings)}`,
      );

      // 2) CAGG layer: events_15m equals the raw aggregate over the anchor window.
      await c.query(
        `CALL refresh_continuous_aggregate('events_15m',
             $1::timestamptz - INTERVAL '2 hours', $1::timestamptz + INTERVAL '2 hours')`,
        [iso(anchor)],
      );
      await c.query(
        `CALL refresh_continuous_aggregate('identity_15m',
             $1::timestamptz - INTERVAL '2 hours', $1::timestamptz + INTERVAL '2 hours')`,
        [iso(anchor)],
      );
      const caggDrift = await c.query(
        `SELECT count(*) AS n FROM (
            (SELECT bucket, events, engaged_events, sessions FROM events_15m
              WHERE tenant_id = $1
               AND bucket >= $2::timestamptz - INTERVAL '2 hours'
               AND bucket <  $2::timestamptz + INTERVAL '2 hours'
              EXCEPT ALL
             SELECT time_bucket('15 minutes', time), count(*),
                    count(*) FILTER (WHERE cat = 'e'), count(DISTINCT session_id)
               FROM events WHERE tenant_id = $1
                AND time >= $2::timestamptz - INTERVAL '2 hours'
                AND time <  $2::timestamptz + INTERVAL '2 hours'
              GROUP BY 1)
          UNION ALL
            (SELECT time_bucket('15 minutes', time), count(*),
                    count(*) FILTER (WHERE cat = 'e'), count(DISTINCT session_id)
               FROM events WHERE tenant_id = $1
                AND time >= $2::timestamptz - INTERVAL '2 hours'
                AND time <  $2::timestamptz + INTERVAL '2 hours'
              GROUP BY 1
              EXCEPT ALL
             SELECT bucket, events, engaged_events, sessions FROM events_15m
              WHERE tenant_id = $1
               AND bucket >= $2::timestamptz - INTERVAL '2 hours'
               AND bucket <  $2::timestamptz + INTERVAL '2 hours')) d`,
        [tenant, iso(anchor)],
      );
      assert.equal(Number(caggDrift.rows[0].n), 0, "events_15m drifted from raw");

      // 3) Daily layer: rollup, then compare against an independent recompute
      //    from sessions_history grouped by UTC day (no tenant_settings → UTC).
      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);
      const dailyDrift = await c.query(
        `WITH truth AS (
            SELECT tenant_id,
                   (time_bucket('1 day', started_at, 'UTC') AT TIME ZONE 'UTC')::date AS day,
                   count(*) AS sessions,
                   count(*) FILTER (WHERE event_count <= 1) AS bounced,
                   sum(extract(epoch FROM last_seen_at - started_at)) AS total_duration_s,
                   sum(heartbeats) AS heartbeat_s,
                   sum(event_count) AS events
              FROM sessions_history
             WHERE tenant_id = $1
             GROUP BY 1, 2
         )
         SELECT count(*) AS n FROM (
            (SELECT tenant_id, local_day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM session_stats_daily WHERE tenant_id = $1
              EXCEPT ALL
             SELECT tenant_id, day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM truth)
          UNION ALL
            (SELECT tenant_id, day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM truth
              EXCEPT ALL
             SELECT tenant_id, local_day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM session_stats_daily WHERE tenant_id = $1)
         ) d`,
        [tenant],
      );
      assert.equal(
        Number(dailyDrift.rows[0].n),
        0,
        "session_stats_daily drifted from history",
      );

      // 4) Identity daily: exact distinct counts through the HLL chain.
      const identDrift = await c.query(
        `WITH truth AS (
            SELECT tenant_id,
                   (time_bucket('1 day', time, 'UTC') AT TIME ZONE 'UTC')::date AS day,
                   count(DISTINCT anon_id) AS anon,
                   count(DISTINCT user_id) AS users
              FROM events WHERE tenant_id = $1 GROUP BY 1, 2
         )
         SELECT count(*) AS n FROM (
            (SELECT tenant_id, local_day, distinct_count(rollup(anon_hll)) AS anon,
                    distinct_count(rollup(users_hll)) AS users
               FROM identity_daily WHERE tenant_id = $1 GROUP BY 1, 2
              EXCEPT ALL
             SELECT tenant_id, day, anon, users FROM truth)
          UNION ALL
            (SELECT tenant_id, day, anon, users FROM truth
              EXCEPT ALL
             SELECT tenant_id, local_day, distinct_count(rollup(anon_hll)),
                    distinct_count(rollup(users_hll))
               FROM identity_daily WHERE tenant_id = $1 GROUP BY 1, 2)) d`,
        [tenant],
      );
      assert.equal(Number(identDrift.rows[0].n), 0, "identity_daily drifted from events");

      // 5) Self-healing: corrupt the daily rows; the next nightly run rewrites them.
      await c.query(
        `UPDATE session_stats_daily SET sessions = sessions + 5 WHERE tenant_id = $1`,
        [tenant],
      );
      await c.query(`UPDATE identity_daily SET users_hll = NULL WHERE tenant_id = $1`, [
        tenant,
      ]);
      await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);
      const healed = await c.query(
        `WITH truth AS (
            SELECT tenant_id,
                   (time_bucket('1 day', started_at, 'UTC') AT TIME ZONE 'UTC')::date AS day,
                   count(*) AS sessions,
                   count(*) FILTER (WHERE event_count <= 1) AS bounced,
                   sum(extract(epoch FROM last_seen_at - started_at)) AS total_duration_s,
                   sum(heartbeats) AS heartbeat_s,
                   sum(event_count) AS events
              FROM sessions_history
             WHERE tenant_id = $1
             GROUP BY 1, 2
         )
         SELECT count(*) AS n FROM (
            (SELECT tenant_id, local_day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM session_stats_daily WHERE tenant_id = $1
              EXCEPT ALL
             SELECT tenant_id, day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM truth)
          UNION ALL
            (SELECT tenant_id, day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM truth
              EXCEPT ALL
             SELECT tenant_id, local_day, sessions, bounced, total_duration_s,
                    heartbeat_s, events FROM session_stats_daily WHERE tenant_id = $1)) d`,
        [tenant],
      );
      assert.equal(Number(healed.rows[0].n), 0, "rollup_daily did not repair the drift");
      const usersBack = await c.query(
        `SELECT distinct_count(rollup(users_hll)) AS u FROM identity_daily WHERE tenant_id = $1`,
        [tenant],
      );
      assert.equal(Number(usersBack.rows[0].u), 2, "identity_daily HLL not rewritten");
    } finally {
      c.release();
    }
  });
});

test("T26a: late partial merges through reconcile without drift (26–50 h window)", async () => {
  await withTestDb("t26a", async ({ pool }) => {
    const anchor = Date.now() - 30 * HOUR; // always inside the reconcile window
    const { tenant, s1 } = await seedChain(pool, anchor);
    await forceArchive(pool); // first archive

    const c = await pool.connect();
    try {
      // A late event for the archived session: new active row → merge on archive.
      const r = await ingest(c, [
        event({
          tenantId: tenant,
          sessionId: s1,
          eventId: randomUuid(),
          seq: 9,
          time: iso(anchor + 20 * MIN),
          name: "click",
          anonId: "a1",
          userId: "u1",
        }),
      ]);
      assert.equal(r.stored.length, 1);
      await forceArchive(pool);

      // The merge landed as exact deltas: reconcile must be silent.
      const warnings = collectWarnings(c);
      await c.query("CALL reconcile_session_stats(NULL, NULL)");
      assert.equal(
        warnings.length,
        0,
        `reconcile must be silent: ${JSON.stringify(warnings)}`,
      );

      // And the merged history row carries the late event.
      const hist = await c.query(
        `SELECT event_count, total_events, last_seen_at FROM sessions_history
          WHERE tenant_id = $1 AND session_id = $2`,
        [tenant, s1],
      );
      assert.equal(hist.rows.length, 1);
      assert.equal(
        Number(hist.rows[0].event_count),
        3,
        "late event missing from history",
      );
      assert.equal(Number(hist.rows[0].total_events), 3);
      assert.equal(
        new Date(hist.rows[0].last_seen_at).toISOString(),
        iso(anchor + 20 * MIN),
      );
    } finally {
      c.release();
    }
  });
});
