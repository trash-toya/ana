// T27 — P3-7 freshness gate (Notion §22): "scheduled aggregation keeps
// dashboards within 1 h under load and after a backlog". The freshness state
// itself is exposed to dashboards by GET /v1/freshness (services/dashboard
// tests); here the DB-side contract is measured:
//
//   T27a: a 90-minute backlog (six 15-min buckets of unmaterialized events) is
//         replayed by ONE refresh_continuous_aggregate call far inside the
//         1-hour gate, and the watermark advances past the backlog end.
//   T27b: the watermark contract after a backlog: buckets ABOVE the watermark
//         are served live (T24d), so the gate applies to the materialization,
//         not the newest data — a late insert into a materialized bucket is
//         bounded by the 15-min cadence, not by the backlog size.
//   T27c: the watermark helper (0016) reports NULL for a never-refreshed CAGG
//         (normalized from the placeholder BC timestamp) — "empty" not "stale".
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { event, ingest, withTestDb } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const MIN = 60_000;
const iso = (t) => new Date(t).toISOString();

test("T27a: 90-minute backlog replayed by one refresh well inside the 1-hour gate", async () => {
  await withTestDb("t27a", async ({ pool }) => {
    const tenant = randomUuid();
    const c = await pool.connect();
    try {
      // Six 15-minute buckets spanning [now-110 min, now-20 min): a backlog
      // three policy runs deep (15-min cadence).
      const start = Date.now() - 110 * MIN;
      const evs = [];
      for (let m = 0; m < 90; m += 15) {
        evs.push(
          event({
            tenantId: tenant,
            sessionId: randomUuid(),
            eventId: randomUuid(),
            seq: 1,
            time: iso(start + m * MIN),
            name: "page_view",
            anonId: "a1",
          }),
        );
      }
      const r = await ingest(c, evs);
      assert.equal(r.stored.length, 6, "backlog seeded");

      // The backlog replay: ONE refresh over the whole range.
      const t0 = Date.now();
      await c.query(
        `CALL refresh_continuous_aggregate('events_15m',
             $1::timestamptz - INTERVAL '15 minutes',
             $1::timestamptz + INTERVAL '95 minutes')`,
        [iso(start)],
      );
      const took = Date.now() - t0;
      assert.ok(
        took < 60_000,
        `backlog replay took ${took} ms (gate: 1 h; 15-min runs replay it in 3)`,
      );

      // The watermark advanced past every backlog bucket: each 15-min bucket
      // holding a backlog event is fully materialized (a bucket is only
      // materialized once it ends, so compare against the events' buckets).
      const unmaterialized = await c.query(
        `SELECT count(*)::int AS n FROM (
            SELECT DISTINCT time_bucket('15 minutes', time) AS b FROM events
             WHERE tenant_id = $1
         ) eb
          LEFT JOIN events_15m ON bucket = eb.b AND events_15m.tenant_id = $1
         WHERE bucket IS NULL`,
        [tenant],
      );
      const wm = await c.query(`SELECT cagg_watermark_ts('events_15m') AS w`);
      assert.equal(
        unmaterialized.rows[0].n,
        0,
        `backlog buckets not materialized (watermark ${wm.rows[0].w})`,
      );

      // Materialized rows match the raw aggregate over the replayed range.
      const drift = await c.query(
        `SELECT count(*) AS n FROM (
            (SELECT bucket, events FROM events_15m WHERE tenant_id = $1
              EXCEPT ALL
             SELECT time_bucket('15 minutes', time), count(*) FROM events
              WHERE tenant_id = $1 GROUP BY 1)
          UNION ALL
            (SELECT time_bucket('15 minutes', time), count(*) FROM events
              WHERE tenant_id = $1 GROUP BY 1
              EXCEPT ALL
             SELECT bucket, events FROM events_15m WHERE tenant_id = $1)) d`,
        [tenant],
      );
      assert.equal(Number(drift.rows[0].n), 0, "CAGG drifted after backlog replay");
    } finally {
      c.release();
    }
  });
});

test("T27b: the gate bounds materialization, not the live data (watermark semantics)", async () => {
  await withTestDb("t27b", async ({ pool }) => {
    const tenant = randomUuid();
    const s1 = randomUuid();
    const c = await pool.connect();
    try {
      // Materialize ONLY up to 2 hours ago: a 2-hour-deep watermark (> gate).
      const start = Date.now() - 150 * MIN;
      await ingest(c, [
        event({
          tenantId: tenant,
          sessionId: s1,
          eventId: randomUuid(),
          seq: 1,
          time: iso(start),
          name: "page_view",
          anonId: "a1",
        }),
      ]);
      await c.query(
        `CALL refresh_continuous_aggregate('events_15m',
             $1::timestamptz - INTERVAL '15 minutes', $1::timestamptz + INTERVAL '15 minutes')`,
        [iso(start)],
      );

      // Watermark is 2 h behind → beyond the 1 h gate on the materialization.
      const wm = await c.query(`SELECT cagg_watermark_ts('events_15m') AS w, now() AS n`);
      const behind = Date.parse(wm.rows[0].n) - Date.parse(wm.rows[0].w);
      assert.ok(
        behind > 3_600_000,
        `expected gate-violating watermark, behind=${behind}`,
      );

      // Yet a NEW event right now is fully visible (live layer, T24d): the
      // dashboard's newest data is never gated by the backlog. The live row
      // sits in a bucket ABOVE the watermark, served by the union without any
      // refresh (anchor the bucket to the event's own time — now() may cross a
      // 15-minute boundary between ingest and query).
      const fresh = await ingest(c, [
        event({
          tenantId: tenant,
          sessionId: s1,
          eventId: randomUuid(),
          seq: 2,
          time: iso(Date.now() - MIN),
          name: "click",
          anonId: "a1",
        }),
      ]);
      assert.equal(fresh.stored.length, 1);
      const live = await c.query(
        `SELECT count(*)::int AS n
           FROM events_15m
          WHERE tenant_id = $1
            AND bucket >= cagg_watermark_ts('events_15m')`,
        [tenant],
      );
      assert.equal(live.rows[0].n, 1, "live bucket above the watermark must be served");
    } finally {
      c.release();
    }
  });
});

test("T27c: watermark helper is NULL for a never-refreshed CAGG (empty, not stale)", async () => {
  await withTestDb("t27c", async ({ pool }) => {
    const c = await pool.connect();
    try {
      // Fresh DB: 0012 created the CAGGs WITH NO DATA and never refreshed them.
      const wm = await c.query(
        `SELECT cagg_watermark_ts('events_15m') AS w,
                cagg_watermark_ts('identity_15m') AS wi`,
      );
      assert.equal(wm.rows[0].w, null, "placeholder BC timestamp leaked through");
      assert.equal(wm.rows[0].wi, null);
    } finally {
      c.release();
    }
  });
});
