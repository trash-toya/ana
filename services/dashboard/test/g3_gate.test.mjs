// G3 gate (08-execution.md): "dashboards read only rollups + live slice; bounce
// rate non-zero and plausible; approximate labels applied to HLL metrics."
//
// The P3-3 endpoint tests already pin shapes, D14/D18 and approximate labels;
// this suite is the gate-level behavioral proof:
//   G3: headline metrics (hourly/daily) do NOT fall back to raw events — with
//       events in the raw hypertable but no archived stats yet, the hours array
//       is empty while the live slice reports the active sessions; after the
//       archive+rollup pass the same range is served entirely from the rollup.
//       Bounce rate is then non-zero and plausible (bounded, matches the seeded
//       pattern, never above 1 or below 0).
import { test } from "vitest";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import request from "supertest";

import { createApp } from "../dist/app.js";
import { event, forceArchive, ingest, withTestDb } from "../../../db/test/harness.mjs";
import { seedTenantWithKeys } from "./helpers.mjs";

const MIN = 60_000;
const HOUR = 3_600_000;
const iso = (t) => new Date(t).toISOString();

test("G3: headline metrics read rollups + live only — no raw-event fallback; bounce plausible", async () => {
  await withTestDb("dash_g3", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const app = createApp({ pool });
    const auth = { "X-API-Key": dashboardKey };

    // 5 sessions seen in the last 3 minutes: 3 bounced (one event), 2 engaged.
    // Raw events exist in the hypertable from the moment ingest_batch stores
    // them; the sessions stay inside the live slice's 5-minute window.
    const s = Array.from({ length: 5 }, () => randomUUID());
    const base = Date.now() - 3 * MIN;
    const evs = [];
    for (let i = 0; i < 5; i++) {
      const bounced = i < 3;
      evs.push(
        event({
          tenantId,
          sessionId: s[i],
          eventId: randomUUID(),
          seq: 1,
          time: iso(base + i * 10_000),
          name: "page_view",
          anonId: `a${i}`,
        }),
      );
      if (!bounced) {
        evs.push(
          event({
            tenantId,
            sessionId: s[i],
            eventId: randomUUID(),
            seq: 2,
            time: iso(base + i * 10_000 + 5_000),
            name: "click",
            anonId: `a${i}`,
          }),
        );
      }
    }
    const ingested = await ingest(pool, evs);
    assert.equal(
      ingested.stored.length,
      7,
      `all stored: ${JSON.stringify({ dups: ingested.dups, capped: ingested.capped, conflicts: ingested.conflicts })}`,
    );

    // BEFORE the archive: raw events exist, session_stats_15m does not.
    const c = await pool.connect();
    try {
      const raw = await c.query(
        `SELECT count(*)::int AS n FROM events WHERE tenant_id = $1`,
        [tenantId],
      );
      assert.equal(raw.rows[0].n, 7, "raw events are stored");
      const stats = await c.query(
        `SELECT count(*)::int AS n FROM session_stats_15m WHERE tenant_id = $1`,
        [tenantId],
      );
      assert.equal(stats.rows[0].n, 0, "no rollup rows yet");
    } finally {
      c.release();
    }

    const from = iso(Date.now() - 2 * HOUR);
    const to = iso(Date.now() + HOUR);
    const before = await request(app)
      .get(`/v1/metrics/hourly?from=${from}&to=${to}`)
      .set(auth);
    assert.equal(before.status, 200);
    assert.deepEqual(
      before.body.hours,
      [],
      "no stats-backed hours (no raw-event fallback)",
    );
    assert.equal(
      before.body.live.sessions,
      5,
      "the live slice still reports the active sessions",
    );
    assert.equal(before.body.live.events, 7);
    assert.equal(before.body.live.live, true);

    // The daily headline is also rollup-backed: no days yet (no zero-filler).
    const dailyBefore = await request(app)
      .get(`/v1/metrics/daily?from=${from}&to=${to}`)
      .set(auth);
    assert.equal(dailyBefore.status, 200);
    assert.deepEqual(dailyBefore.body.days, []);

    // The archive + rollup pass: the same range is now served from rollups.
    await forceArchive(pool);
    const c2 = await pool.connect();
    try {
      await c2.query(
        `CALL refresh_continuous_aggregate('events_15m', $1::timestamptz, now()::timestamptz)`,
        [from],
      );
    } finally {
      c2.release();
    }

    const after = await request(app)
      .get(`/v1/metrics/hourly?from=${from}&to=${to}`)
      .set(auth);
    assert.equal(after.status, 200);
    if (after.body.hours.length !== 1) {
      const dbg = await pool.query(
        `SELECT bucket, sessions, events FROM session_stats_15m
          WHERE tenant_id = $1
            AND bucket >= $2::timestamptz AND bucket < $3::timestamptz
          ORDER BY bucket`,
        [tenantId, from, to],
      );
      console.error(
        "DEBUG hours:",
        JSON.stringify(after.body.hours),
        "stats rows:",
        JSON.stringify(dbg.rows),
      );
    }
    assert.equal(
      after.body.hours.length,
      1,
      `stats-backed hour appears after the rollup: ${JSON.stringify(after.body.hours)}`,
    );
    const hour = after.body.hours[0];
    assert.equal(Number(hour.sessions), 5);
    assert.equal(Number(hour.events), 7);
    assert.equal(
      after.body.live.sessions,
      0,
      "archived sessions leave the live slice (D18)",
    );
    assert.equal(after.body.live.live, true);

    // Bounce: 3 of 5 sessions have exactly one event → 0.6, within [0, 1].
    assert.ok(hour.bounce_rate !== null, "bounce rate is non-zero");
    assert.ok(
      hour.bounce_rate > 0 && hour.bounce_rate <= 1,
      `bounce_rate plausible (got ${hour.bounce_rate})`,
    );
    assert.equal(hour.bounce_rate, 0.6, "bounce rate matches the seeded pattern (3/5)");
  });
});
