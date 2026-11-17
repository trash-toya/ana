// Freshness gate exposed to dashboards (P3-7, Notion §22): GET /v1/freshness
// publishes each CAGG's materialization watermark and a worst-of state.
//
// Semantics (measured on the pinned image, see t27_freshness.test.mjs): the
// watermark only advances over buckets that CONTAIN data — empty windows leave
// it at the last materialized data bucket. `behind_ms` is therefore the
// aggregation DEBT against data: the tenant's latest completed 15-min bucket
// end minus the watermark, clamped at 0. State machine (app.ts): empty (no
// tenant data) → fresh (debt ≤ 2×cadence) → degraded (≤ the 1-hour gate) →
// stale (> the gate, or data exists but was never materialized). Buckets above
// the watermark are served live (T24d/T27b) — the gate bounds the scheduled
// aggregation, never the newest data.
import { test } from "vitest";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import request from "supertest";

import { createApp } from "../dist/app.js";
import { event, ingest, withTestDb } from "../../../db/test/harness.mjs";
import { seedTenantWithKeys } from "./helpers.mjs";

const MIN = 60_000;
const BUCKET = 900_000; // 15 minutes
const iso = (t) => new Date(t).toISOString();

/** Epoch-aligned bucket boundary before `t` (bucket starts are global). */
function baseBucket(t) {
  return Math.floor(t / BUCKET) * BUCKET;
}

/** Refresh both CAGGs so their watermark lands at the end of [start, end). */
async function refresh(pool, startMs, endMs) {
  const c = await pool.connect();
  try {
    for (const view of ["events_15m", "identity_15m"]) {
      await c.query(
        `CALL refresh_continuous_aggregate('${view}', $1::timestamptz, $2::timestamptz)`,
        [iso(startMs), iso(endMs)],
      );
    }
  } finally {
    c.release();
  }
}

test("freshness: empty install → state 'empty', NULL watermarks, published shape", async () => {
  await withTestDb("dash_fresh_empty", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    const res = await request(createApp({ pool }))
      .get("/v1/freshness")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    assert.equal(res.body.state, "empty");
    assert.equal(res.body.gate_ms, 3_600_000);
    assert.equal(res.body.cadence_ms, 900_000);
    assert.deepEqual(
      res.body.caggs.map((c) => [c.view, c.watermark, c.behind_ms, c.state]),
      [
        ["events_15m", null, null, "empty"],
        ["identity_15m", null, null, "empty"],
      ],
    );
  });
});

test("freshness: everything materialized → debt 0, 'fresh'; auth required", async () => {
  await withTestDb("dash_fresh_fresh", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    await ingest(pool, [
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(Date.now() - 30 * MIN),
        name: "page_view",
        anonId: "a1",
      }),
    ]);
    // Materialize everything up to now: the watermark lands on the end of the
    // tenant's latest data bucket → debt 0 → fresh.
    await refresh(pool, Date.now() - 60 * MIN, Date.now());

    // No key → 401 (the route is tenant-scoped like every other endpoint).
    const anon = await request(createApp({ pool })).get("/v1/freshness");
    assert.equal(anon.status, 401);

    const res = await request(createApp({ pool }))
      .get("/v1/freshness")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    assert.equal(res.body.state, "fresh");
    const events = res.body.caggs.find((c) => c.view === "events_15m");
    assert.ok(events.watermark, "events watermark published");
    assert.equal(events.behind_ms, 0, "fully materialized data has no debt");
    assert.equal(events.state, "fresh");
  });
});

test("freshness: 45-min aggregation debt → 'degraded' (inside the gate)", async () => {
  await withTestDb("dash_fresh_deg", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const base = baseBucket(Date.now());
    // A at base-3h (bucket [base-3h, base-2h45)); B at base-2h15 (bucket
    // [base-2h15, base-2h)). Materialize ONLY up to just past A's bucket end:
    // the watermark sits 45 min behind B's bucket end.
    await ingest(pool, [
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(base - 3 * 60 * MIN),
        name: "page_view",
        anonId: "a1",
      }),
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(base - 135 * MIN),
        name: "page_view",
        anonId: "a2",
      }),
    ]);
    await refresh(pool, base - 4 * 60 * MIN, base - 165 * MIN + MIN);
    const res = await request(createApp({ pool }))
      .get("/v1/freshness")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    assert.equal(res.body.state, "degraded");
    const events = res.body.caggs.find((c) => c.view === "events_15m");
    assert.equal(
      events.behind_ms,
      45 * MIN,
      `debt must be 45 min (got ${events.behind_ms})`,
    );
  });
});

test("freshness: 2 h aggregation debt → 'stale' (gate violated)", async () => {
  await withTestDb("dash_fresh_stale", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const base = baseBucket(Date.now());
    // A at base-3h, B at base-1h (bucket end base-45m). Materialize up to
    // base-2h45: the debt is 2 h — beyond the 1 h gate.
    await ingest(pool, [
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(base - 3 * 60 * MIN),
        name: "page_view",
        anonId: "a1",
      }),
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(base - 60 * MIN),
        name: "page_view",
        anonId: "a2",
      }),
    ]);
    await refresh(pool, base - 4 * 60 * MIN, base - 165 * MIN + MIN);
    const res = await request(createApp({ pool }))
      .get("/v1/freshness")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    assert.equal(res.body.state, "stale");
    const events = res.body.caggs.find((c) => c.view === "events_15m");
    assert.ok(
      events.behind_ms > res.body.gate_ms,
      `debt ${events.behind_ms} ms must exceed the gate`,
    );
    assert.equal(events.state, "stale");
  });
});

test("freshness: events materialized, identity never refreshed → worst-of 'stale'", async () => {
  await withTestDb("dash_fresh_worst", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    await ingest(pool, [
      event({
        tenantId,
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(Date.now() - 30 * MIN),
        name: "page_view",
        anonId: "a1",
      }),
    ]);
    // Only events_15m refreshed; identity_15m keeps its NULL (never-refreshed)
    // watermark. Data exists → that is 'stale', and the overall state is the
    // worst of the two.
    const c = await pool.connect();
    try {
      await c.query(
        `CALL refresh_continuous_aggregate('events_15m', $1::timestamptz, $2::timestamptz)`,
        [iso(Date.now() - 60 * MIN), iso(Date.now())],
      );
    } finally {
      c.release();
    }
    const res = await request(createApp({ pool }))
      .get("/v1/freshness")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 200);
    const identity = res.body.caggs.find((c) => c.view === "identity_15m");
    assert.equal(identity.watermark, null);
    assert.equal(identity.state, "stale");
    assert.equal(res.body.state, "stale");
  });
});
