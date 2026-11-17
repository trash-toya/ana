// Endpoint behaviour against the real DB (04-services.md §Dashboard API):
// output shapes, tenant scoping, the D18 anti-join, the funnel tie-break and
// the approximate labelling of HLL-derived counts. Event seeding uses the
// db/test harness ingest_batch() (same convention as the ingest suite).
import { test } from "vitest";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import request from "supertest";

import { createApp } from "../dist/app.js";
import {
  event,
  forceArchive,
  ingest,
  recentBase,
  withTestDb,
} from "../../../db/test/harness.mjs";
import { assertInvariant } from "../../../db/test/harness.mjs";
import { seedTenantWithKeys } from "./helpers.mjs";

const MIN = 60_000;

function iso(t) {
  return new Date(t).toISOString();
}

/** Basic app: real pool, real auth. */
function app(pool) {
  return createApp({ pool });
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

test("replay: session row + events ordered by (time, seq NULLS LAST, event_id)", async () => {
  await withTestDb("dash_replay", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const sessionId = randomUUID();
    const base = recentBase(2);
    const events = [
      event({
        tenantId,
        sessionId,
        eventId: randomUUID(),
        seq: 2,
        time: iso(base + 2 * MIN),
        name: "click",
        url: null,
        props: { plan: "pro" },
      }),
      event({
        tenantId,
        sessionId,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
        props: {},
      }),
      // Same time, no seq: ordered after seq'd events, then by event_id.
      event({
        tenantId,
        sessionId,
        eventId: randomUUID(),
        seq: null,
        time: iso(base),
        name: "page_view",
      }),
    ];
    const c = await pool.connect();
    try {
      const res = await ingest(c, events);
      assert.equal(res.stored.length, 3, "all stored");
      const out = await request(app(pool))
        .get(`/v1/sessions/${sessionId}/replay`)
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      assert.equal(out.body.session.session_id, sessionId);
      assert.equal(out.body.session.event_count, 3);
      assert.equal(out.body.session.entry_url, "/p");
      assert.equal(out.body.events.length, 3);
      // seq 1 first, then the two t=0 events (seq NULLS LAST → both after the
      // seq'd pair at t=2? No: time ASC dominates. seq 2 @ t+2 is LAST).
      const seqs = out.body.events.map((e) => e.seq);
      assert.deepEqual(
        out.body.events.map((e) => [e.time, e.seq]),
        [iso(base), iso(base), iso(base + 2 * MIN)].map((t, i) => [t, [1, null, 2][i]]),
      );
      assert.equal(seqs[0], 1);
      assert.equal(out.body.events[2].props.plan, "pro");
    } finally {
      c.release();
    }
  });
});

test("replay: archived session found via guard's home_day (history branch)", async () => {
  await withTestDb("dash_replay_hist", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const sessionId = randomUUID();
    const base = recentBase(2);
    const events = [
      event({
        tenantId,
        sessionId,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
      }),
    ];
    const c = await pool.connect();
    try {
      await ingest(c, events);
      await forceArchive(pool);
      const hist = await c.query(
        "SELECT count(*)::int AS n FROM sessions_history WHERE session_id = $1",
        [sessionId],
      );
      assert.equal(hist.rows[0].n, 1, "session archived");
      const out = await request(app(pool))
        .get(`/v1/sessions/${sessionId}/replay`)
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      assert.equal(out.body.session.total_events, 1);
      assert.equal(out.body.events.length, 1);
    } finally {
      c.release();
    }
  });
});

test("replay: unknown session → 404; another tenant's session → 404 (scoping)", async () => {
  await withTestDb("dash_replay_scope", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    // Tenant A has a real session; tenant B is a second tenant with its own key.
    const sessionId = randomUUID();
    const c = await pool.connect();
    try {
      await ingest(c, [
        event({
          tenantId,
          sessionId,
          eventId: randomUUID(),
          seq: 1,
          time: iso(recentBase(2)),
          name: "page_view",
        }),
      ]);
    } finally {
      c.release();
    }
    const otherTenant = (
      await pool.query("INSERT INTO tenants (name) VALUES ('other') RETURNING tenant_id")
    ).rows[0].tenant_id;
    const otherKey = `dk_${randomUUID()}`;
    await pool.query(
      `INSERT INTO api_keys (key_hash, tenant_id, kind) VALUES ($1, $2, 'dashboard')`,
      [createHash("sha256").update(otherKey, "utf8").digest(), otherTenant],
    );

    const unknown = await request(app(pool))
      .get(`/v1/sessions/${randomUUID()}/replay`)
      .set("X-API-Key", dashboardKey);
    assert.equal(unknown.status, 404);

    const cross = await request(app(pool))
      .get(`/v1/sessions/${sessionId}/replay`)
      .set("X-API-Key", otherKey);
    assert.equal(cross.status, 404, "tenant B must not read tenant A's session");
  });
});

test("replay: malformed session id → 400", async () => {
  await withTestDb("dash_replay_badid", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    const res = await request(app(pool))
      .get("/v1/sessions/not-a-uuid/replay")
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 400);
  });
});

// ---------------------------------------------------------------------------
// hourly
// ---------------------------------------------------------------------------

test("hourly: stats buckets + labelled live slice; D18 excludes closed sessions", async () => {
  await withTestDb("dash_hourly", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const c = await pool.connect();
    try {
      // Two closed hours of stats: 10 sessions, 4 bounced (ec_0+ec_1), 10 events.
      const b0 = new Date(recentBase(3)).toISOString();
      const b1 = new Date(recentBase(3) + 60 * MIN).toISOString();
      await c.query(
        `INSERT INTO session_stats_15m (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
                                         total_duration_s, heartbeat_s, events)
         VALUES ($1, $2, 4, 1, 1, 1, 1, 400, 100, 5),
                ($1, $3, 6, 1, 1, 2, 2, 600, 200, 5)`,
        [tenantId, b0, b1],
      );

      // One genuinely live session (touched this 10-minute bucket).
      const liveSession = randomUUID();
      await c.query(
        `INSERT INTO sessions_active (tenant_id, session_id, home_day, anon_id, started_at,
                                      last_seen_at, event_count, total_events, touch_bucket)
         VALUES ($1, $2, date_trunc('day', now()), 'a-live', now() - interval '2 minutes',
                 now() - interval '30 seconds', 2, 3,
                 floor(extract(epoch FROM now()) / 600)::int)`,
        [tenantId, liveSession],
      );

      // A late partial: session already has history (closed) but an active row.
      const lateSession = randomUUID();
      await c.query(
        `INSERT INTO sessions_history (tenant_id, session_id, home_day, anon_id, started_at,
                                       last_seen_at, event_count, total_events, heartbeats)
         VALUES ($1, $2, date_trunc('day', now()), 'a-late', now() - interval '3 hours',
                 now() - interval '2 hours', 1, 1, 0)`,
        [tenantId, lateSession],
      );
      await c.query(
        `INSERT INTO sessions_active (tenant_id, session_id, home_day, anon_id, started_at,
                                      last_seen_at, event_count, total_events, touch_bucket)
         VALUES ($1, $2, date_trunc('day', now()), 'a-late', now() - interval '2 minutes',
                 now() - interval '40 seconds', 1, 2,
                 floor(extract(epoch FROM now()) / 600)::int)`,
        [tenantId, lateSession],
      );

      const from = iso(recentBase(3) - MIN);
      const to = iso(Date.now());
      const out = await request(app(pool))
        .get(
          `/v1/metrics/hourly?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
        )
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);

      // Hours come from stats only; live is labelled separately.
      assert.equal(out.body.hours.length, 2);
      assert.equal(out.body.hours[0].sessions, 4);
      assert.equal(out.body.hours[1].sessions, 6);
      assert.equal(out.body.hours[0].bounce_rate, 0.5); // (1+1)/4
      assert.equal(out.body.hours[0].source, "stats_15m");
      assert.deepEqual(out.body.live, {
        sessions: 1,
        events: 3,
        engaged_events: 2,
        live: true,
      });
    } finally {
      c.release();
    }
  });
});

test("hourly: invalid range → 400", async () => {
  await withTestDb("dash_hourly_bad", async ({ pool }) => {
    const { dashboardKey } = await seedTenantWithKeys(pool);
    const res = await request(app(pool))
      .get(`/v1/metrics/hourly?from=${iso(recentBase(1))}&to=${iso(recentBase(3))}`)
      .set("X-API-Key", dashboardKey);
    assert.equal(res.status, 400);
    const big = await request(app(pool))
      .get(`/v1/metrics/hourly?from=${iso(0)}&to=${iso(Date.now() + 200 * 86_400_000)}`)
      .set("X-API-Key", dashboardKey);
    assert.equal(big.status, 400);
  });
});

// ---------------------------------------------------------------------------
// daily
// ---------------------------------------------------------------------------

test("daily: closed days from the rollup, open days from the 15m CAGG, HLL identity approximate", async () => {
  await withTestDb("dash_daily", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const c = await pool.connect();
    try {
      await c.query(
        `INSERT INTO tenant_settings (tenant_id, tz) VALUES ($1, 'Asia/Kolkata')`,
        [tenantId],
      );

      // Closed day (current_date - 2 in the tenant tz): daily-table rows.
      const closedDay = await c.query(
        `SELECT ((now() AT TIME ZONE 'Asia/Kolkata')::date - 2)::text AS d`,
      );
      const closed = closedDay.rows[0].d;
      await c.query(
        `INSERT INTO session_stats_daily (tenant_id, local_day, sessions, bounced,
                                          total_duration_s, heartbeat_s, events)
         VALUES ($1, $2::date, 10, 4, 1000, 300, 20)`,
        [tenantId, closed],
      );
      await c.query(
        `INSERT INTO identity_daily (tenant_id, local_day, anon_hll, users_hll)
         VALUES ($1, $2::date,
                 (SELECT hyperloglog(4096, x) FROM unnest(ARRAY['v1','v2','v3']) x),
                 (SELECT hyperloglog(4096, u) FROM unnest(ARRAY['u1','u2']) u))`,
        [tenantId, closed],
      );

      // Open day (now): real events → archive → 15m stats; CAGG for identity.
      const base = recentBase(2);
      const s1 = randomUUID();
      const s2 = randomUUID();
      const evs = [
        event({
          tenantId,
          sessionId: s1,
          eventId: randomUUID(),
          seq: 1,
          time: iso(base),
          name: "page_view",
          anonId: "va1",
          userId: "ou1",
        }),
        event({
          tenantId,
          sessionId: s1,
          eventId: randomUUID(),
          seq: 2,
          time: iso(base + MIN),
          name: "click",
          anonId: "va1",
          userId: "ou1",
        }),
        event({
          tenantId,
          sessionId: s2,
          eventId: randomUUID(),
          seq: 1,
          time: iso(base + 2 * MIN),
          name: "page_view",
          anonId: "va2",
        }),
      ];
      const r = await ingest(c, evs);
      assert.equal(r.stored.length, 3);
      // archive_sessions populates session_stats_15m (a plain table, not a CAGG);
      // identity_15m is a CAGG and is refreshed over the events' window.
      await forceArchive(pool);
      await c.query(
        `CALL refresh_continuous_aggregate('identity_15m',
           $1::timestamptz - INTERVAL '15 minutes', $2::timestamptz + INTERVAL '15 minutes')`,
        [new Date(base - MIN), new Date(base + 5 * MIN)],
      );

      const from = iso(base - 4 * 86_400_000);
      const to = iso(Date.now());
      const out = await request(app(pool))
        .get(
          `/v1/metrics/daily?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
        )
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      assert.equal(out.body.tz, "Asia/Kolkata");

      const closedRow = out.body.days.find((d) => d.day === closed);
      assert.ok(
        closedRow,
        `closed day ${closed} present in ${JSON.stringify(out.body.days)}`,
      );
      assert.equal(closedRow.sessions, 10);
      assert.equal(closedRow.source, "daily");
      assert.equal(closedRow.visitors, 3);
      assert.equal(closedRow.users, 2);
      assert.equal(closedRow.approximate, true);

      const openRow = out.body.days.find((d) => d.day !== closed);
      assert.ok(openRow, "open day present");
      assert.equal(openRow.source, "15m");
      assert.equal(openRow.sessions, 2); // s1 + s2 (15m CAGG refreshed)
      assert.equal(openRow.visitors, 2); // va1, va2
      assert.equal(openRow.users, 1); // ou1
      assert.equal(openRow.approximate, true);

      // The open day aggregates by the TENANT tz: the 18:xx UTC boundary
      // idiom is exercised in T25; here the day split must equal the tz day.
      const dayOfOpen = await c.query(
        `SELECT (time_bucket('1 day', $1::timestamptz, 'Asia/Kolkata')
                  AT TIME ZONE 'Asia/Kolkata')::date::text AS d`,
        [new Date(base).toISOString()],
      );
      assert.equal(openRow.day, dayOfOpen.rows[0].d);
    } finally {
      c.release();
    }
  });
});

test("daily: tz override takes precedence over tenant_settings; invalid tz → 400", async () => {
  await withTestDb("dash_daily_tz", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const c = await pool.connect();
    try {
      await c.query(
        `INSERT INTO tenant_settings (tenant_id, tz) VALUES ($1, 'Asia/Kolkata')`,
        [tenantId],
      );
      const from = iso(recentBase(24));
      const to = iso(Date.now());
      const out = await request(app(pool))
        .get(
          `/v1/metrics/daily?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&tz=UTC`,
        )
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      assert.equal(out.body.tz, "UTC");

      const bad = await request(app(pool))
        .get(
          `/v1/metrics/daily?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&tz=Not/AZone`,
        )
        .set("X-API-Key", dashboardKey);
      assert.equal(bad.status, 400);
    } finally {
      c.release();
    }
  });
});

// ---------------------------------------------------------------------------
// entry-pages
// ---------------------------------------------------------------------------

test("entry-pages: grouped by entry_url with bounce rate; D18 via the sessions view", async () => {
  await withTestDb("dash_entry", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const base = recentBase(2);
    const sA = randomUUID();
    const sB = randomUUID();
    const evs = [
      // sA: entered on /landing, two events → not bounced.
      event({
        tenantId,
        sessionId: sA,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
        url: "/landing",
      }),
      event({
        tenantId,
        sessionId: sA,
        eventId: randomUUID(),
        seq: 2,
        time: iso(base + MIN),
        name: "click",
        url: "/landing",
      }),
      // sB: entered on /promo, one event → bounced.
      event({
        tenantId,
        sessionId: sB,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
        url: "/promo",
      }),
    ];
    const c = await pool.connect();
    try {
      await ingest(c, evs);
      await forceArchive(pool);
      const out = await request(app(pool))
        .get(
          `/v1/entry-pages?from=${encodeURIComponent(iso(base - MIN))}&to=${encodeURIComponent(iso(Date.now()))}`,
        )
        .set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      const pages = out.body.entry_pages;
      assert.equal(pages.length, 2);
      const landing = pages.find((p) => p.entry_url === "/landing");
      const promo = pages.find((p) => p.entry_url === "/promo");
      assert.equal(landing.sessions, 1);
      assert.equal(landing.bounce_rate, 0);
      assert.ok(landing.avg_duration_s >= 60, "duration from last_seen - started");
      assert.equal(promo.sessions, 1);
      assert.equal(promo.bounce_rate, 1);
      assert.ok(promo.avg_duration_s < 60);
    } finally {
      c.release();
    }
  });
});

// ---------------------------------------------------------------------------
// funnels
// ---------------------------------------------------------------------------

test("funnel: ordered sequence counts sessions that reach each step in order", async () => {
  await withTestDb("dash_funnel", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const base = recentBase(2);
    const s1 = randomUUID();
    const s2 = randomUUID();
    const s3 = randomUUID();
    const evs = [
      // s1 completes all three steps.
      event({
        tenantId,
        sessionId: s1,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
      }),
      event({
        tenantId,
        sessionId: s1,
        eventId: randomUUID(),
        seq: 2,
        time: iso(base + MIN),
        name: "click",
      }),
      event({
        tenantId,
        sessionId: s1,
        eventId: randomUUID(),
        seq: 3,
        time: iso(base + 2 * MIN),
        name: "checkout",
      }),
      // s2 reaches step 2.
      event({
        tenantId,
        sessionId: s2,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
      }),
      event({
        tenantId,
        sessionId: s2,
        eventId: randomUUID(),
        seq: 2,
        time: iso(base + MIN),
        name: "click",
      }),
      // s3 has a click but never a page_view: reaches nothing.
      event({
        tenantId,
        sessionId: s3,
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "click",
      }),
      // Other tenant's session must not leak into tenant A's funnel.
      event({
        tenantId: randomUUID(),
        sessionId: randomUUID(),
        eventId: randomUUID(),
        seq: 1,
        time: iso(base),
        name: "page_view",
      }),
    ];
    const c = await pool.connect();
    try {
      const r = await ingest(c, evs);
      assert.equal(r.stored.length, 7, "all events stored");
      const out = await request(app(pool))
        .post("/v1/funnels")
        .set("X-API-Key", dashboardKey)
        .send({
          from: iso(base - MIN),
          to: iso(base + 5 * MIN),
          steps: ["page_view", "click", "checkout"],
        });
      assert.equal(out.status, 200);
      assert.deepEqual(
        out.body.steps.map((s) => s.sessions),
        [2, 2, 1],
        `got ${JSON.stringify(out.body.steps)}`,
      );
      assert.deepEqual(
        out.body.steps.map((s) => s.conversion),
        [1, 1, 0.5],
      );
      assert.deepEqual(
        out.body.steps.map((s) => s.name),
        ["page_view", "click", "checkout"],
      );
    } finally {
      c.release();
    }
  });
});

test("funnel: tie-break is (time, seq NULLS LAST, event_id) — the storage order", async () => {
  await withTestDb("dash_funnel_tie", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const t = iso(recentBase(2));
    const sSame = randomUUID();
    const sOne = randomUUID();
    const evs = [
      // sSame: TWO page_views at the same timestamp, no seq → both usable, in
      // event_id order; the session reaches step 2 of [page_view, page_view].
      event({
        tenantId,
        sessionId: sSame,
        eventId: randomUUID(),
        seq: null,
        time: t,
        name: "page_view",
      }),
      event({
        tenantId,
        sessionId: sSame,
        eventId: randomUUID(),
        seq: null,
        time: t,
        name: "page_view",
      }),
      // sOne: a single page_view → step 1 only.
      event({
        tenantId,
        sessionId: sOne,
        eventId: randomUUID(),
        seq: null,
        time: t,
        name: "page_view",
      }),
    ];
    const c = await pool.connect();
    try {
      await ingest(c, evs);
      const out = await request(app(pool))
        .post("/v1/funnels")
        .set("X-API-Key", dashboardKey)
        .send({
          from: iso(recentBase(3)),
          to: iso(Date.now()),
          steps: ["page_view", "page_view"],
        });
      assert.equal(out.status, 200);
      assert.deepEqual(
        out.body.steps.map((s) => s.sessions),
        [2, 1],
      );
    } finally {
      c.release();
    }
  });
});

test("funnel: out-of-order steps do not count; validation caps steps at 10", async () => {
  await withTestDb("dash_funnel_order", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const base = recentBase(2);
    const s = randomUUID();
    const c = await pool.connect();
    try {
      await ingest(c, [
        event({
          tenantId,
          sessionId: s,
          eventId: randomUUID(),
          seq: 1,
          time: iso(base),
          name: "click",
        }),
        event({
          tenantId,
          sessionId: s,
          eventId: randomUUID(),
          seq: 2,
          time: iso(base + MIN),
          name: "page_view",
        }),
      ]);
      // click BEFORE page_view in time → [page_view, click] step 2 unreached.
      const out = await request(app(pool))
        .post("/v1/funnels")
        .set("X-API-Key", dashboardKey)
        .send({
          from: iso(base - MIN),
          to: iso(base + 3 * MIN),
          steps: ["page_view", "click"],
        });
      assert.equal(out.status, 200);
      assert.deepEqual(
        out.body.steps.map((s) => s.sessions),
        [1, 0],
      );

      const tooMany = await request(app(pool))
        .post("/v1/funnels")
        .set("X-API-Key", dashboardKey)
        .send({
          from: iso(base - MIN),
          to: iso(base + 3 * MIN),
          steps: Array.from({ length: 11 }, (_, i) => `e${i}`),
        });
      assert.equal(tooMany.status, 400);
    } finally {
      c.release();
    }
  });
});

// ---------------------------------------------------------------------------
// live
// ---------------------------------------------------------------------------

test("live: 5-minute window + touch_bucket bound; D18 excludes sessions with history", async () => {
  await withTestDb("dash_live", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const c = await pool.connect();
    try {
      const touchNow = `floor(extract(epoch FROM now()) / 600)::int`;
      // Genuinely live.
      const live1 = randomUUID();
      // Seen 10 minutes ago → outside the 5-minute window even though the
      // touch_bucket index bound alone would include it.
      const stale = randomUUID();
      // Touched now but already archived → D18 exclusion.
      const late = randomUUID();
      await c.query(
        `INSERT INTO sessions_active (tenant_id, session_id, home_day, anon_id, started_at,
                                      last_seen_at, event_count, total_events, touch_bucket,
                                      entry_url, device, country, utm)
         VALUES ($1, $2, date_trunc('day', now()), 'a1', now() - interval '3 minutes',
                 now() - interval '30 seconds', 2, 3, ${touchNow}, '/live', 'mobile', 'DE', '{"source":"x"}'),
                ($1, $3, date_trunc('day', now()), 'a2', now() - interval '20 minutes',
                 now() - interval '10 minutes', 1, 1, ${touchNow}, '/stale', null, null, null),
                ($1, $4, date_trunc('day', now()), 'a3', now() - interval '3 minutes',
                 now() - interval '40 seconds', 1, 2, ${touchNow}, '/late', null, null, null)`,
        [tenantId, live1, stale, late],
      );
      await c.query(
        `INSERT INTO sessions_history (tenant_id, session_id, home_day, anon_id, started_at,
                                       last_seen_at, event_count, total_events, heartbeats)
         VALUES ($1, $2, date_trunc('day', now()), 'a3', now() - interval '3 hours',
                 now() - interval '2 hours', 1, 1, 0)`,
        [tenantId, late],
      );

      const out = await request(app(pool)).get("/v1/live").set("X-API-Key", dashboardKey);
      assert.equal(out.status, 200);
      assert.equal(out.body.live, true);
      assert.equal(out.body.count, 1, `only live1: ${JSON.stringify(out.body.sessions)}`);
      assert.equal(out.body.sessions[0].session_id, live1);
      assert.equal(out.body.sessions[0].entry_url, "/live");
    } finally {
      c.release();
    }
  });
});

// ---------------------------------------------------------------------------
// invariants still hold with the dashboard service exercised
// ---------------------------------------------------------------------------

test("dashboard reads never mutate: READ ONLY transaction + invariants", async () => {
  await withTestDb("dash_readonly", async ({ pool }) => {
    const { tenantId, dashboardKey } = await seedTenantWithKeys(pool);
    const sessionId = randomUUID();
    const base = recentBase(2);
    const c = await pool.connect();
    try {
      await ingest(c, [
        event({
          tenantId,
          sessionId,
          eventId: randomUUID(),
          seq: 1,
          time: iso(base),
          name: "page_view",
        }),
      ]);
      await forceArchive(pool);
      for (const inv of ["i1_no_duplicate_events", "i4_one_history_row"]) {
        await assertInvariant(c, inv);
      }
    } finally {
      c.release();
    }
    const out = await request(app(pool)).get("/v1/live").set("X-API-Key", dashboardKey);
    assert.equal(out.status, 200);
  });
});
