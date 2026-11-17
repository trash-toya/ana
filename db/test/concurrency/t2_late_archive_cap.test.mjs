// T2 — cap across archiving (P0-5 fixture). Fill to 4,900, force archiving, send 200
// late events: 100 stored (room = 100), one history row with total_events = 5000,
// stats session count unchanged, duration delta exact (+100 s). Exercises D18: the
// late partial sits in sessions_active while history already holds the session.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, forceArchive } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

const START = Date.UTC(2026, 10, 15, 10, 0, 0);

test("T2: late events after archiving respect the cap and merge exactly", async () => {
  await withTestDb("t2", async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const c = await pool.connect();

    // 1. Fill to 4,900: one event per second starting at 10:00:00.
    const first = [];
    for (let i = 0; i < 4900; i++) {
      first.push(
        event({
          tenantId,
          sessionId,
          eventId: randomUuid(),
          seq: i + 1,
          time: new Date(START + i * 1000),
          name: "page_view",
          cat: "e",
        }),
      );
    }
    const r1 = await ingest(c, first);
    assert.equal(r1.stored.length, 4900);
    assert.equal(r1.capped.length, 0);

    // 2. Force archiving (age touch_bucket, then run the job).
    await forceArchive(pool);

    const hist1 = await c.query(
      "SELECT total_events, started_at, last_seen_at FROM sessions_history",
    );
    assert.equal(hist1.rows.length, 1, "one history row after archiving");
    assert.equal(hist1.rows[0].total_events, 4900);

    const stats1 = await c.query(
      "SELECT sessions, ec_3p, total_duration_s, events FROM session_stats_15m",
    );
    assert.equal(Number(stats1.rows[0].sessions), 1);
    assert.equal(Number(stats1.rows[0].ec_3p), 1);
    assert.equal(Number(stats1.rows[0].events), 4900);
    const dur1 = Number(stats1.rows[0].total_duration_s);
    assert.equal(dur1, 4899, "duration = last_seen - started = 4899 s");

    // 3. 200 late events (cap room is 100). Times continue 1 s after last_seen.
    const lastSeen = new Date(hist1.rows[0].last_seen_at);
    const late = [];
    for (let i = 0; i < 200; i++) {
      late.push(
        event({
          tenantId,
          sessionId,
          eventId: randomUuid(),
          seq: 4901 + i,
          time: new Date(lastSeen.getTime() + (i + 1) * 1000),
          name: "page_view",
          cat: "e",
        }),
      );
    }
    const r2 = await ingest(c, late);
    assert.equal(r2.stored.length, 100, "only 100 stored — the cap survived archiving");
    assert.equal(r2.capped.length, 100);

    const guard = await c.query(
      "SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2",
      [tenantId, sessionId],
    );
    assert.equal(guard.rows[0].total_events, 5000);

    const active = await c.query("SELECT total_events FROM sessions_active");
    assert.equal(Number(active.rows[0].total_events), 100, "late partial is its own row");

    // D18: the live view must not show the session twice. The rebuilt view
    // (0017) carries tenant predicates on both arms, so bind the tenant first
    // (session-local: these are autocommit statements, not one transaction).
    await c.query("SELECT set_config('app.tenant_id', $1, false)", [tenantId]);
    const view = await c.query("SELECT count(*) AS n FROM sessions");
    assert.equal(Number(view.rows[0].n), 1, "no double count in the sessions view");

    // 4. Archive again: the partial merges with exact deltas.
    await forceArchive(pool);

    const hist2 = await c.query(
      "SELECT total_events, last_seen_at FROM sessions_history",
    );
    assert.equal(hist2.rows.length, 1, "still exactly one history row (I4)");
    assert.equal(hist2.rows[0].total_events, 5000);

    const stats2 = await c.query(
      "SELECT sessions, total_duration_s, events FROM session_stats_15m",
    );
    assert.equal(
      Number(stats2.rows[0].sessions),
      1,
      "session count unchanged by the merge",
    );
    assert.equal(Number(stats2.rows[0].events), 5000);
    const dur2 = Number(stats2.rows[0].total_duration_s);
    assert.equal(dur2 - dur1, 100, "duration delta is exactly the late span (100 s)");
    assert.equal(Number(await countActive(c)), 0, "live table empty after archiving");

    c.release();
  });
});

async function countActive(c) {
  const r = await c.query("SELECT count(*) AS n FROM sessions_active");
  return r.rows[0].n;
}
