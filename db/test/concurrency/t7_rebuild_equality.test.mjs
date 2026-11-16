// T7 — random late partials merged into history. session_stats_15m must equal a full
// rebuild with zero drift on EVERY column (I5; D12). Uses the merge/advisory-lock
// machinery: archive, send late partials for closed sessions, archive again, compare.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, forceArchive, assertInvariant } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

// Seeded PRNG so failures are reproducible.
function mulberry32(seed) {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rand = mulberry32(0x5a17);

test("T7: late partials merge; stats equal a full rebuild on every column", async () => {
  await withTestDb("t7", async ({ pool }) => {
    const tenantId = randomUuid();
    const SESSIONS = 12;
    const BASE = Date.UTC(2026, 10, 15, 8, 0, 0);
    const c = await pool.connect();

    // Phase 1: initial sessions with random event counts and categories.
    for (const sessionId of Array.from({ length: SESSIONS }, () => randomUuid())) {
      const n = 3 + Math.floor(rand() * 20);
      const batch = [];
      for (let i = 0; i < n; i++) {
        const roll = rand();
        const [name, cat] =
          roll < 0.6
            ? ["page_view", "e"]
            : roll < 0.9
              ? ["click", "e"]
              : ["heartbeat", "s"];
        batch.push(
          event({
            tenantId,
            sessionId,
            eventId: randomUuid(),
            seq: i + 1,
            time: new Date(BASE + i * 30_000),
            name,
            cat,
          }),
        );
      }
      const r = await ingest(c, batch);
      assert.equal(r.stored.length, n);
    }

    await forceArchive(pool);

    // Phase 2: late partials for a random subset of closed sessions.
    const hist = await c.query("SELECT session_id, last_seen_at FROM sessions_history");
    for (const h of hist.rows) {
      if (rand() < 0.6) {
        const n = 1 + Math.floor(rand() * 5);
        const last = new Date(h.last_seen_at).getTime();
        const batch = [];
        for (let i = 0; i < n; i++) {
          batch.push(
            event({
              tenantId,
              sessionId: h.session_id,
              eventId: randomUuid(),
              seq: 9000 + i,
              time: new Date(last + (i + 1) * 45_000),
              name: "click",
              cat: "e",
            }),
          );
        }
        const r = await ingest(c, batch);
        assert.equal(r.stored.length, n);
      }
    }

    await forceArchive(pool);

    // Phase 3: rebuild the whole range and compare every column, both directions.
    const range = await c.query(
      `SELECT min(started_at) AS lo, max(started_at) AS hi FROM sessions_history`,
    );
    await c.query("CALL rebuild_session_stats($1, $2, $3)", [
      null,
      new Date(range.rows[0].lo.getTime() - 3_600_000),
      new Date(range.rows[0].hi.getTime() + 3_600_000),
    ]);

    // EXCEPT ALL in both directions over the full row.
    const diff = await c.query(`
      SELECT count(*) AS n FROM (
        (SELECT * FROM session_stats_15m
          EXCEPT ALL
         SELECT s.tenant_id, time_bucket('15 minutes', s.started_at),
                count(*), count(*) FILTER (WHERE s.event_count = 0),
                count(*) FILTER (WHERE s.event_count = 1),
                count(*) FILTER (WHERE s.event_count = 2),
                count(*) FILTER (WHERE s.event_count >= 3),
                sum(extract(epoch FROM s.last_seen_at - s.started_at)),
                sum(s.heartbeats) * 30, sum(s.event_count)
           FROM sessions_history s GROUP BY 1, 2)
        UNION ALL
        (SELECT s.tenant_id, time_bucket('15 minutes', s.started_at),
                count(*), count(*) FILTER (WHERE s.event_count = 0),
                count(*) FILTER (WHERE s.event_count = 1),
                count(*) FILTER (WHERE s.event_count = 2),
                count(*) FILTER (WHERE s.event_count >= 3),
                sum(extract(epoch FROM s.last_seen_at - s.started_at)),
                sum(s.heartbeats) * 30, sum(s.event_count)
           FROM sessions_history s GROUP BY 1, 2
          EXCEPT ALL
         SELECT * FROM session_stats_15m)) d`);
    assert.equal(
      Number(diff.rows[0].n),
      0,
      "session_stats_15m equals the aggregate of sessions_history (I5)",
    );

    const agg = await c.query(
      `SELECT coalesce(sum(sessions), 0) AS s, coalesce(sum(events), 0) AS e
         FROM session_stats_15m`,
    );
    const nHist = await c.query(
      "SELECT count(*) AS sessions, coalesce(sum(event_count), 0) AS events FROM sessions_history",
    );
    assert.equal(Number(agg.rows[0].s), Number(nHist.rows[0].sessions));
    assert.equal(Number(agg.rows[0].e), Number(nHist.rows[0].events));

    await assertInvariant(c, "i4_one_history_row");
    c.release();
  });
});
