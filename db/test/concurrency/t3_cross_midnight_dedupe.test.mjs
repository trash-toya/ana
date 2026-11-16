// T3 — cross-midnight dedupe: 10 workers send the same 500 events (event IDs reused),
// in two rounds. Expected: each event stored once; no duplicates; the second round is
// reported as duplicates (accepted), regardless of the midnight boundary — the dedupe
// key is (tenant_id, event_id) with no day in it (Appendix A 1.2).
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const tenantId = randomUuid();
const sessionId = randomUuid();

// Events straddle midnight UTC (23:59:50 → 00:00:49).
function same500(round, offsetMs = 0) {
  const events = [];
  for (let i = 0; i < 500; i++) {
    const t = Date.UTC(2026, 10, 15, 23, 59, 50) + i * 1000 + offsetMs;
    events.push(event({
      tenantId, sessionId,
      eventId: `00000000-0000-7000-8000-${String(i + 1).padStart(12, "0")}`,
      seq: i + 1, time: new Date(t), name: "page_view", cat: "e",
    }));
  }
  return events;
}

test("T3: same 500 events, 10 workers, two rounds across midnight", async () => {
  await withTestDb("t3", async ({ pool }) => {
    const allOutcomes = [];

    for (const round of [1, 2]) {
      const workers = Array.from({ length: 10 }, async () => {
        const client = await pool.connect();
        try {
          return await ingest(client, same500(round));
        } finally {
          client.release();
        }
      });
      const results = await Promise.all(workers);
      allOutcomes.push(...results);
    }

    const stored = allOutcomes.flatMap((r) => r.stored);
    const dups = allOutcomes.flatMap((r) => r.dups);
    const capped = allOutcomes.flatMap((r) => r.capped);
    const conflicts = allOutcomes.flatMap((r) => r.conflicts);

    assert.equal(capped.length + conflicts.length, 0);
    assert.equal(new Set([...stored, ...dups]).size, 500,
      "every submitted ID reported, each event id exactly once per worker-round");
    assert.equal(stored.length, 500, "exactly one copy of each event stored");
    // 9,500 duplicate reports = 4,500 intra-round races (9 losers per event in round 1)
    // + 5,000 replays in round 2. All are accepted outcomes.
    assert.equal(dups.length, 9500, "races and replays are all duplicates");

    const c = await pool.connect();
    try {
      const nEvents = await c.query("SELECT count(*) AS n FROM events");
      assert.equal(Number(nEvents.rows[0].n), 500, "each event stored once");

      const nDedupe = await c.query("SELECT count(*) AS n FROM event_dedupe");
      assert.equal(Number(nDedupe.rows[0].n), 500);

      const guard = await c.query(
        "SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, sessionId],
      );
      assert.equal(guard.rows[0].total_events, 500, "counter counts stored rows once");

      const active = await c.query(
        "SELECT total_events FROM sessions_active WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, sessionId],
      );
      assert.equal(Number(active.rows[0].total_events), 500, "no double count live either");
    } finally {
      c.release();
    }
  });
});
