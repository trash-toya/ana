// T5 — attribution is order-independent. Three batches carry the same session's
// events but attach user_id / entry_url / campaign data to different events. Send
// them in all six arrival orders (fresh database per permutation); the final
// attribution (user_id, entry_url, utm, device) must be identical, because the merge
// compares timestamps: the earliest event carrying the attribute wins (Appendix A 1.3).
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

const BASE = Date.UTC(2026, 10, 15, 9, 0, 0);
// Batch A: user_id on the earliest event, session_start carries utm+device.
// Batch B: user_id mid-session, earliest page_view.
// Batch C: user_id latest, latest page_view.
function batchA(tenantId, sessionId) {
  return [
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 1,
      time: new Date(BASE), name: "session_start", cat: "l", url: null,
      props: { utm: { source: "a-src", campaign: "a-camp" }, device: "desktop",
               country: "DE" }, userId: "user_A" }),
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 2,
      time: new Date(BASE + 60_000), name: "page_view", cat: "e", url: "/a-entry" }),
  ];
}
function batchB(tenantId, sessionId) {
  return [
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 3,
      time: new Date(BASE + 120_000), name: "page_view", cat: "e", url: "/b-entry",
      userId: "user_B" }),
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 4,
      time: new Date(BASE + 180_000), name: "heartbeat", cat: "s", url: null }),
  ];
}
function batchC(tenantId, sessionId) {
  return [
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 5,
      time: new Date(BASE + 240_000), name: "page_view", cat: "e", url: "/c-entry",
      userId: "user_C" }),
    event({ tenantId, sessionId, eventId: randomUuid(), seq: 6,
      time: new Date(BASE + 300_000), name: "click", cat: "e", url: "/c-click",
      props: { utm: { source: "c-src", campaign: "c-camp" }, device: "mobile",
               country: "FR" } }),
  ];
}

const EXPECTED = {
  user_id: "user_A",          // earliest event carrying a user_id wins
  entry_url: "/a-entry",      // earliest page_view wins
  utm: { source: "a-src", campaign: "a-camp" },  // earliest session_start wins
  device: "desktop",
  country: "DE",
};

async function runPermutation(order) {
  return withTestDb(`t5${order.join("").toLowerCase()}`, async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const c = await pool.connect();
    try {
      const batches = { A: batchA(tenantId, sessionId), B: batchB(tenantId, sessionId),
        C: batchC(tenantId, sessionId) };
      let ord = 0;
      for (const key of order) {
        for (const ev of batches[key]) ev.ord = ord++;
      }
      for (const key of order) {
        const r = await ingest(c, batches[key]);
        assert.equal(r.capped.length + r.conflicts.length, 0, `${key} all stored`);
      }

      const row = await c.query(
        `SELECT user_id, entry_url, utm, device, country, event_count, total_events,
                started_at, last_seen_at, heartbeats
           FROM sessions_active WHERE tenant_id = $1 AND session_id = $2`,
        [tenantId, sessionId],
      );
      assert.equal(row.rows.length, 1, "one session row");
      const s = row.rows[0];
      assert.equal(s.user_id, EXPECTED.user_id);
      assert.equal(s.entry_url, EXPECTED.entry_url);
      assert.deepEqual(s.utm, EXPECTED.utm);
      assert.equal(s.device, EXPECTED.device);
      assert.equal(s.country, EXPECTED.country);
      assert.equal(s.total_events, 6);
      assert.equal(s.event_count, 4, "engagement events counted");
      assert.equal(s.heartbeats, 1);
      assert.equal(new Date(s.started_at).getTime(), BASE);
      assert.equal(new Date(s.last_seen_at).getTime(), BASE + 300_000);

      const guard = await c.query(
        "SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, sessionId],
      );
      assert.equal(Number(guard.rows[0].total_events), 6);

      const im = await c.query(
        "SELECT user_id, min(first_seen) AS fs FROM identity_map GROUP BY user_id ORDER BY user_id",
      );
      assert.deepEqual(im.rows.map((r) => r.user_id), ["user_A", "user_B", "user_C"],
        "all identities committed with their events");
      return im.rows.map((r) => new Date(r.fs).getTime());
    } finally {
      c.release();
    }
  });
}

test("T5: attribution identical in all six arrival orders", async () => {
  for (const order of permutations(["A", "B", "C"])) {
    await runPermutation(order);
  }
});

function permutations(arr) {
  if (arr.length <= 1) return [arr];
  const out = [];
  for (let i = 0; i < arr.length; i++) {
    const rest = [...arr.slice(0, i), ...arr.slice(i + 1)];
    for (const p of permutations(rest)) out.push([arr[i], ...p]);
  }
  return out;
}
