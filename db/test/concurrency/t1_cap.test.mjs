// T1 — the test that proves the cap fix (06-testing.md).
// 20 workers create the same new session at once, 500 distinct events each
// (10,000 total). Expected: exactly 5,000 stored; guard.total_events = 5000;
// other 5,000 session_capped; one session row; ZERO dedupe rows for capped events (I6).
// Plus: retrying a capped event still returns session_capped (D1/D9).
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import {
  withTestDb,
  ingest,
  event,
  sha,
  assertInvariant,
  assertNoCustomJobs,
  recentBase,
} from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T1: hard cap under 20 concurrent creators", async () => {
  await withTestDb("t1", async ({ pool }) => {
    const setup = await pool.connect();
    try {
      await assertNoCustomJobs(setup);
    } finally {
      setup.release();
    }

    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const WORKERS = 20;
    const PER_WORKER = 500;
    // One shared recent timestamp: inside the I3 24 h window on any run date.
    const time = new Date(recentBase());

    const worker = async (w) => {
      const client = await pool.connect();
      try {
        const events = [];
        for (let i = 0; i < PER_WORKER; i++) {
          const eventId = randomUuid();
          events.push(
            event({
              tenantId,
              sessionId,
              eventId,
              seq: w * PER_WORKER + i + 1,
              time,
              name: "page_view",
              cat: "e",
              url: "/p",
            }),
          );
        }
        return await ingest(client, events);
      } finally {
        client.release();
      }
    };

    const results = await Promise.all(
      Array.from({ length: WORKERS }, (_, w) => worker(w)),
    );

    const stored = results.flatMap((r) => r.stored);
    const capped = results.flatMap((r) => r.capped);
    const dups = results.flatMap((r) => r.dups);
    const conflicts = results.flatMap((r) => r.conflicts);

    assert.equal(stored.length, 5000, "exactly 5,000 stored");
    assert.equal(capped.length, 5000, "5,000 session_capped");
    assert.equal(
      dups.length + conflicts.length,
      0,
      "no dups/conflicts in a fresh session",
    );
    assert.equal(
      new Set(stored).size + new Set(capped).size,
      10000,
      "every submitted ID has exactly one outcome",
    );

    const c = await pool.connect();
    try {
      const guard = await c.query(
        "SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, sessionId],
      );
      assert.equal(guard.rows[0].total_events, 5000, "guard counter is exactly the cap");

      const nEvents = await c.query("SELECT count(*) AS n FROM events");
      assert.equal(Number(nEvents.rows[0].n), 5000);

      const nDedupe = await c.query("SELECT count(*) AS n FROM event_dedupe");
      assert.equal(Number(nDedupe.rows[0].n), 5000, "dedupe rows == stored events");

      const sessions = await c.query("SELECT count(*) AS n FROM sessions_active");
      assert.equal(Number(sessions.rows[0].n), 1, "one session row");

      // Invariants after the storm.
      await assertInvariant(c, "i1_no_duplicate_events");
      await assertInvariant(c, "i2_cap_never_exceeded");
      await assertInvariant(c, "i3_guard_matches_events");
      await assertInvariant(c, "i6_dedupe_ledger");
      await assertInvariant(c, "i8_no_orphan_events");

      // D9: a retry of a capped event is re-evaluated and rejected again.
      const cappedId = capped[0];
      const retry = [
        {
          ...event({
            tenantId,
            sessionId,
            eventId: cappedId,
            seq: 99999,
            time: new Date(recentBase() + 1_000),
          }),
          payload_hash: sha(["retry", cappedId]),
        },
      ];
      const r2 = await ingest(c, retry);
      assert.deepEqual(r2.capped, [cappedId], "capped retry stays session_capped");
      assert.equal(r2.stored.length, 0);
      assert.equal(r2.dups.length, 0);
    } finally {
      c.release();
    }
  });
});
