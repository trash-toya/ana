// T6 — continuous ingest while archive_sessions runs every few seconds. Sessions are
// aged out (touch_bucket moved back) so the job competes with real writers.
// Expected: no lost events; no session archived while being written in a way that
// loses data; no session twice in history; guard counters == stored events.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, forceArchive, assertInvariant } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T6: ingest races archive without loss", async () => {
  await withTestDb("t6", async ({ pool }) => {
    const tenantId = randomUuid();
    const SESSIONS = 8;
    const sessionIds = Array.from({ length: SESSIONS }, () => randomUuid());
    const DURATION_MS = 6000;

    let stop = false;
    const perSessionStored = new Map(sessionIds.map((s) => [s, 0]));

    // Background: age + archive every ~700 ms while writers are active.
    const archiver = (async () => {
      while (!stop) {
        await forceArchive(pool);
        await new Promise((r) => setTimeout(r, 700));
      }
    })();

    // Writers: each worker pushes batches to its session as fast as it can.
    const seqCounter = new Map(sessionIds.map((s) => [s, 0]));
    let t = Date.UTC(2026, 10, 15, 14, 0, 0);

    const writers = sessionIds.map(async (sessionId) => {
      while (!stop) {
        const client = await pool.connect();
        try {
          const batch = [];
          const baseSeq = seqCounter.get(sessionId);
          for (let i = 0; i < 50; i++) {
            const seq = baseSeq + i + 1;
            batch.push(event({
              tenantId, sessionId, eventId: randomUuid(), seq,
              time: new Date(t + seq * 1000), name: "page_view", cat: "e",
            }));
          }
          t += 50_000;
          const r = await ingest(client, batch);
          perSessionStored.set(sessionId,
            perSessionStored.get(sessionId) + r.stored.length);
          seqCounter.set(sessionId, baseSeq + r.stored.length);
          // Age the session so the archiver competes for it mid-write.
          if (Math.random() < 0.3) {
            await client.query(
              `UPDATE sessions_active
                  SET touch_bucket = floor(extract(epoch FROM now()) / 600)::int - 6
                WHERE tenant_id = $1 AND session_id = $2`,
              [tenantId, sessionId],
            );
          }
        } finally {
          client.release();
        }
      }
    });

    await new Promise((r) => setTimeout(r, DURATION_MS));
    stop = true;
    await Promise.allSettled(writers);
    await archiver;

    // Final archive: everything closed, all counters in history.
    await forceArchive(pool);

    const c = await pool.connect();
    try {
      const totalStored = [...perSessionStored.values()].reduce((a, b) => a + b, 0);

      const nEvents = await c.query("SELECT count(*) AS n FROM events");
      assert.equal(Number(nEvents.rows[0].n), totalStored, "no lost events");

      const nDedupe = await c.query("SELECT count(*) AS n FROM event_dedupe");
      assert.equal(Number(nDedupe.rows[0].n), totalStored, "dedupe == stored");

      const hist = await c.query(
        `SELECT session_id, count(*) AS rows, sum(total_events) AS total
           FROM sessions_history GROUP BY session_id`,
      );
      assert.equal(hist.rows.length, SESSIONS, "one history row per session (I4)");
      for (const row of hist.rows) {
        const stored = perSessionStored.get(row.session_id);
        assert.equal(Number(row.total), stored,
          `history total for ${row.session_id.slice(0, 8)} equals stored`);
      }

      const nActive = await c.query("SELECT count(*) AS n FROM sessions_active");
      assert.equal(Number(nActive.rows[0].n), 0, "everything archived at the end");

      const stats = await c.query(
        `SELECT coalesce(sum(sessions), 0) AS s, coalesce(sum(events), 0) AS e
           FROM session_stats_15m`,
      );
      assert.equal(Number(stats.rows[0].s), SESSIONS);
      assert.equal(Number(stats.rows[0].e), totalStored, "stats events == stored");

      await assertInvariant(c, "i1_no_duplicate_events");
      await assertInvariant(c, "i2_cap_never_exceeded");
      await assertInvariant(c, "i3_guard_matches_events");
      await assertInvariant(c, "i4_one_history_row");
      await assertInvariant(c, "i6_dedupe_ledger");
      await assertInvariant(c, "i8_no_orphan_events");
    } finally {
      c.release();
    }
  });
});
