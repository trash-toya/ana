// T23 — dedupe retention horizon (Notion §21 "Idempotency and replay horizon"):
// event_dedupe is retained 90 days, matching the raw-event retention window, so
// a producer replay inside the horizon keeps getting idempotent outcomes. The
// 3-day rule in the same job applies ONLY to session-lifecycle rows (guard,
// bootstrap). Without this test a 3-day shortcut silently reopens the duplicate
// window for replays between day 3 and day 90.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, sha, assertNoCustomJobs } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T23: dedupe survives to 90 days; session-lifecycle rows go at 3 days", async () => {
  await withTestDb("t23", async ({ pool }) => {
    const c = await pool.connect();
    try {
      await assertNoCustomJobs(c);
      const tenant = randomUuid();

      // Dedupe rows at 89 days (keep) and 91 days (drop).
      const keepId = randomUuid();
      const dropId = randomUuid();
      await c.query(
        `INSERT INTO event_dedupe (tenant_id, event_id, payload_hash, received_at)
         VALUES ($1, $2, $3, now() - interval '89 days'),
                ($1, $4, $3, now() - interval '91 days')`,
        [tenant, keepId, sha(["h"]), dropId],
      );

      // Guards at 2 days (keep) and 4 days (drop).
      const guardKeep = randomUuid();
      const guardDrop = randomUuid();
      await c.query(
        `INSERT INTO session_guard (tenant_id, session_id, home_day, total_events, created_at)
         VALUES ($1, $2, now() - interval '2 days', 1, now() - interval '2 days'),
                ($1, $3, now() - interval '4 days', 1, now() - interval '4 days')`,
        [tenant, guardKeep, guardDrop],
      );

      // Bootstrap records use last_event_at for the same 3-day lifecycle horizon.
      const bootstrapKeep = randomUuid();
      const bootstrapDrop = randomUuid();
      await c.query(
        `INSERT INTO session_bootstrap
           (tenant_id, session_id, anon_id, started_at, last_event_at)
         VALUES ($1, $2, 'anon-keep', now() - interval '2 days', now() - interval '2 days'),
                ($1, $3, 'anon-drop', now() - interval '4 days', now() - interval '4 days')`,
        [tenant, bootstrapKeep, bootstrapDrop],
      );

      await c.query("CALL cleanup_idempotency($1, $2)", [0, "{}"]);

      const dedupe = await c.query(
        "SELECT event_id FROM event_dedupe WHERE tenant_id = $1",
        [tenant],
      );
      assert.deepEqual(
        dedupe.rows.map((r) => r.event_id).sort(),
        [keepId].sort(),
        "89-day dedupe row retained, 91-day row purged",
      );

      const guards = await c.query(
        "SELECT session_id FROM session_guard WHERE tenant_id = $1",
        [tenant],
      );
      assert.deepEqual(
        guards.rows.map((r) => r.session_id),
        [guardKeep],
        "4-day guard purged, 2-day guard retained",
      );

      const bootstraps = await c.query(
        "SELECT session_id FROM session_bootstrap WHERE tenant_id = $1",
        [tenant],
      );
      assert.deepEqual(
        bootstraps.rows.map((r) => r.session_id),
        [bootstrapKeep],
        "4-day bootstrap purged, 2-day bootstrap retained",
      );
    } finally {
      c.release();
    }
  });
});

test("T23b: a replay whose ledger row is 60 days old is still a duplicate", async () => {
  await withTestDb("t23b", async ({ pool }) => {
    const c = await pool.connect();
    try {
      const tenantId = randomUuid();
      const sessionId = randomUuid();
      const first = event({
        tenantId,
        sessionId,
        eventId: randomUuid(),
        seq: 1,
        time: new Date(),
      });
      const firstOut = await ingest(c, [first]);
      assert.equal(firstOut.stored?.length ?? 0, 1, "first ingest stored");

      // Age the ledger row to day 60 — inside the horizon, outside any
      // session-lifecycle window.
      await c.query(
        `UPDATE event_dedupe SET received_at = now() - interval '60 days'
          WHERE tenant_id = $1 AND event_id = $2`,
        [tenantId, first.event_id],
      );

      const replay = await ingest(c, [first]);
      const dups = replay.dups?.map((d) => d.event_id) ?? [];
      const stored = replay.stored?.map((d) => d.event_id) ?? [];
      assert.ok(
        dups.includes(first.event_id) || stored.length === 0,
        `replay at day 60 must be idempotent, got ${JSON.stringify(replay)}`,
      );
      const n = await c.query(
        "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1 AND event_id = $2",
        [tenantId, first.event_id],
      );
      assert.equal(n.rows[0].n, 1, "exactly one logical event after the replay");
    } finally {
      c.release();
    }
  });
});
