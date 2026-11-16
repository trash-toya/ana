// T12 — Rev 5 sessionless events and provenance (D29, D32).
//
// Machine-originated events (webhooks, background jobs) have no human session.
// They still deduplicate, still store source/received_at, and must not create
// session_guard/sessions_active/history rows or identity stitching.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T12: sessionless events store, deduplicate and leave no session state", async () => {
  await withTestDb("t12", async ({ pool }) => {
    const tenantId = randomUuid();
    const orderId = randomUuid();
    const now = new Date();
    const ev = event({
      tenantId,
      sessionId: null,
      eventId: orderId,
      seq: null,
      time: now,
      name: "order_placed",
      anonId: null,
      userId: "user_42",
      source: "webhook",
      receivedAt: now.toISOString(),
    });
    const c = await pool.connect();
    try {
      const r1 = await ingest(c, [ev]);
      assert.deepEqual(r1.stored, [orderId], "sessionless event stored");
      assert.equal(r1.capped.length, 0, "no cap applies");

      const row = (
        await c.query(
          `SELECT session_id, anon_id, user_id, source, received_at, time
             FROM events WHERE tenant_id = $1 AND event_id = $2`,
          [tenantId, orderId],
        )
      ).rows[0];
      assert.equal(row.session_id, null);
      assert.equal(row.anon_id, null);
      assert.equal(row.user_id, "user_42");
      assert.equal(row.source, "webhook");
      assert.equal(row.received_at.toISOString(), now.toISOString());
      assert.equal(row.time.toISOString(), now.toISOString());

      // Nothing session-scoped was invented.
      for (const table of ["session_guard", "sessions_active", "sessions_history", "identity_map"]) {
        const n = Number(
          (await c.query(`SELECT count(*) AS n FROM ${table} WHERE tenant_id = $1`, [tenantId]))
            .rows[0].n,
        );
        assert.equal(n, 0, `${table} must stay empty for a sessionless event`);
      }

      // Retry is an exact duplicate, not a conflict and not a second row.
      const r2 = await ingest(c, [{ ...ev, ord: 1 }]);
      assert.deepEqual(r2.dups, [orderId]);
      assert.equal(r2.conflicts.length, 0);
      const stored = Number(
        (await c.query("SELECT count(*) AS n FROM events WHERE tenant_id = $1", [tenantId]))
          .rows[0].n,
      );
      assert.equal(stored, 1);

      // I8 counts only session-bound events: a NULL session cannot be an orphan.
      const orphans = Number(
        (
          await c.query(
            `SELECT count(*) AS n FROM events e
              WHERE e.tenant_id = $1 AND e.session_id IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM session_guard g
                                 WHERE g.tenant_id = e.tenant_id
                                   AND g.session_id = e.session_id)`,
            [tenantId],
          )
        ).rows[0].n,
      );
      assert.equal(orphans, 0);
    } finally {
      c.release();
    }
  });
});

test("T12b: a mixed batch stores session and sessionless rows independently", async () => {
  await withTestDb("t12b", async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const now = new Date();
    const bound = event({
      tenantId, sessionId, eventId: randomUuid(), seq: 1, time: now,
      name: "page_view", source: "server",
    });
    const loose = event({
      tenantId, sessionId: null, eventId: randomUuid(), seq: null, time: now,
      name: "report_generated", anonId: null, source: "server",
    });
    const c = await pool.connect();
    try {
      const r = await ingest(c, [bound, loose]);
      assert.equal(r.stored.length, 2);
      assert.equal(r.capped.length, 0);

      const counts = await c.query(
        `SELECT count(*) FILTER (WHERE session_id IS NULL) AS loose,
                count(*) FILTER (WHERE session_id IS NOT NULL) AS bound
           FROM events WHERE tenant_id = $1`,
        [tenantId],
      );
      assert.equal(Number(counts.rows[0].loose), 1);
      assert.equal(Number(counts.rows[0].bound), 1);

      const guard = await c.query(
        "SELECT total_events FROM session_guard WHERE tenant_id = $1",
        [tenantId],
      );
      assert.equal(guard.rows.length, 1, "only the session-bound event creates a guard");
      assert.equal(Number(guard.rows[0].total_events), 1, "sessionless events consume no cap");
    } finally {
      c.release();
    }
  });
});
