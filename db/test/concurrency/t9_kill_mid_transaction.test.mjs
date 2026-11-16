// T9 — kill the backend mid-transaction (pg_terminate_backend), repeatedly, and prove
// there are no partial effects: after every kill, events, dedupe, guard and session
// counters still agree, and a follow-up ingest succeeds cleanly.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import pg from "pg";
import { withTestDb, ingest, event, assertInvariant } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

const BASE = Date.UTC(2026, 10, 15, 13, 0, 0);

function batch(tenantId, sessionId, offset, count, seqBase) {
  const out = [];
  for (let i = 0; i < count; i++) {
    out.push(event({
      tenantId, sessionId, eventId: randomUuid(),
      seq: seqBase + i,
      time: new Date(BASE + (offset + i) * 1000), name: "page_view", cat: "e",
    }));
  }
  return out;
}

test("T9: pg_terminate_backend mid-transaction leaves no partial effects", async () => {
  await withTestDb("t9", async ({ pool, url }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const admin = await pool.connect();

    const KILLS = 12;
    let completed = 0;

    for (let k = 0; k < KILLS; k++) {
      const victim = new pg.Client({ connectionString: url });
      // The terminate can deliver the connection FATAL outside the awaited call
      // (e.g. when it lands on an idle backend): swallow it on the client too.
      victim.on("error", () => {});
      await victim.connect();
      const { pid } = (await victim.query("SELECT pg_backend_pid() AS pid")).rows[0];

      const seqBase = completed * 300 + 1;
      const call = victim.query("SELECT ingest_batch($1) AS r",
        [JSON.stringify(batch(tenantId, sessionId, completed * 300, 300, seqBase))]);
      // Attach the handler NOW: the terminate can reject this promise before the
      // try/catch below reaches `await call`, and an unhandled-rejection window in
      // between fails the test run. (Also swallowed on the client via victim.on('error').)
      call.catch(() => {});

      // Terminate while the CALL is in flight; delay varies so we hit different
      // statement boundaries inside the function.
      await new Promise((r) => setTimeout(r, k % 7));
      await admin.query("SELECT pg_terminate_backend($1)", [pid]);

      // The killed client errors; that is expected. Await to surface it.
      try {
        await call;
      } catch {
        /* terminated — expected */
      }
      try { await victim.end(); } catch { /* already gone */ }

      // Whatever happened, the database must be consistent.
      const c = await pool.connect();
      try {
        const counts = await c.query(`
          SELECT (SELECT count(*) FROM events) AS ev,
                 (SELECT count(*) FROM event_dedupe) AS dd,
                 (SELECT coalesce(sum(total_events), 0) FROM session_guard
                   WHERE tenant_id = $1 AND session_id = $2) AS g,
                 (SELECT coalesce(sum(total_events), 0) FROM sessions_active
                   WHERE tenant_id = $1 AND session_id = $2) AS a`,
          [tenantId, sessionId]);
        const { ev, dd, g, a } = counts.rows[0];
        assert.equal(Number(ev), Number(dd), "events == dedupe rows after a kill");
        assert.equal(Number(g), Number(ev), "guard counter == events after a kill");
        assert.equal(Number(a), Number(ev), "live counters == events after a kill");
      } finally {
        c.release();
      }

      // A clean batch after the kill goes through normally and is counted.
      const survivor = await pool.connect();
      try {
        const r = await ingest(survivor,
          batch(tenantId, sessionId, completed * 300, 50, 900000 + completed * 50));
        completed += r.stored.length;
      } finally {
        survivor.release();
      }
    }

    const c = await pool.connect();
    try {
      await assertInvariant(c, "i1_no_duplicate_events");
      await assertInvariant(c, "i3_guard_matches_events");
      await assertInvariant(c, "i6_dedupe_ledger");
      await assertInvariant(c, "i8_no_orphan_events");

      const n = await c.query(
        "SELECT count(*) AS n FROM events WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, sessionId],
      );
      assert.equal(Number(n.rows[0].n), KILLS * 50,
        "each surviving batch stored exactly once, no kill residue");
    } finally {
      c.release();
      admin.release();
    }
  });
});
