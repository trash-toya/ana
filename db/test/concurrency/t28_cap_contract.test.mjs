// T28 — P3-8: the session-cap CONTRACT (Notion §22), decided before any async
// session association (P4). Decision (03-write-path.md §Session-cap contract):
// the 5,000 cap stays SYNCHRONOUS guard enforcement and is never analytical —
// the quota is computed from session_guard.total_events under a row lock inside
// ingest_batch's transaction; summary maintenance (sessions_active, history,
// stats) is deferrable and is NEVER an input to the cap.
//
// Pinned here:
//   T28a: with every summary deferred or deleted (no sessions_active row, no
//         history, no stats), the cap still stops exactly at 5,000 — quota
//         reads the guard only. Retries of capped events stay session_capped.
//   T28b: the invariant that makes the contract auditable: a writer that
//         inserts event rows without claiming guard room trips I2/I3 (the cap
//         IS detectably weakened, never silently). Async association in P4
//         must go through ingest_batch; a direct insert is a loud invariant
//         violation, not an accepted state.
//   T28c: after the summaries are rebuilt (reconcile), I3 closes again — the
//         deferred maintenance converges and the guard remains the single
//         quota authority.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";

import { event, forceArchive, ingest, withTestDb, recentBase } from "../harness.mjs";
import { readFileSync } from "node:fs";

const randomUuid = () => crypto.randomUUID();
const CAP = 5000;

const i2 = readFileSync(
  new URL("../sql/i2_cap_never_exceeded.sql", import.meta.url),
  "utf8",
);
const i3 = readFileSync(
  new URL("../sql/i3_guard_matches_events.sql", import.meta.url),
  "utf8",
);

/** Fill a session to exactly CAP events through ingest_batch. */
async function fillToCap(pool, tenantId, sessionId, base) {
  const stored = [];
  for (let b = 0; b < CAP / 500; b++) {
    const evs = [];
    for (let i = 0; i < 500; i++) {
      const n = b * 500 + i + 1;
      evs.push(
        event({
          tenantId,
          sessionId,
          eventId: randomUuid(),
          seq: n,
          time: new Date(base + n * 1000).toISOString(),
          name: "page_view",
          anonId: "a1",
        }),
      );
    }
    const r = await ingest(pool, evs);
    stored.push(...r.stored);
  }
  return stored;
}

test("T28a: deferred summaries do not weaken the cap — the guard is the quota authority", async () => {
  await withTestDb("t28a", async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const base = recentBase();
    const stored = await fillToCap(pool, tenantId, sessionId, base);
    assert.equal(stored.length, CAP);

    // Simulate deferred (never-run) summary maintenance: no active row, no
    // history, no stats. Only the guard exists — as it would between an async
    // association and its first summary pass.
    const c = await pool.connect();
    try {
      await c.query(
        `DELETE FROM sessions_active WHERE tenant_id = $1 AND session_id = $2`,
        [tenantId, sessionId],
      );

      // The cap still holds, read from the guard: 10 more events are capped.
      const r = await ingest(
        pool,
        Array.from({ length: 10 }, (_, i) =>
          event({
            tenantId,
            sessionId,
            eventId: randomUuid(),
            seq: CAP + i + 1,
            time: new Date(base + (CAP + i + 1) * 1000).toISOString(),
            name: "click",
            anonId: "a1",
          }),
        ),
      );
      assert.equal(r.capped.length, 10, "all overflow events session_capped");
      assert.equal(r.stored.length, 0);
      const g = await c.query(
        `SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2`,
        [tenantId, sessionId],
      );
      assert.equal(Number(g.rows[0].total_events), CAP, "guard pinned at the cap");

      // Retries of capped events remain session_capped (D1/D9): the cap is
      // synchronous and idempotency never converts a rejection.
      const retry = await ingest(pool, [
        event({
          tenantId,
          sessionId,
          eventId: r.capped[0],
          seq: CAP + 1,
          time: new Date(base + (CAP + 1) * 1000).toISOString(),
          name: "click",
          anonId: "a1",
        }),
      ]);
      assert.deepEqual(retry.capped, [r.capped[0]], "capped retry stays session_capped");
      assert.equal(retry.stored.length, 0);
      assert.equal(retry.dups.length, 0, "no dedupe claim was made for capped events");
    } finally {
      c.release();
    }
  });
});

test("T28b: an out-of-contract writer (no guard claim) trips the invariants loudly", async () => {
  await withTestDb("t28b", async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const base = recentBase();
    const stored = await fillToCap(pool, tenantId, sessionId, base);
    assert.equal(stored.length, CAP);

    const c = await pool.connect();
    try {
      // The ONE thing the contract forbids: a P4-style async association
      // inserting event rows directly, without claiming room from the guard.
      // The result is over-cap and MUST be detectable, never silent. Two
      // writer shapes, both loud:
      //   (a) events inserted, guard untouched → guard and events diverge (I3);
      //   (b) events inserted AND the guard bumped to match → the counter
      //       itself exceeds the cap (I2).
      await c.query(
        `INSERT INTO events (time, tenant_id, session_id, event_id, cat, anon_id, name, received_at)
         SELECT now() + make_interval(secs => s), $1, $2, $3, 'e', 'a4', 'click', now()
           FROM generate_series(1, 7) s`,
        [tenantId, sessionId, randomUuid()],
      );
      const i3a = await c.query(i3);
      assert.equal(
        Number(i3a.rows[0].n),
        1,
        "I3 must flag guard/event divergence (shape a)",
      );

      await c.query(
        `UPDATE session_guard SET total_events = total_events + 7
          WHERE tenant_id = $1 AND session_id = $2`,
        [tenantId, sessionId],
      );
      const i2r = await c.query(i2);
      assert.equal(
        Number(i2r.rows[0].n),
        1,
        "I2 must flag the counter above the cap (shape b)",
      );
      const i3b = await c.query(i3);
      assert.equal(
        Number(i3b.rows[0].n),
        0,
        "shape b is internally consistent — only I2 catches it",
      );
    } finally {
      c.release();
    }
  });
});

test("T28c: deferred summaries converge (reconcile) — the guard stays the single authority", async () => {
  await withTestDb("t28c", async ({ pool }) => {
    const tenantId = randomUuid();
    const sessionId = randomUuid();
    const base = recentBase();
    const stored = await fillToCap(pool, tenantId, sessionId, base);
    assert.equal(stored.length, CAP);

    const c = await pool.connect();
    try {
      // Summaries never ran; force the deferred maintenance pass now.
      await forceArchive(pool);
      await c.query("CALL reconcile_session_stats(NULL, NULL)");

      // Invariants all close: the cap was never weakened and the summaries
      // agree with the guard again.
      const i2r = await c.query(i2);
      assert.equal(Number(i2r.rows[0].n), 0, "I2: cap never exceeded");
      const i3r = await c.query(i3);
      assert.equal(
        Number(i3r.rows[0].n),
        0,
        "I3: guard matches events after the summary pass",
      );
      const hist = await c.query(
        `SELECT total_events FROM sessions_history WHERE tenant_id = $1 AND session_id = $2`,
        [tenantId, sessionId],
      );
      assert.equal(hist.rows.length, 1, "summary pass produced exactly one history row");
      assert.equal(
        Number(hist.rows[0].total_events),
        CAP,
        "history total equals the cap",
      );
    } finally {
      c.release();
    }
  });
});
