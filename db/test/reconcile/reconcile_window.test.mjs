// G0 criterion 4: reconcile_session_stats reports ZERO drift over a fully populated
// 26–50 h window (a ≥51 h simulated span), and DETECTS drift when it is injected.
// Data is placed directly in sessions_history at wall-clock-relative timestamps
// (now-49h … now-27h), rebuilt into session_stats_15m, and then the nightly job runs.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, assertInvariant } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

async function collectWarnings(client) {
  const warnings = [];
  client.on("notice", (n) => {
    if (n.severity === "WARNING") warnings.push(n.message);
  });
  return warnings;
}

test("reconcile: zero drift over a fully populated 26–50 h window; detects injected drift",
  async () => {
    await withTestDb("rec", async ({ pool }) => {
      const c = await pool.connect();
      try {
        const tenantId = randomUuid();

        // 88 sessions across the 22-hour span, one per 15-minute bucket, so the whole
        // comparison range is populated (≥51 h simulated: -49 h … -27 h + margins).
        const now = Date.now();
        for (let b = 0; b < 88; b++) {
          const startedAt = new Date(now - (49 - b * 0.25) * 3600_000);
          const lastSeenAt = new Date(startedAt.getTime() + 120_000);
          await c.query(
            `INSERT INTO sessions_history
               (tenant_id, session_id, home_day, anon_id, started_at, last_seen_at,
                event_count, total_events, heartbeats, user_id, user_at, entry_url,
                entry_at, device, country, utm, attr_at)
             VALUES ($1,$2,$3,'a1',$4,$5,2,2,1,null,null,null,null,null,null,null,null)`,
            [tenantId, randomUuid(), new Date(startedAt), startedAt, lastSeenAt],
          );
        }

        // Populate the stats exactly, then run the nightly reconcile: it must be silent.
        await c.query(
          "CALL rebuild_session_stats(NULL::uuid, now() - INTERVAL '50 hours', now() - INTERVAL '26 hours')");
        const warnings = await collectWarnings(c);
        await c.query("CALL reconcile_session_stats(NULL, NULL)");
        assert.equal(warnings.length, 0,
          `reconcile must be silent: ${JSON.stringify(warnings)}`);

        const agg1 = await c.query(
          "SELECT sum(sessions) AS s, sum(events) AS e FROM session_stats_15m");
        assert.equal(Number(agg1.rows[0].s), 88);
        assert.equal(Number(agg1.rows[0].e), 176);

        // Negative case: inject drift, reconcile must WARN and the rebuild inside
        // restores the aggregate (the job fixes its range by construction).
        await c.query(
          "UPDATE session_stats_15m SET sessions = sessions + 1 WHERE bucket = (SELECT min(bucket) FROM session_stats_15m)");
        await c.query("CALL reconcile_session_stats(NULL, NULL)");
        assert.equal(warnings.length, 1, "reconcile must warn on injected drift");

        const agg2 = await c.query(
          "SELECT sum(sessions) AS s FROM session_stats_15m");
        assert.equal(Number(agg2.rows[0].s), 88, "range restored after reconcile");

        await assertInvariant(c, "i4_one_history_row");
      } finally {
        c.release();
      }
    });
  });
