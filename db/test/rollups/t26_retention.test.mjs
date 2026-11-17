// T26c/d — P3-5 done-when part 2: "retention drops whole chunks".
//
//   * T26c: the 90 d tier is registered on exactly the five raw/rollup tables
//     (events, sessions_history, session_stats_15m, events_15m, identity_15m);
//     the 5 y tier on session_stats_daily/identity_daily has NO policy — it is
//     "none (upserted)" in the retention table of truth (07-operations.md);
//     the monthly tier is explicitly deferred out of P0–P4.
//   * T26d: drop_chunks drops WHOLE chunks (spiked on the pinned image: SELECT
//     drop_chunks — it is a function on 2.32, not a procedure; a 90-day-old
//     boundary day is retained, 91 days is not). The harness deletes policy_*
//     jobs for hermeticity, so the drop is invoked explicitly here.
//   * Also: the daily tables survive raw retention — the reason the 5 y tier
//     exists: identity_daily keeps HLL states long after the 15m sketches
//     and raw events are gone.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import pg from "pg";

import { applyMigrations, BOOTSTRAP_URL } from "../scripts/migrate.mjs";
import { removePolicyJobs, withTestDb } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T26c: 90 d retention on the five raw/rollup tables; none on the daily tier", async () => {
  // Manual DB lifecycle: the hermetic harness deletes policy_* jobs AFTER
  // migration, so this test reads them in that window (same pattern as T24e).
  const dbName = `sa_test_t26c_${randomUuid().replaceAll("-", "").slice(0, 12)}`;
  const boot = new pg.Client({ connectionString: BOOTSTRAP_URL });
  await boot.connect();
  try {
    await boot.query(`CREATE DATABASE ${dbName}`);
  } finally {
    await boot.end();
  }
  const url = await applyMigrations(dbName);
  const client = new pg.Client({ connectionString: url });
  try {
    await client.connect();
    const jobs = await client.query(
      `SELECT hypertable_name, config->>'drop_after' AS drop_after
         FROM timescaledb_information.jobs
        WHERE proc_name LIKE 'policy_retention%'
        ORDER BY hypertable_name`,
    );
    const byTable = new Map(jobs.rows.map((r) => [r.hypertable_name, r.drop_after]));
    for (const table of [
      "events",
      "sessions_history",
      "session_stats_15m",
      "events_15m",
      "identity_15m",
    ]) {
      assert.equal(
        byTable.get(table),
        "90 days",
        `${table} must carry a 90-day retention policy (got ${byTable.get(table)})`,
      );
    }
    // The 5 y tier is upserted daily rows with no deletion mechanism; the
    // monthly tier is explicitly out of P0–P4 scope.
    assert.equal(
      byTable.has("session_stats_daily"),
      false,
      "session_stats_daily must have no retention policy in P0–P4",
    );
    assert.equal(
      byTable.has("identity_daily"),
      false,
      "identity_daily must have no retention policy in P0–P4",
    );
  } finally {
    await client.end();
    await removePolicyJobs(url);
    const dropper = new pg.Client({ connectionString: BOOTSTRAP_URL });
    try {
      await dropper.connect();
      await dropper.query(`DROP DATABASE ${dbName}`);
    } finally {
      await dropper.end();
    }
  }
});

test("T26d: retention drops whole chunks; the 90-day boundary day is retained", async () => {
  await withTestDb("t26d", async ({ pool }) => {
    const tenant = randomUuid();
    const c = await pool.connect();
    try {
      // Events at 89 d, 90 d (boundary — retained), 91 d, 92 d (dropped).
      // 1-day chunks mean each of those days is exactly one chunk.
      for (const days of [89, 90, 91, 92]) {
        await c.query(
          `INSERT INTO events (time, tenant_id, session_id, event_id, cat, anon_id, name,
                                received_at)
           VALUES (now() - ($1 * INTERVAL '1 day'), $2, $3, $4, 'e', 'a', 'page_view',
                   now() - ($1 * INTERVAL '1 day'))`,
          [days, tenant, randomUuid(), randomUuid()],
        );
      }
      // An 89-day-old history row and a 100-day-old daily row (5 y tier).
      await c.query(
        `INSERT INTO sessions_history (tenant_id, session_id, home_day, anon_id,
             started_at, last_seen_at, event_count, total_events, heartbeats)
         VALUES ($1, $2, (now() - INTERVAL '91 days'), 'a', now() - INTERVAL '91 days',
                 now() - INTERVAL '91 days', 1, 1, 0)`,
        [tenant, randomUuid()],
      );
      await c.query(
        `INSERT INTO session_stats_daily (tenant_id, local_day, sessions, bounced,
             total_duration_s, heartbeat_s, events)
         VALUES ($1, (now() - INTERVAL '100 days')::date, 5, 2, 100, 10, 5)`,
        [tenant],
      );
      await c.query(
        `INSERT INTO identity_daily (tenant_id, local_day, anon_hll, users_hll)
         VALUES ($1, (now() - INTERVAL '100 days')::date,
                 (SELECT hyperloglog(4096, x) FROM unnest(ARRAY['v1','v2']) x),
                 (SELECT hyperloglog(4096, u) FROM unnest(ARRAY['u1']) u))`,
        [tenant],
      );

      // The retention mechanism itself: drop chunks older than 90 days.
      await c.query("SELECT drop_chunks('events', INTERVAL '90 days')");
      await c.query("SELECT drop_chunks('sessions_history', INTERVAL '90 days')");
      await c.query("SELECT drop_chunks('events_15m', INTERVAL '90 days')");
      await c.query("SELECT drop_chunks('identity_15m', INTERVAL '90 days')");

      // events: 89 d and the 90 d boundary survive; 91/92 d are gone whole.
      const evs = await c.query(
        `SELECT extract(day FROM now() - min(time))::int AS oldest,
                extract(day FROM now() - max(time))::int AS newest,
                count(*)::int AS n FROM events WHERE tenant_id = $1`,
        [tenant],
      );
      assert.equal(evs.rows[0].n, 2, "drop_chunks must drop whole chunks only");
      assert.equal(Number(evs.rows[0].oldest), 90, "the 90-day boundary day is retained");
      assert.equal(Number(evs.rows[0].newest), 89);

      // history: the 91-day-old row is gone.
      const hist = await c.query(
        `SELECT count(*)::int AS n FROM sessions_history WHERE tenant_id = $1`,
        [tenant],
      );
      assert.equal(hist.rows[0].n, 0, "91-day-old history row must be dropped");

      // The 5 y tier survives everything above.
      const daily = await c.query(
        `SELECT local_day::text AS day, sessions FROM session_stats_daily WHERE tenant_id = $1`,
        [tenant],
      );
      const expectedDay = await c.query(
        `SELECT (now() - INTERVAL '100 days')::date::text AS d`,
      );
      assert.equal(daily.rows.length, 1, "daily rows must survive raw retention");
      assert.equal(daily.rows[0].day, expectedDay.rows[0].d);
      const ident = await c.query(
        `SELECT distinct_count(rollup(anon_hll)) AS anon,
                distinct_count(rollup(users_hll)) AS users
           FROM identity_daily WHERE tenant_id = $1`,
        [tenant],
      );
      assert.equal(
        Number(ident.rows[0].anon),
        2,
        "HLL states must survive raw retention",
      );
      assert.equal(Number(ident.rows[0].users), 1);
    } finally {
      c.release();
    }
  });
});
