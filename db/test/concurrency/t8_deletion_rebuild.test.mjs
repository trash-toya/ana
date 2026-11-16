// T8 — deletion rebuild (P0 portion of the P4-8 workflow). Delete a tenant's sessions
// from sessions_history, rebuild the range: no stale bucket remains, the other tenant
// is untouched. The full seven-step workflow is exercised in P4 (T8 there covers this).
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, forceArchive } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();

test("T8: rebuild after deleting a tenant's sessions leaves no stale bucket", async () => {
  await withTestDb("t8", async ({ pool }) => {
    const tenantA = randomUuid();
    const tenantB = randomUuid();
    const BASE = Date.UTC(2026, 10, 15, 11, 0, 0);
    const c = await pool.connect();

    for (const [tenantId, nSessions] of [
      [tenantA, 3],
      [tenantB, 2],
    ]) {
      for (let s = 0; s < nSessions; s++) {
        const sessionId = randomUuid();
        const batch = [];
        for (let i = 0; i < 4; i++) {
          batch.push(
            event({
              tenantId,
              sessionId,
              eventId: randomUuid(),
              seq: i + 1,
              time: new Date(BASE + i * 60_000),
              name: "page_view",
              cat: "e",
            }),
          );
        }
        await ingest(c, batch);
      }
    }

    await forceArchive(pool);

    const before = await c.query(
      "SELECT tenant_id, sum(sessions) AS s FROM session_stats_15m GROUP BY 1 ORDER BY 1",
    );
    const aBefore = before.rows.find((r) => r.tenant_id === tenantA);
    const bBefore = before.rows.find((r) => r.tenant_id === tenantB);
    assert.equal(Number(aBefore.s), 3);
    assert.equal(Number(bBefore.s), 2);

    // Deletion step 2/3 (P0 form): remove tenant A's sessions and events directly.
    await c.query("DELETE FROM sessions_history WHERE tenant_id = $1", [tenantA]);
    await c.query("DELETE FROM sessions_active  WHERE tenant_id = $1", [tenantA]);
    await c.query("DELETE FROM session_guard    WHERE tenant_id = $1", [tenantA]);
    await c.query("DELETE FROM event_dedupe     WHERE tenant_id = $1", [tenantA]);
    await c.query("DELETE FROM events           WHERE tenant_id = $1", [tenantA]);
    await c.query("DELETE FROM identity_map     WHERE tenant_id = $1", [tenantA]);

    // Rebuild the covered range; buckets that only held tenant A disappear.
    const range = await c.query(
      `SELECT min(started_at) AS lo, max(started_at) AS hi FROM sessions_history`,
    );
    await c.query("CALL rebuild_session_stats($1, $2, $3)", [
      null,
      new Date(new Date(range.rows[0].lo).getTime() - 3_600_000),
      new Date(new Date(range.rows[0].hi).getTime() + 3_600_000),
    ]);

    const after = await c.query(
      "SELECT tenant_id, sum(sessions) AS s FROM session_stats_15m GROUP BY 1 ORDER BY 1",
    );
    assert.equal(after.rows.length, 1, "tenant A left no stale bucket");
    assert.equal(after.rows[0].tenant_id, tenantB);
    assert.equal(Number(after.rows[0].s), 2, "tenant B untouched");

    // In-range aggregation matches history for B.
    const check = await c.query(
      `SELECT count(*) AS n FROM (
         SELECT tenant_id, time_bucket('15 minutes', started_at) AS bucket, count(*) AS k,
                sum(event_count) AS ev FROM sessions_history GROUP BY 1, 2
         EXCEPT ALL
         SELECT tenant_id, bucket, sessions, events FROM session_stats_15m) d`,
    );
    assert.equal(Number(check.rows[0].n), 0);
    c.release();
  });
});
