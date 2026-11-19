// T13 — the same tenant/event_id racing across different sessions must
// serialize at the dedupe key and preserve payload-conflict semantics.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event } from "../harness.mjs";

test("T13: concurrent same event id with different payloads stores one and reports one conflict", async () => {
  await withTestDb("t13_event_race", async ({ pool }) => {
    const tenantId = crypto.randomUUID();
    const eventId = crypto.randomUUID();
    const now = new Date();
    const left = event({
      tenantId,
      sessionId: crypto.randomUUID(),
      eventId,
      seq: 1,
      time: now,
      props: { source_payload: "left" },
      ord: 0,
    });
    const right = event({
      tenantId,
      sessionId: crypto.randomUUID(),
      eventId,
      seq: 1,
      time: now,
      props: { source_payload: "right" },
      ord: 0,
    });
    const a = await pool.connect();
    const b = await pool.connect();
    try {
      const results = await Promise.all([ingest(a, [left]), ingest(b, [right])]);
      const stored = results.flatMap((r) => r.stored);
      const conflicts = results.flatMap((r) => r.conflicts);
      const duplicates = results.flatMap((r) => r.dups);
      assert.deepEqual(stored, [eventId]);
      assert.deepEqual(conflicts, [eventId]);
      assert.deepEqual(duplicates, []);

      const rows = await pool.query(
        "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1 AND event_id = $2",
        [tenantId, eventId],
      );
      assert.equal(rows.rows[0].n, 1);
    } finally {
      a.release();
      b.release();
    }
  });
});
