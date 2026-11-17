// I10 — tenant event-name cardinality monitor (P4-3). The query is a monitor,
// not an invariant: it is expected to return rows when abuse or a client bug
// happens, and the alert threshold is documented in 07-operations.md.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import { withTestDb, ingest, event, recentBase } from "../harness.mjs";

const MONITOR = fs.readFileSync(
  new URL("../../sql/monitors/tenant_event_cardinality.sql", import.meta.url),
  "utf8",
);

async function seedNames(pool, tenantId, count) {
  const sessionId = crypto.randomUUID();
  const time = new Date(recentBase());
  const events = Array.from({ length: count }, (_, i) =>
    event({
      tenantId,
      sessionId,
      eventId: crypto.randomUUID(),
      seq: i,
      time,
      name: `synthetic_${i}`,
      receivedAt: new Date(),
    }),
  );
  const client = await pool.connect();
  try {
    const result = await ingest(client, events);
    assert.equal(result.stored.length, count, "every seeded event is stored");
  } finally {
    client.release();
  }
  return sessionId;
}

test("I10: flags a tenant above 100 distinct event names per hour only", async () => {
  await withTestDb("i10_cardinality", async ({ pool }) => {
    const noisy = crypto.randomUUID();
    const quiet = crypto.randomUUID();

    await seedNames(pool, noisy, 101);
    await seedNames(pool, quiet, 100);

    const { rows } = await pool.query(MONITOR);
    assert.equal(rows.length, 1, "only the tenant above the threshold is returned");
    assert.equal(rows[0].tenant_id, noisy);
    assert.equal(rows[0].event_names, 101);
  });
});

test("I10: ignores names outside the one-hour receipt window", async () => {
  await withTestDb("i10_window", async ({ pool }) => {
    const tenantId = crypto.randomUUID();
    const sessionId = crypto.randomUUID();
    const time = new Date(recentBase());
    const old = new Date(Date.now() - 2 * 3_600_000);
    const events = Array.from({ length: 101 }, (_, i) =>
      event({
        tenantId,
        sessionId,
        eventId: crypto.randomUUID(),
        seq: i,
        time,
        name: `old_${i}`,
        receivedAt: old,
      }),
    );
    const client = await pool.connect();
    try {
      await ingest(client, events);
    } finally {
      client.release();
    }

    const { rows } = await pool.query(MONITOR);
    assert.deepEqual(rows, [], "stale names do not alert");
  });
});
