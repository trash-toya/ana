// End-to-end: validateBatch → ingest_batch() (real DB) → assembleOutcomes.
// This is the flush pipeline minus buffering (P1-4 adds the buffer in front
// of exactly these steps).
import { test } from "vitest";
import assert from "node:assert/strict";
import { withTestDb, ingest } from "../../../db/test/harness.mjs";
import { seedTenantWithKeys } from "./helpers.mjs";
import { assembleOutcomes, validateBatch } from "../dist/validate.js";

const SESSION = "018f2b00-0000-7000-8000-0000000000c7";

function envelope(events, sentAt) {
  return {
    schema_version: 1,
    anon_id: "a_7f3c9e",
    sent_at: sentAt,
    events,
  };
}

let n = 0;
function ev(overrides = {}) {
  return {
    event_id: `018f2c00-0000-7000-8000-${(++n).toString(16).padStart(12, "0")}`,
    session_id: SESSION,
    time: new Date(Date.now() - 60_000).toISOString(),
    name: "click",
    ...overrides,
  };
}

test("validated rows store through the real ingest_batch and assemble as accepted", async () => {
  await withTestDb("ingest_e2e", async ({ pool }) => {
    const { tenantId } = await seedTenantWithKeys(pool);
    const receivedAt = new Date();
    const sentAt = new Date(receivedAt.getTime() - 10_000).toISOString();

    const batch = [
      ev(),
      ev({ name: "heartbeat" }),
      ev({ name: "session_start", props: { utm_source: "news" } }),
      ev({ user_id: "u1", url: "/pricing", props: { el: "cta" }, seq: 3 }),
    ];
    const outcome = validateBatch(envelope(batch, sentAt), { tenantId, receivedAt });
    assert.equal(outcome.rows.length, 4);

    const client = await pool.connect();
    try {
      const result = await ingest(client, outcome.rows);
      const assembled = assembleOutcomes(outcome, result);
      assert.equal(assembled.accepted.length, 4);
      assert.equal(assembled.rejected.length, 0);
    } finally {
      client.release();
    }

    const { rows: evCount } = await pool.query(
      "SELECT count(*)::int AS n, count(DISTINCT cat)::int AS cats FROM events WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(evCount[0].n, 4);
    assert.ok(evCount[0].cats >= 2); // click=e, heartbeat=s, session_start=l

    const { rows: sess } = await pool.query(
      "SELECT event_count, total_events, user_id FROM sessions_active WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(sess.length, 1);
    assert.equal(sess[0].event_count, 2); // engagement only: 2 clicks (heartbeat=s, session_start=l)
    assert.equal(sess[0].total_events, 4);
    assert.equal(sess[0].user_id, "u1"); // first non-null user_id (first-wins)
  });
});

test("a retry of the same batch is accepted again without storing twice", async () => {
  await withTestDb("ingest_e2e_retry", async ({ pool }) => {
    const { tenantId } = await seedTenantWithKeys(pool);
    const receivedAt = new Date();
    const sentAt = new Date(receivedAt.getTime() - 10_000).toISOString();
    const batch = [ev(), ev()];

    const client = await pool.connect();
    try {
      const first = assembleOutcomes(
        validateBatch(envelope(batch, sentAt), { tenantId, receivedAt }),
        await ingest(
          client,
          validateBatch(envelope(batch, sentAt), { tenantId, receivedAt }).rows,
        ),
      );
      assert.equal(first.accepted.length, 2);

      // exact retry: same ids, same payloads — reported as accepted (dups)
      const retryOutcome = validateBatch(envelope(batch, sentAt), {
        tenantId,
        receivedAt,
      });
      const retry = assembleOutcomes(
        retryOutcome,
        await ingest(client, retryOutcome.rows),
      );
      assert.equal(retry.accepted.length, 2);
      assert.equal(retry.rejected.length, 0);

      const { rows: c } = await pool.query(
        "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1",
        [tenantId],
      );
      assert.equal(c[0].n, 2); // idempotent: no double store
    } finally {
      client.release();
    }
  });
});

test("same id, different payload across requests → conflicting_duplicate", async () => {
  await withTestDb("ingest_e2e_conflict", async ({ pool }) => {
    const { tenantId } = await seedTenantWithKeys(pool);
    const receivedAt = new Date();
    const sentAt = new Date(receivedAt.getTime() - 10_000).toISOString();
    const id = "018f2c00-0000-7000-8000-0000000000f1";
    const v1 = {
      event_id: id,
      session_id: SESSION,
      time: new Date(Date.now() - 60_000).toISOString(),
      name: "click",
      props: { a: 1 },
    };
    const v2 = { ...v1, props: { a: 2 } };

    const client = await pool.connect();
    try {
      const first = assembleOutcomes(
        validateBatch(envelope([v1], sentAt), { tenantId, receivedAt }),
        await ingest(
          client,
          validateBatch(envelope([v1], sentAt), { tenantId, receivedAt }).rows,
        ),
      );
      assert.deepEqual(first.accepted, [id]);

      const secondOutcome = validateBatch(envelope([v2], sentAt), {
        tenantId,
        receivedAt,
      });
      const second = assembleOutcomes(
        secondOutcome,
        await ingest(client, secondOutcome.rows),
      );
      assert.deepEqual(second.accepted, []);
      assert.deepEqual(second.rejected, [
        { event_id: id, reason: "conflicting_duplicate" },
      ]);
    } finally {
      client.release();
    }
  });
});
