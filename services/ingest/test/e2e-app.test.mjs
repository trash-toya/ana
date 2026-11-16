// App-level tests with the real DB and the real buffer+flush pipeline:
// concurrent requests, per-request outcome partitioning, and the G1
// criterion "no acknowledged batch lost on a forced 503".
import { test } from "vitest";
import assert from "node:assert/strict";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { Flusher } from "../dist/flusher.js";
import { seedTenantWithKeys } from "./helpers.mjs";

/** Poll a condition with real timers (deterministic test choreography). */
async function waitFor(condition, { timeoutMs = 5_000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("waitFor: condition not met in time");
    await new Promise((r) => setTimeout(r, stepMs));
  }
}

const SESSION = "018f2b00-0000-7000-8000-0000000000c7";

let n = 0;
function ev() {
  return {
    event_id: `018f2c00-0000-7000-8000-${(++n).toString(16).padStart(12, "0")}`,
    session_id: SESSION,
    time: new Date(Date.now() - 60_000).toISOString(),
    name: "click",
  };
}

function envelope(events) {
  return {
    schema_version: 1,
    anon_id: "a1",
    sent_at: new Date().toISOString(),
    events,
  };
}

test("concurrent requests: every event lands in exactly one list and stores once", async () => {
  await withTestDb("ingest_app_conc", async ({ pool }) => {
    const { tenantId, ingestKey } = await seedTenantWithKeys(pool);
    const flusher = new Flusher({ pool, flushIdleMs: 10, flushIntervalMs: 1_000 });
    const app = createApp({ pool, flusher });

    // 5 requests × 20 events, fired concurrently
    const posts = Array.from({ length: 5 }, () =>
      request(app)
        .post("/v1/events")
        .set("X-API-Key", ingestKey)
        .send(envelope(Array.from({ length: 20 }, () => ev()))),
    );
    const responses = await Promise.all(posts);
    for (const r of responses) {
      assert.equal(r.status, 200);
      assert.equal(r.body.rejected.length, 0);
      assert.equal(r.body.accepted.length, 20);
    }
    // every event id reported exactly once across responses
    const reported = responses.flatMap((r) => r.body.accepted);
    assert.equal(new Set(reported).size, 100);

    const { rows } = await pool.query(
      "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(rows[0].n, 100);
    await flusher.shutdown();
  });
});

test("forced 503 (BUFFER_MAX): acknowledged events stored, unacknowledged retry succeeds", async () => {
  await withTestDb("ingest_app_503", async ({ pool }) => {
    const { tenantId, ingestKey } = await seedTenantWithKeys(pool);
    // A slow first flush occupies the buffer so a second concurrent request
    // trips BUFFER_MAX (25 + 25 > 30).
    let release;
    const gate = new Promise((r) => (release = r));
    const realCall = async (rows) => {
      const client = await pool.connect();
      try {
        const res = await client.query("SELECT ingest_batch($1) AS r", [
          JSON.stringify(rows),
        ]);
        const r = res.rows[0].r;
        return {
          stored: r.stored ?? [],
          dups: r.dups ?? [],
          capped: r.capped ?? [],
          conflicts: r.conflicts ?? [],
        };
      } finally {
        client.release();
      }
    };
    const flusher = new Flusher({
      pool,
      bufferMax: 30,
      flushIdleMs: 5,
      flushIntervalMs: 60_000,
      submitBatch: async (rows) => {
        await gate;
        return realCall(rows);
      },
    });

    const app = createApp({ pool, flusher });
    const batchA = Array.from({ length: 25 }, () => ev());
    const batchB = Array.from({ length: 25 }, () => ev());

    // supertest chains are lazy — the request only fires once a .then is
    // attached. Capture the promise immediately so A is actually in flight.
    const a = request(app)
      .post("/v1/events")
      .set("X-API-Key", ingestKey)
      .send(envelope(batchA));
    const aPromise = a.then((r) => r);
    // deterministic: wait until A's 25 rows are inside the flusher (buffered
    // or in flight) before posting B — then B always trips BUFFER_MAX
    await waitFor(() => flusher.pending === 25);
    const b = request(app)
      .post("/v1/events")
      .set("X-API-Key", ingestKey)
      .send(envelope(batchB));
    const bPromise = b.then((r) => r);

    // B trips the buffer limit immediately (A's flush is still in flight)
    const resB = await bPromise;
    assert.equal(resB.status, 503);
    assert.equal(resB.headers["retry-after"], "5");
    assert.equal(resB.body.error, "buffer_full");

    // A commits only once the (artificially slowed) flush completes
    release();
    const resA = await aPromise;
    assert.equal(resA.status, 200);
    assert.equal(resA.body.accepted.length, 25);

    // Nothing acknowledged was lost: A's 25 events are all in the DB.
    const { rows: afterA } = await pool.query(
      "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(afterA[0].n, 25);

    // B was never acknowledged; the SDK retries it (dedupe makes this safe)
    const retry = await request(app)
      .post("/v1/events")
      .set("X-API-Key", ingestKey)
      .send(envelope(batchB));
    assert.equal(retry.status, 200);
    assert.equal(retry.body.accepted.length, 25);

    const { rows: total } = await pool.query(
      "SELECT count(*)::int AS n FROM events WHERE tenant_id = $1",
      [tenantId],
    );
    assert.equal(total[0].n, 50);
    await flusher.shutdown();
  });
});

test("validation rejections merge with DB outcomes in one response", async () => {
  await withTestDb("ingest_app_mixed", async ({ pool }) => {
    const { ingestKey } = await seedTenantWithKeys(pool);
    const app = createApp({ pool, flusher: new Flusher({ pool, flushIdleMs: 5 }) });

    const good = ev();
    const badId = { ...ev(), event_id: "not-a-uuid" };
    const badName = { ...ev(), name: "Click" };
    const res = await request(app)
      .post("/v1/events")
      .set("X-API-Key", ingestKey)
      .send(envelope([good, badId, badName]));

    assert.equal(res.status, 200);
    assert.deepEqual(res.body.accepted, [good.event_id]);
    const byId = Object.fromEntries(res.body.rejected.map((r) => [r.event_id, r.reason]));
    assert.equal(byId["not-a-uuid"], "invalid_uuid");
    assert.equal(byId[badName.event_id], "bad_name");
  });
});
