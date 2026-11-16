import test from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { deadPool, seedTenantWithKeys } from "./helpers.mjs";

const SESSION = "018f2b00-0000-7000-8000-0000000000c7";
let eventCounter = 0;

function event() {
  const n = (++eventCounter).toString(16).padStart(4, "0");
  return {
    event_id: `018f2b00-0000-7000-8000-00000000${n}`,
    session_id: SESSION,
    seq: 1,
    time: new Date(Date.now() - 60_000).toISOString(), // 1 min old: safely in-window
    name: "click",
  };
}

test("key auth: missing, unknown, wrong kind, revoked, valid", async () => {
  await withTestDb("ingest_auth", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const app = createApp({ pool });

    const missing = await request(app)
      .post("/v1/events")
      .send({ schema_version: 1, anon_id: "a1", sent_at: new Date().toISOString(), events: [] });
    assert.equal(missing.status, 401);
    assert.equal(missing.body.error, "missing_key");

    const unknown = await request(app)
      .post("/v1/events")
      .set("X-API-Key", "sk_unknown_" + Math.random().toString(36).slice(2))
      .send({ schema_version: 1, anon_id: "a1", sent_at: new Date().toISOString(), events: [event()] });
    assert.equal(unknown.status, 401);
    assert.equal(unknown.body.error, "invalid_key");

    const wrongKind = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.dashboardKey)
      .send({ schema_version: 1, anon_id: "a1", sent_at: new Date().toISOString(), events: [event()] });
    assert.equal(wrongKind.status, 401);

    const revoked = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.revokedKey)
      .send({ schema_version: 1, anon_id: "a1", sent_at: new Date().toISOString(), events: [event()] });
    assert.equal(revoked.status, 403);
    assert.equal(revoked.body.error, "key_revoked");

    // valid key passes auth and the whole write path: the event flushes to
    // the DB and the response carries the per-event outcome
    const valid = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .send({ schema_version: 1, anon_id: "a1", sent_at: new Date().toISOString(), events: [event()] });
    assert.equal(valid.status, 200);
    assert.deepEqual(valid.body.rejected, []);
    assert.equal(valid.body.accepted.length, 1);
  });
});

test("submit seam: injected function receives tenantId and validated rows", async () => {
  await withTestDb("ingest_submit", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const seen = [];
    const app = createApp({
      pool,
      submit: async (r) => {
        seen.push(r);
        return {
          stored: ["018f2b00-0000-7000-8000-0000000000e1"],
          dups: [],
          capped: ["018f2b00-0000-7000-8000-0000000000e2"],
          conflicts: [],
        };
      },
    });
    const id1 = "018f2b00-0000-7000-8000-0000000000e1";
    const id2 = "018f2b00-0000-7000-8000-0000000000e2";
    const recent = new Date(Date.now() - 60_000).toISOString();
    const res = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .send({
        schema_version: 1,
        anon_id: "a1",
        sent_at: new Date().toISOString(),
        events: [
          { event_id: id1, session_id: SESSION, time: recent, name: "click" },
          { event_id: id2, session_id: SESSION, time: recent, name: "click" },
        ],
      });
    assert.equal(res.status, 200);
    // e1 stored → accepted; e2 capped → rejected with its reason
    assert.deepEqual(res.body, {
      accepted: [id1],
      rejected: [{ event_id: id2, reason: "session_capped" }],
    });
    assert.equal(seen.length, 1);
    assert.equal(seen[0].tenantId, keys.tenantId);
    assert.equal(seen[0].outcome.rows.length, 2);
    assert.equal(seen[0].outcome.rows[0].event_id, id1);
    assert.ok(seen[0].receivedAt instanceof Date);
  });
});

test("malformed JSON is 400, oversized body is 413", async () => {
  await withTestDb("ingest_body", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const app = createApp({ pool });

    const bad = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .set("Content-Type", "application/json")
      .send('{"events":');
    assert.equal(bad.status, 400);
    assert.equal(bad.body.error, "bad_json");

    const big = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .set("Content-Type", "application/json")
      .send("x".repeat(2 * 1024 * 1024));
    assert.equal(big.status, 413);
    assert.equal(big.body.error, "body_too_large");
  });
});

test("health endpoints do not require a key", async () => {
  const pool = deadPool(pg);
  try {
    const app = createApp({ pool });
    assert.equal((await request(app).get("/healthz")).status, 200);
    assert.equal((await request(app).get("/readyz")).status, 503);
  } finally {
    await pool.end();
  }
});
