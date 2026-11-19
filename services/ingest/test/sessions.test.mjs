// Rev 5 server-first ingestion: session bootstrap/resume, signed context,
// provenance authorization and sessionless events through the real app.
import { test } from "vitest";
import assert from "node:assert/strict";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { Flusher } from "../dist/flusher.js";
import { MIN_CONTEXT_SECRET_LENGTH, SessionService } from "../dist/sessions.js";
import { seedTenantWithKeys } from "./helpers.mjs";

const SECRET = "test-secret-that-is-long-enough";
const T0 = Date.UTC(2026, 10, 15, 12, 0, 0);

function makeSessions(pool, clock = { now: new Date(T0) }) {
  return {
    service: new SessionService({ pool, secret: SECRET, now: () => clock.now }),
    clock,
  };
}

function event(overrides = {}) {
  return {
    event_id: crypto.randomUUID(),
    session_id: null,
    time: new Date(Date.now() - 60_000).toISOString(),
    name: "order_placed",
    ...overrides,
  };
}

function envelope(events, overrides = {}) {
  return {
    schema_version: 1,
    sent_at: new Date().toISOString(),
    events,
    ...overrides,
  };
}

test("bootstrap: create, resume, generate anon_id, reject client keys and bad bodies", async () => {
  await withTestDb("ingest_sessions", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = makeSessions(pool);
    const app = createApp({ pool, sessions: sessions.service });

    // Server key required: a public client key cannot mint sessions.
    const denied = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.ingestKey)
      .send({ user_id: "u1" });
    assert.equal(denied.status, 403);
    assert.equal(denied.body.error, "server_key_required");

    const malformed = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ user_id: 42 });
    assert.equal(malformed.status, 400);

    const created = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ user_id: "u1", properties: { device: "desktop", utm: { source: "hn" } } });
    assert.equal(created.status, 200);
    assert.equal(created.body.resumed, false);
    assert.equal(created.body.emitted_events, 0);
    assert.match(created.body.session_id, /^[0-9a-f-]{36}$/);
    assert.ok(created.body.anon_id, "anon_id is generated when absent");
    assert.match(created.body.context, /^v1\.[0-9a-f-]{36}\.\d+\.[A-Za-z0-9_-]+$/);
    assert.equal(created.body.properties.device, "desktop");

    const resumed = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ anon_id: created.body.anon_id, user_id: "u1" });
    assert.equal(resumed.status, 200);
    assert.equal(resumed.body.resumed, true);
    assert.equal(resumed.body.session_id, created.body.session_id);
  });
});

test("validate: valid, tampered, malformed, wrong tenant, expired", async () => {
  await withTestDb("ingest_validate_session", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = makeSessions(pool);
    const app = createApp({ pool, sessions: sessions.service });

    const created = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ anon_id: "a1", user_id: "u1" });
    const { context, session_id } = created.body;

    const ok = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", keys.serverKey)
      .send({ context });
    assert.equal(ok.status, 200);
    assert.equal(ok.body.valid, true);
    assert.equal(ok.body.session_id, session_id);
    assert.equal(ok.body.emitted_events, 0);

    const malformed = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", keys.serverKey)
      .send({ context: "not-a-token" });
    assert.equal(malformed.body.valid, false);
    assert.equal(malformed.body.reason, "malformed");

    const parts = context.split(".");
    parts[3] = `${parts[3].slice(0, -2)}aa`;
    const tampered = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", keys.serverKey)
      .send({ context: parts.join(".") });
    assert.equal(tampered.body.valid, false);
    assert.equal(tampered.body.reason, "bad_signature");

    // A second tenant reusing the token: the MAC is tenant-bound.
    const other = await seedTenantWithKeys(pool);
    const crossTenant = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", other.serverKey)
      .send({ context });
    assert.equal(crossTenant.body.valid, false);
    assert.equal(crossTenant.body.reason, "bad_signature");

    // Clock: past the hard age → the token is expired.
    sessions.clock.now = new Date(T0 + 24 * 60 * 60 * 1000 + 1000);
    const expired = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", keys.serverKey)
      .send({ context });
    assert.equal(expired.body.valid, false);
    assert.equal(expired.body.reason, "expired");
  });
});

test("validate: a valid token for a session that idled out is rejected", async () => {
  await withTestDb("ingest_validate_idle", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = makeSessions(pool);
    const app = createApp({ pool, sessions: sessions.service });

    const created = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ anon_id: "a1" });
    // No events, then 31 minutes pass: the DB idle rule rejects it even though
    // the token's hard-age expiry is still in the future.
    sessions.clock.now = new Date(T0 + 31 * 60 * 1000);
    const res = await request(app)
      .post("/v1/sessions/validate")
      .set("X-API-Key", keys.serverKey)
      .send({ context: created.body.context });
    assert.equal(res.body.valid, false);
    assert.equal(res.body.reason, "expired");
    assert.ok(res.body.expires_at);
  });
});

test("source authorization: client keys cannot forge server/webhook provenance", async () => {
  await withTestDb("ingest_sources", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = makeSessions(pool);
    const app = createApp({ pool, sessions: sessions.service });

    const forged = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .send(envelope([event({ source: "server" })], { anon_id: "a1" }));
    assert.equal(forged.status, 403);
    assert.equal(forged.body.error, "source_not_allowed");

    const n = await pool.query("SELECT count(*) AS n FROM events WHERE tenant_id = $1", [
      keys.tenantId,
    ]);
    assert.equal(Number(n.rows[0].n), 0, "a rejected batch stores nothing");
  });
});

test("end to end: server key emits a sessionless webhook event and a session-bound event", async () => {
  await withTestDb("ingest_server_e2e", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = makeSessions(pool);
    const flusher = new Flusher({ pool, flushIdleMs: 10, flushIntervalMs: 1_000 });
    const app = createApp({ pool, flusher, sessions: sessions.service });

    // Bootstrap, then emit the first event for that session (server source).
    const created = await request(app)
      .post("/v1/sessions")
      .set("X-API-Key", keys.serverKey)
      .send({ user_id: "u1", properties: { device: "mobile", country: "ES" } });

    const now = new Date();
    const bound = event({
      session_id: created.body.session_id,
      event_id: crypto.randomUUID(),
      time: now.toISOString(),
      name: "page_view",
      source: "server",
      seq: 1,
      user_id: "u1",
    });
    const loose = event({
      session_id: null,
      event_id: crypto.randomUUID(),
      time: now.toISOString(),
      name: "payment_succeeded",
      source: "webhook",
      props: { provider: "stripe" },
    });

    const res = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.serverKey)
      .send(envelope([bound, loose], { anon_id: created.body.anon_id }));
    assert.equal(res.status, 200);
    assert.equal(res.body.accepted.length, 2);
    assert.deepEqual(res.body.rejected, []);

    const rows = await pool.query(
      `SELECT event_id, session_id, source, received_at IS NOT NULL AS has_received
         FROM events WHERE tenant_id = $1 ORDER BY source`,
      [keys.tenantId],
    );
    assert.equal(rows.rows.length, 2);
    const webhook = rows.rows.find((r) => r.source === "webhook");
    const server = rows.rows.find((r) => r.source === "server");
    assert.ok(webhook && server);
    assert.equal(webhook.session_id, null);
    assert.equal(server.session_id, created.body.session_id);
    assert.equal(webhook.has_received, true);
    assert.equal(server.has_received, true);

    // Replaying the webhook (same event_id) is an accepted duplicate.
    const replay = await request(app)
      .post("/v1/events")
      .set("X-API-Key", keys.serverKey)
      .send(envelope([loose]));
    assert.deepEqual(replay.body.accepted, [loose.event_id]);
    const count = await pool.query(
      "SELECT count(*) AS n FROM events WHERE tenant_id = $1 AND event_id = $2",
      [keys.tenantId, loose.event_id],
    );
    assert.equal(Number(count.rows[0].n), 1, "webhook idempotency: one stored event");

    await flusher.shutdown();
  });
});

test("session service: a short secret is a configuration error", () => {
  assert.throws(
    () =>
      new SessionService({ pool: {}, secret: "x".repeat(MIN_CONTEXT_SECRET_LENGTH - 1) }),
    /at least/,
  );
});


test("bootstrap: concurrent first requests for one visitor converge on one session", async () => {
  await withTestDb("ingest_sessions_bootstrap_race", async ({ pool }) => {
    const sessions = makeSessions(pool);
    const tenantId = crypto.randomUUID();
    const anonId = "same-visitor";
    const [left, right] = await Promise.all([
      sessions.service.start(tenantId, { anon_id: anonId, user_id: "u1" }),
      sessions.service.start(tenantId, { anon_id: anonId, user_id: "u1" }),
    ]);
    assert.equal(left.session_id, right.session_id);
    assert.equal([left.resumed, right.resumed].filter(Boolean).length, 1);
    const count = await pool.query(
      "SELECT count(*)::int AS n FROM session_bootstrap WHERE tenant_id = $1 AND anon_id = $2",
      [tenantId, anonId],
    );
    assert.equal(count.rows[0].n, 1);
  });
});
