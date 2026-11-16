// T13 — session bootstrap/resume (Rev 5 §19.2, D30).
//
// The bootstrap record is separate from analytics rows; resume has three
// rotation rules (idle 30 min, age 24 h, user switch); concurrent bootstraps
// for one visitor return one session; only real events advance the idle clock.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const T0 = Date.UTC(2026, 10, 15, 12, 0, 0);

async function bootstrap(client, params) {
  const res = await client.query("SELECT bootstrap_session($1) AS r", [JSON.stringify(params)]);
  return res.rows[0].r;
}

async function lookup(client, tenantId, sessionId, now) {
  const res = await client.query("SELECT lookup_session($1, $2, $3) AS r", [
    tenantId,
    sessionId,
    new Date(now).toISOString(),
  ]);
  return res.rows[0].r;
}

test("T13: create, resume, idle/age/user rotation, and the event clock", async () => {
  await withTestDb("t13", async ({ pool }) => {
    const tenantId = randomUuid();
    const anonId = randomUuid();
    const c = await pool.connect();
    try {
      const first = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        user_id: null,
        properties: { device: "desktop", utm: { source: "hn" } },
        now: new Date(T0).toISOString(),
      });
      assert.equal(first.resumed, false);
      assert.equal(first.emitted_events, 0);
      assert.equal(first.properties.device, "desktop");
      assert.ok(first.session_id, "a session id is minted");

      // The guard exists before the session does (same invariant as ingest).
      const guard = await c.query(
        "SELECT total_events FROM session_guard WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, first.session_id],
      );
      assert.equal(guard.rows.length, 1);

      // Resume inside both windows returns the same session; resume is not activity.
      const resumed = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        now: new Date(T0 + 10 * 60_000).toISOString(),
      });
      assert.equal(resumed.resumed, true);
      assert.equal(resumed.session_id, first.session_id);
      assert.equal(
        new Date(resumed.last_event_at).toISOString(),
        new Date(first.last_event_at).toISOString(),
        "resume must not move the idle clock",
      );

      // A real event advances the clock and the stored count.
      const evt = event({
        tenantId,
        sessionId: first.session_id,
        eventId: randomUuid(),
        seq: 1,
        time: new Date(T0 + 12 * 60_000),
        name: "page_view",
        source: "server",
      });
      const r = await ingest(c, [evt]);
      assert.equal(r.stored.length, 1);

      const afterEvent = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        now: new Date(T0 + 20 * 60_000).toISOString(),
      });
      assert.equal(afterEvent.resumed, true);
      assert.equal(afterEvent.session_id, first.session_id);
      assert.equal(afterEvent.emitted_events, 1, "stored events count from session_guard");
      assert.equal(
        new Date(afterEvent.last_event_at).toISOString(),
        new Date(T0 + 12 * 60_000).toISOString(),
        "the event advanced the idle clock",
      );

      // Idle > 30 min rotates.
      const idle = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        now: new Date(T0 + 12 * 60_000 + 31 * 60_000).toISOString(),
      });
      assert.equal(idle.resumed, false);
      assert.notEqual(idle.session_id, first.session_id);

      // Age > 24 h rotates even when the last event is recent.
      const aged = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        now: new Date(T0 + 24 * 60 * 60_000 + 60_000).toISOString(),
      });
      assert.equal(aged.resumed, false);
      assert.notEqual(aged.session_id, idle.session_id);

      // Anonymous → signed-in keeps the session and records the user.
      const login = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        user_id: "user_7",
        now: new Date(T0 + 24 * 60 * 60_000 + 2 * 60_000).toISOString(),
      });
      assert.equal(login.resumed, true);
      assert.equal(login.session_id, aged.session_id);
      assert.equal(login.user_id, "user_7");

      // A different known user rotates (logout/login switch).
      const switched = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: anonId,
        user_id: "user_8",
        now: new Date(T0 + 24 * 60 * 60_000 + 3 * 60_000).toISOString(),
      });
      assert.equal(switched.resumed, false);
      assert.notEqual(switched.session_id, login.session_id);

      // Validation read: found, with live expiry and the stored event count.
      const found = await lookup(c, tenantId, aged.session_id, T0 + 24 * 60 * 60_000 + 4 * 60_000);
      assert.equal(found.found, true);
      assert.equal(found.emitted_events, 0);

      const missing = await lookup(c, tenantId, randomUuid(), T0);
      assert.equal(missing.found, false);
    } finally {
      c.release();
    }
  });
});

test("T13b: concurrent bootstraps for one visitor return one session", async () => {
  await withTestDb("t13b", async ({ pool }) => {
    const tenantId = randomUuid();
    const anonId = randomUuid();
    const now = new Date(T0).toISOString();
    const clients = await Promise.all(Array.from({ length: 8 }, () => pool.connect()));
    try {
      const results = await Promise.all(
        clients.map((client) =>
          client.query("BEGIN").then(() =>
            bootstrap(client, { tenant_id: tenantId, anon_id: anonId, now }).then(
              (r) => client.query("COMMIT").then(() => r),
            ),
          ),
        ),
      );
      const ids = new Set(results.map((r) => r.session_id));
      assert.equal(ids.size, 1, `expected one session, got ${ids.size}`);
      assert.equal(results.filter((r) => r.resumed === false).length, 1, "exactly one creator");

      const rows = await pool.query(
        "SELECT count(*) AS n FROM session_bootstrap WHERE tenant_id = $1 AND anon_id = $2",
        [tenantId, anonId],
      );
      assert.equal(Number(rows.rows[0].n), 1);
    } finally {
      for (const client of clients) client.release();
    }
  });
});

test("T13c: bootstrap guard is respected by the cap", async () => {
  await withTestDb("t13c", async ({ pool }) => {
    const tenantId = randomUuid();
    const c = await pool.connect();
    try {
      const s = await bootstrap(c, {
        tenant_id: tenantId,
        anon_id: randomUuid(),
        now: new Date(T0).toISOString(),
      });
      // Pretend the session is nearly full; the bootstrap-created guard is the
      // row the cap reads, so the next event is rejected rather than extending.
      await c.query(
        "UPDATE session_guard SET total_events = 5000 WHERE tenant_id = $1 AND session_id = $2",
        [tenantId, s.session_id],
      );
      const evt = event({
        tenantId, sessionId: s.session_id, eventId: randomUuid(), seq: 1,
        time: new Date(T0 + 60_000), name: "page_view",
      });
      const r = await ingest(c, [evt]);
      assert.equal(r.stored.length, 0);
      assert.deepEqual(r.capped, [evt.event_id]);
    } finally {
      c.release();
    }
  });
});
