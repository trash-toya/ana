// G1B: the server SDK against the real ingest app and Postgres.
//
// A backend bootstraps a session, emits a session-bound server event and a
// sessionless webhook event, and a replayed webhook stores once. This is the
// P1B-4/P1B-5 done-when at the HTTP boundary, not the SQL unit level.
import test from "node:test";
import assert from "node:assert/strict";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { Flusher } from "../dist/flusher.js";
import { SessionService } from "../dist/sessions.js";
import { ServerAnalytics } from "../../../packages/sdk-server/dist/index.js";
import { seedTenantWithKeys } from "./helpers.mjs";

const SECRET = "test-secret-that-is-long-enough";

async function listen(app) {
  const server = app.listen(0, "127.0.0.1");
  await new Promise((resolve) => server.once("listening", resolve));
  const address = server.address();
  return { server, base: `http://127.0.0.1:${address.port}` };
}

function close(server) {
  return new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}

test("G1B: bootstrap, session-bound server event, sessionless webhook, replay", async () => {
  await withTestDb("ingest_sdk_server", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const sessions = new SessionService({ pool, secret: SECRET });
    const flusher = new Flusher({ pool, flushIdleMs: 10, flushIntervalMs: 1_000 });
    const app = createApp({ pool, flusher, sessions });
    const { server, base } = await listen(app);

    const sdk = new ServerAnalytics({
      endpoint: base,
      apiKey: keys.serverKey,
      flushDebounceMs: 10,
    });

    try {
      const session = await sdk.startSession({
        userId: "user_1",
        properties: { device: "mobile", country: "ES", utm: { source: "hn" } },
      });
      assert.equal(session.resumed, false);
      assert.equal(session.emitted_events, 0);

      // The context the backend propagates to its own API validates.
      const validation = await sdk.validateSession(session.context);
      assert.equal(validation.valid, true);
      assert.equal(validation.session_id, session.session_id);

      const orderId = sdk.track("order_placed", {
        props: { order_id: "o_42", amount: 49.9 },
      });

      const providerId = "0199b0a0-0000-7000-8000-0000000000aa";
      sdk.track("payment_succeeded", {
        sessionId: null,
        source: "webhook",
        eventId: providerId,
        props: { provider: "stripe" },
      });

      await sdk.flush();
      assert.equal(sdk.pending(), 0);

      const rows = await pool.query(
        `SELECT event_id, name, session_id, source, anon_id, received_at
           FROM events WHERE tenant_id = $1 ORDER BY name`,
        [keys.tenantId],
      );
      assert.equal(rows.rows.length, 3, "session_start + order + webhook");

      const start = rows.rows.find((row) => row.name === "session_start");
      const order = rows.rows.find((row) => row.name === "order_placed");
      const webhook = rows.rows.find((row) => row.name === "payment_succeeded");
      assert.ok(start && order && webhook);

      assert.equal(start.session_id, session.session_id);
      assert.equal(start.source, "server");
      assert.equal(order.event_id, orderId);
      assert.equal(order.session_id, session.session_id);
      assert.equal(order.anon_id, session.anon_id);
      assert.equal(order.source, "server");
      assert.ok(order.received_at instanceof Date);

      assert.equal(webhook.event_id, providerId);
      assert.equal(webhook.session_id, null, "webhook is sessionless");
      assert.equal(webhook.source, "webhook");

      // The bootstrap record's idle clock advanced with real events.
      const bs = await pool.query(
        "SELECT last_event_at FROM session_bootstrap WHERE tenant_id = $1 AND session_id = $2",
        [keys.tenantId, session.session_id],
      );
      assert.ok(bs.rows[0].last_event_at > new Date(Date.now() - 60_000));

      // Re-delivering the same webhook (same event_id) stores nothing new.
      sdk.track("payment_succeeded", {
        sessionId: null,
        source: "webhook",
        eventId: providerId,
        props: { provider: "stripe" },
      });
      await sdk.flush();

      const replay = await pool.query(
        "SELECT count(*) AS n FROM events WHERE tenant_id = $1 AND event_id = $2",
        [keys.tenantId, providerId],
      );
      assert.equal(Number(replay.rows[0].n), 1);
    } finally {
      await sdk.shutdown();
      await flusher.shutdown();
      await close(server);
    }
  });
});
