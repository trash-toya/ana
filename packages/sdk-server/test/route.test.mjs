// P4-5: the server SDK's batches carry no envelope anon_id, so the routing key
// comes from the first event's anon_id (server batches are per-session by
// construction) and the header keeps a backend's session_guard locks local.
import { test } from "vitest";
import assert from "node:assert/strict";
import { routeKey, sendBatch, selectBatch } from "../dist/transport.js";

test("routeKey: falls back to per-event identifiers", () => {
  assert.equal(
    routeKey({
      events: [
        { session_id: "s1", anon_id: "visitor-a" },
        { session_id: "s2", anon_id: "visitor-b" },
      ],
    }),
    "visitor-a",
  );
  assert.equal(routeKey({ events: [{ session_id: "session-only" }] }), "session-only");
  assert.equal(routeKey({ events: [{ anon_id: "" }, {}] }), null);
});

test("sendBatch: sets X-Analytics-Route on the wire", async () => {
  let seen;
  const outcome = await sendBatch({
    url: "https://ingest.test/v1/events",
    body: {
      schema_version: 1,
      sent_at: "x",
      events: [{ event_id: "e1", anon_id: "visitor-srv", session_id: "s1" }],
    },
    apiKey: "sk_server",
    batchIds: ["e1"],
    fetchFn: async (_url, init) => {
      seen = init.headers;
      return {
        status: 200,
        json: async () => ({ accepted: ["e1"], rejected: [] }),
        headers: { get: () => null },
      };
    },
  });
  assert.equal(outcome.kind, "ok");
  assert.equal(seen["X-Analytics-Route"], "visitor-srv");
});

test("server batch sizing uses UTF-8 bytes for multibyte properties", () => {
  const pad = "漢🙂".repeat(100);
  const events = [
    { event_id: "e1", name: "custom", props: { pad } },
    { event_id: "e2", name: "custom", props: { pad } },
  ];
  const maxBytes = 1800;
  const plan = selectBatch(events, maxBytes);
  assert.equal(
    plan.events.length,
    1,
    "the second multibyte event must exceed the byte budget",
  );
  assert.ok(plan.bytes <= maxBytes);
  const actualBody = {
    schema_version: 1,
    anon_id: "visitor",
    sent_at: new Date().toISOString(),
    events: plan.events,
  };
  assert.ok(new TextEncoder().encode(JSON.stringify(actualBody)).byteLength <= maxBytes);
});
