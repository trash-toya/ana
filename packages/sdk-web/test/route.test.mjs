// P4-5: the web SDK sends the LB routing header derived from the batch, so
// one visitor's flushes hash to one ingest instance. The header is advisory —
// only locality depends on it.
import { test } from "vitest";
import assert from "node:assert/strict";
import { routeKey, sendBatch, selectBatch } from "../dist/transport.js";

test("routeKey: envelope anon_id wins, then event anon_id, then session_id", () => {
  assert.equal(routeKey({ anon_id: "visitor-1", events: [] }), "visitor-1");
  assert.equal(
    routeKey({
      events: [{ session_id: "s1" }, { anon_id: "visitor-2", session_id: "s2" }],
    }),
    "visitor-2",
  );
  assert.equal(routeKey({ events: [{ session_id: "s3" }] }), "s3");
  assert.equal(routeKey({ events: [] }), null);
  assert.equal(routeKey(null), null);
});

test("sendBatch: sets X-Analytics-Route from the envelope anon_id", async () => {
  let seen;
  const fetchFn = async (_url, init) => {
    seen = init.headers;
    return {
      status: 200,
      json: async () => ({ accepted: ["e1"], rejected: [] }),
      headers: { get: () => null },
    };
  };
  const result = await sendBatch(
    "https://ingest.test/v1/events",
    { schema_version: 1, anon_id: "visitor-9", sent_at: "x", events: [] },
    "sk_test",
    ["e1"],
    fetchFn,
  );
  assert.deepEqual(result.acked, ["e1"]);
  assert.equal(seen["X-Analytics-Route"], "visitor-9");
  assert.equal(seen["X-API-Key"], "sk_test");
});

test("sendBatch: omits the header when the batch has no routing key", async () => {
  let seen;
  await sendBatch(
    "https://ingest.test/v1/events",
    { schema_version: 1, sent_at: "x", events: [] },
    "sk_test",
    [],
    async (_url, init) => {
      seen = init.headers;
      return {
        status: 200,
        json: async () => ({ accepted: [], rejected: [] }),
        headers: { get: () => null },
      };
    },
  );
  assert.equal("X-Analytics-Route" in seen, false);
});

test("web batch sizing uses UTF-8 bytes for multibyte properties", () => {
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
