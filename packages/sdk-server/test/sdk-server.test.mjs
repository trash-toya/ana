// Unit tests for @session-analytics/sdk-server (P1B-4): session bootstrap,
// session_start emission, sessionless tracking, per-source time, and the
// full retry matrix against a fake transport (no network, no database).
import test from "node:test";
import assert from "node:assert/strict";
import {
  ServerAnalytics,
  ServerAnalyticsError,
  retryAfterMs,
} from "../dist/index.js";

const BASE = "https://ingest.test";
const KEY = "sk_server_test";
const T0 = Date.UTC(2026, 10, 15, 12, 0, 0);

function descriptor(overrides = {}) {
  return {
    session_id: "0199b0a0-0000-7000-8000-000000000001",
    anon_id: "a_visitor",
    user_id: "user_1",
    started_at: new Date(T0).toISOString(),
    last_event_at: new Date(T0).toISOString(),
    expires_at: new Date(T0 + 30 * 60_000).toISOString(),
    resumed: false,
    emitted_events: 0,
    properties: { device: "desktop", country: "ES", utm: { source: "hn" } },
    context: "v1.0199b0a0-0000-7000-8000-000000000001.1794830400.sig",
    ...overrides,
  };
}

/**
 * Fake transport. `handler({ url, body, index })` returns:
 *   { status?, body? } | "network"
 * A 200 with no body accepts every id in the request.
 */
function recorder(handler = () => ({ status: 200 })) {
  const calls = [];
  const fetchFn = async (url, init) => {
    const body = JSON.parse(init.body);
    const index = calls.length;
    calls.push({ url, body, headers: init.headers });
    const result = await handler({ url, body, index });
    if (result === "network") throw new Error("network down");
    const status = result?.status ?? 200;
    return {
      status,
      headers: {
        get: (name) => result?.headers?.[name.toLowerCase()] ?? null,
      },
      json: async () =>
        result?.body ?? {
          accepted: (body.events ?? []).map((event) => event.event_id),
          rejected: [],
        },
    };
  };
  fetchFn.calls = calls;
  return fetchFn;
}

function client(fetchFn, overrides = {}) {
  return new ServerAnalytics({
    endpoint: BASE,
    apiKey: KEY,
    fetchFn,
    autoFlush: false,
    now: () => T0,
    jitter: () => 0,
    retryBaseMs: 1,
    ...overrides,
  });
}

function eventCalls(fetchFn) {
  return fetchFn.calls.filter((call) => call.url === `${BASE}/v1/events`);
}

test("track: sessionless event has no session/seq, source defaults to server", async () => {
  const fetchFn = recorder();
  const sdk = client(fetchFn);

  const id = sdk.track("order_placed", { sessionId: null, props: { order: "o1" } });
  assert.equal(sdk.pending(), 1);
  await sdk.flush();

  const [call] = eventCalls(fetchFn);
  assert.ok(call);
  const [event] = call.body.events;
  assert.equal(event.event_id, id);
  assert.equal(event.name, "order_placed");
  assert.equal(event.source, "server");
  assert.equal(event.session_id, undefined);
  assert.equal(event.anon_id, undefined);
  assert.equal(event.seq, undefined, "D31: seq is omitted by default");
  assert.deepEqual(event.props, { order: "o1" });
  assert.match(event.event_id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.equal(sdk.pending(), 0);
});

test("bootstrap: session_start is emitted once with the persisted attribution", async () => {
  const fetchFn = recorder(({ url }) =>
    url.endsWith("/v1/sessions") ? { status: 200, body: descriptor() } : { status: 200 },
  );
  const sdk = client(fetchFn);

  const session = await sdk.startSession({ userId: "user_1" });
  assert.equal(session.resumed, false);
  assert.equal(session.emitted_events, 0);

  sdk.track("page_view", { url: "/pricing" });
  sdk.track("checkout_started");
  await sdk.flush();

  const [first] = eventCalls(fetchFn);
  assert.ok(first);
  const names = first.body.events.map((event) => event.name);
  assert.deepEqual(names, ["session_start", "page_view", "checkout_started"]);
  const start = first.body.events[0];
  assert.equal(start.session_id, session.session_id);
  assert.equal(start.anon_id, "a_visitor");
  assert.equal(start.user_id, "user_1");
  assert.equal(start.source, "server");
  assert.deepEqual(start.props, descriptor().properties);
  assert.equal(start.seq, undefined);
  assert.equal(first.body.events[1].url, "/pricing");
  assert.equal(sdk.pending(), 0);
});

test("resume with stored events does not emit session_start", async () => {
  const fetchFn = recorder(({ url }) =>
    url.endsWith("/v1/sessions")
      ? { status: 200, body: descriptor({ resumed: true, emitted_events: 12 }) }
      : { status: 200 },
  );
  const sdk = client(fetchFn);
  await sdk.startSession({ anonId: "a_visitor" });
  sdk.track("page_view");
  await sdk.flush();

  const [call] = eventCalls(fetchFn);
  assert.deepEqual(call.body.events.map((event) => event.name), ["page_view"]);
});

test("session-bound tracking without a visitor id is an SDK error", () => {
  const sdk = client(recorder());
  assert.throws(
    () => sdk.track("page_view", { sessionId: descriptor().session_id }),
    (error) =>
      error instanceof ServerAnalyticsError && error.code === "missing_anon_id",
  );
  assert.throws(
    () => sdk.track("Page View"),
    (error) => error instanceof ServerAnalyticsError && error.code === "bad_name",
  );
});

test("validateSession returns the server verdict; auth errors throw", async () => {
  const fetchFn = recorder(({ body }) => ({
    status: 200,
    body: { valid: true, session_id: body.context, emitted_events: 3 },
  }));
  const sdk = client(fetchFn);
  const result = await sdk.validateSession("v1.context");
  assert.equal(result.valid, true);
  assert.equal(result.emitted_events, 3);

  const denied = client(recorder(() => ({ status: 403 })));
  await assert.rejects(
    () => denied.validateSession("v1.context"),
    (error) => error instanceof ServerAnalyticsError && error.code === "auth_failed",
  );
});

test("retry: 400 drops the batch permanently", async () => {
  const fetchFn = recorder(() => ({ status: 400 }));
  const sdk = client(fetchFn);
  sdk.track("bad_event");
  await sdk.flush();
  assert.equal(sdk.pending(), 0);
  assert.equal(eventCalls(fetchFn).length, 1);
});

test("retry: 401 retains the queue and records the auth error", async () => {
  const fetchFn = recorder(() => ({ status: 401 }));
  const sdk = client(fetchFn);
  sdk.track("page_view");
  await sdk.flush();
  assert.equal(sdk.pending(), 1);
  assert.equal(sdk.authError(), 401);
});

test("retry: 413 halves the batch until it fits", async () => {
  const fetchFn = recorder(({ body, index }) =>
    index === 0 ? { status: 413 } : { status: 200, body: { accepted: body.events.map((e) => e.event_id), rejected: [] } },
  );
  const sdk = client(fetchFn);
  sdk.track("e1");
  sdk.track("e2");
  await sdk.flush();

  const sizes = eventCalls(fetchFn).map((call) => call.body.events.length);
  assert.deepEqual(sizes, [2, 1, 1]);
  assert.equal(sdk.pending(), 0);
});

test("retry: 429 honours Retry-After then succeeds", async () => {
  const fetchFn = recorder(({ body, index }) =>
    index === 0
      ? { status: 429, headers: { "retry-after": "0" } }
      : { status: 200, body: { accepted: body.events.map((e) => e.event_id), rejected: [] } },
  );
  const sdk = client(fetchFn);
  sdk.track("page_view");
  await sdk.flush();
  assert.equal(eventCalls(fetchFn).length, 2);
  assert.equal(sdk.pending(), 0);
});

test("retry: 5xx and network errors back off and recover", async () => {
  let mode = "500";
  const fetchFn = recorder(({ body }) => {
    if (mode === "500") {
      mode = "network";
      return { status: 500 };
    }
    if (mode === "network") {
      mode = "ok";
      return "network";
    }
    return { status: 200, body: { accepted: body.events.map((e) => e.event_id), rejected: [] } };
  });
  const sdk = client(fetchFn);
  sdk.track("page_view");
  await sdk.flush();
  assert.equal(sdk.pending(), 0);
  assert.equal(eventCalls(fetchFn).length, 3);
});

test("webhook re-delivery keeps the caller event id (server deduplicates)", async () => {
  const fetchFn = recorder(({ body }) => ({
    status: 200,
    body: { accepted: body.events.map((e) => e.event_id), rejected: [] },
  }));
  const sdk = client(fetchFn);
  const providerId = "0199b0a0-0000-7000-8000-0000000000aa";
  sdk.track("payment_succeeded", {
    sessionId: null,
    source: "webhook",
    eventId: providerId,
    props: { provider: "stripe" },
  });
  await sdk.flush();
  sdk.track("payment_succeeded", {
    sessionId: null,
    source: "webhook",
    eventId: providerId,
    props: { provider: "stripe" },
  });
  await sdk.flush();

  const [first, second] = eventCalls(fetchFn);
  assert.equal(first.body.events[0].event_id, providerId);
  assert.equal(second.body.events[0].event_id, providerId);
  assert.equal(first.body.events[0].source, "webhook");
});

test("shutdown drains the buffer and refuses further tracking", async () => {
  const fetchFn = recorder();
  const sdk = client(fetchFn);
  sdk.track("page_view");
  await sdk.shutdown();
  assert.equal(sdk.pending(), 0);
  assert.equal(eventCalls(fetchFn).length, 1);
  assert.throws(
    () => sdk.track("late"),
    (error) => error instanceof ServerAnalyticsError && error.code === "closed",
  );
});

test("time: producer timestamps are preserved and strictly increasing by default", async () => {
  const fetchFn = recorder();
  const sdk = client(fetchFn);
  const fixed = "2026-11-15T11:59:00.000Z";
  sdk.track("provider_event", { sessionId: null, source: "webhook", time: fixed });
  sdk.track("page_view");
  sdk.track("page_view");
  await sdk.flush();

  const events = eventCalls(fetchFn)[0].body.events;
  assert.equal(events[0].time, fixed, "a webhook/provider time is not rewritten");
  assert.equal(events[1].time, new Date(T0).toISOString());
  assert.ok(Date.parse(events[2].time) > Date.parse(events[1].time));
});

test("retryAfterMs accepts delta-seconds and HTTP dates", () => {
  assert.equal(retryAfterMs("5"), 5000);
  const at = Date.UTC(2026, 10, 15, 12, 0, 10);
  assert.equal(retryAfterMs(new Date(at).toUTCString(), at - 4000), 4000);
  assert.equal(retryAfterMs("nonsense"), undefined);
  assert.equal(retryAfterMs(null), undefined);
});
