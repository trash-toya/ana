// Web-specific SDK unit tests. The shared behaviour cases (session expiry,
// heartbeats, transport outcomes, consent, multi-tab, restart reconciliation)
// live in the conformance suite: packages/shared/test/conformance, driven by
// packages/sdk-web/test/conformance.test.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { init } from "../dist/index.js";
import { SESSION_KEY } from "../dist/session.js";
import { EVENT_PREFIX, queueDepth, readQueue } from "../dist/queue.js";

function fakeStorage(initial = {}) {
  const m = new Map(Object.entries(initial));
  return {
    keys: () => [...m.keys()],
    get: (k) => (m.has(k) ? m.get(k) : null),
    set: (k, v) => m.set(k, String(v)),
    remove: (k) => m.delete(k),
  };
}

/** Web-Lock mutex: callbacks serialise exactly like navigator.locks. */
function fakeLocks() {
  let chain = Promise.resolve();
  return {
    request: (name, cb) => {
      const run = chain.then(cb);
      chain = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    },
  };
}

/** Recording fetch with a programmable status per call. */
function fakeFetch(statusFn) {
  const calls = [];
  const fn = async (url, opts) => {
    calls.push({ url, opts, body: JSON.parse(opts.body) });
    const status = typeof statusFn === "function" ? statusFn(calls.length, calls) : statusFn;
    if (status === "network") throw new TypeError("fetch failed");
    return {
      status,
      json: async () => ({
        accepted: [],
        rejected: [],
        ...(status === 200 ? { accepted: calls.at(-1).body.events.map((e) => e.event_id) } : {}),
      }),
    };
  };
  fn.calls = calls;
  return fn;
}

function makeSdk(t, { status = 200, startMs = 1_700_000_000_000, storage, locks } = {}) {
  const store = storage ?? fakeStorage();
  const clock = { now: startMs };
  const fetchFn = fakeFetch(status);
  const sdk = init({
    apiKey: "sk_test",
    endpoint: "https://ingest.test/v1/events",
    heartbeatMs: 0, // interval tested separately
    flushDebounceMs: 60_000, // no auto-flush: tests call sdk.flush() explicitly
    retryBaseMs: 1, // fast backoff in tests
    browser: {
      storage: store,
      locks: locks ?? fakeLocks(),
      fetchFn,
      now: () => clock.now,
      jitter: () => 0,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      visibility: () => true,
    },
  });
  t.after(() => sdk.shutdown());
  return { sdk, store, clock, fetchFn };
}

// ---------------------------------------------------------------------------
// Minting order (web: storage keys are sorted by UUIDv7, D25)
// ---------------------------------------------------------------------------

test("fresh session: session_start (seq 1) then page_view (seq 2), trigger event seq 3", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t);
  await sdk.track("click");
  const queued = readQueue(store);
  assert.deepEqual(
    queued.map((e) => e.name),
    ["session_start", "page_view", "click"],
  );
  assert.deepEqual(
    queued.map((e) => e.seq),
    [1, 2, 3],
  );
  // one batch, correct envelope
  await sdk.flush();
  assert.equal(fetchFn.calls.length, 1);
  const body = fetchFn.calls[0].body;
  assert.equal(body.schema_version, 1);
  assert.equal(body.events.length, 3);
  assert.match(body.anon_id, /^[0-9a-f-]{36}$/);
});

test("page_view trigger is the page_view itself (no duplicate)", async (t) => {
  const { sdk, store } = makeSdk(t);
  await sdk.track("page_view");
  assert.deepEqual(
    readQueue(store).map((e) => e.name),
    ["session_start", "page_view"],
  );
});

test("durability: the event is persisted before track() resolves", async (t) => {
  const { sdk, store } = makeSdk(t);
  await sdk.track("click");
  assert.ok(store.get(`${EVENT_PREFIX}${readQueue(store).at(-1).event_id}`));
});

// ---------------------------------------------------------------------------
// Transport (web-specific bits)
// ---------------------------------------------------------------------------

test("network error: queue preserved and a retry is scheduled", async (t) => {
  let fails = 2;
  const store = fakeStorage();
  const sdk = init({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 1,
    retryBaseMs: 1,
    browser: {
      storage: store,
      locks: fakeLocks(),
      fetchFn: async () => {
        if (fails-- > 0) throw new TypeError("fetch failed");
        return {
          status: 200,
          json: async () => ({ accepted: readQueue(store).map((e) => e.event_id), rejected: [] }),
        };
      },
      now: () => 1_700_000_000_000,
      jitter: () => 0,
    },
  });
  t.after(() => sdk.shutdown());
  await sdk.track("click");
  await sdk.flush(); // retries through the two failures (backoff 1ms, 2ms)
  assert.equal(queueDepth(store), 0);
});

test("identify: user_id rides on subsequent events", async (t) => {
  const { sdk, store } = makeSdk(t);
  await sdk.track("click");
  sdk.identify("u_123");
  await sdk.track("purchase");
  const ev = readQueue(store).at(-1);
  assert.equal(ev.user_id, "u_123");
  assert.equal(readQueue(store)[0].user_id, undefined);
});

test("batch selection: ≤500 events and ≤60 KB per request", async (t) => {
  const { sdk, store, fetchFn, clock } = makeSdk(t);
  await sdk.track("click"); // fresh session: 3 events
  for (let i = 0; i < 600; i++) {
    clock.now += 1000;
    await sdk.track("click", { n: i, pad: "x".repeat(100) });
  }
  await sdk.flush();
  // every batch ≤ 500 and the queue drains fully
  for (const call of fetchFn.calls) {
    assert.ok(call.body.events.length <= 500);
    assert.ok(JSON.stringify(call.body).length <= 60 * 1024 + 4096);
  }
  assert.equal(queueDepth(store), 0);
  assert.ok(fetchFn.calls.length >= 2);
});

// ---------------------------------------------------------------------------
// Lifecycle wiring (P2-1)
// ---------------------------------------------------------------------------

test("shutdown removes the lifecycle listeners", async (t) => {
  const added = [];
  const lifecycle = {
    add: (type, cb) => added.push([type, cb]),
    remove: (type) => {
      const i = added.findIndex(([ty]) => ty === type);
      if (i >= 0) added.splice(i, 1);
    },
  };
  const sdk = init({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    heartbeatMs: 0,
    browser: {
      storage: fakeStorage(),
      locks: fakeLocks(),
      fetchFn: fakeFetch(200),
      now: () => 1_700_000_000_000,
      lifecycle,
    },
  });
  assert.deepEqual(
    added.map(([ty]) => ty).sort(),
    ["pagehide", "pageshow", "visibilitychange"],
  );
  sdk.shutdown();
  assert.equal(added.length, 0);
});

test("consent off also clears the pending drop counter", async (t) => {
  const { sdk, store } = makeSdk(t);
  await sdk.track("click");
  // simulate a drop having been counted
  const { addDropped } = await import("../dist/queue.js");
  addDropped(store, 3);
  assert.equal(sdk.__internal.droppedPending(), 3);
  sdk.setConsent(false);
  assert.equal(sdk.__internal.droppedPending(), 0);
});

test("session record keeps the two clocks apart (D24)", async (t) => {
  const { sdk, store, clock } = makeSdk(t);
  await sdk.track("click");
  const rec = JSON.parse(store.get(SESSION_KEY));
  assert.equal(typeof rec.last_event_at, "number");
  assert.equal(rec.last_event_at, rec.last_seen_at); // equal after engagement
  clock.now += 10_000;
  await sdk.track("heartbeat");
  const after = JSON.parse(store.get(SESSION_KEY));
  assert.equal(after.last_seen_at, rec.last_seen_at); // engagement clock unmoved
  assert.equal(after.last_event_at, 1_700_000_000_000 + 10_000); // event time = now
});
