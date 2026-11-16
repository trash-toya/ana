// P2-2: the full web retry matrix (04-services.md status table).
// Every row of the table has a test: 200/400/401/403 live in the conformance
// suite; 400/401/403/413/429/5xx/network and the attempt cap are here.
import { test, onTestFinished } from "vitest";
import assert from "node:assert/strict";
import { init } from "../dist/index.js";
import { retryAfterMs } from "../dist/transport.js";
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

/** Recording fetch. handler({index, body}) → {status, headers?, body?} | "network". */
function fakeFetch(handler) {
  const calls = [];
  const fn = async (url, opts) => {
    const body = JSON.parse(opts.body);
    const index = calls.length;
    calls.push({ url, body, headers: opts.headers });
    const result = await handler({ index, body });
    if (result === "network") throw new TypeError("fetch failed");
    const status = result?.status ?? 200;
    return {
      status,
      headers: { get: (name) => result?.headers?.[name.toLowerCase()] ?? null },
      json: async () =>
        result?.body ?? {
          accepted: (body.events ?? []).map((e) => e.event_id),
          rejected: [],
        },
    };
  };
  fn.calls = calls;
  return fn;
}

function makeSdk(t, handler, { retryBaseMs = 1 } = {}) {
  const store = fakeStorage();
  const fetchFn = fakeFetch(handler);
  const sdk = init({
    apiKey: "sk_test",
    endpoint: "https://ingest.test/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 60_000,
    retryBaseMs,
    browser: {
      storage: store,
      locks: fakeLocks(),
      fetchFn,
      now: () => 1_700_000_000_000,
      jitter: () => 0,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      visibility: () => true,
    },
  });
  onTestFinished(() => sdk.shutdown());
  return { sdk, store, fetchFn };
}

test("400: the whole batch is dropped permanently, one request only", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t, () => ({ status: 400 }));
  await sdk.track("click");
  await sdk.flush();
  assert.equal(queueDepth(store), 0);
  assert.equal(fetchFn.calls.length, 1, "no retry loop on an SDK bug");
});

test("401 and 403: queue retained, flush stops", async (t) => {
  // A fresh track() mints session_start + page_view + click = 3 queued events.
  for (const status of [401, 403]) {
    const { sdk, store, fetchFn } = makeSdk(t, () => ({ status }));
    await sdk.track("click");
    await sdk.flush();
    assert.equal(queueDepth(store), 3, `${status}: nothing is lost`);
    assert.equal(fetchFn.calls.length, 1, `${status}: no retry loop`);
  }
});

test("413: the batch is halved until it fits", async (t) => {
  // Two tracks mint 4 events; MAX_EVENTS_PER_REQUEST is 500, so the first
  // attempt is the full queue of 4, then the halving kicks in: 4 → 2 → 2.
  const { sdk, store, fetchFn } = makeSdk(t, ({ index }) =>
    index === 0 ? { status: 413 } : { status: 200 },
  );
  await sdk.track("click");
  await sdk.track("purchase");
  await sdk.flush();
  const sizes = fetchFn.calls.map((call) => call.body.events.length);
  assert.deepEqual(sizes, [4, 2, 2]);
  assert.equal(queueDepth(store), 0);
});

test("413: a single event that can never fit is dropped and counted", async (t) => {
  // Only the oversized event 413s; the fitting session_start/page_view batch
  // drains normally, then the huge event is retried alone and dropped once.
  const { sdk, store, fetchFn } = makeSdk(t, ({ body }) =>
    body.events.some((e) => e.name === "huge") ? { status: 413 } : { status: 200 },
  );
  await sdk.track("huge", { props: { blob: "x".repeat(70_000) } });
  await sdk.flush();
  assert.equal(queueDepth(store), 0, "the undeliverable event leaves the queue");
  assert.equal(sdk.__internal.droppedPending(), 1, "counted for sdk_dropped");
  assert.equal(
    fetchFn.calls.length,
    2,
    "one request for the fitting batch, one for the huge event",
  );
});

test("429: Retry-After is honoured, then the batch drains", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t, ({ index }) =>
    index === 0 ? { status: 429, headers: { "retry-after": "0" } } : { status: 200 },
  );
  await sdk.track("click");
  await sdk.flush();
  assert.equal(fetchFn.calls.length, 2);
  assert.equal(queueDepth(store), 0);
});

test("5xx: bounded attempts per flush, queue retained for the next call", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t, () => ({ status: 500 }));
  await sdk.track("click");
  await sdk.flush();
  assert.equal(fetchFn.calls.length, 8, "MAX_FLUSH_ATTEMPTS bounds the loop");
  assert.equal(queueDepth(store), 3, "the three minted events stay for the next call");
  // The next flush starts a fresh attempt budget and can still succeed.
  const {
    sdk: sdk2,
    store: store2,
    fetchFn: fetchFn2,
  } = makeSdk(t, ({ index }) => (index < 2 ? { status: 500 } : { status: 200 }));
  await sdk2.track("click");
  await sdk2.flush();
  assert.equal(queueDepth(store2), 0);
  assert.equal(fetchFn2.calls.length, 3);
});

test("network errors: back off and recover within one flush", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t, ({ index }) =>
    index < 2 ? "network" : { status: 200 },
  );
  await sdk.track("click");
  await sdk.flush();
  assert.equal(fetchFn.calls.length, 3);
  assert.equal(queueDepth(store), 0);
});

test("no-progress 200 (server reports nothing): bounded like a transient failure", async (t) => {
  const { sdk, store, fetchFn } = makeSdk(t, () => ({
    status: 200,
    body: { accepted: [], rejected: [] },
  }));
  await sdk.track("click");
  await sdk.flush();
  assert.equal(fetchFn.calls.length, 8, "a silent server cannot spin the loop forever");
  assert.equal(queueDepth(store), 3, "the three minted events stay queued");
});

test("retryAfterMs: delta-seconds, HTTP dates, garbage", () => {
  assert.equal(retryAfterMs("5"), 5000);
  assert.equal(retryAfterMs("0"), 0);
  const at = Date.UTC(2026, 10, 15, 12, 0, 10);
  assert.equal(retryAfterMs(new Date(at).toUTCString(), at - 4000), 4000);
  assert.equal(retryAfterMs("soon"), undefined);
  assert.equal(retryAfterMs(null), undefined);
});

test("backoff restarts from the base after a success", async (t) => {
  // Sequence: 500, 500, 200 (success resets the schedule), then 500 again.
  // With retryBaseMs=200 and jitter 0 the delays are 100 + 200 for the first
  // two failures; the third failure would also wait ~100 if (and only if) the
  // schedule was reset, vs 800 if it had kept the grown ceiling.
  let step = 0;
  const { sdk } = makeSdk(
    t,
    () => {
      step += 1;
      // 500, 500, 200 (reset), 500, 200: the last 500 must wait ~100ms.
      return step <= 2 || step === 4 ? { status: 500 } : { status: 200 };
    },
    { retryBaseMs: 200 },
  );
  await sdk.track("click");
  const started = Date.now();
  await sdk.flush();
  const elapsed = Date.now() - started;
  assert.ok(elapsed >= 280, `expected the 100+200ms backoff, got ${elapsed}ms`);
  assert.ok(
    elapsed < 1_500,
    `backoff must not compound past one success, got ${elapsed}ms`,
  );
});
