// RN-specific tests (P2-4/P2-5/P2-6; Ref 5 T10/T18, D10 rules, D16).
// Generation token, early-call buffering with original timestamps, deep-link
// forced expiry persistence, size-triggered flush, persisted identify,
// shutdown mid-flight and the headless guard.
import { test } from "vitest";
import assert from "node:assert/strict";

import { initRn, parseDeepLink } from "../dist/index.js";
import { SESSION_KEY, USER_KEY, readQueue } from "../../sdk-web/dist/index.js";
import {
  fakeServer,
  fakeStorage,
  sleep,
  START_MS,
} from "../../shared/test/conformance/index.mjs";

function makeMmkv(fake) {
  return {
    getString: (k) => fake.get(k) ?? undefined,
    set: (k, v) => fake.set(k, v),
    delete: (k) => fake.remove(k),
    contains: (k) => fake.get(k) !== null,
    getAllKeys: () => fake.keys(),
  };
}

function makeAppState() {
  const listeners = new Set();
  return {
    currentState: "active",
    addEventListener: (_t, cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    change(state) {
      this.currentState = state;
      for (const cb of listeners) cb(state);
    },
  };
}

function makeDeepLinks({ initial = null, gate } = {}) {
  const listeners = new Set();
  return {
    // A gate is a FACTORY: every init() calls getInitialURL and gets its own
    // promise, matching linking.getInitialURL() semantics.
    getInitialURL: gate ?? (async () => initial),
    addEventListener: (_t, cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    open(url) {
      for (const cb of listeners) cb(url);
    },
  };
}

function harness({ initial = null, gate, status, heartbeatMs = 0 } = {}) {
  const fake = fakeStorage();
  const clock = { now: START_MS };
  const server = fakeServer();
  if (status !== undefined) server.status = status;
  const appState = makeAppState();
  const deepLinks = makeDeepLinks({ initial, gate });
  const mk = () =>
    initRn({
      apiKey: "k",
      endpoint: "https://x/v1/events",
      heartbeatMs,
      flushMs: 60_000, // no cadence flushes in these tests
      storage: makeMmkv(fake),
      appState,
      deepLinks,
      fetchFn: server,
      now: () => clock.now,
      screen: () => "/s",
    });
  return { fake, clock, server, appState, deepLinks, mk };
}

test("T18: an init() left pending on getInitialURL cannot leak into the new instance", async () => {
  // Each init gets its OWN pending gate; resolvers[0] is a's, [1] is b's.
  const resolvers = [];
  const h = harness({
    gate: () => new Promise((resolve) => resolvers.push(resolve)),
  });
  const a = h.mk(); // stuck in initializing, waiting on getInitialURL
  const early = a.track("early"); // buffered; must resolve once a dies
  a.shutdown(); // gen bumped; the pending init continuation must die
  await early; // must not hang: shutdown unblocks the gate

  const b = h.mk(); // the "newer init()", on its own gate
  resolvers[0]("app://x/campaign?utm_source=stale&utm_medium=push"); // STALE URL finally resolves
  resolvers[1](null); // b's init completes with no campaign
  await sleep(10);
  await b.track("click");
  await b.flush();

  // No stale campaign: a's late-resolving URL must never attribute anything.
  const all = [...h.server.store.values()];
  assert.ok(
    all.every((e) => !e.props?.utm_source),
    "stale campaign URL leaked",
  );
  assert.ok(
    all.every((e) => e.name !== "early"),
    "a's buffered event leaked",
  );
  assert.ok(
    all.every((e) => e.session_id === b.__internal.session()?.session_id),
    "an event landed outside b's session",
  );
  a.shutdown();
  b.shutdown();
});

test("T18b: a warm deep link attributes the NEXT session, then never leaks", async () => {
  const h = harness();
  const sdk = h.mk();
  await sdk.track("click"); // session A exists
  const firstSession = sdk.__internal.session().session_id;
  h.deepLinks.open("app://x/hello?utm_source=push&utm_campaign=winter");
  assert.equal(h.fake.get(SESSION_KEY), null, "deep link did not force expiry");
  await sdk.track("click"); // new session B with the campaign attribution
  const second = sdk.__internal.session().session_id;
  assert.notEqual(second, firstSession, "deep link did not rotate the session");
  await sdk.flush();
  const start = [...h.server.store.values()].find(
    (e) => e.name === "session_start" && e.session_id === second,
  );
  assert.equal(start.props.utm_source, "push");
  assert.equal(start.props.utm_campaign, "winter");
  // Subsequent events carry no campaign props (the URL is consumed once).
  await sdk.track("click");
  await sdk.flush();
  const after = [...h.server.store.values()].filter(
    (e) => e.session_id === second && e.name === "click",
  );
  assert.ok(after.length >= 1);
  assert.ok(
    after.every((e) => !e.props?.utm_source),
    "campaign leaked into later events",
  );
  sdk.shutdown();
});

test("deep-link expiry survives a process kill before the next event", async () => {
  const h = harness();
  const sdk = h.mk();
  await sdk.track("click");
  const first = sdk.__internal.session().session_id;
  h.deepLinks.open("app://x/deep");
  assert.equal(h.fake.get(SESSION_KEY), null, "expiry not persisted");

  // Process kill: a brand-new instance over the same storage. The old session
  // must stay gone — no resurrection.
  const sdk2 = h.mk();
  await sdk2.track("click");
  const second = sdk2.__internal.session().session_id;
  assert.notEqual(second, first, "the killed session was resurrected");
  await sdk2.flush();
  const ids = new Set([...h.server.store.values()].map((e) => e.session_id));
  assert.equal(ids.size, 2, "expected exactly two sessions across the kill");
  sdk.shutdown();
  sdk2.shutdown();
});

test("early track() calls replay with their original timestamps, bounded at 200", async () => {
  let release;
  const h = harness({ gate: () => new Promise((r) => (release = r)) });
  const sdk = h.mk(); // init never lands until release
  h.clock.now += 1000;
  const pA = sdk.track("before_a"); // resolves only once init lands
  h.clock.now += 1000;
  const pB = sdk.track("before_b");
  // init still pending: nothing durable, nothing sent, promises unresolved
  assert.equal(h.fake.keys().filter((k) => k.startsWith("sa_ev:")).length, 0);
  assert.equal(h.server.batches.length, 0);

  h.clock.now += 1000; // "later": replay must NOT use this time
  release("app://x/ok");
  await pA;
  await pB;
  // 2 buffered + session_start + page_view
  assert.equal(sdk.__internal.queued().length, 4, "buffered calls were lost");
  await sdk.flush();
  const evs = [...h.server.store.values()].filter((e) => e.name.startsWith("before_"));
  assert.equal(evs.length, 2);
  const [tA, tB] = evs.map((e) => new Date(e.time).getTime());
  // 997, not exactly 1000: the core bumps each minted event +1ms within an
  // emit for intra-batch uniqueness (session_start +1, page_view +2, event +3).
  assert.ok(tB - tA >= 990 && tB - tA <= 1000, "replay lost the captured spacing");
  // The real property: replayed events carry their ORIGINAL capture times
  // (clock advanced 1000 ms more before the replay ran).
  assert.ok(tB <= h.clock.now - 1000, "replay did not pin the ORIGINAL capture times");

  // Bound: more than 200 early calls — the excess is dropped, not unbounded.
  const h2 = harness({ gate: () => new Promise(() => {}) });
  const sdk2 = h2.mk();
  for (let i = 0; i < 250; i++) void sdk2.track(`burst_${i}`);
  h2.clock.now += 1000;
  sdk2.shutdown(); // die before init: nothing persisted by the dead instance
  assert.equal(
    h2.fake.keys().filter((k) => k.startsWith("sa_ev:")).length,
    0,
    "a dead instance leaked buffered events",
  );
  sdk.shutdown();
});

test("D10 rule 1: the queue reaching 100 events triggers a flush", async () => {
  const h = harness();
  const sdk = h.mk();
  await sdk.track("click"); // mints 3 events (session_start, page_view, click)
  for (let i = 0; i < 96; i++) {
    h.clock.now += 1;
    await sdk.track(`e${i}`);
  }
  assert.equal(sdk.__internal.queued().length, 99);
  assert.equal(h.server.batches.length, 0, "flushed before the trigger");
  h.clock.now += 1;
  await sdk.track("the_100th"); // queue reaches 100 → flush
  await sleep(10);
  assert.equal(h.server.batches.length >= 1, true, "size trigger did not flush");
  assert.equal(h.server.batches[0].events.length, 100, "the whole queue went out");
  sdk.shutdown();
});

test("D10 rule 4: identify() before init completes is persisted", async () => {
  let release;
  const h = harness({ gate: () => new Promise((r) => (release = r)) });
  const sdk = h.mk(); // init never lands until release
  sdk.identify("user_42");
  assert.equal(h.fake.get(USER_KEY), null, "identify persisted before init");
  release("app://x/ok");
  await sleep(10); // init lands and applies the pending identify
  assert.equal(h.fake.get(USER_KEY), "user_42", "identify was lost across init");
  await sdk.track("click");
  await sdk.flush();
  const all = [...h.server.store.values()];
  assert.ok(
    all.every((e) => e.user_id === "user_42"),
    "user_id missing on events",
  );
  sdk.shutdown();
});

test("T10: shutdown mid-flight ignores the in-flight result and loses nothing", async () => {
  const h = harness({ status: "hang" });
  const sdk = h.mk();
  await sdk.track("click");
  void sdk.flush();
  await sleep(10); // request is out, hung
  sdk.shutdown(); // gen bump: the hung flush's result must be ignored
  assert.equal(sdk.__internal.queued().length, 3, "shutdown lost the queue");

  // Re-init over the same storage: the queue replays, nothing duplicated.
  const sdk2 = h.mk();
  await sleep(5);
  h.server.status = 200;
  await sdk2.flush();
  assert.equal(h.server.store.size, 3, "stored set changed across shutdown");
  assert.equal(h.server.batches.filter((b) => b.events.length === 3).length, 2);
  sdk2.shutdown();
});

test("headless instance never writes or sends", async () => {
  const h = harness();
  const before = h.fake.keys().length;
  const sdk = initRn({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    storage: makeMmkv(h.fake),
    fetchFn: h.server,
    now: () => h.clock.now,
    headless: true,
  });
  await sdk.track("push_received");
  await sdk.flush();
  sdk.identify("u");
  sdk.setConsent(true);
  sdk.onDeepLink("app://x");
  assert.equal(h.fake.keys().length, before, "headless wrote to storage");
  assert.equal(h.server.batches.length, 0, "headless sent a batch");
  assert.equal(sdk.state(), "uninitialized");
});

test("heartbeats fire only while foregrounded and within the 5-minute interaction window (D16)", async () => {
  // Hung server: flushes (e.g. the backgrounding one) can't drain the queue,
  // so the durable queue is a faithful count of heartbeats created.
  const h = harness({ heartbeatMs: 5, status: "hang" });
  const count = () => readQueue(h.fake).filter((e) => e.name === "heartbeat").length;
  const sdk = h.mk();
  await sdk.track("click"); // interaction: marks recent activity
  await sleep(25); // a few heartbeats fire while active + recently active
  const before = count();
  assert.ok(before >= 1, "eligible heartbeat was suppressed");

  // Backgrounded: no heartbeats created (the flush itself is hung, fine).
  h.appState.change("background");
  await sleep(25);
  assert.equal(count(), before, "heartbeat created while backgrounded");

  // Foregrounded but idle for > 5 min: suppressed.
  h.appState.change("active");
  h.clock.now += 6 * 60 * 1000;
  await sleep(25);
  assert.equal(count(), before, "heartbeat fired outside the interaction window");
  sdk.shutdown();
});

test("state machine: uninitialized → ready → shutting_down → uninitialized", async () => {
  const h = harness();
  const sdk = h.mk();
  assert.equal(sdk.state(), "initializing");
  await sleep(5);
  assert.equal(sdk.state(), "ready");
  await sdk.track("click");
  assert.equal(sdk.__internal.core() !== null, true);
  sdk.shutdown();
  assert.equal(sdk.state(), "uninitialized");
  assert.equal(sdk.__internal.core(), null);
});

test("parseDeepLink extracts the path and only utm_* parameters", () => {
  const { url, utm } = parseDeepLink(
    "https://app.example.com/orders?id=7&utm_source=email&utm_campaign=x",
  );
  assert.equal(url, "/orders");
  assert.deepEqual(utm, { utm_source: "email", utm_campaign: "x" });
  assert.deepEqual(parseDeepLink("not a url"), { url: null, utm: {} });
});
