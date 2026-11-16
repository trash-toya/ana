// RN SDK conformance runner: the SAME shared case list the web SDK runs
// (packages/shared/test/conformance), driven through RN shims — a Map-backed
// MMKV, a fake AppState, a fireable deep-link source, the fake conformance
// server and an injectable clock. No Web Locks exist on RN: the lease mutex
// runs single-context (browser.locks: null).
import { test } from "vitest";
import assert from "node:assert/strict";

import { initRn } from "../dist/index.js";
import {
  conformance,
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
    addEventListener: (_type, cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    /** Test hook: transition and notify. */
    change(state) {
      this.currentState = state;
      for (const cb of listeners) cb(state);
    },
  };
}

function makeDeepLinks({ initial = null } = {}) {
  const listeners = new Set();
  return {
    getInitialURL: async () => initial,
    addEventListener: (_type, cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    /** Test hook: a warm deep link. */
    open(url) {
      for (const cb of listeners) cb(url);
    },
  };
}

function makeEnv(opts = {}) {
  const fake = fakeStorage();
  const storage = makeMmkv(fake);
  const clock = { now: START_MS };
  const server = fakeServer();
  if (opts.status !== undefined) server.status = opts.status;
  const appState = makeAppState();
  const deepLinks = makeDeepLinks(opts);

  const env = {
    sdk: null,
    storage: fake,
    clock,
    server,
    hidden: false,
    appState,
    deepLinks,
    setQuota: (n) => fake.setBudget(n),
    storageUsedBytes: () =>
      [...fake.keys()].reduce((a, k) => a + (fake.get(k)?.length ?? 0), 0),
    setHidden: (b) => {
      env.hidden = b;
      appState.change(b ? "background" : "active");
    },
    _all: [],
  };

  function build(o = {}) {
    const sdk = initRn({
      apiKey: "k",
      endpoint: "https://x/v1/events",
      heartbeatMs: o.heartbeatMs ?? opts.heartbeatMs ?? 30_000,
      flushMs: o.flushMs ?? opts.flushMs ?? 250,
      storage,
      appState,
      deepLinks,
      fetchFn: server,
      now: () => clock.now,
      screen: () => "/s",
    });
    env._all.push(sdk);
    return sdk;
  }

  env.sdk = build();
  env.reinit = (o = {}) => {
    const next = build(o);
    env.sdk = next;
    return next;
  };
  env.restart = () => env.reinit();
  // Crash-window tampering helpers (shared case 10).
  env.tamperRecord = (patch) => {
    const raw = fake.get("sa:session");
    if (!raw) throw new Error("no record to tamper");
    fake.set("sa:session", JSON.stringify({ ...JSON.parse(raw), ...patch }));
  };
  env.tamperQueueAdd = (patch) => {
    const ghost = {
      event_id: "01900000-0000-7000-8000-0000000000ff",
      session_id: JSON.parse(fake.get("sa:session")).session_id,
      time: new Date(clock.now).toISOString(),
      name: "ghost",
      props: {},
      ...patch,
    };
    fake.set(`sa_ev:${ghost.event_id}`, JSON.stringify(ghost));
  };
  return env;
}

const cases = conformance(makeEnv);
if (cases.length !== 16)
  throw new Error(`expected 16 conformance cases, got ${cases.length}`);

for (const c of cases) {
  test(`rn conformance: ${c.name}`, async () => {
    const env = makeEnv();
    try {
      await c.run(env);
    } finally {
      for (const sdk of env._all) sdk.shutdown();
    }
  });
}

// RN-specific conformance support: backgrounding flushes immediately.

test("rn conformance: backgrounding flushes immediately without waiting for the cadence", async () => {
  const env = makeEnv({ flushMs: 60_000 });
  await env.sdk.track("click");
  if (env.server.batches.length !== 0) throw new Error("flushed before backgrounding");
  env.setHidden(true); // AppState → background
  await sleep(10);
  assert.equal(env.server.batches.length >= 1, true, "background did not flush");
  assert.equal(env.sdk.__internal.queued().length, 0);
  env.sdk.shutdown();
});
