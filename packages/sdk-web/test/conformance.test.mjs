// Web SDK conformance runner: the shared case list (packages/shared/test/
// conformance) driven through the web shims — Map-backed localStorage, a
// promise-chain Web-Lock mutex, the fake conformance server, an injectable
// clock and a fireable lifecycle source.
import test from "node:test";
import assert from "node:assert/strict";

import { init } from "../dist/index.js";
import {
  conformance,
  fakeLifecycle,
  fakeLocks,
  fakeServer,
  fakeStorage,
  sleep,
  START_MS,
} from "../../shared/test/conformance/index.mjs";

function makeEnv(opts = {}) {
  const storage = fakeStorage();
  const clock = { now: START_MS };
  const server = fakeServer();
  if (opts.status !== undefined) server.status = opts.status;
  const locks = fakeLocks();
  const lifecycle = fakeLifecycle();

  const env = {
    sdk: null,
    storage,
    clock,
    server,
    hidden: !!opts.hidden,
    fire: lifecycle.fire,
    setQuota: (n) => storage.setBudget(n),
    storageUsedBytes: () =>
      [...storage.keys()].reduce((a, k) => a + (storage.get(k)?.length ?? 0), 0),
    setHidden: (b) => (env.hidden = b),
    /** Every context built on this env — the runner shuts them all down. */
    _all: [],
  };

  function build(o = {}) {
    const sdk = init({
      apiKey: "k",
      endpoint: "https://x/v1/events",
      heartbeatMs: o.heartbeatMs ?? opts.heartbeatMs ?? 30_000,
      flushDebounceMs: o.flushDebounceMs ?? opts.flushDebounceMs ?? 250,
      browser: {
        storage,
        locks,
        fetchFn: server,
        now: () => clock.now,
        page: () => ({ url: "/p", referrer: null, utm: {} }),
        visibility: () => !env.hidden,
        lifecycle,
      },
    });
    env._all.push(sdk);
    return sdk;
  }

  env.sdk = build();
  // reinit: a second context over the SAME storage/clock/server (a "kill"
  // keeps the durable state; the old instance is simply abandoned).
  env.reinit = (o = {}) => {
    const next = build(o);
    env.sdk = next;
    return next;
  };
  env.restart = () => env.reinit();
  // Crash-window tampering helpers (shared case 10).
  env.tamperRecord = (patch) => {
    const raw = storage.get("sa:session");
    if (!raw) throw new Error("no record to tamper");
    storage.set("sa:session", JSON.stringify({ ...JSON.parse(raw), ...patch }));
  };
  env.tamperQueueAdd = (patch) => {
    const ghost = {
      event_id: "01900000-0000-7000-8000-0000000000ff",
      session_id: JSON.parse(storage.get("sa:session")).session_id,
      time: new Date(clock.now).toISOString(),
      name: "ghost",
      props: {},
      ...patch,
    };
    storage.set(`sa_ev:${ghost.event_id}`, JSON.stringify(ghost));
  };
  return env;
}

const cases = conformance(makeEnv);
if (cases.length !== 15) throw new Error(`expected 15 conformance cases, got ${cases.length}`);

for (const c of cases) {
  test(`conformance: ${c.name}`, async () => {
    const env = makeEnv();
    try {
      await c.run(env);
    } finally {
      for (const sdk of env._all) sdk.shutdown();
    }
  });
}

// Web-specific conformance support tests (unload flush, mirror recovery).

test("conformance: pagehide flushes immediately without waiting for the debounce", async () => {
  const env = makeEnv({ flushDebounceMs: 60_000 });
  await env.sdk.track("click");
  if (env.server.batches.length !== 0) throw new Error("flushed before pagehide");
  env.fire("pagehide");
  await sleep(10);
  assert.equal(env.server.batches.length >= 1, true, "pagehide did not flush");
  assert.equal(env.sdk.__internal.queued().length, 0);
  env.sdk.shutdown();
});

test("conformance: bfcache pageshow {persisted} resumes the same session", async () => {
  const env = makeEnv({ flushDebounceMs: 60_000 });
  await env.sdk.track("click");
  env.fire("pagehide");
  await sleep(5);
  env.fire("pageshow", { persisted: true });
  const id = env.sdk.__internal.session().session_id;
  env.clock.now += 2000;
  await env.sdk.track("click"); // no new session mint on bfcache resume
  assert.equal(env.sdk.__internal.session().session_id, id);
  env.sdk.shutdown();
});

test("conformance: mirror recovers events the primary lost", async () => {
  const env = makeEnv({ flushDebounceMs: 60_000 });
  const mirror = {
    store: new Map(),
    async put(e) { this.store.set(e.event_id, JSON.parse(JSON.stringify(e))); },
    async delete(ids) { for (const id of ids) this.store.delete(id); },
    async readAll() { return [...this.store.values()]; },
  };
  const sdk = init({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 60_000,
    browser: {
      storage: env.storage,
      locks: undefined, // no locks env; direct call
      fetchFn: env.server,
      now: () => env.clock.now,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      mirror: Promise.resolve(mirror),
    },
  });
  await sdk.track("click");
  await sdk.__internal.mirrorReady();
  assert.equal(mirror.store.size, 3, "mirror did not receive the events");

  // localStorage loses everything (eviction); the mirror still holds them.
  for (const k of env.storage.keys()) if (k.startsWith("sa_ev:")) env.storage.remove(k);
  assert.equal(sdk.__internal.queued().length, 0);
  sdk.shutdown();

  // A restart re-runs restore recovery: the mirror refills the primary.
  const sdk2 = init({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 60_000,
    browser: {
      storage: env.storage,
      locks: undefined,
      fetchFn: env.server,
      now: () => env.clock.now,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      mirror: Promise.resolve(mirror),
    },
  });
  await sdk2.__internal.mirrorReady();
  assert.equal(sdk2.__internal.queued().length, 3, "recovery did not refill the primary");
  await sdk2.flush();
  assert.equal(env.server.store.size, 3, "recovered events were not flushed");
  sdk2.shutdown();
});

test("conformance: quota fallback persists through the awaited mirror write", async () => {
  const env = makeEnv({ flushDebounceMs: 60_000 });
  const mirror = {
    store: new Map(),
    async put(e) { this.store.set(e.event_id, e); },
    async delete(ids) { for (const id of ids) this.store.delete(id); },
    async readAll() { return [...this.store.values()]; },
  };
  const sdk = init({
    apiKey: "k",
    endpoint: "https://x/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 60_000,
    browser: {
      storage: env.storage,
      locks: undefined,
      fetchFn: env.server,
      now: () => env.clock.now,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      mirror: Promise.resolve(mirror),
    },
  });
  await sdk.track("click"); // 3 events, mirrored
  assert.equal(mirror.store.size, 3);
  env.setQuota(env.storageUsedBytes()); // primary frozen
  await sdk.track("click"); // overflow: victims dropped and counted; the new
  // event may land in the primary (after freeing) or the mirror — either way
  // it is durable before track() resolves.
  assert.equal(sdk.__internal.droppedPending() >= 1, true, "overflow was not counted");
  const all = [...sdk.__internal.queued(), ...mirror.store.values()];
  const clickSeqs = all.filter((e) => e.name === "click").map((e) => e.seq);
  assert.equal(clickSeqs.length >= 2, true, "the overflow click was lost entirely");
  assert.equal(clickSeqs.includes(4), true, "the overflow click is not durable");
  sdk.shutdown();
});
