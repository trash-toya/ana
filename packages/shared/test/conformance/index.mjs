// Shared SDK conformance suite (docs/plan/05-sdks.md §"Conformance suite").
// One fake clock + one fake server (with server-side dedupe semantics) + a
// restart primitive. Both SDKs run the SAME cases: `conformance(createEnv)`
// returns [{name, run}]; each package's test file drives it through its own
// shims. The env contract every SDK must provide:
//
//   env.sdk          — the SDK under test (init'd)
//   env.storage      — the durable store (readable for assertions)
//   env.clock        — { now: number } the SDK reads
//   env.server       — fakeServer(): .batches, .store, .status, .requests
//   env.restart()    — new SDK over the same storage/clock/server (a "kill")
//   env.fire(type)   — lifecycle events (pagehide, visibilitychange, pageshow)
//   env.setHidden(b) — visibility toggle
//   env.setQuota(n)  — byte budget for the primary store (quota simulation)
//
// The fake server validates like the ingest API would: 200 with per-event
// outcomes, `invalid_event` names permanently rejected, duplicate event_ids
// deduped (stored set grows only on first sight).

export const START_MS = 1_700_000_000_000;

export function fakeServer() {
  const store = new Map(); // event_id → event (server-side durable set)
  const batches = [];
  const requests = [];
  const state = { status: 200 }; // number | fn(callIndex) | "network" | "hang" | "commit-drop"
  const fn = async (url, opts) => {
    const body = JSON.parse(opts.body);
    batches.push(body);
    const idx = batches.length;
    const status = typeof state.status === "function" ? state.status(idx) : state.status;
    requests.push(status);
    if (status === "hang") return new Promise(() => {});
    if (status === "commit-drop") {
      // The server commits the batch, then the response is lost (network).
      commit(body);
      throw new TypeError("fetch failed");
    }
    if (status === "network") throw new TypeError("fetch failed");
    if (status !== 200) return { status, json: async () => ({}) };
    const accepted = [];
    const rejected = [];
    commit(body, { accepted, rejected });
    return { status: 200, json: async () => ({ accepted, rejected }) };
  };
  function commit(body, out = {}) {
    for (const e of body.events) {
      if (store.has(e.event_id)) {
        out.accepted?.push(e.event_id);
        continue;
      }
      if (e.name === "invalid_event") {
        out.rejected?.push({ event_id: e.event_id, reason: "bad_name" });
        continue;
      }
      store.set(e.event_id, e);
      out.accepted?.push(e.event_id);
    }
  }
  fn.store = store;
  fn.batches = batches;
  fn.requests = requests;
  // `server.status = …` must reach the internal state (tests overwrite it).
  Object.defineProperty(fn, "status", {
    get: () => state.status,
    set: (v) => {
      state.status = v;
    },
  });
  return fn;
}

export function fakeStorage() {
  const m = new Map();
  let budget = Infinity;
  const size = () => [...m.values()].reduce((a, v) => a + v.length, 0);
  return {
    keys: () => [...m.keys()],
    get: (k) => (m.has(k) ? m.get(k) : null),
    set(k, v) {
      const prev = m.get(k);
      if (size() - (prev?.length ?? 0) + String(v).length > budget) {
        throw { name: "QuotaExceededError", code: 22 };
      }
      m.set(k, String(v));
    },
    remove: (k) => m.delete(k),
    setBudget: (n) => (budget = n),
  };
}

export function fakeLocks() {
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

export function fakeLifecycle() {
  const listeners = new Map();
  return {
    add: (type, cb) => listeners.set(type, cb),
    remove: (type) => listeners.delete(type),
    fire: (type, ev) => listeners.get(type)?.(ev),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function conformance(createEnv) {
  return [
    {
      name: "idle expiry at 31 min: new session, seq restarts at 1",
      run: async (env) => {
        await env.sdk.track("click");
        const oldId = env.sdk.__internal.session().session_id;
        env.clock.now += 31 * 60 * 1000;
        await env.sdk.track("click");
        await env.sdk.flush();
        const rec = env.sdk.__internal.session();
        if (rec.session_id === oldId) throw new Error("session did not rotate");
        const seqs = [...env.server.store.values()]
          .filter((e) => e.session_id === rec.session_id)
          .map((e) => e.seq);
        if (seqs[0] !== 1) throw new Error(`seq does not restart: ${seqs[0]}`);
      },
    },
    {
      name: "gap at 29 min: same session continues",
      run: async (env) => {
        await env.sdk.track("click");
        const id = env.sdk.__internal.session().session_id;
        env.clock.now += 29 * 60 * 1000;
        await env.sdk.track("click");
        if (env.sdk.__internal.session().session_id !== id) {
          throw new Error("session rotated inside the idle window");
        }
      },
    },
    {
      name: "24 h age: new session even with continuous events",
      run: async (env) => {
        await env.sdk.track("click");
        const oldId = env.sdk.__internal.session().session_id;
        env.clock.now += 24 * 60 * 60 * 1000 + 1;
        await env.sdk.track("click");
        if (env.sdk.__internal.session().session_id === oldId) {
          throw new Error("session did not rotate on age");
        }
      },
    },
    {
      name: "5,000 events: rotation, no event lost",
      run: async (env) => {
        await env.sdk.track("click"); // session_start + page_view + click = count 3
        for (let i = 0; i < 4_997; i++) {
          env.clock.now += 1;
          await env.sdk.track("click");
        }
        // count is now exactly 5,000 → the next event rotates
        await env.sdk.track("click");
        await env.sdk.flush();
        const rec = env.sdk.__internal.session();
        if (rec.seq !== 3) throw new Error(`new session seq = ${rec.seq}, want 3`);
        if (env.server.store.size !== 3 + 4_997 + 3) {
          throw new Error(
            `stored ${env.server.store.size} events, expected ${3 + 4_997 + 3}`,
          );
        }
      },
    },
    {
      name: "heartbeat while hidden: nothing emitted, no session created",
      run: async (env) => {
        env.setHidden(true);
        await env.sdk.track("heartbeat");
        if (env.sdk.__internal.session() !== null) {
          throw new Error("heartbeat created a session");
        }
        if (env.server.batches.length !== 0)
          throw new Error("heartbeat sent while hidden");
      },
    },
    {
      name: "heartbeat after 6 min idle: nothing emitted, session not extended",
      run: async (env) => {
        await env.sdk.track("click");
        const before = env.sdk.__internal.session().last_seen_at;
        env.clock.now += 6 * 60 * 1000;
        await env.sdk.track("heartbeat");
        const rec = env.sdk.__internal.session();
        if (rec.last_seen_at !== before)
          throw new Error("heartbeat extended the session");
        const stored = [...env.server.store.values()].filter(
          (e) => e.name === "heartbeat",
        );
        if (stored.length !== 0) throw new Error("idle heartbeat emitted");
      },
    },
    {
      name: "batch with one invalid event: valid stored, invalid removed, counts identical",
      run: async (env) => {
        await env.sdk.track("good_one");
        env.clock.now += 1000;
        await env.sdk.track("invalid_event"); // fake server rejects by name
        env.clock.now += 1000;
        await env.sdk.track("good_two");
        await env.sdk.flush();
        if (env.sdk.__internal.queued().length !== 0)
          throw new Error("queue did not drain");
        const names = [...env.server.store.values()].map((e) => e.name);
        if (names.includes("invalid_event")) throw new Error("invalid event stored");
        // session_start + page_view + good_one + good_two (invalid removed)
        if (names.length !== 4)
          throw new Error(`stored ${names.length} valid events, expected 4`);
      },
    },
    {
      name: "replay after a lost response: identical stored set, no duplicates",
      run: async (env) => {
        // Two attempts commit server-side but lose the response; the third
        // attempt gets its 200. The retry replays the SAME events.
        env.server.status = (i) => (i <= 2 ? "commit-drop" : 200);
        await env.sdk.track("click");
        await env.sdk.flush();
        const firstSet = [...env.server.store.values()].map((e) => e.event_id).sort();
        if (firstSet.length !== 3) throw new Error(`stored ${firstSet.length}, want 3`);
        const sentIds = env.server.batches.map((b) =>
          b.events.map((e) => e.event_id).sort(),
        );
        if (JSON.stringify(sentIds[0]) !== JSON.stringify(sentIds[2])) {
          throw new Error("retry did not replay the same event ids");
        }
        if (env.sdk.__internal.queued().length !== 0)
          throw new Error("queue did not drain");
      },
    },
    {
      name: "kill mid-flush: after restart the queue holds everything unacknowledged",
      run: async (env) => {
        await env.sdk.track("click");
        env.server.status = "hang"; // request in flight, ack never processed
        const flushPromise = env.sdk.flush();
        await sleep(10); // let the (hung) request go out
        env.restart(); // kill: no ack, nothing dequeued
        if (env.sdk.__internal.queued().length !== 3) {
          throw new Error(
            `queued ${env.sdk.__internal.queued().length} after kill, want 3`,
          );
        }
        env.server.status = 200;
        await env.sdk.flush();
        if (env.server.store.size !== 3)
          throw new Error("stored set changed across the kill");
      },
    },
    {
      name: "kill between session save and enqueue: counters reconciled, no duplicate seq",
      run: async (env) => {
        await env.sdk.track("click");
        await env.sdk.track("click");
        // Crash window: a durable event whose seq the record does not know.
        env.tamperRecord({ seq: 4 });
        env.tamperQueueAdd({ seq: 5 });
        env.restart();
        await env.sdk.track("click");
        await env.sdk.flush();
        const rec = env.sdk.__internal.session();
        const seqs = [...env.server.store.values()].map((e) => e.seq);
        if (rec.seq !== Math.max(...seqs)) throw new Error("record seq below queued max");
      },
    },
    {
      name: "401 then 200: queue preserved, then drained",
      run: async (env) => {
        env.server.status = (i) => (i === 1 ? 401 : 200);
        await env.sdk.track("click");
        await env.sdk.flush();
        if (env.sdk.__internal.queued().length !== 3)
          throw new Error("401 dropped the queue");
        await env.sdk.flush();
        if (env.sdk.__internal.queued().length !== 0)
          throw new Error("queue not drained");
      },
    },
    {
      name: "400: batch dropped once, no loop",
      run: async (env) => {
        env.server.status = 400;
        await env.sdk.track("click");
        await env.sdk.flush();
        if (env.sdk.__internal.queued().length !== 0)
          throw new Error("queue kept after 400");
        if (env.server.batches.length !== 1)
          throw new Error(`looped: ${env.server.batches.length} batches`);
      },
    },
    {
      name: "overflow: heartbeats dropped first, then oldest; sdk_dropped counts match",
      run: async (env) => {
        await env.sdk.track("click"); // 3 events persisted
        await env.sdk.track("heartbeat"); // 1 heartbeat
        env.setQuota(env.storageUsedBytes()); // freeze: any new write overflows
        await env.sdk.track("click"); // overflow → drop heartbeats first, then oldest
        const pending = env.sdk.__internal.droppedPending();
        if (pending === 0) throw new Error("drop not counted");
        // The report's own write may evict further victims; keep flushing
        // until the counter settles (it converges: each report is smaller
        // than what it makes room for, and the queue drains).
        for (let i = 0; i < 5 && env.sdk.__internal.droppedPending() > 0; i++) {
          await env.sdk.flush();
        }
        if (env.sdk.__internal.droppedPending() !== 0)
          throw new Error("drop report did not converge");
        const reports = [...env.server.store.values()].filter(
          (e) => e.name === "sdk_dropped",
        );
        if (reports.length === 0) throw new Error("sdk_dropped not reported");
        const total = reports.reduce((a, r) => a + r.props.dropped, 0);
        if (total < pending) throw new Error(`reported ${total}, counted ${pending}`);
        const names = [...env.server.store.values()].map((e) => e.name);
        if (names.includes("heartbeat")) {
          throw new Error("dropped heartbeat was still stored");
        }
      },
    },
    {
      name: "sdk_dropped after idle expiry: report still goes out",
      run: async (env) => {
        await env.sdk.track("click");
        env.setQuota(env.storageUsedBytes()); // freeze: any new write overflows
        await env.sdk.track("click"); // overflow → drops counted
        if (env.sdk.__internal.droppedPending() === 0)
          throw new Error("drop not counted");
        // Idle past expiry: the session is gone by the time the report runs.
        // D10 rule 2: the report must not be guarded by `&& sess` — it rides
        // the normal emit path (a new session may start under normal expiry
        // rules), but the drops are never lost to the dead session.
        env.clock.now += 31 * 60 * 1000;
        for (let i = 0; i < 5 && env.sdk.__internal.droppedPending() > 0; i++) {
          await env.sdk.flush();
        }
        const reports = [...env.server.store.values()].filter(
          (e) => e.name === "sdk_dropped",
        );
        if (reports.length === 0)
          throw new Error("sdk_dropped not reported after expiry");
      },
    },
    {
      name: "consent off: queue and session cleared, nothing sent",
      run: async (env) => {
        await env.sdk.track("click");
        env.sdk.setConsent(false);
        if (env.sdk.__internal.queued().length !== 0)
          throw new Error("queue not cleared");
        if (env.sdk.__internal.session() !== null) throw new Error("session not cleared");
        await env.sdk.track("click");
        if (env.server.batches.length !== 0) throw new Error("sent after consent off");
      },
    },
    {
      name: "init() twice: one shared session, no duplicate seq or time",
      run: async (env) => {
        const second = env.reinit();
        await Promise.all([
          env.sdk.track("click"),
          second.track("click"),
          env.sdk.track("click"),
          second.track("click"),
        ]);
        const seqs = [...env.server.store.values()].map((e) => e.seq);
        if (new Set(seqs).size !== seqs.length)
          throw new Error("duplicate seq across contexts");
        const times = [...env.server.store.values()].map((e) => e.time);
        if (new Set(times).size !== times.length)
          throw new Error("duplicate time across contexts");
      },
    },
  ];
}

export { sleep };
