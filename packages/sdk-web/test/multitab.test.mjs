// P2-3: two-context multi-tab correctness and the no-Web-Locks fallback.
// With Web Locks, two SDK instances over one storage share one session record
// — no duplicate time, no duplicate seq (05-sdks.md §"Multi-tab correctness").
// Without Web Locks, the localStorage lease (session.ts `withLease`) plays the
// role of the mutex; if the election fails, the emit proceeds anyway and the
// D17 residual race applies (a raced cold start yields short counted sessions,
// never a lost event). A single JS context cannot interleave two emit
// prologues, so the two-session outcome is only reachable across real tabs —
// here we prove the mechanisms the SDK actually guarantees.
import { test } from "vitest";
import assert from "node:assert/strict";
import { init } from "../dist/index.js";
import { queueDepth } from "../dist/queue.js";
import { LEASE_KEY, SESSION_KEY, loadRecord, readLease } from "../dist/session.js";

function fakeStorage(initial = {}) {
  const m = new Map(Object.entries(initial));
  return {
    keys: () => [...m.keys()],
    get: (k) => (m.has(k) ? m.get(k) : null),
    set: (k, v) => m.set(k, String(v)),
    remove: (k) => m.delete(k),
  };
}

/** Web-Lock mutex serialising every holder exactly like navigator.locks. */
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

/** Shared recording fetch so both tabs feed one observable server. */
function fakeFetch() {
  const calls = [];
  const fn = async (url, opts) => {
    const body = JSON.parse(opts.body);
    calls.push(body);
    return {
      status: 200,
      json: async () => ({
        accepted: body.events.map((e) => e.event_id),
        rejected: [],
      }),
    };
  };
  fn.calls = calls;
  return fn;
}

const NOW = 1_700_000_000_000;

function makeTab(store, { locks, lease } = {}) {
  const fetchFn = fakeFetch();
  const sdk = init({
    apiKey: "sk_test",
    endpoint: "https://ingest.test/v1/events",
    heartbeatMs: 0,
    flushDebounceMs: 60_000,
    retryBaseMs: 1,
    browser: {
      storage: store,
      // Node ≥18 implements real Web Locks; `null` opts a tab out so the
      // no-locks tests actually exercise the lease-based election.
      locks: locks ?? null,
      fetchFn,
      now: () => NOW,
      jitter: () => 0,
      page: () => ({ url: "/p", referrer: null, utm: {} }),
      visibility: () => true,
      ...(lease ? { lease } : {}),
    },
  });
  return { sdk, fetchFn };
}

/** Flush both tabs and flatten what the (shared) server received. */
async function drainAndCollect(tabs) {
  for (const t of tabs) await t.sdk.flush();
  return tabs[0].fetchFn.calls.flatMap((body) => body.events);
}

function assertNoDuplicateTimeOrSeq(events) {
  const seqs = events.map((e) => e.seq);
  assert.equal(new Set(seqs).size, seqs.length, "no duplicate seq");
  const times = events.map((e) => e.time);
  assert.equal(new Set(times).size, times.length, "no duplicate time");
  const sessions = new Set(events.map((e) => e.session_id));
  return { seqs, times, sessions };
}

test("two tabs with Web Locks, concurrent cold start: one session, no duplicate seq/time", async () => {
  const store = fakeStorage();
  const locks = fakeLocks();
  const a = makeTab(store, { locks });
  const b = makeTab(store, { locks });
  await Promise.all([a.sdk.track("click"), b.sdk.track("click")]);

  const events = await drainAndCollect([a, b]);
  // Tab A cold-starts (session_start + page_view + click = 3). Tab B joins the
  // stored session and appends only its own click: session_start/page_view are
  // minted once per session, not once per tab.
  assert.equal(events.length, 4, "one cold start + one appended event");
  const { seqs, sessions } = assertNoDuplicateTimeOrSeq(events);
  assert.equal(sessions.size, 1, "one shared session");
  assert.deepEqual(
    [...seqs].sort((x, y) => x - y),
    [1, 2, 3, 4],
    "seq allocated once across tabs",
  );
  assert.equal(loadRecord(store)?.session_id, events[0].session_id);
});

test("two tabs with Web Locks, interleaved tracks: strictly increasing shared time and seq", async () => {
  const store = fakeStorage();
  const locks = fakeLocks();
  const a = makeTab(store, { locks });
  const b = makeTab(store, { locks });
  await a.sdk.track("click");
  await b.sdk.track("purchase");
  await a.sdk.track("click");
  await b.sdk.track("click");

  const events = await drainAndCollect([a, b]);
  // 3 + 1 + 1 + 1 = 6 (tab A's cold start mints three events)
  assert.equal(events.length, 6);
  const { seqs, times, sessions } = assertNoDuplicateTimeOrSeq(events);
  assert.equal(sessions.size, 1);
  assert.deepEqual(
    [...seqs].sort((x, y) => x - y),
    [1, 2, 3, 4, 5, 6],
  );
  // frozen clock: the strictly-increasing rule still mints distinct times
  const sorted = [...times].sort();
  assert.equal(new Set(sorted).size, sorted.length);
});

test("no Web Locks: the lease serialises two tabs' concurrent cold start", async () => {
  const store = fakeStorage();
  const a = makeTab(store); // locks: undefined → lease path
  const b = makeTab(store);
  await Promise.all([a.sdk.track("click"), b.sdk.track("click")]);

  const events = await drainAndCollect([a, b]);
  assert.equal(events.length, 4, "one cold start + one appended event — nothing lost");
  const { seqs, sessions } = assertNoDuplicateTimeOrSeq(events);
  assert.equal(sessions.size, 1, "the lease let exactly one cold start win");
  assert.deepEqual(
    [...seqs].sort((x, y) => x - y),
    [1, 2, 3, 4],
  );
});

test("no Web Locks: the lease is released after the emit and a stale lease is taken over", async () => {
  const store = fakeStorage();
  const a = makeTab(store);
  await a.sdk.track("click");
  assert.equal(readLease(store), null, "lease released after a successful emit");

  // A foreign lease that has already expired is taken over immediately.
  store.set(LEASE_KEY, JSON.stringify({ holder: "some-other-tab", expires_at: NOW - 1 }));
  const started = Date.now();
  const b = makeTab(store);
  await b.sdk.track("click");
  assert.ok(
    Date.now() - started < 200,
    `stale lease taken over, took ${Date.now() - started}ms`,
  );
  assert.equal(readLease(store), null, "and released again after the emit");
  assert.equal(queueDepth(store), 3 + 1, "both emits landed (3 cold + 1 appended)");
  store.remove(LEASE_KEY);
});

test("no Web Locks, election fails: the emit proceeds anyway, nothing is lost (D17)", async () => {
  // A foreign lease that never expires on the (frozen) SDK clock, plus a tiny
  // wall-clock budget: both tabs exhaust the budget and proceed unlocked.
  const store = fakeStorage({
    [LEASE_KEY]: JSON.stringify({
      holder: "a-real-other-tab",
      expires_at: NOW + 3_600_000,
    }),
  });
  const a = makeTab(store, { lease: { budgetMs: 5 } });
  const b = makeTab(store, { lease: { budgetMs: 5 } });
  await Promise.all([a.sdk.track("click"), b.sdk.track("click")]);

  const events = await drainAndCollect([a, b]);
  assert.equal(events.length, 4, "the degraded path never loses an event");
  assertNoDuplicateTimeOrSeq(events); // one JS context: the sync emit prologue serialises
  assert.ok(
    events.every((e) => e.session_id),
    "every event carries a session",
  );
  assert.ok(loadRecord(store), "a session record exists");
  store.remove(LEASE_KEY);
});

test("no Web Locks, concurrent emits while a lease is held: waiters join instead of double-minting", async () => {
  const store = fakeStorage();
  const a = makeTab(store);
  const b = makeTab(store, { lease: { budgetMs: 1_000 } });
  // Start three emits at once; the lease holder runs first, the rest wait and
  // append to the stored record with fresh seq/time.
  await Promise.all([
    a.sdk.track("click"),
    a.sdk.track("purchase"),
    b.sdk.track("click"),
  ]);
  const events = await drainAndCollect([a, b]);
  assert.equal(events.length, 5); // 3 cold + 2 appended
  const { seqs, sessions } = assertNoDuplicateTimeOrSeq(events);
  assert.equal(sessions.size, 1);
  assert.deepEqual(
    [...seqs].sort((x, y) => x - y),
    [1, 2, 3, 4, 5],
  );
});

test("locks absent in one tab, present in the other: the lease and the lock agree on one session", async () => {
  // A mixed browser (one modern tab, one Safari < 15.4 tab) still yields one
  // session: the locked tab serialises via the Web Lock, the unlocked one via
  // the lease — both guard the same record.
  const store = fakeStorage();
  const locks = fakeLocks();
  const a = makeTab(store, { locks });
  const b = makeTab(store); // no locks
  await Promise.all([a.sdk.track("click"), b.sdk.track("click")]);
  await a.sdk.track("purchase");
  await b.sdk.track("purchase");

  const events = await drainAndCollect([a, b]);
  assert.equal(events.length, 3 + 1 + 1 + 1); // A cold start (3) + B's click + 2 purchases
  const { seqs, sessions } = assertNoDuplicateTimeOrSeq(events);
  assert.equal(sessions.size, 1, "one session despite the mixed lock mechanisms");
  assert.deepEqual(
    [...seqs].sort((x, y) => x - y),
    [1, 2, 3, 4, 5, 6],
  );
});

test("the session record and lease keys do not collide with queue keys", () => {
  assert.equal(SESSION_KEY.startsWith("sa_ev:"), false);
  assert.equal(LEASE_KEY.startsWith("sa_ev:"), false);
});
