// Buffer + flush unit tests with an injected batch executor and fake timers.
// Vitest fake timers are synchronous (advanceTimersByTime), so after every
// tick the test awaits the waiter — the flush continuation drains on microtasks.
import { test, onTestFinished, vi } from "vitest";
import assert from "node:assert/strict";
import { BufferFullError, Flusher } from "../dist/flusher.js";

function row(n, sessionId = "018f2b00-0000-7000-8000-0000000000c7") {
  return {
    tenant_id: "018f0000-0000-7000-8000-0000000000t0",
    event_id: `018f2c00-0000-7000-8000-${n.toString(16).padStart(12, "0")}`,
    session_id: sessionId,
    name: "click",
    cat: "e",
    time: "2026-11-15T11:59:00.000Z",
    seq: null,
    anon_id: "a1",
    user_id: null,
    url: null,
    props: {},
    payload_hash: "h" + n,
    ord: 0,
  };
}

const ledgerOf = (rows) => ({ rows, rejected: [], dups: [] });

function makeFlusher(t, { submitBatch, ...opts }) {
  const calls = [];
  const fn =
    submitBatch ??
    (async (rows) => {
      calls.push(rows);
      return { stored: rows.map((r) => r.event_id), dups: [], capped: [], conflicts: [] };
    });
  const flusher = new Flusher({
    pool: /** @type {any} */ ({}), // unused with an injected executor
    flushIdleMs: opts.flushIdleMs ?? 50,
    flushIntervalMs: opts.flushIntervalMs ?? 60_000,
    submitBatch: fn,
    ...opts,
  });
  onTestFinished(() => flusher.shutdown());
  return { flusher, calls };
}

test("idle flush: one call after FLUSH_IDLE_MS, waiter resolves with outcomes", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  const { flusher, calls } = makeFlusher(t, {});
  const rows = [row(1), row(2)];
  const waiter = flusher.enqueue("t", ledgerOf(rows));
  assert.equal(flusher.depth, 2);

  vi.advanceTimersByTime(50);
  const result = await waiter;
  assert.equal(calls.length, 1);
  assert.deepEqual(
    calls[0].map((r) => r.event_id),
    rows.map((r) => r.event_id),
  );
  assert.deepEqual(
    result.stored,
    rows.map((r) => r.event_id),
  );
});

test("buffer reaching MAX_BATCH flushes immediately without waiting for the timer", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  const { flusher, calls } = makeFlusher(t, { maxBatch: 2 });
  const w1 = flusher.enqueue("t", ledgerOf([row(1)]));
  const w2 = flusher.enqueue("t", ledgerOf([row(2)]));
  // no timer tick needed
  const [r1, r2] = await Promise.all([w1, w2]);
  assert.equal(calls.length, 1);
  assert.equal(r1.stored.length, 1);
  assert.equal(r2.stored.length, 1);
});

test("MAX_BATCH splits a request across flushes; the waiter aggregates both", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  const { flusher, calls } = makeFlusher(t, { maxBatch: 2 });
  const rows = [row(1), row(2), row(3)];
  const waiter = flusher.enqueue("t", ledgerOf(rows)); // immediate flush of the first 2

  vi.advanceTimersByTime(100); // rescheduled idle window flushes the remainder
  const result = await waiter;
  assert.equal(calls.length, 2);
  assert.equal(calls[0].length, 2);
  assert.deepEqual(result.stored.sort(), rows.map((r) => r.event_id).sort());
});

test("multiple requests share one ingest_batch call; results are partitioned per waiter", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  const { flusher, calls } = makeFlusher(t, {
    // result contains ids for BOTH requests plus a foreign id — filtering
    // must give each waiter only its own
    submitBatch: async (rows) => {
      calls.push(rows);
      return {
        stored: [rows[0].event_id, "018f2c00-0000-7000-8000-0000000000ff"],
        dups: rows.length > 1 ? [rows[1].event_id] : [],
        capped: [],
        conflicts: [],
      };
    },
  });
  const w1 = flusher.enqueue("t", ledgerOf([row(1)]));
  const w2 = flusher.enqueue("t", ledgerOf([row(2)]));

  vi.advanceTimersByTime(50);
  const [r1, r2] = await Promise.all([w1, w2]);
  assert.equal(calls.length, 1); // batch mixing: one call, several requests
  assert.deepEqual(r1.stored, [row(1).event_id]);
  assert.deepEqual(r1.dups, []);
  assert.deepEqual(r2.stored, []);
  assert.deepEqual(r2.dups, [row(2).event_id]);
});

test("at most one ingest_batch in flight; the timer never stacks flushes", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  let release;
  const gate = new Promise((r) => (release = r));
  const started = [];
  const { flusher } = makeFlusher(t, {
    submitBatch: async (rows) => {
      started.push(rows);
      await gate;
      return { stored: rows.map((r) => r.event_id), dups: [], capped: [], conflicts: [] };
    },
    maxBatch: 1,
  });
  const w1 = flusher.enqueue("t", ledgerOf([row(1)]));
  await Promise.resolve(); // let the immediate flush start and block on the gate
  assert.equal(started.length, 1);

  const w2 = flusher.enqueue("t", ledgerOf([row(2)]));
  flusher.enqueue("t", ledgerOf([row(3)]));
  vi.advanceTimersByTime(5_000); // timers fire while in flight: no stacking
  await Promise.resolve();
  assert.equal(started.length, 1);
  assert.equal(flusher.pending, 3); // 1 in flight + 2 buffered

  release();
  await Promise.all([w1, w2]);
  await flusher.shutdown();
  assert.equal(flusher.pending, 0);
  assert.equal(started.length, 3); // drained one at a time
});

test("BUFFER_MAX: the overflowing request 503s, buffered events stay", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  let release;
  const gate = new Promise((r) => (release = r));
  const { flusher, calls } = makeFlusher(t, {
    submitBatch: async (rows) => {
      calls.push(rows);
      await gate;
      return { stored: rows.map((r) => r.event_id), dups: [], capped: [], conflicts: [] };
    },
    bufferMax: 3,
    maxBatch: 10,
  });

  const keep = flusher.enqueue("t", ledgerOf([row(1), row(2)]));
  vi.advanceTimersByTime(50); // the two rows move from buffer to in-flight
  await Promise.resolve();
  assert.equal(flusher.pending, 2);
  // a request needing 2 more slots would exceed bufferMax=3 → 503
  assert.throws(() => flusher.enqueue("t", ledgerOf([row(3), row(4)])), BufferFullError);
  // backpressure does not un-buffer: exactly one slot still fits
  const fits = flusher.enqueue("t", ledgerOf([row(5)]));
  assert.equal(flusher.pending, 3);

  release();
  const [rKeep, rFits] = await Promise.all([keep, fits]);
  assert.equal(rKeep.stored.length, 2);
  assert.equal(rFits.stored.length, 1);
  // two submit calls inside ONE flush: [row1,row2] while blocked, [row5]
  // picked up by the same drain loop once the gate released
  assert.deepEqual(
    calls.map((c) => c.map((r) => r.event_id)),
    [[row(1).event_id, row(2).event_id], [row(5).event_id]],
  );
});

test("submitBatch failure rejects only the affected waiters; the rest stay buffered", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  let release;
  const gate = new Promise((r) => (release = r));
  const calls = [];
  const flusher = new Flusher({
    pool: /** @type {any} */ ({}),
    maxBatch: 1,
    flushIdleMs: 50,
    flushIntervalMs: 60_000,
    submitBatch: async (rows) => {
      calls.push(rows.map((r) => r.event_id));
      await gate; // first call fails, later calls succeed
      if (calls.length === 1) throw new Error("db down");
      return { stored: rows.map((r) => r.event_id), dups: [], capped: [], conflicts: [] };
    },
  });
  onTestFinished(() => flusher.shutdown());
  const bad = flusher.enqueue("t", ledgerOf([row(1)]));
  await Promise.resolve(); // the flush starts and blocks on the gate
  const good = flusher.enqueue("t", ledgerOf([row(2)])); // buffered behind it
  assert.equal(flusher.pending, 2);

  release();
  await assert.rejects(bad, /db down/);
  // the drain continuation flushes `good` immediately — it was NOT rejected
  const r2 = await good;
  assert.deepEqual(r2.stored, [row(2).event_id]);
  assert.equal(flusher.pending, 0);
});
test("enqueue after shutdown throws BufferFullError", async (t) => {
  vi.useFakeTimers({ toFake: ["setTimeout", "setInterval"] });
  onTestFinished(() => vi.useRealTimers());
  const { flusher } = makeFlusher(t, {});
  await flusher.shutdown();
  assert.throws(() => flusher.enqueue("t", ledgerOf([row(1)])), BufferFullError);
});
