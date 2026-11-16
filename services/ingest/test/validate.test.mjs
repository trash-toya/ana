import test from "node:test";
import assert from "node:assert/strict";
import {
  EnvelopeError,
  assembleOutcomes,
  validateBatch,
} from "../dist/validate.js";

const SESSION = "018f2b00-0000-7000-8000-0000000000c7";
const RECEIVED = new Date("2026-11-15T12:00:00.000Z");
const SENT_AT = "2026-11-15T11:59:50.000Z"; // received 10 s after send
const CTX = { tenantId: "018f0000-0000-7000-8000-0000000000t0", receivedAt: RECEIVED };

let n = 0;
function id() {
  return `018f2c00-0000-7000-8000-${(++n).toString(16).padStart(12, "0")}`;
}

function env(events, overrides = {}) {
  return {
    schema_version: 1,
    anon_id: "a_7f3c9e",
    sent_at: SENT_AT,
    events,
    ...overrides,
  };
}

function ev(overrides = {}) {
  return {
    event_id: id(),
    session_id: SESSION,
    time: "2026-11-15T11:59:00.000Z", // 1 min before receipt
    name: "click",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Envelope gate (400s)
// ---------------------------------------------------------------------------

test("unsupported schema_version → 400 schema_unsupported", () => {
  assert.throws(
    () => validateBatch(env([ev()], { schema_version: 2 }), CTX),
    (e) => e instanceof EnvelopeError && e.code === "schema_unsupported",
  );
  // previous version accepted
  const out = validateBatch(env([ev()], { schema_version: 0 }), CTX);
  assert.equal(out.rows.length, 1);
});

test("malformed envelope → 400 malformed", () => {
  for (const bad of [
    env([ev()], { anon_id: "" }),
    env([ev()], { sent_at: "not-a-date" }),
    env([]), // no events
    { schema_version: 1, anon_id: "a1" }, // missing sent_at/events
    null,
  ]) {
    assert.throws(
      () => validateBatch(bad, CTX),
      (e) => e instanceof EnvelopeError && e.code === "malformed",
      JSON.stringify(bad),
    );
  }
});

test("more than 500 events → 400 malformed", () => {
  assert.throws(
    () => validateBatch(env(Array.from({ length: 501 }, () => ev())), CTX),
    (e) => e instanceof EnvelopeError && e.code === "malformed",
  );
});

// ---------------------------------------------------------------------------
// Structural event failures (400s)
// ---------------------------------------------------------------------------

test("structurally broken events → 400 malformed (SDK bug)", () => {
  for (const bad of [
    ev({ event_id: undefined }),
    ev({ seq: "3" }),
    ev({ url: 7 }),
    ev({ props: null }),
    ev({ time: "2026-11-15" }), // date-only is not an ISO datetime
    ev({ spoofed: true }), // unknown field
  ]) {
    assert.throws(
      () => validateBatch(env([bad]), CTX),
      (e) => e instanceof EnvelopeError && e.code === "malformed",
      JSON.stringify(bad),
    );
  }
});

// ---------------------------------------------------------------------------
// Per-event value violations (200 rejections)
// ---------------------------------------------------------------------------

test("invalid uuid → rejected invalid_uuid", () => {
  const out = validateBatch(
    env([ev({ event_id: "nope" }), ev({ session_id: "also-nope" }), ev()]),
    CTX,
  );
  assert.deepEqual(out.rejected, [
    { event_id: "nope", reason: "invalid_uuid" },
    { event_id: out.rejected[1].event_id, reason: "invalid_uuid" },
  ]);
  assert.equal(out.rows.length, 1); // the valid one still stores
});

test("bad name → rejected bad_name", () => {
  const out = validateBatch(env([ev({ name: "Click" }), ev({ name: "1click" })]), CTX);
  assert.deepEqual(
    out.rejected.map((r) => r.reason),
    ["bad_name", "bad_name"],
  );
});

test("props violations → rejected props_too_large", () => {
  const out = validateBatch(
    env([
      ev({ props: Object.fromEntries(Array.from({ length: 21 }, (_, i) => [`k${i}`, 1])) }),
      ev({ props: { s: "x".repeat(4100) } }),
      ev({ props: { a: { b: { c: 1 } } } }), // depth 3
    ]),
    CTX,
  );
  assert.deepEqual(
    out.rejected.map((r) => r.reason),
    ["props_too_large", "props_too_large", "props_too_large"],
  );
});

// ---------------------------------------------------------------------------
// Clock-skew correction and the corrected-time window
// ---------------------------------------------------------------------------

test("t' = t + (received − sent); row carries the corrected time", () => {
  const out = validateBatch(env([ev({ time: "2026-11-15T11:59:00.000Z" })]), CTX);
  // received 10 s after sent → corrected is 11:59:10
  assert.equal(out.rows[0].time, "2026-11-15T11:59:10.000Z");
});

test("corrected time older than 24 h → too_old; more than 5 min ahead → too_new", () => {
  const out = validateBatch(
    env([
      ev({ time: "2026-11-14T11:59:00.000Z" }), // corrected: 24 h 50 s ago → too_old
      ev({ time: "2026-11-15T12:04:59.000Z" }), // corrected: 5:09 ahead → too_new
      ev({ time: "2026-11-14T12:00:00.000Z" }), // corrected: 23:59:50 ago → OK
      ev({ time: "2026-11-15T12:04:40.000Z" }), // corrected: 4:50 ahead → OK
    ]),
    CTX,
  );
  assert.deepEqual(
    out.rejected.map((r) => r.reason),
    ["too_old", "too_new"],
  );
  assert.equal(out.rows.length, 2);
});

test("a slow device clock is rescued by the sent_at correction", () => {
  // Device clock 6 h slow: it stamps the event 05:59 and sends with sent_at
  // 05:59. The server receives at 12:00 → skew = +6 h 1 s → corrected time is
  // 11:59:01, well inside the window.
  const out = validateBatch(
    env([ev({ time: "2026-11-15T05:59:00.000Z" })], {
      sent_at: "2026-11-15T05:59:00.000Z",
    }),
    CTX,
  );
  assert.equal(out.rejected.length, 0);
  assert.equal(out.rows.length, 1);
  // 05:59:00 + (12:00:00 − 05:59:00) = 12:00:00
  assert.equal(out.rows[0].time, "2026-11-15T12:00:00.000Z");
});

// ---------------------------------------------------------------------------
// Classification and row shape
// ---------------------------------------------------------------------------

test("cat comes from the category map, anon_id from the envelope", () => {
  const out = validateBatch(
    env([
      ev({ name: "heartbeat" }),
      ev({ name: "session_start" }),
      ev({ name: "custom_event" }),
      ev({ user_id: "u1", url: "/pricing", props: { el: "cta" }, seq: 7 }),
    ]),
    CTX,
  );
  assert.deepEqual(
    out.rows.map((r) => r.cat),
    ["s", "l", "e", "e"],
  );
  assert.ok(out.rows.every((r) => r.anon_id === "a_7f3c9e"));
  const full = out.rows[3];
  assert.equal(full.user_id, "u1");
  assert.equal(full.url, "/pricing");
  assert.deepEqual(full.props, { el: "cta" });
  assert.equal(full.seq, 7);
});

test("survivors get ord 0..n−1 in request order", () => {
  const out = validateBatch(env([ev(), ev(), ev(), ev()]), CTX);
  assert.deepEqual(
    out.rows.map((r) => r.ord),
    [0, 1, 2, 3],
  );
});

// ---------------------------------------------------------------------------
// In-batch dedupe
// ---------------------------------------------------------------------------

test("exact duplicate (same id, same payload, different key order) → dup of first", () => {
  const e = ev({ props: { a: 1, b: 2 } });
  const twin = {
    ...e,
    props: { b: 2, a: 1 }, // same canonical payload, different key order
  };
  const out = validateBatch(env([e, twin]), CTX);
  assert.equal(out.rows.length, 1);
  assert.deepEqual(out.dups, [{ event_id: e.event_id, of: e.event_id }]);
  assert.equal(out.rejected.length, 0);
});

test("same id, different payload → first wins, rest conflicting_duplicate", () => {
  const e1 = ev({ props: { a: 1 } });
  const e2 = { ...e1, props: { a: 2 } };
  const out = validateBatch(env([e1, e2]), CTX);
  assert.equal(out.rows.length, 1);
  assert.equal(out.rows[0].payload_hash, out.rows[0].payload_hash);
  assert.deepEqual(out.rejected, [
    { event_id: e2.event_id, reason: "conflicting_duplicate" },
  ]);
});

test("triple: row, exact dup, conflicting variant", () => {
  const e1 = ev({ props: { a: 1 } });
  const e2 = { ...e1, props: { a: 1 } }; // exact dup
  const e3 = { ...e1, props: { a: 3 } }; // conflict
  const out = validateBatch(env([e1, e2, e3]), CTX);
  assert.equal(out.rows.length, 1);
  assert.deepEqual(out.dups, [{ event_id: e1.event_id, of: e1.event_id }]);
  assert.deepEqual(out.rejected, [
    { event_id: e3.event_id, reason: "conflicting_duplicate" },
  ]);
});

test("hash uses the client's ORIGINAL time (pre-correction)", () => {
  const a = validateBatch(env([ev({ time: "2026-11-15T11:59:00.000Z" })]), CTX);
  const b = validateBatch(
    env([ev({ time: "2026-11-15T11:59:00.000Z" })], {
      sent_at: "2026-11-15T11:59:40.000Z", // different skew (+20 s vs +10 s)
    }),
    CTX,
  );
  // corrected times differ; the hash must not
  assert.notEqual(a.rows[0].time, b.rows[0].time);
  assert.equal(a.rows[0].payload_hash, b.rows[0].payload_hash);
});

// ---------------------------------------------------------------------------
// Outcome assembly: every ID in exactly one list
// ---------------------------------------------------------------------------

test("assembleOutcomes maps the function result onto the ledger", () => {
  const e1 = ev(); // stored
  const e2 = ev(); // already stored (dup)
  const e3 = ev(); // capped
  const e4 = ev(); // conflict
  const out = validateBatch(env([e1, e2, e3, e4]), CTX);
  const res = {
    stored: [e1.event_id],
    dups: [e2.event_id],
    capped: [e3.event_id],
    conflicts: [e4.event_id],
  };
  const assembled = assembleOutcomes(out, res);
  assert.deepEqual(assembled.accepted, [e1.event_id, e2.event_id]);
  assert.deepEqual(assembled.rejected, [
    { event_id: e3.event_id, reason: "session_capped" },
    { event_id: e4.event_id, reason: "conflicting_duplicate" },
  ]);
});

test("an ID the function did not report is an accepted duplicate", () => {
  const out = validateBatch(env([ev()]), CTX);
  const assembled = assembleOutcomes(out, {
    stored: [], dups: [], capped: [], conflicts: [],
  });
  assert.equal(assembled.accepted.length, 1);
  assert.equal(assembled.rejected.length, 0);
});

test("in-batch exact dups mirror the first occurrence's outcome", () => {
  const e1 = ev({ props: { a: 1 } });
  const twin = { ...e1, props: { a: 1 } };
  const out = validateBatch(env([e1, twin]), CTX);

  const stored = assembleOutcomes(out, { stored: [e1.event_id], dups: [], capped: [], conflicts: [] });
  assert.deepEqual(stored.accepted, [e1.event_id]); // one entry per ID

  const capped = assembleOutcomes(out, { stored: [], dups: [], capped: [e1.event_id], conflicts: [] });
  assert.deepEqual(capped.rejected, [
    { event_id: e1.event_id, reason: "session_capped" }, // one entry per ID
  ]);

  // first rejected at validation → the dup (same ID) mirrors that; the ID is
  // still reported exactly once
  const old = ev({ time: "2026-11-14T00:00:00.000Z" });
  const oldTwin = { ...old };
  const out2 = validateBatch(env([old, oldTwin]), CTX);
  assert.equal(out2.rows.length, 0);
  const assembled = assembleOutcomes(out2, { stored: [], dups: [], capped: [], conflicts: [] });
  assert.deepEqual(assembled.rejected, [
    { event_id: old.event_id, reason: "too_old" },
  ]);
  assert.equal(assembled.accepted.length, 0);
});

test("contract invariant: every submitted ID appears in exactly one list", () => {
  const e1 = ev();
  const e2 = ev({ props: { a: 1 } });
  const twin = { ...e2, props: { a: 1 } };
  const e3 = ev({ time: "2026-11-14T00:00:00.000Z" }); // too_old
  const e4 = { ...e2, props: { a: 9 } }; // conflicting
  const out = validateBatch(env([e1, e2, twin, e3, e4]), CTX);
  const assembled = assembleOutcomes(out, {
    stored: [e1.event_id, e2.event_id],
    dups: [],
    capped: [],
    conflicts: [], // the in-batch conflicting variant never reaches the function
  });
  // distinct IDs (twin and e4 share e2's); the first occurrence (e2, stored)
  // decides, so the later conflicting variant folds into the same outcome
  const all = [...new Set([e1, e2, e3, e4].map((e) => e.event_id))].sort();
  const reported = [...assembled.accepted, ...assembled.rejected.map((r) => r.event_id)].sort();
  assert.deepEqual(reported, all);
  // and no double reporting
  assert.equal(new Set(reported).size, reported.length);
  assert.deepEqual(assembled.accepted, [e1.event_id, e2.event_id]);
  assert.deepEqual(assembled.rejected, [
    { event_id: e3.event_id, reason: "too_old" },
  ]);
});

// ---------------------------------------------------------------------------
// Rev 5: provenance, sessionless events, source-dependent time (D29, D32)
// ---------------------------------------------------------------------------

test("sessionless events need no anon_id; session-bound ones do", () => {
  const { anon_id, ...noAnon } = env([]);
  const loose = ev({ session_id: null });
  const out = validateBatch(
    { ...noAnon, events: [loose] },
    CTX,
  );
  assert.equal(out.rows.length, 1);
  assert.equal(out.rows[0].session_id, null);
  assert.equal(out.rows[0].anon_id, null);

  const bound = validateBatch({ ...noAnon, events: [ev()] }, CTX);
  assert.deepEqual(bound.rejected, [
    { event_id: bound.rejected[0].event_id, reason: "missing_anon_id" },
  ]);
  assert.equal(bound.rows.length, 0);
});

test("source: absent means client; unknown values are a shape failure (400)", () => {
  const out = validateBatch(env([ev()]), CTX);
  assert.equal(out.rows[0].source, "client");

  assert.throws(
    () => validateBatch(env([ev({ source: "partner" })]), CTX),
    (e) => e instanceof EnvelopeError && e.code === "malformed",
  );
});

test("time: client events are skew-corrected, server/webhook events are not", () => {
  const original = "2026-11-15T11:59:00.000Z"; // 60 s before receipt; sent_at is 10 s before
  const client = validateBatch(env([ev({ time: original })]), CTX);
  // +10 s correction
  assert.equal(client.rows[0].time, "2026-11-15T11:59:10.000Z");
  assert.equal(client.rows[0].received_at, RECEIVED.toISOString());

  const server = validateBatch(env([ev({ time: original, source: "server" })]), CTX);
  assert.equal(server.rows[0].time, original, "a backend clock is not shifted");
  assert.equal(server.rows[0].received_at, RECEIVED.toISOString());

  const webhook = validateBatch(env([ev({ time: original, source: "webhook" })]), CTX);
  assert.equal(webhook.rows[0].time, original, "a provider event time is preserved");
});

test("time window still applies to server and webhook sources", () => {
  const veryOld = "2026-11-13T11:00:00.000Z";
  const out = validateBatch(env([ev({ time: veryOld, source: "webhook" })]), CTX);
  assert.deepEqual(out.rejected, [
    { event_id: out.rejected[0].event_id, reason: "too_old" },
  ]);
});

test("per-event anon_id overrides the envelope (server batches mix visitors)", () => {
  const e1 = ev({ anon_id: "visitor_a" });
  const e2 = ev({ session_id: null, anon_id: null }); // explicit null wins
  const e3 = ev(); // falls back to the envelope
  const out = validateBatch(env([e1, e2, e3]), CTX);
  assert.equal(out.rows.length, 3);
  assert.equal(out.rows[0].anon_id, "visitor_a");
  assert.equal(out.rows[1].anon_id, null);
  assert.equal(out.rows[2].anon_id, "a_7f3c9e");
});

test("per-event null anon_id on a session-bound event → missing_anon_id", () => {
  const out = validateBatch(env([ev({ anon_id: null })]), CTX);
  assert.deepEqual(out.rejected, [
    { event_id: out.rejected[0].event_id, reason: "missing_anon_id" },
  ]);
});
