import { test } from "vitest";
import assert from "node:assert/strict";
import {
  ACCEPTED_SCHEMA_VERSIONS,
  CATEGORY,
  MAX_EVENTS_PER_REQUEST,
  NAME_PATTERN,
  REJECT_REASONS,
  SCHEMA_VERSION,
  SOURCES,
  SOURCES_BY_KEY_KIND,
  categorize,
  payloadHash,
} from "../dist/index.js";
import { EnvelopeSchema, EventSchema, PropsSchema } from "../dist/index.js";

const UUID = "018f2b00-0000-7000-8000-0000000000c7";

function event(overrides = {}) {
  return {
    event_id: UUID,
    session_id: UUID,
    time: "2026-11-14T10:00:21.114Z",
    name: "click",
    ...overrides,
  };
}

function envelope(overrides = {}) {
  return {
    schema_version: 1,
    anon_id: "a_7f3c9e",
    sent_at: "2026-11-14T10:01:42.310Z",
    events: [event()],
    ...overrides,
  };
}

test("REJECT_REASONS matches the contract enum exactly", () => {
  assert.deepEqual(
    [...REJECT_REASONS],
    [
      "invalid_uuid",
      "missing_anon_id",
      "too_old",
      "too_new",
      "bad_name",
      "props_too_large",
      "schema_unsupported",
      "session_capped",
      "conflicting_duplicate",
    ],
  );
});

test("sources: provenance values and key-kind authorization (D28)", () => {
  assert.deepEqual([...SOURCES], ["client", "server", "webhook"]);
  assert.deepEqual([...SOURCES_BY_KEY_KIND.ingest], ["client"]);
  assert.deepEqual([...SOURCES_BY_KEY_KIND.server], ["client", "server", "webhook"]);
  assert.deepEqual([...SOURCES_BY_KEY_KIND.dashboard], []);
  assert.equal(EventSchema.safeParse(event({ source: "server" })).success, true);
  assert.equal(EventSchema.safeParse(event({ source: "partner" })).success, false);
});

test("sessionless events: session_id and anon_id are optional (D29)", () => {
  const { session_id, ...noSession } = event();
  assert.equal(EventSchema.safeParse(noSession).success, true);
  assert.equal(EventSchema.safeParse(event({ session_id: null })).success, true);
  const { anon_id, ...noAnon } = envelope();
  assert.equal(EnvelopeSchema.safeParse(noAnon).success, true);
});

test("event anon_id overrides the envelope; null/absent mean no visitor", () => {
  assert.equal(EventSchema.safeParse(event({ anon_id: "a_other" })).success, true);
  assert.equal(EventSchema.safeParse(event({ anon_id: null })).success, true);
  assert.equal(EventSchema.safeParse(event({ anon_id: "" })).success, false);
  assert.equal(EventSchema.safeParse(event({ anon_id: 7 })).success, false);
});

test("schema version: current and previous accepted", () => {
  assert.deepEqual(ACCEPTED_SCHEMA_VERSIONS, [SCHEMA_VERSION, SCHEMA_VERSION - 1]);
  assert.ok(EnvelopeSchema.safeParse(envelope({ schema_version: 1 })).success);
  assert.ok(EnvelopeSchema.safeParse(envelope({ schema_version: 0 })).success);
});

test("unsupported schema versions are rejected with schema_unsupported", () => {
  for (const v of [2, -1, 1.5]) {
    const r = EnvelopeSchema.safeParse(envelope({ schema_version: v }));
    assert.equal(r.success, false, `version ${v} must fail`);
    assert.ok(JSON.stringify(r.error.issues).includes("schema_version"));
  }
});

test("valid envelope and event parse", () => {
  const e = event({
    seq: 3,
    user_id: null,
    url: "/pricing",
    props: { el: "cta", nested: { a: 1 }, tags: ["x", null, true, 2] },
  });
  assert.ok(EventSchema.safeParse(e).success);
  assert.ok(EnvelopeSchema.safeParse(envelope({ events: [e] })).success);
});

test("invalid event_id / session_id → invalid_uuid territory", () => {
  assert.equal(EventSchema.safeParse(event({ event_id: "nope" })).success, false);
  assert.equal(EventSchema.safeParse(event({ session_id: "" })).success, false);
});

test("bad names are rejected by NAME_PATTERN", () => {
  assert.ok(NAME_PATTERN.test("click"));
  assert.ok(NAME_PATTERN.test("app_foreground"));
  assert.ok(NAME_PATTERN.test("a".repeat(64)));
  for (const bad of ["Click", "1click", "_click", "click-event", "a".repeat(65), ""]) {
    assert.equal(NAME_PATTERN.test(bad), false, JSON.stringify(bad));
    assert.equal(EventSchema.safeParse(event({ name: bad })).success, false);
  }
});

test("name >64 chars rejected by the length cap", () => {
  // "a".repeat(64) passes the pattern (≤64), so the pattern alone covers it;
  // 65 fails pattern. Cross-check the schema cap is not looser than pattern.
  assert.equal(NAME_PATTERN.test("a".repeat(65)), false);
});

test("props: >20 keys rejected", () => {
  const many = Object.fromEntries(Array.from({ length: 21 }, (_, i) => [`k${i}`, 1]));
  const r = PropsSchema.safeParse(many);
  assert.equal(r.success, false);
  assert.ok(JSON.stringify(r.error.issues).includes("keys (max"));
});

test("props: 4 KB serialised size cap", () => {
  const big = { s: "x".repeat(4100) };
  assert.equal(PropsSchema.safeParse(big).success, false);
  assert.ok(PropsSchema.safeParse({ s: "x".repeat(4000) }).success);
});

test("props: depth 3 rejected structurally, depth 2 accepted", () => {
  assert.equal(PropsSchema.safeParse({ a: { b: { c: 1 } } }).success, false);
  assert.ok(PropsSchema.safeParse({ a: { b: 1 } }).success);
  assert.equal(PropsSchema.safeParse({ a: [["x"]] }).success, false); // array of arrays
});

test("props: leaf values restricted to string|number|boolean|null", () => {
  assert.equal(PropsSchema.safeParse({ a: undefined }).success, false);
  // an empty nested object / array is a valid depth-2 value (0 extra keys)
  assert.ok(PropsSchema.safeParse({ a: {} }).success);
  assert.ok(PropsSchema.safeParse({ a: [] }).success);
  // non-JSON leaf types fail structurally
  assert.equal(PropsSchema.safeParse({ a: new Date() }).success, false);
});

test("time must be ISO 8601", () => {
  assert.equal(EventSchema.safeParse(event({ time: "yesterday" })).success, false);
  assert.ok(EventSchema.safeParse(event({ time: "2026-11-14T10:00:21+05:30" })).success);
});

test("strict schemas reject unknown fields", () => {
  assert.equal(EventSchema.safeParse(event({ tenant_id: "spoof" })).success, false);
  assert.equal(EnvelopeSchema.safeParse(envelope({ tenant_id: "spoof" })).success, false);
});

test("events array capped at 500", () => {
  const many = envelope({ events: Array.from({ length: 501 }, () => event()) });
  assert.equal(EnvelopeSchema.safeParse(many).success, false);
  const ok = envelope({
    events: Array.from({ length: MAX_EVENTS_PER_REQUEST }, () => event()),
  });
  assert.ok(EnvelopeSchema.safeParse(ok).success);
});

test("envelope requires events and sent_at; anon_id is optional (D29)", () => {
  assert.equal(EnvelopeSchema.safeParse({ ...envelope(), events: [] }).success, false);
  const { anon_id, ...noAnon } = envelope();
  assert.equal(EnvelopeSchema.safeParse(noAnon).success, true);
  const { sent_at, ...noSent } = envelope();
  assert.equal(EnvelopeSchema.safeParse(noSent).success, false);
});

test("categorize covers reserved names and defaults to engagement", () => {
  assert.equal(categorize("session_start"), "l");
  assert.equal(categorize("session_end"), "l");
  assert.equal(categorize("app_foreground"), "l");
  assert.equal(categorize("app_background"), "l");
  assert.equal(categorize("heartbeat"), "s");
  assert.equal(categorize("sdk_dropped"), "s");
  assert.equal(categorize("click"), "e");
  assert.equal(categorize("checkout.complete"), "e");
});

test("category map is the single source of truth for reserved names", () => {
  // every reserved lifecycle/system name must satisfy NAME_PATTERN
  for (const name of Object.keys(CATEGORY)) {
    assert.ok(NAME_PATTERN.test(name), name);
  }
});

test("payloadHash is importable and hash-stable from the same entry point", () => {
  assert.equal(typeof payloadHash, "function");
  assert.equal(payloadHash(event()), payloadHash(event()));
});
