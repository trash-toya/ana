import { test } from "vitest";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { canonicalJson, payloadHash } from "../dist/index.js";

const base = {
  session_id: "018f2b00-0000-7000-8000-0000000000c7",
  name: "click",
  url: "/pricing",
  props: { el: "cta" },
  time: "2026-11-14T10:00:21.114Z",
  seq: 3,
};

test("golden value: matches an independently computed sha256 of the canonical form", () => {
  const expected = createHash("sha256")
    .update(
      '{"name":"click","props":{"el":"cta"},"seq":3,' +
        '"session_id":"018f2b00-0000-7000-8000-0000000000c7",' +
        '"time":"2026-11-14T10:00:21.114Z","url":"/pricing"}',
    )
    .digest("hex");
  assert.equal(payloadHash(base), expected);
  // sanity: canonicalJson produces exactly that string for `base`
  assert.equal(
    canonicalJson(base),
    '{"name":"click","props":{"el":"cta"},"seq":3,' +
      '"session_id":"018f2b00-0000-7000-8000-0000000000c7",' +
      '"time":"2026-11-14T10:00:21.114Z","url":"/pricing"}',
  );
});

test("deterministic across calls", () => {
  assert.equal(payloadHash(base), payloadHash({ ...base }));
});

test("props key order does not change the hash", () => {
  assert.equal(
    payloadHash({ ...base, props: { a: 1, b: 2, c: 3 } }),
    payloadHash({ ...base, props: { c: 3, a: 1, b: 2 } }),
  );
});

test("top-level key order does not change the hash", () => {
  const reordered = {
    seq: 3,
    time: base.time,
    props: base.props,
    url: base.url,
    name: base.name,
    session_id: base.session_id,
  };
  assert.equal(payloadHash(base), payloadHash(reordered));
});

test("absent optional fields and explicit null hash identically", () => {
  const explicit = { ...base, url: null, seq: null, props: {} };
  const absent = { ...base };
  delete absent.url;
  delete absent.seq;
  delete absent.props;
  assert.equal(payloadHash(explicit), payloadHash(absent));
});

test("user_id and anon_id are excluded from the hash", () => {
  const withUser = { ...base, user_id: "u1", anon_id: "a_1" };
  const withOther = { ...base, user_id: "u2", anon_id: "a_2" };
  assert.equal(payloadHash(withUser), payloadHash(withOther));
  assert.equal(payloadHash(withUser), payloadHash(base));
});

test("differing time, name, url, props or seq change the hash", () => {
  const variants = [
    { ...base, time: "2026-11-14T10:00:21.115Z" },
    { ...base, name: "submit" },
    { ...base, url: null },
    { ...base, props: { el: "header" } },
    { ...base, seq: 4 },
    { ...base, session_id: "018f2b00-0000-7000-8000-0000000000ff" },
  ];
  const ref = payloadHash(base);
  for (const v of variants) assert.notEqual(payloadHash(v), ref, JSON.stringify(v));
});
