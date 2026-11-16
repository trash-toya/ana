import test from "node:test";
import assert from "node:assert/strict";
import { canonicalJson } from "../dist/index.js";

test("object keys are sorted regardless of insertion order", () => {
  assert.equal(canonicalJson({ b: 2, a: 1 }), '{"a":1,"b":2}');
  assert.equal(canonicalJson({ a: 1, b: 2 }), '{"a":1,"b":2}');
});

test("nested objects are sorted and output has no whitespace", () => {
  const a = { props: { z: [1, 2], a: { y: true, x: null } }, name: "click" };
  const b = { name: "click", props: { a: { x: null, y: true }, z: [1, 2] } };
  const out = canonicalJson(a);
  assert.equal(out, canonicalJson(b));
  assert.equal(out, '{"name":"click","props":{"a":{"x":null,"y":true},"z":[1,2]}}');
  assert.ok(!out.includes(" "));
});

test("undefined object values are dropped", () => {
  assert.equal(canonicalJson({ a: undefined, b: 1 }), '{"b":1}');
});

test("undefined array elements serialise as null (JSON.stringify parity)", () => {
  assert.equal(canonicalJson([1, undefined, 2]), "[1,null,2]");
});

test("null, booleans, strings and finite numbers are preserved", () => {
  assert.equal(canonicalJson(null), "null");
  assert.equal(canonicalJson(true), "true");
  assert.equal(canonicalJson(false), "false");
  assert.equal(canonicalJson("a\"b"), '"a\\"b"');
  assert.equal(canonicalJson(0), "0");
  assert.equal(canonicalJson(-1.5), "-1.5");
});

test("empty containers", () => {
  assert.equal(canonicalJson({}), "{}");
  assert.equal(canonicalJson([]), "[]");
});

test("non-serialisable values throw", () => {
  assert.throws(() => canonicalJson(10n), TypeError);
  assert.throws(() => canonicalJson(NaN), TypeError);
  assert.throws(() => canonicalJson(() => {}), TypeError);
  assert.throws(() => canonicalJson(Symbol("x")), TypeError);
});
