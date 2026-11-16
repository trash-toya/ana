import { test } from "vitest";
import assert from "node:assert/strict";
import pg from "pg";
import request from "supertest";
import { createApp, isInternalAddress } from "../dist/app.js";
import { Metrics } from "../dist/metrics.js";
import { deadPool } from "./helpers.mjs";

test("metrics registry: counters aggregate by label set, gauges overwrite", () => {
  const m = new Metrics();
  m.inc("http_requests_total", { method: "POST", route: "/v1/events", status: "200" });
  m.inc("http_requests_total", { method: "POST", route: "/v1/events", status: "200" });
  m.inc("http_requests_total", { method: "POST", route: "/v1/events", status: "503" });
  m.gauge("buffer_depth", 42);

  const out = m.render();
  assert.match(out, /^# TYPE http_requests_total counter$/m);
  assert.match(out, /^# TYPE buffer_depth gauge$/m);
  // label order is canonical (sorted), so the two 200s aggregate into one series
  assert.match(
    out,
    /^http_requests_total\{method="POST",route="\/v1\/events",status="200"\} 2$/m,
  );
  assert.match(
    out,
    /^http_requests_total\{method="POST",route="\/v1\/events",status="503"\} 1$/m,
  );
  assert.match(out, /^buffer_depth 42$/m);
});

test("metrics label order invariance", () => {
  const a = new Metrics();
  a.inc("x_total", { b: "2", a: "1" });
  const b = new Metrics();
  b.inc("x_total", { a: "1", b: "2" });
  assert.equal(a.render(), b.render());
});

test("GET /metrics renders Prometheus text and counts requests", async () => {
  const pool = deadPool(pg);
  try {
    const metrics = new Metrics();
    const app = createApp({ pool, metrics });
    await request(app).get("/healthz");
    const res = await request(app).get("/metrics");
    assert.equal(res.status, 200);
    assert.match(res.headers["content-type"] ?? "", /text\/plain/);
    assert.match(res.text, /^# TYPE http_requests_total counter$/m);
    // the /healthz hit is counted; note the /metrics request itself is
    // counted on finish, i.e. after the body it renders was already sent
    assert.match(
      res.text,
      /^http_requests_total\{method="GET",route="\/healthz",status="200"\} 1$/m,
    );
    assert.doesNotMatch(res.text, /route="\/metrics"/);
  } finally {
    await pool.end();
  }
});

test("GET /metrics is internal-only by default", async () => {
  const pool = deadPool(pg);
  try {
    const app = createApp({ pool, metricsAllowed: () => false });
    const res = await request(app).get("/metrics");
    assert.equal(res.status, 403);
  } finally {
    await pool.end();
  }
});

test("isInternalAddress covers loopback and RFC 1918", () => {
  assert.equal(isInternalAddress("127.0.0.1"), true);
  assert.equal(isInternalAddress("::1"), true);
  assert.equal(isInternalAddress("::ffff:127.0.0.1"), true);
  assert.equal(isInternalAddress("10.1.2.3"), true);
  assert.equal(isInternalAddress("192.168.0.5"), true);
  assert.equal(isInternalAddress("172.16.0.1"), true);
  assert.equal(isInternalAddress("172.31.255.1"), true);
  assert.equal(isInternalAddress("172.32.0.1"), false);
  assert.equal(isInternalAddress("8.8.8.8"), false);
  assert.equal(isInternalAddress("fd00::5"), false);
  assert.equal(isInternalAddress(undefined), false);
});
