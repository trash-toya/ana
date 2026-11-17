// Abuse protection (P4-3): bucket math with an injectable clock, then the
// wired middleware against the real app — 429 + Retry-After, scope metrics,
// disabled scopes, and per-session event-count charging.
import { test } from "vitest";
import assert from "node:assert/strict";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import {
  LIMIT_DEFAULTS,
  RateLimiter,
  TokenBucket,
  sendRateLimited,
} from "../dist/limits.js";
import { Metrics } from "../dist/metrics.js";
import { seedTenantWithKeys } from "./helpers.mjs";

const SESSION_A = "018f2b00-0000-7000-8000-0000000000a1";
const SESSION_B = "018f2b00-0000-7000-8000-0000000000b1";

let n = 0;
function event(sessionId = SESSION_A) {
  const id = (++n).toString(16).padStart(12, "0");
  return {
    event_id: `018f2c00-0000-7000-8000-${id}`,
    session_id: sessionId,
    time: new Date(Date.now() - 60_000).toISOString(),
    name: "click",
  };
}

function envelope(events) {
  return {
    schema_version: 1,
    anon_id: "a1",
    sent_at: new Date().toISOString(),
    events,
  };
}

/** Every scope disabled except the ones a test enables. */
function only(scope, spec) {
  return { ip: null, key: null, tenant: null, session: null, [scope]: spec };
}

/** Stub write path: echoes rows as stored, so no flusher/DB write is needed. */
const stubSubmit = async ({ outcome }) => ({
  stored: outcome.rows.map((r) => r.event_id),
  dups: [],
  capped: [],
  conflicts: [],
});

// ---------------------------------------------------------------------------
// Token bucket math
// ---------------------------------------------------------------------------

test("bucket: burst admits, sustained refill is continuous, denial consumes nothing", () => {
  let now = 1_000;
  const bucket = new TokenBucket({ ratePerSec: 10, burst: 3 }, () => now);

  assert.deepEqual(bucket.charge(), { ok: true });
  assert.deepEqual(bucket.charge(), { ok: true });
  assert.deepEqual(bucket.charge(), { ok: true });
  assert.deepEqual(bucket.charge(), { ok: false, retryAfterSec: 1 });

  // 100 ms at 10/s refills exactly one token; the denied charge took nothing.
  now += 100;
  assert.deepEqual(bucket.charge(), { ok: true });
  assert.deepEqual(bucket.charge(), { ok: false, retryAfterSec: 1 });

  // Refill never exceeds capacity.
  now += 60_000;
  assert.equal(bucket.available(3), true);
  bucket.consume(3);
  assert.equal(bucket.available(1), false);
});

test("bucket: cost > burst is admitted rather than looped forever", () => {
  const bucket = new TokenBucket({ ratePerSec: 1, burst: 5 }, () => 0);
  // The request cap (500 events) is below the default session burst (2000);
  // a cost above the bucket still must not deadlock a client.
  assert.deepEqual(bucket.charge(6), { ok: true });
  assert.equal(bucket.available(6), true);
});

test("limiter: per-id isolation, checkCosts is all-or-nothing", () => {
  let now = 0;
  const limiter = new RateLimiter(
    { ip: null, key: null, tenant: null, session: { ratePerSec: 1, burst: 10 } },
    { now: () => now },
  );

  // Leave a with exactly one token and c with two.
  assert.deepEqual(limiter.check("session", "a", 9), { ok: true });
  assert.deepEqual(limiter.check("session", "c", 8), { ok: true });

  // The batch needs 1 from a (payable) and 4 from c (not): denied as a whole,
  // and a's last token was NOT consumed by the failed attempt.
  const denied = limiter.checkCosts(
    "session",
    new Map([
      ["a", 1],
      ["c", 4],
    ]),
  );
  assert.equal(denied.ok, false);
  assert.equal(denied.scope, "session");
  assert.deepEqual(limiter.check("session", "a"), { ok: true });
  assert.deepEqual(limiter.check("session", "a"), {
    ok: false,
    scope: "session",
    retryAfterSec: 1,
  });

  // c can still pay exactly its remaining two tokens.
  assert.deepEqual(limiter.check("session", "c", 2), { ok: true });
  assert.equal(limiter.check("session", "c").ok, false);

  now += 1_000; // one token per second
  assert.deepEqual(limiter.check("session", "c"), { ok: true });
});

test("limiter: disabled scope always allows; idle full buckets are pruned", () => {
  let now = 0;
  const limiter = new RateLimiter(
    { ip: null, key: null, tenant: null, session: null },
    { now: () => now },
  );
  for (let i = 0; i < 100; i++) assert.equal(limiter.check("session", "a").ok, true);
  assert.equal(limiter.size(), 0, "a disabled scope keeps no buckets");

  const pruning = new RateLimiter(
    { ip: { ratePerSec: 100, burst: 2 }, key: null, tenant: null, session: null },
    { now: () => now, idleMs: 1_000, pruneEvery: 2 },
  );
  assert.equal(pruning.check("ip", "1.2.3.4").ok, true);
  assert.equal(pruning.size(), 1);
  now += 5_000; // 1.2.3.4 is full again (refill capped at burst) and idle
  assert.equal(pruning.check("ip", "5.6.7.8").ok, true); // second check → prune
  assert.equal(pruning.size(), 1, "the idle bucket was dropped, the new one kept");
  assert.equal(pruning.check("ip", "9.9.9.9").ok, true);
  assert.equal(pruning.size(), 2, "5.6.7.8 is fresh, so it survives");
});

test("defaults: session burst accepts a full SDK flush (500 events)", () => {
  assert.ok(LIMIT_DEFAULTS.session);
  assert.ok(
    LIMIT_DEFAULTS.session.burst >= 500,
    "one flush must fit in the session bucket",
  );
  const limiter = new RateLimiter(LIMIT_DEFAULTS, { now: () => 0 });
  const costs = new Map([[SESSION_A, 500]]);
  assert.deepEqual(limiter.checkCosts("session", costs), { ok: true });
});

test("sendRateLimited: 429 contract with Retry-After and metric", () => {
  const metrics = new Metrics();
  const res = {
    status(code) {
      this.code = code;
      return this;
    },
    set(name, value) {
      (this.headers ??= {})[name] = value;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
  sendRateLimited(res, { ok: false, scope: "key", retryAfterSec: 3 }, metrics);
  assert.equal(res.code, 429);
  assert.equal(res.headers["Retry-After"], "3");
  assert.deepEqual(res.body, { error: "rate_limited", scope: "key" });
  assert.match(metrics.render(), /rate_limited_total\{scope="key"\} 1/);
});

// ---------------------------------------------------------------------------
// Wired middleware
// ---------------------------------------------------------------------------

test("ip scope: limits unauthenticated /v1 traffic and leaves other routes alone", async () => {
  await withTestDb("ingest_limit_ip", async ({ pool }) => {
    const metrics = new Metrics();
    const app = createApp({
      pool,
      submit: stubSubmit,
      metrics,
      limits: only("ip", { ratePerSec: 1, burst: 2 }),
    });

    // No key: auth would answer 401; the ip bucket answers 429 on the third.
    const first = await request(app)
      .post("/v1/events")
      .send(envelope([event()]));
    const second = await request(app)
      .post("/v1/events")
      .send(envelope([event()]));
    const third = await request(app)
      .post("/v1/events")
      .send(envelope([event()]));
    assert.equal(first.status, 401);
    assert.equal(second.status, 401);
    assert.equal(third.status, 429);
    assert.equal(third.body.error, "rate_limited");
    assert.equal(third.body.scope, "ip");
    assert.equal(third.headers["retry-after"], "1");

    // Health checks are not rate limited (internal, and the LB needs them).
    assert.equal((await request(app).get("/healthz")).status, 200);
    assert.match(metrics.render(), /rate_limited_total\{scope="ip"\} 1/);
  });
});

test("key and tenant scopes: per-key isolation and a shared tenant bucket", async () => {
  await withTestDb("ingest_limit_key_tenant", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);

    const perKey = createApp({
      pool,
      submit: stubSubmit,
      limits: only("key", { ratePerSec: 1, burst: 1 }),
    });
    assert.equal(
      (
        await request(perKey)
          .post("/v1/events")
          .set("X-API-Key", keys.ingestKey)
          .send(envelope([event()]))
      ).status,
      200,
    );
    const blockedKey = await request(perKey)
      .post("/v1/events")
      .set("X-API-Key", keys.ingestKey)
      .send(envelope([event()]));
    assert.equal(blockedKey.status, 429);
    assert.equal(blockedKey.body.scope, "key");
    // A different key of the same tenant has its own bucket.
    assert.equal(
      (
        await request(perKey)
          .post("/v1/events")
          .set("X-API-Key", keys.serverKey)
          .send(envelope([event()]))
      ).status,
      200,
    );

    const perTenant = createApp({
      pool,
      submit: stubSubmit,
      limits: only("tenant", { ratePerSec: 1, burst: 1 }),
    });
    assert.equal(
      (
        await request(perTenant)
          .post("/v1/events")
          .set("X-API-Key", keys.ingestKey)
          .send(envelope([event()]))
      ).status,
      200,
    );
    const blockedTenant = await request(perTenant)
      .post("/v1/events")
      .set("X-API-Key", keys.serverKey)
      .send(envelope([event()]));
    assert.equal(blockedTenant.status, 429);
    assert.equal(blockedTenant.body.scope, "tenant");
  });
});

test("session scope: charges validated events per session, nothing on rejection", async () => {
  await withTestDb("ingest_limit_session", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const submitted = [];
    const app = createApp({
      pool,
      limits: only("session", { ratePerSec: 1, burst: 5 }),
      submit: async (req) => {
        submitted.push(req.outcome.rows.length);
        return {
          stored: req.outcome.rows.map((r) => r.event_id),
          dups: [],
          capped: [],
          conflicts: [],
        };
      },
    });
    const post = (events) =>
      request(app)
        .post("/v1/events")
        .set("X-API-Key", keys.ingestKey)
        .send(envelope(events));

    // 4 events for A consume most of its bucket.
    assert.equal(
      (
        await post([
          event(SESSION_A),
          event(SESSION_A),
          event(SESSION_A),
          event(SESSION_A),
        ])
      ).status,
      200,
    );

    // 2 more cannot be paid: 429, and the write path was not called twice.
    const denied = await post([event(SESSION_A), event(SESSION_A)]);
    assert.equal(denied.status, 429);
    assert.equal(denied.body.scope, "session");
    assert.deepEqual(submitted, [4], "a rejected batch never reaches submit");

    // A different session has its own bucket.
    assert.equal((await post([event(SESSION_B), event(SESSION_B)])).status, 200);
    assert.deepEqual(submitted, [4, 2]);

    // Per-event validation rejections cost nothing: a batch of bad rows is
    // still limited only by what survives validation.
    const badName = { ...event(SESSION_A), name: "bad name with spaces" };
    assert.equal((await post([badName])).status, 200);
    assert.deepEqual(submitted, [4, 2]);
  });
});

test("limits: null disables every scope", async () => {
  await withTestDb("ingest_limit_off", async ({ pool }) => {
    const keys = await seedTenantWithKeys(pool);
    const app = createApp({ pool, submit: stubSubmit, limits: null });
    for (let i = 0; i < 50; i++) {
      const r = await request(app)
        .post("/v1/events")
        .set("X-API-Key", keys.ingestKey)
        .send(envelope([event()]));
      assert.equal(r.status, 200);
    }
  });
});
