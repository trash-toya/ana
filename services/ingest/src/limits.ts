// Abuse protection (04-services.md §Abuse protection, P4-3): in-process token
// buckets, one per scope.
//
//   ip       — every /v1 request, before auth (an unauthenticated flood is the
//              cheapest attack to mount).
//   key      — per API-key digest, after auth.
//   tenant   — per tenant, after auth (a tenant with two keys shares one).
//   session  — per session_id, charged by EVENT COUNT from the request body.
//
// Deliberate leniency, documented in 04-services.md: key/tenant buckets are
// PER INSTANCE — with N active instances the effective limit is N× the
// configured value. That is accepted until a Postgres-side counter exists
// (the per-tenant daily quota is deferred, DEVIATIONS.md D38); the LB is
// consistent-hash, so the ip/session buckets (the ones that actually protect
// one hot visitor or one abusive host) stay meaningful per instance.
//
// Rejected requests never reach the buffer or the database: 429 carries
// `Retry-After` (seconds, rounded up) and `{error: "rate_limited", scope}`.
// Tokens refill continuously (ratePerSec), capacity is the burst.

import type { Request, RequestHandler, Response } from "express";
import type { Metrics } from "./metrics.js";
import "./types.js";

export type LimitScope = "ip" | "key" | "tenant" | "session";

export interface BucketSpec {
  /** Sustained tokens per second. */
  ratePerSec: number;
  /** Bucket capacity; also the largest single charge accepted. */
  burst: number;
}

export interface LimitConfig {
  ip: BucketSpec | null;
  key: BucketSpec | null;
  tenant: BucketSpec | null;
  session: BucketSpec | null;
}

/**
 * Defaults sized for the SDK contract, not for pennies: a flush can carry 500
 * events (MAX_EVENTS_PER_REQUEST) for one session after a long offline period,
 * so the session burst must accept that in one request. These stop runaway
 * producers, not normal traffic.
 */
export const LIMIT_DEFAULTS: LimitConfig = {
  ip: { ratePerSec: 200, burst: 400 },
  key: { ratePerSec: 100, burst: 200 },
  tenant: { ratePerSec: 300, burst: 600 },
  session: { ratePerSec: 1000, burst: 2000 },
};

export type Decision =
  | { ok: true }
  | { ok: false; scope: LimitScope; retryAfterSec: number };

/**
 * The one 429 response shape (04-services.md): the SDK keeps the queue and
 * retries after `Retry-After`; a rejected request never reaches the buffer or
 * the database.
 */
export function sendRateLimited(
  res: Response,
  decision: Extract<Decision, { ok: false }>,
  metrics?: Metrics,
): void {
  metrics?.inc("rate_limited_total", { scope: decision.scope });
  res
    .status(429)
    .set("Retry-After", String(decision.retryAfterSec))
    .json({ error: "rate_limited", scope: decision.scope });
}

export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly spec: BucketSpec,
    private readonly now: () => number,
  ) {
    this.tokens = spec.burst;
    this.last = now();
  }

  /** Refill to "now", then report whether `n` tokens are available. */
  available(n = 1): boolean {
    this.touch();
    return n > this.spec.burst || this.tokens >= n;
  }

  /** Consume `n` tokens; caller must have checked `available(n)` first. */
  consume(n = 1): void {
    if (n <= this.spec.burst) this.tokens -= n;
  }

  /** Charge `n` tokens all-or-nothing. A denied charge consumes nothing. */
  charge(n = 1): { ok: true } | { ok: false; retryAfterSec: number } {
    this.touch();
    if (n > this.spec.burst) {
      // A single charge larger than the bucket can never succeed; allow it
      // rather than loop the client forever (documented in LIMIT_DEFAULTS).
      return { ok: true };
    }
    if (this.tokens >= n) {
      this.tokens -= n;
      return { ok: true };
    }
    return { ok: false, retryAfterSec: this.retryAfter(n) };
  }

  /** Seconds until `n` tokens would be available. */
  retryAfter(n = 1): number {
    const missing = Math.max(0, n - this.tokens);
    return Math.max(1, Math.ceil(missing / this.spec.ratePerSec));
  }

  /** True when the bucket is full and idle (prunable). */
  isIdle(idleMs: number): boolean {
    const now = this.now();
    if (now - this.last < idleMs) return false;
    this.touch();
    return this.tokens >= this.spec.burst;
  }

  private touch(): void {
    const t = this.now();
    const elapsedSec = Math.max(0, t - this.last) / 1000;
    this.tokens = Math.min(
      this.spec.burst,
      this.tokens + elapsedSec * this.spec.ratePerSec,
    );
    this.last = t;
  }
}

export interface RateLimiterOptions {
  /** Injectable clock (tests). */
  now?: () => number;
  /** Buckets untouched and full for this long are dropped. */
  idleMs?: number;
  /** Prune the bucket map at most this often. */
  pruneEvery?: number;
}

export class RateLimiter {
  private readonly buckets = new Map<string, TokenBucket>();
  private readonly now: () => number;
  private readonly idleMs: number;
  private readonly pruneEvery: number;
  private checks = 0;

  constructor(
    private readonly config: LimitConfig,
    options: RateLimiterOptions = {},
  ) {
    this.now = options.now ?? Date.now;
    this.idleMs = options.idleMs ?? 10 * 60_000;
    this.pruneEvery = options.pruneEvery ?? 1000;
  }

  /** Charge one token for `scope`/`id`. Disabled scopes always allow. */
  check(scope: LimitScope, id: string, cost = 1): Decision {
    const spec = this.config[scope];
    if (!spec) return { ok: true };
    this.maybePrune();
    const key = `${scope}\u0000${id}`;
    let bucket = this.buckets.get(key);
    if (!bucket) {
      bucket = new TokenBucket(spec, this.now);
      this.buckets.set(key, bucket);
    }
    const result = bucket.charge(cost);
    if (!result.ok) return { ok: false, scope, retryAfterSec: result.retryAfterSec };
    return { ok: true };
  }

  /** Charge the same `cost` for every id; all-or-nothing across the group. */
  checkMany(scope: LimitScope, ids: string[], cost = 1): Decision {
    const costs = new Map<string, number>();
    for (const id of ids) costs.set(id, cost);
    return this.checkCosts(scope, costs);
  }

  /**
   * Charge a per-id cost map (event counts per session, say) all-or-nothing:
   * either every bucket can pay and all are debited, or nothing is consumed.
   */
  checkCosts(scope: LimitScope, costs: Map<string, number>): Decision {
    const spec = this.config[scope];
    if (!spec || costs.size === 0) return { ok: true };
    this.maybePrune();
    const payable: { bucket: TokenBucket; cost: number }[] = [];
    for (const [id, cost] of costs) {
      const key = `${scope}\u0000${id}`;
      let bucket = this.buckets.get(key);
      if (!bucket) {
        bucket = new TokenBucket(spec, this.now);
        this.buckets.set(key, bucket);
      }
      if (!bucket.available(cost)) {
        return { ok: false, scope, retryAfterSec: bucket.retryAfter(cost) };
      }
      payable.push({ bucket, cost });
    }
    for (const p of payable) p.bucket.consume(p.cost);
    return { ok: true };
  }

  /** Express middleware charging one token for the given scope and id. */
  middleware(
    scope: LimitScope,
    idOf: (req: Request) => string | undefined,
    metrics?: Metrics,
  ): RequestHandler {
    return (req: Request, res: Response, next) => {
      const id = idOf(req);
      if (id === undefined) {
        next();
        return;
      }
      const decision = this.check(scope, id);
      if (decision.ok) {
        next();
        return;
      }
      sendRateLimited(res, decision, metrics);
    };
  }

  /** Number of live buckets (tests/diagnostics). */
  size(): number {
    return this.buckets.size;
  }

  private maybePrune(): void {
    if (++this.checks < this.pruneEvery) return;
    this.checks = 0;
    for (const [key, bucket] of this.buckets) {
      if (bucket.isIdle(this.idleMs)) this.buckets.delete(key);
    }
  }
}
