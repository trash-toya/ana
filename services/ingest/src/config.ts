// Ingest service configuration (04-services.md §Config). No secrets in the
// image: everything here is tunables.

import { LIMIT_DEFAULTS, type BucketSpec, type LimitConfig } from "./limits.js";

export interface IngestConfig {
  port: number;
  /** Max events per ingest_batch() call (and per request). */
  maxBatch: number;
  /** Buffer bound; requests that would exceed it get 503 + Retry-After: 5. */
  bufferMax: number;
  /** Adaptive idle flush delay: flush this long after the last enqueue. */
  flushIdleMs: number;
  /** Flush ceiling timer: the longest a buffered event waits with no flush. */
  flushIntervalMs: number;
  /** Abuse-protection buckets (per instance; see limits.ts and D38). */
  limits: LimitConfig;
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): IngestConfig {
  return {
    port: Number(env.PORT ?? 3001),
    maxBatch: Number(env.MAX_BATCH ?? 500),
    bufferMax: Number(env.BUFFER_MAX ?? 5000),
    flushIdleMs: Number(env.FLUSH_IDLE_MS ?? 50),
    flushIntervalMs: Number(env.FLUSH_INTERVAL_MS ?? 1000),
    limits: {
      ip: bucketFromEnv(env, "IP", LIMIT_DEFAULTS.ip),
      key: bucketFromEnv(env, "KEY", LIMIT_DEFAULTS.key),
      tenant: bucketFromEnv(env, "TENANT", LIMIT_DEFAULTS.tenant),
      session: bucketFromEnv(env, "SESSION", LIMIT_DEFAULTS.session),
    },
  };
}

/**
 * `RATE_LIMIT_<SCOPE>_PER_SEC` / `RATE_LIMIT_<SCOPE>_BURST`. A rate of 0 (or
 * less) disables the scope; a missing burst defaults to the per-second rate,
 * so a limiter can be configured with one variable.
 */
function bucketFromEnv(
  env: NodeJS.ProcessEnv,
  scope: "IP" | "KEY" | "TENANT" | "SESSION",
  fallback: BucketSpec | null,
): BucketSpec | null {
  const rawRate = env[`RATE_LIMIT_${scope}_PER_SEC`];
  if (rawRate === undefined) return fallback;
  const ratePerSec = Number(rawRate);
  if (!Number.isFinite(ratePerSec) || ratePerSec <= 0) return null;
  const rawBurst = env[`RATE_LIMIT_${scope}_BURST`];
  const burst = rawBurst === undefined ? ratePerSec : Number(rawBurst);
  return {
    ratePerSec,
    burst: Number.isFinite(burst) && burst > 0 ? burst : ratePerSec,
  };
}
