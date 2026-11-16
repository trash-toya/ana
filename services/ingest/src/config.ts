// Ingest service configuration (04-services.md §Config). No secrets in the
// image: everything here is tunables.

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
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): IngestConfig {
  return {
    port: Number(env.PORT ?? 3001),
    maxBatch: Number(env.MAX_BATCH ?? 500),
    bufferMax: Number(env.BUFFER_MAX ?? 5000),
    flushIdleMs: Number(env.FLUSH_IDLE_MS ?? 50),
    flushIntervalMs: Number(env.FLUSH_INTERVAL_MS ?? 1000),
  };
}
