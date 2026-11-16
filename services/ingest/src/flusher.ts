// Buffer + flush (04-services.md §"Buffering and response semantics").
//
// An HTTP response is only written after the batch that contains its events
// commits: enqueue() hands each request a waiter that resolves when ALL of
// its rows have outcomes — possibly across several flushes when MAX_BATCH
// splits it. At most ONE ingest_batch() call is in flight; the timer never
// stacks flushes. Backpressure does not un-buffer: the request that trips
// BUFFER_MAX gets 503 + Retry-After: 5 while everything already buffered
// stays and eventually commits.

import type { Pool } from "pg";
import type { Metrics } from "./metrics.js";
import type { IngestBatchResult, PreparedEvent, ValidationOutcome } from "./validate.js";

// Captured at module load, before any test enables mock timers.
const setTimeoutReal = setTimeout;

export class BufferFullError extends Error {
  readonly status = 503;
  readonly retryAfter = 5; // seconds, per the response contract
  readonly code = "buffer_full";
  constructor() {
    super("ingest buffer is full");
    this.name = "BufferFullError";
  }
}

interface BufferItem {
  row: PreparedEvent;
  req: PendingRequest;
}

interface PendingRequest {
  outcome: ValidationOutcome;
  rowsLeft: number;
  results: {
    stored: Set<string>;
    dups: Set<string>;
    capped: Set<string>;
    conflicts: Set<string>;
  };
  resolve: (r: IngestBatchResult) => void;
  reject: (e: unknown) => void;
}

export interface FlusherOptions {
  pool: Pool;
  maxBatch?: number;
  bufferMax?: number;
  flushIdleMs?: number;
  flushIntervalMs?: number;
  metrics?: Metrics;
  /** Injectable batch executor (tests). Default: one ingest_batch() call. */
  submitBatch?: (rows: PreparedEvent[]) => Promise<IngestBatchResult>;
}

export class Flusher {
  private readonly pool: Pool;
  private readonly maxBatch: number;
  private readonly bufferMax: number;
  private readonly flushIdleMs: number;
  private readonly flushIntervalMs: number;
  private readonly metrics?: Metrics;
  private readonly submitBatchFn: (rows: PreparedEvent[]) => Promise<IngestBatchResult>;

  private buffer: BufferItem[] = [];
  private inflight = false;
  private inflightCount = 0;
  private flushPromise: Promise<void> | null = null;
  private stopped = false;
  private idleTimer: NodeJS.Timeout | null = null;
  private ceilingTimer: NodeJS.Timeout | null = null;

  constructor(options: FlusherOptions) {
    this.pool = options.pool;
    this.maxBatch = options.maxBatch ?? 500;
    this.bufferMax = options.bufferMax ?? 5000;
    this.flushIdleMs = options.flushIdleMs ?? 50;
    this.flushIntervalMs = options.flushIntervalMs ?? 1000;
    this.metrics = options.metrics;
    this.submitBatchFn = options.submitBatch ?? ((rows) => this.defaultSubmitBatch(rows));
    // Safety ceiling: flush() no-ops when empty or already in flight, so this
    // can never stack flushes — it only guarantees eventual progress.
    this.ceilingTimer = setInterval(() => void this.flush(), this.flushIntervalMs);
    this.ceilingTimer.unref();
  }

  get depth(): number {
    return this.buffer.length;
  }

  /** Buffered + in-flight rows — the number the BUFFER_MAX guard uses. */
  get pending(): number {
    return this.buffer.length + this.inflightCount;
  }

  /**
   * Enqueue a validated batch atomically and return a waiter that resolves
   * with the aggregated ingest_batch() result once ALL of the request's rows
   * have outcomes. Throws BufferFullError (→ 503 + Retry-After: 5) when the
   * request would push the buffer past bufferMax; already-buffered events
   * stay untouched.
   */
  enqueue(_tenantId: string, outcome: ValidationOutcome): Promise<IngestBatchResult> {
    if (this.stopped) throw new BufferFullError();
    if (this.pending + outcome.rows.length > this.bufferMax) {
      throw new BufferFullError();
    }
    const req: PendingRequest = {
      outcome,
      rowsLeft: outcome.rows.length,
      results: {
        stored: new Set(),
        dups: new Set(),
        capped: new Set(),
        conflicts: new Set(),
      },
      resolve: () => {},
      reject: () => {},
    };
    const promise = new Promise<IngestBatchResult>((resolve, reject) => {
      req.resolve = resolve;
      req.reject = reject;
    });
    for (const row of outcome.rows) {
      this.buffer.push({ row, req });
    }
    this.setGauge();
    if (outcome.rows.length === 0) {
      // Nothing to store: resolve immediately with empty results.
      req.resolve({ stored: [], dups: [], capped: [], conflicts: [] });
    } else {
      this.maybeTrigger();
    }
    return promise;
  }

  /** Stop accepting new work and drain the buffer (bounded: 10 s real time).
   *  A flush stuck on a hung DB connection cannot block shutdown forever. */
  async shutdown(): Promise<void> {
    this.stopped = true;
    if (this.idleTimer) clearTimeout(this.idleTimer);
    if (this.ceilingTimer) clearInterval(this.ceilingTimer);
    this.idleTimer = null;
    this.ceilingTimer = null;
    const deadline = Date.now() + 10_000;
    while (this.pending > 0 && Date.now() < deadline) {
      await Promise.race([
        this.flush(),
        new Promise<void>((resolve) => setTimeoutReal(resolve, 250).unref()),
      ]);
    }
  }

  // ------------------------------------------------------------------

  private maybeTrigger(): void {
    if (this.stopped) return;
    if (this.buffer.length >= this.maxBatch) {
      void this.flush();
      return;
    }
    // Adaptive idle flush, only when no flush is in flight (one at a time).
    this.scheduleIdleFlush();
  }

  /** One flush at a time: concurrent calls join the same in-flight flush. */
  private flush(): Promise<void> {
    if (this.flushPromise) return this.flushPromise;
    // runFlush never rejects (it handles submit errors internally); the
    // continuation MUST run after flushPromise is cleared, otherwise a
    // buffered remainder would wait for the ceiling timer.
    this.flushPromise = this.runFlush().finally(() => {
      this.flushPromise = null;
      if (this.buffer.length > 0 && !this.stopped) {
        if (this.buffer.length >= this.maxBatch) void this.flush();
        else if (!this.inflight && !this.idleTimer) this.scheduleIdleFlush();
      }
    });
    return this.flushPromise;
  }

  private scheduleIdleFlush(): void {
    if (this.idleTimer || this.inflight) return;
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      void this.flush();
    }, this.flushIdleMs);
    this.idleTimer.unref();
  }

  private async runFlush(): Promise<void> {
    if (this.inflight || this.buffer.length === 0) return;
    this.inflight = true;
    try {
      while (this.buffer.length > 0) {
        const items = this.buffer.splice(0, this.maxBatch);
        this.inflightCount += items.length;
        const byReq = new Map<PendingRequest, PreparedEvent[]>();
        for (const item of items) {
          const rows = byReq.get(item.req);
          if (rows) rows.push(item.row);
          else byReq.set(item.req, [item.row]);
        }
        try {
          const result = await this.submitBatchFn(items.map((i) => i.row));
          for (const [req, rows] of byReq) {
            const ids = new Set(rows.map((r) => r.event_id));
            const subset = filterResult(result, ids);
            subset.stored.forEach((id) => req.results.stored.add(id));
            subset.dups.forEach((id) => req.results.dups.add(id));
            subset.capped.forEach((id) => req.results.capped.add(id));
            subset.conflicts.forEach((id) => req.results.conflicts.add(id));
            req.rowsLeft -= rows.length;
            if (req.rowsLeft === 0) {
              req.resolve({
                stored: [...req.results.stored],
                dups: [...req.results.dups],
                capped: [...req.results.capped],
                conflicts: [...req.results.conflicts],
              });
            }
          }
        } catch (err) {
          // DB error: every waiter whose rows were in this batch → 503.
          // The rows are not re-buffered (the client's deduping retry is the
          // recovery path); everything not in this batch stays buffered.
          for (const [req] of byReq) req.reject(err);
          this.metrics?.inc("ingest_flush_errors_total");
          break;
        } finally {
          this.inflightCount -= items.length;
        }
      }
    } finally {
      this.inflight = false;
      this.setGauge();
    }
  }

  private setGauge(): void {
    this.metrics?.gauge("ingest_buffer_depth", this.buffer.length);
  }

  private async defaultSubmitBatch(rows: PreparedEvent[]): Promise<IngestBatchResult> {
    const client = await this.pool.connect();
    try {
      const res = await client.query<{ r: Partial<IngestBatchResult> | null }>(
        "SELECT ingest_batch($1) AS r",
        [JSON.stringify(rows)],
      );
      const r = res.rows[0]?.r ?? {};
      return {
        stored: r.stored ?? [],
        dups: r.dups ?? [],
        capped: r.capped ?? [],
        conflicts: r.conflicts ?? [],
      };
    } finally {
      client.release();
    }
  }
}

function filterResult(result: IngestBatchResult, ids: Set<string>): IngestBatchResult {
  const keep = (list: string[]) => list.filter((id) => ids.has(id));
  return {
    stored: keep(result.stored),
    dups: keep(result.dups),
    capped: keep(result.capped),
    conflicts: keep(result.conflicts),
  };
}
