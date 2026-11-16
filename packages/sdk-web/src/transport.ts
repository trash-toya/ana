// Transport: plain fetch with keepalive, batch selection, per-status handling
// (04-services.md response contract). The full retry matrix with backoff and
// jitter is P2-2; this minimal version never loses acknowledged events and
// never loops on a permanent failure.
//
// No runtime dependency on @session-analytics/shared: it contributes types
// only, and constants are duplicated here (the shared package's zod would
// blow the 12 KB budget, 05-sdks.md §Packaging).

/** Same limit as the ingest API (packages/shared contract.ts). */
export const MAX_EVENTS_PER_REQUEST = 500;

import type { QueuedEvent } from "./queue.js";

/** 60 KB — keepalive bodies above this are rejected by the browser. */
export const MAX_BATCH_BYTES = 60 * 1024;

export interface Outcomes {
  accepted: string[];
  rejected: { event_id: string; reason: string }[];
}

/** Minimal response shape, so tests need no real undici Response. */
export interface FetchResponse {
  status: number;
  headers?: { get(name: string): string | null };
  json(): Promise<unknown>;
}

export interface SendResult {
  /** IDs acknowledged (accepted or permanently rejected) — safe to dequeue. */
  acked: string[];
  /** IDs still unacknowledged — stay queued. */
  retained: string[];
  /** Whether the SDK should schedule another attempt soon. */
  retrySoon: boolean;
  /** Response class, driving the flush loop (04-services.md status table). */
  kind: "ok" | "permanent" | "auth" | "too_large" | "retry";
  /** Server-suggested delay (Retry-After), when the response carried one. */
  retryAfterMs?: number;
}

export interface BatchPlan {
  events: QueuedEvent[];
  bytes: number;
}

/** Select the next batch: ≤ maxEvents events, ≤ maxBytes serialised. */
export function selectBatch(
  events: QueuedEvent[],
  maxBytes = MAX_BATCH_BYTES,
  maxEvents = MAX_EVENTS_PER_REQUEST,
): BatchPlan {
  const batch: QueuedEvent[] = [];
  let bytes = 0;
  for (const e of events) {
    const size = JSON.stringify(e).length + 2; // + comma/quote overhead
    if (batch.length >= maxEvents) break;
    if (batch.length > 0 && bytes + size > maxBytes) break;
    batch.push(e);
    bytes += size;
  }
  return { events: batch, bytes };
}

export function envelope(events: QueuedEvent[], anonId: string, sentAt: string) {
  return { schema_version: 1, anon_id: anonId, sent_at: sentAt, events };
}

/**
 * POST one batch. Returns which IDs were acknowledged.
 *  - 200: accepted ∪ rejected are both final → ack everything reported.
 *  - 400: SDK bug — drop the whole batch (counted upstream), never loop.
 *  - 401/403: key problem — keep the queue, back off.
 *  - 413: batch too large — caller halves and retries (retrySoon).
 *  - 429/5xx/network: transient — keep queue, retry soon; Retry-After wins
 *    over the local backoff schedule when present.
 */
export async function sendBatch(
  endpoint: string,
  body: unknown,
  apiKey: string,
  batchIds: string[],
  fetchFn: (url: string, init: RequestInit) => Promise<FetchResponse>,
): Promise<SendResult> {
  let res: FetchResponse;
  try {
    res = await fetchFn(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": apiKey,
      },
      body: JSON.stringify(body),
      keepalive: true,
    });
  } catch {
    return { acked: [], retained: batchIds, retrySoon: true, kind: "retry" }; // network error
  }

  if (res.status === 200) {
    const outcomes = (await res.json()) as Outcomes;
    const acked = [...outcomes.accepted, ...outcomes.rejected.map((r) => r.event_id)];
    const ackedSet = new Set(acked);
    return {
      acked,
      retained: batchIds.filter((id) => !ackedSet.has(id)),
      retrySoon: false,
      kind: "ok",
    };
  }
  if (res.status === 400) {
    return { acked: batchIds, retained: [], retrySoon: false, kind: "permanent" };
  }
  if (res.status === 401 || res.status === 403) {
    return { acked: [], retained: batchIds, retrySoon: false, kind: "auth" };
  }
  if (res.status === 413) {
    return { acked: [], retained: batchIds, retrySoon: true, kind: "too_large" };
  }
  const delayMs = retryAfterMs(res.headers?.get("retry-after") ?? null);
  return {
    acked: [],
    retained: batchIds,
    retrySoon: true,
    kind: "retry",
    ...(delayMs !== undefined ? { retryAfterMs: delayMs } : {}),
  };
}

/** Retry-After is delta-seconds or an HTTP date; both map to a delay. */
export function retryAfterMs(
  value: string | null,
  nowMs = Date.now(),
): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  const at = Date.parse(value);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - nowMs);
}
