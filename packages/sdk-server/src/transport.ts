// Transport: batch selection + one POST with per-status handling
// (04-services.md response contract, full retry matrix — P2-2 for the server).
//
// No runtime dependency on @session-analytics/shared: constants are duplicated
// here so the package builds before the shared package (workspace build order
// is alphabetical) and ships with no runtime dependencies.

/** Same limit as the ingest API (packages/shared contract.ts). */
export const MAX_EVENTS_PER_REQUEST = 500;

/** A server batch is not constrained by the browser keepalive limit. */
export const DEFAULT_MAX_BATCH_BYTES = 256 * 1024;

export interface QueuedEvent {
  event_id: string;
  [key: string]: unknown;
}

export interface BatchPlan {
  events: QueuedEvent[];
  bytes: number;
}

/** Select the next batch: ≤ maxEvents events, ≤ maxBytes serialised. */
export function selectBatch(
  events: QueuedEvent[],
  maxBytes = DEFAULT_MAX_BATCH_BYTES,
  maxEvents = MAX_EVENTS_PER_REQUEST,
): BatchPlan {
  const batch: QueuedEvent[] = [];
  let bytes = 0;
  for (const event of events) {
    const size = JSON.stringify(event).length + 2;
    if (batch.length >= maxEvents) break;
    if (batch.length > 0 && bytes + size > maxBytes) break;
    batch.push(event);
    bytes += size;
  }
  return { events: batch, bytes };
}

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

export type FetchLike = (url: string, init: RequestInit) => Promise<FetchResponse>;

export type SendOutcome =
  /** 200: these ids are final (accepted ∪ rejected); anything else is unknown. */
  | { kind: "ok"; acked: string[]; retained: string[] }
  /** 400: SDK bug — drop the whole batch, never loop. */
  | { kind: "permanent" }
  /** 401/403: credential problem — retain the queue and stop. */
  | { kind: "auth"; status: number }
  /** 413: batch too large — caller halves it. */
  | { kind: "too_large" }
  /** 429/5xx/network: transient. `delayMs` is the server's Retry-After, if any. */
  | { kind: "retry"; delayMs?: number };

export interface SendBatchOptions {
  url: string;
  body: unknown;
  apiKey: string;
  batchIds: string[];
  fetchFn: FetchLike;
}

export async function sendBatch(options: SendBatchOptions): Promise<SendOutcome> {
  let res: FetchResponse;
  try {
    res = await options.fetchFn(options.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": options.apiKey,
      },
      body: JSON.stringify(options.body),
    });
  } catch {
    return { kind: "retry" }; // network error
  }

  if (res.status === 200) {
    const outcomes = (await res.json()) as Outcomes;
    const acked = [...outcomes.accepted, ...outcomes.rejected.map((r) => r.event_id)];
    const ackedSet = new Set(acked);
    return {
      kind: "ok",
      acked,
      retained: options.batchIds.filter((id) => !ackedSet.has(id)),
    };
  }
  if (res.status === 400) return { kind: "permanent" };
  if (res.status === 401 || res.status === 403) return { kind: "auth", status: res.status };
  if (res.status === 413) return { kind: "too_large" };
  const delayMs = retryAfterMs(res.headers?.get("retry-after") ?? null);
  return delayMs === undefined ? { kind: "retry" } : { kind: "retry", delayMs };
}

/** Retry-After is delta-seconds or an HTTP date; both map to a delay. */
export function retryAfterMs(value: string | null, nowMs = Date.now()): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  const at = Date.parse(value);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - nowMs);
}
