// @session-analytics/sdk-server — the application backend's client for the
// canonical ingestion pipeline (Notion Rev 5 §19, P1B-4).
//
// The backend owns the analytics session:
//   startSession()    POST /v1/sessions          (create or resume)
//   validateSession() POST /v1/sessions/validate (check a propagated context)
//   track()           buffers one event, bound to a session or sessionless
//   flush()           drains the buffer with the full retry matrix
//   shutdown()        final drain, stops timers
//
// Guarantees:
//  - events are authoritative server observations: `source` defaults to
//    `server`, and the producer's `time` is accepted as-is by the ingest API
//    (D32);
//  - `seq` is omitted by default (D31): a backend cannot know the browser's
//    next sequence number for the same session;
//  - `session_start` is emitted exactly once when the bootstrap says the
//    session has stored no events yet (`emitted_events === 0`);
//  - sessionless events (`sessionId: null`) are first-class: background jobs
//    and webhooks need no session and no visitor id (D29);
//  - retries never lose an acknowledged event: 400 drops a bad batch, 401/403
//    retain it and stop, 413 halves it, 429 honours Retry-After, 5xx/network
//    back off with jitter;
//  - `eventId` is caller-supplied when the producer has its own idempotency
//    key, so re-delivering a webhook stores one event (the DB deduplicates).
//
// Durability: the buffer is in memory. A backend that must not lose events on
// a crash writes them to a transactional outbox and calls track() after the
// business transaction commits (05-sdks.md).

import { randomBytes, uuidv7 } from "./ids.js";
import {
  DEFAULT_MAX_BATCH_BYTES,
  sendBatch,
  selectBatch,
  type FetchLike,
  type QueuedEvent,
} from "./transport.js";

/** Exported for callers that implement their own transport/timeout policy. */
export { retryAfterMs } from "./transport.js";
export type { FetchLike, FetchResponse } from "./transport.js";

/** Wire schema version (packages/shared contract.ts). */
export const SCHEMA_VERSION = 1;

export type EventSource = "client" | "server" | "webhook";

export interface SessionStartOptions {
  /** Visitor correlation id. Generated when absent; reuse it to resume. */
  anonId?: string;
  /** Authenticated application user, resolved by the backend. */
  userId?: string | null;
  /** session_start attribution: device, country, utm, … (≤4 KB). */
  properties?: Record<string, unknown>;
}

/** Response of POST /v1/sessions (mirrors packages/shared sessions.ts). */
export interface SessionDescriptor {
  session_id: string;
  anon_id: string;
  user_id: string | null;
  started_at: string;
  last_event_at: string;
  expires_at: string;
  resumed: boolean;
  emitted_events: number;
  properties: Record<string, unknown>;
  /** Opaque signed context to propagate as `X-Analytics-Session`. */
  context: string;
}

export type SessionInvalidReason =
  | "malformed"
  | "bad_signature"
  | "expired"
  | "unknown_session";

export interface SessionValidation {
  valid: boolean;
  reason?: SessionInvalidReason;
  session_id?: string;
  anon_id?: string;
  user_id?: string | null;
  expires_at?: string;
  emitted_events?: number;
  properties?: Record<string, unknown>;
}

export interface TrackOptions {
  /**
   * Session to attach the event to. Omit to use the active bootstrapped
   * session; pass `null` for a sessionless (machine-originated) event.
   */
  sessionId?: string | null;
  /** Visitor id. Defaults to the active session's; required when sessionId is set. */
  anonId?: string | null;
  userId?: string | null;
  /** Producer's observation time. Defaults to now (strictly increasing). */
  time?: Date | string;
  /** Event name; must match ^[a-z][a-z0-9_.]{0,63}$. */
  props?: Record<string, unknown>;
  url?: string | null;
  /** Event provenance. Defaults to `server`. */
  source?: EventSource;
  /** Caller-minted id (webhook/provider idempotency key). */
  eventId?: string;
  /** Optional explicit sequence number; omitted by default (D31). */
  seq?: number | null;
}

export interface ServerAnalyticsOptions {
  /** Service base URL, e.g. https://ingest.example.com (or with /v1/events). */
  endpoint: string;
  /** Server-kind API key. A client ingest key cannot bootstrap sessions. */
  apiKey: string;
  /** Override fetch (tests, custom agents). */
  fetchFn?: FetchLike;
  /** Injectable clock, milliseconds. */
  now?: () => number;
  /** Automatic flush debounce after track(); default 1000 ms. */
  flushDebounceMs?: number;
  /** `false` disables the timer; call flush() explicitly. */
  autoFlush?: boolean;
  /** Batch size ceiling in serialised bytes. */
  maxBatchBytes?: number;
  /** First backoff step; default 500 ms. */
  retryBaseMs?: number;
  /** Backoff ceiling; default 30 s. */
  retryMaxMs?: number;
  /** Attempts per flush() call before leaving the queue for the next call. */
  maxFlushAttempts?: number;
  /** Jitter source (tests) returning [0, 1). Default Math.random. */
  jitter?: () => number;
  /** Structured log sink; logs are never emitted to stdout by default. */
  logger?: (entry: Record<string, unknown>) => void;
}

export type ServerAnalyticsErrorCode =
  | "closed"
  | "bad_name"
  | "missing_anon_id"
  | "network_error"
  | "auth_failed"
  | "invalid_request"
  | "bootstrap_failed"
  | "validation_failed";

export class ServerAnalyticsError extends Error {
  readonly code: ServerAnalyticsErrorCode;
  readonly status?: number;

  constructor(code: ServerAnalyticsErrorCode, message: string, status?: number) {
    super(message);
    this.name = "ServerAnalyticsError";
    this.code = code;
    this.status = status;
  }
}

const NAME_PATTERN = /^[a-z][a-z0-9_.]{0,63}$/;
const DEFAULT_FLUSH_DEBOUNCE_MS = 1_000;
const DEFAULT_RETRY_BASE_MS = 500;
const DEFAULT_RETRY_MAX_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 8;

type IntrinsicName = string;

interface EnqueueSpec {
  sessionId: string | null;
  anonId: string | null;
  userId: string | null;
  source: EventSource;
  props: Record<string, unknown>;
  url?: string | null;
  seq?: number | null;
  time?: Date | string;
  eventId?: string;
}

export class ServerAnalytics {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly fetchFn: FetchLike;
  private readonly now: () => number;
  private readonly flushDebounceMs: number;
  private readonly autoFlush: boolean;
  private readonly maxBatchBytes: number;
  private readonly retryBaseMs: number;
  private readonly retryMaxMs: number;
  private readonly maxFlushAttempts: number;
  private readonly jitter: () => number;
  private readonly logger: (entry: Record<string, unknown>) => void;

  private readonly queue: QueuedEvent[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private flushing: Promise<void> | null = null;
  private closed = false;
  private sessionInfo: SessionDescriptor | null = null;
  private pendingSessionStart = false;
  private lastEventMs = 0;
  private mintMs = -1;
  private mintCounter = 0;
  private lastAuthError: number | null = null;

  constructor(options: ServerAnalyticsOptions) {
    if (!options.endpoint) throw new ServerAnalyticsError("invalid_request", "endpoint is required");
    if (!options.apiKey) throw new ServerAnalyticsError("invalid_request", "apiKey is required");
    this.baseUrl = normalizeBase(options.endpoint);
    this.apiKey = options.apiKey;
    this.fetchFn = options.fetchFn ?? ((url, init) => fetch(url, init));
    this.now = options.now ?? (() => Date.now());
    this.flushDebounceMs = options.flushDebounceMs ?? DEFAULT_FLUSH_DEBOUNCE_MS;
    this.autoFlush = options.autoFlush ?? true;
    this.maxBatchBytes = options.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES;
    this.retryBaseMs = options.retryBaseMs ?? DEFAULT_RETRY_BASE_MS;
    this.retryMaxMs = options.retryMaxMs ?? DEFAULT_RETRY_MAX_MS;
    this.maxFlushAttempts = options.maxFlushAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.jitter = options.jitter ?? Math.random;
    this.logger = options.logger ?? (() => {});
  }

  /** The active bootstrapped session, if any. */
  session(): SessionDescriptor | null {
    return this.sessionInfo;
  }

  /** Events buffered but not yet acknowledged. */
  pending(): number {
    return this.queue.length;
  }

  // ------------------------------------------------------------------
  // Session bootstrap / resume
  // ------------------------------------------------------------------

  async startSession(options: SessionStartOptions = {}): Promise<SessionDescriptor> {
    const { status, body } = await this.request("/v1/sessions", {
      anon_id: options.anonId,
      user_id: options.userId ?? null,
      properties: options.properties ?? {},
    });
    if (status === 401 || status === 403) {
      throw new ServerAnalyticsError("auth_failed", "session bootstrap rejected", status);
    }
    if (status !== 200 || !isSessionDescriptor(body)) {
      throw new ServerAnalyticsError("bootstrap_failed", `session bootstrap failed (${status})`, status);
    }
    this.sessionInfo = body;
    // The server SDK must open the session's event stream exactly once.
    this.pendingSessionStart = body.emitted_events === 0;
    return body;
  }

  async validateSession(context: string): Promise<SessionValidation> {
    const { status, body } = await this.request("/v1/sessions/validate", { context });
    if (status === 401 || status === 403) {
      throw new ServerAnalyticsError("auth_failed", "session validation rejected", status);
    }
    if (status !== 200 || typeof body !== "object" || body === null) {
      throw new ServerAnalyticsError("validation_failed", `session validation failed (${status})`, status);
    }
    return body as SessionValidation;
  }

  // ------------------------------------------------------------------
  // Tracking
  // ------------------------------------------------------------------

  /** Buffer one event. Returns its event_id. */
  track(name: string, options: TrackOptions = {}): string {
    if (this.closed) {
      throw new ServerAnalyticsError("closed", "analytics client is shut down");
    }
    if (!NAME_PATTERN.test(name)) {
      throw new ServerAnalyticsError("bad_name", `invalid event name: ${name}`);
    }

    const active = this.sessionInfo;
    const sessionId = "sessionId" in options
      ? options.sessionId ?? null
      : active?.session_id ?? null;
    const isActive = active !== null && sessionId === active.session_id;

    let anonId: string | null;
    if ("anonId" in options) anonId = options.anonId ?? null;
    else if (isActive) anonId = active.anon_id;
    else anonId = null;

    if (sessionId !== null && !anonId) {
      throw new ServerAnalyticsError(
        "missing_anon_id",
        "a session-bound event needs an anonId (pass the active session or anonId explicitly)",
      );
    }

    const userId = options.userId !== undefined
      ? options.userId
      : isActive
        ? active.user_id
        : null;
    const source = options.source ?? "server";

    // session_start is emitted by the SDK, not demanded from the caller, and
    // only for a session that has stored no events yet (D30).
    if (isActive && this.pendingSessionStart && name !== "session_start") {
      this.enqueue("session_start", {
        sessionId: sessionId as string,
        anonId: anonId as string,
        userId,
        source,
        props: { ...active.properties },
      });
      this.pendingSessionStart = false;
    }
    if (isActive && name === "session_start") {
      this.pendingSessionStart = false;
    }

    return this.enqueue(name, {
      sessionId,
      anonId,
      userId,
      source,
      props: options.props ?? {},
      ...(options.url !== undefined ? { url: options.url } : {}),
      ...(options.seq !== undefined ? { seq: options.seq } : {}),
      ...(options.time !== undefined ? { time: options.time } : {}),
      ...(options.eventId !== undefined ? { eventId: options.eventId } : {}),
    });
  }

  private enqueue(name: IntrinsicName, spec: EnqueueSpec): string {
    const eventId = spec.eventId ?? this.mintId();
    const event: Record<string, unknown> = {
      event_id: eventId,
      time: this.nextTime(spec.time),
      name,
      source: spec.source,
    };
    if (spec.sessionId !== null) event.session_id = spec.sessionId;
    if (spec.anonId !== null) event.anon_id = spec.anonId;
    if (spec.userId !== null) event.user_id = spec.userId;
    if (spec.url !== undefined && spec.url !== null) event.url = spec.url;
    // D31: seq is omitted by default so it cannot collide with a client seq.
    if (spec.seq !== undefined && spec.seq !== null) event.seq = spec.seq;
    event.props = spec.props;

    this.queue.push(event as QueuedEvent);
    this.scheduleFlush();
    return eventId;
  }

  private mintId(): string {
    const ms = this.now();
    if (ms === this.mintMs) this.mintCounter += 1;
    else {
      this.mintMs = ms;
      this.mintCounter = 0;
    }
    const bytes = randomBytes();
    // Counter in the first two random bytes keeps same-ms ids in mint order.
    bytes[0] = (this.mintCounter >> 8) & 0xff;
    bytes[1] = this.mintCounter & 0xff;
    return uuidv7(ms, bytes);
  }

  /** Producer time: strictly increasing within this client instance. */
  private nextTime(override?: Date | string): string {
    let ms: number;
    if (override === undefined) {
      ms = Math.max(this.now(), this.lastEventMs + 1);
    } else {
      ms = override instanceof Date ? override.getTime() : Date.parse(override);
      if (Number.isNaN(ms)) throw new ServerAnalyticsError("invalid_request", "invalid time");
    }
    this.lastEventMs = Math.max(this.lastEventMs, ms);
    return new Date(ms).toISOString();
  }

  // ------------------------------------------------------------------
  // Flush
  // ------------------------------------------------------------------

  /** Drain the buffer. Concurrent callers share one drain. */
  async flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    this.flushing = this.drain().finally(() => {
      this.flushing = null;
    });
    return this.flushing;
  }

  private async drain(): Promise<void> {
    let attempt = 0;
    let maxEvents = MAX_BATCH_EVENTS;

    while (this.queue.length > 0) {
      const plan = selectBatch(this.queue, this.maxBatchBytes, maxEvents);
      if (plan.events.length === 0) return; // single oversized event: caller drops it
      const ids = plan.events.map((event) => event.event_id);
      const body = {
        schema_version: SCHEMA_VERSION,
        sent_at: new Date(this.now()).toISOString(),
        events: plan.events,
      };
      const outcome = await sendBatch({
        url: `${this.baseUrl}/v1/events`,
        body,
        apiKey: this.apiKey,
        batchIds: ids,
        fetchFn: this.fetchFn,
      });

      if (outcome.kind === "ok") {
        this.dequeue(outcome.acked);
        attempt = 0;
        if (outcome.retained.length > 0) {
          // The server did not report these ids; retry them like a transient
          // failure, but never spin without progress.
          if (outcome.acked.length === 0) attempt += 1;
          if (attempt >= this.maxFlushAttempts) return;
        }
        continue;
      }

      if (outcome.kind === "permanent") {
        // 400 is an SDK bug: the whole batch is unstoreable. Drop it rather
        // than loop forever, and surface it in the log.
        this.dequeue(ids);
        this.logger({ msg: "batch_dropped", reason: "400", events: ids.length });
        attempt = 0;
        continue;
      }

      if (outcome.kind === "auth") {
        this.lastAuthError = outcome.status;
        this.logger({ msg: "flush_stopped", reason: "auth", status: outcome.status });
        return; // retain the queue; the next flush retries
      }

      if (outcome.kind === "too_large") {
        if (plan.events.length <= 1) {
          // A single event over the limit can never succeed.
          this.dequeue(ids);
          this.logger({ msg: "batch_dropped", reason: "413", events: 1 });
          attempt = 0;
          continue;
        }
        maxEvents = Math.max(1, Math.floor(plan.events.length / 2));
        continue;
      }

      // transient: honour Retry-After, else exponential backoff with jitter
      attempt += 1;
      if (attempt >= this.maxFlushAttempts) return;
      const delayMs = outcome.delayMs ?? this.backoffMs(attempt);
      await sleep(delayMs);
    }
  }

  private backoffMs(attempt: number): number {
    const ceiling = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** (attempt - 1));
    return Math.round(ceiling / 2 + this.jitter() * (ceiling / 2));
  }

  private dequeue(ids: string[]): void {
    if (ids.length === 0) return;
    const drop = new Set(ids);
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const event = this.queue[i];
      if (event && drop.has(event.event_id)) this.queue.splice(i, 1);
    }
  }

  private scheduleFlush(): void {
    if (!this.autoFlush || this.timer || this.closed) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.flush();
    }, this.flushDebounceMs);
    // A backend batch cadence must not hold the host event loop open.
    if (typeof this.timer === "object" && "unref" in this.timer) {
      (this.timer as { unref: () => void }).unref();
    }
  }

  /** Final flush, then stop automatic work. Idempotent. */
  async shutdown(): Promise<void> {
    if (this.closed) {
      await this.flush();
      return;
    }
    this.closed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.flush();
    if (this.queue.length > 0) {
      this.logger({ msg: "shutdown_incomplete", pending: this.queue.length });
    }
  }

  /** Last status that stopped a flush (401/403), for health reporting. */
  authError(): number | null {
    return this.lastAuthError;
  }

  // ------------------------------------------------------------------
  // HTTP
  // ------------------------------------------------------------------

  private async request(
    path: string,
    body: unknown,
  ): Promise<{ status: number; body: unknown }> {
    let res;
    try {
      res = await this.fetchFn(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": this.apiKey,
        },
        body: JSON.stringify(body),
      });
    } catch {
      throw new ServerAnalyticsError("network_error", `request to ${path} failed`);
    }
    let parsed: unknown = null;
    try {
      parsed = await res.json();
    } catch {
      /* non-JSON error body */
    }
    return { status: res.status, body: parsed };
  }
}

const MAX_BATCH_EVENTS = 500;

function normalizeBase(endpoint: string): string {
  let base = endpoint.replace(/\/+$/, "");
  if (base.endsWith("/v1/events")) base = base.slice(0, -"/v1/events".length);
  if (base.endsWith("/v1/sessions")) base = base.slice(0, -"/v1/sessions".length);
  return base;
}

function isSessionDescriptor(value: unknown): value is SessionDescriptor {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.session_id === "string" &&
    typeof candidate.anon_id === "string" &&
    typeof candidate.context === "string" &&
    typeof candidate.emitted_events === "number"
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
