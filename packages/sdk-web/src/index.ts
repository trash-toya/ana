// Minimal web SDK (P1-5 + P2-1): init, track, identify, setConsent, flush.
//
// Guarantees (05-sdks.md §"Shared behaviour"):
//  - the event is persisted (localStorage `sa_ev:<id>`, mirrored to IndexedDB)
//    before track() resolves — on the quota fallback path the MIRROR write is
//    awaited, so the guarantee holds there too;
//  - the session record is saved BEFORE the event is enqueued (a kill between
//    the two writes leaves a harmless seq gap, never a ghost event);
//  - seq and time are allocated inside the Web Lock from the STORED record, so
//    two tabs share one session without duplicates;
//  - heartbeats never start or extend a session (D16);
//  - quota overflow drops heartbeats first, then oldest, counted in
//    `sdk_dropped` and reported on the next flush through the normal emit
//    path (no stale-session resurrection);
//  - pagehide/visibilitychange flush immediately (keepalive), bfcache
//    `pageshow` resumes without minting a new session;
//  - setConsent(false) stops tracking and clears queue + session.
//
// P2-2 owns the full retry matrix; P2-3 the no-Web-Locks leader election.

import { pageContext, type PageContext } from "./context.js";
import {
  addDropped,
  DROPPED_KEY,
  dequeueManyWithMirror,
  enqueueResilient,
  maxQueuedSeq,
  readQueue,
  recoverFromMirror,
  takeDropped,
  type QueueStorage,
} from "./queue.js";
import {
  ANON_KEY,
  clearRecord,
  CONSENT_KEY,
  expiryReason,
  LEASE_KEY,
  loadRecord,
  newSessionId,
  saveRecord,
  LOCK_NAME,
  USER_KEY,
  withLease,
  type LockManagerLike,
  type SessionRecord,
} from "./session.js";
import {
  envelope,
  MAX_BATCH_BYTES,
  MAX_EVENTS_PER_REQUEST,
  selectBatch,
  sendBatch,
} from "./transport.js";
import { openMirror, type EventMirror } from "./mirror.js";
import { randomBytes, uuidv7 } from "./ids.js";

export interface Browser {
  /** Synchronous key/value storage (localStorage in the browser). */
  storage: QueueStorage;
  /** Web Locks (navigator.locks); `null` forces the no-locks lease path in tests. */
  locks?: LockManagerLike | null;
  fetchFn: typeof fetch;
  now: () => number;
  /** Page context source; optional in non-DOM runtimes. */
  page?: () => PageContext;
  /** Document visibility for heartbeats; optional in non-DOM runtimes. */
  visibility?: () => boolean;
  /** IndexedDB mirror source (defaults to the real IndexedDB). */
  mirror?: Promise<EventMirror | undefined>;
  /** Jitter source in [0,1) for retry backoff (tests). Default Math.random. */
  jitter?: () => number;
  /** No-Web-Locks lease tuning (tests); defaults in session.ts `withLease`. */
  lease?: { ttlMs?: number; budgetMs?: number; pollMs?: number };
  /**
   * Lifecycle events for the unload flush and bfcache resume. Defaults to the
   * real `pagehide`/`pageshow` (window), `visibilitychange` (document) and
   * `storage` (window); absent in non-DOM runtimes.
   */
  lifecycle?: LifecycleSource;
}

/** Minimal event-source shape (window/document) for lifecycle wiring. */
export interface LifecycleSource {
  add(
    type: string,
    cb: (ev?: { persisted?: boolean; storageKey?: string | null }) => void,
  ): void;
  remove(
    type: string,
    cb: (ev?: { persisted?: boolean; storageKey?: string | null }) => void,
  ): void;
}

export interface InitOptions {
  apiKey: string;
  /** Ingest endpoint, e.g. https://ingest.example.com/v1/events */
  endpoint: string;
  /** Override the browser environment (tests). */
  browser?: Partial<Browser>;
  /** Heartbeat cadence; 0 disables. Default 30 s. */
  heartbeatMs?: number;
  /** Debounce before an automatic flush after track(). Default 250 ms. */
  flushDebounceMs?: number;
  /** First backoff step after a transient failure. Default 1000 ms. */
  retryBaseMs?: number;
}

export interface AnalyticsSdk {
  /** Track an event; resolves once the event is durably queued. */
  track(name: string, props?: Record<string, unknown>): Promise<void>;
  /** Attach a user id to all subsequent events. */
  identify(userId: string): void;
  /** Consent switch: false stops tracking and clears queue + session. */
  setConsent(granted: boolean): void;
  /** Send everything queued now. Resolves when the batch attempt finishes. */
  flush(): Promise<void>;
  /** Remove listeners and timers; the durable queue survives. */
  shutdown(): void;
  /** Testing/introspection only. */
  __internal: {
    session(): SessionRecord | null;
    queued(): { event_id: string }[];
    consented(): boolean;
    /** Pending-but-unreported quota drop count. */
    droppedPending(): number;
    /** Resolves once the mirror is open and restore recovery ran. */
    mirrorReady(): Promise<void>;
  };
}

const DEFAULT_HEARTBEAT_MS = 30_000;
const DEFAULT_FLUSH_DEBOUNCE_MS = 250;
const INTERACTION_WINDOW_MS = 5 * 60 * 1000;
const RETRY_BASE_MS = 1_000;
const RETRY_MAX_MS = 30_000;
/** Attempts per flush() call before leaving the queue for the next call. */
const MAX_FLUSH_ATTEMPTS = 8;

export function init(options: InitOptions): AnalyticsSdk {
  const browser: Browser = {
    storage: options.browser?.storage ?? defaultStorage(),
    locks: options.browser?.locks === undefined ? defaultLocks() : options.browser?.locks,
    fetchFn: options.browser?.fetchFn ?? defaultFetch(),
    now: options.browser?.now ?? (() => Date.now()),
    page: options.browser?.page ?? defaultPage(),
    visibility: options.browser?.visibility ?? defaultVisibility(),
    mirror: options.browser?.mirror ?? openMirror(),
    lifecycle: options.browser?.lifecycle ?? defaultLifecycle(),
  };
  const heartbeatMs = options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
  const debounceMs = options.flushDebounceMs ?? DEFAULT_FLUSH_DEBOUNCE_MS;
  const retryBase = options.retryBaseMs ?? RETRY_BASE_MS;
  const jitterFn = browser.jitter ?? Math.random;
  const storage = browser.storage;

  const state = {
    closed: false,
    consented: storage.get(CONSENT_KEY) !== "false",
    flushing: null as Promise<void> | null,
    flushTimer: null as ReturnType<typeof setTimeout> | null,
    retryDelay: retryBase,
    lastInteraction: browser.now(),
    heartbeatTimer: null as ReturnType<typeof setInterval> | null,
    interactionListeners: [] as (() => void)[],
    lifecycleListeners: [] as [
      string,
      (ev?: { persisted?: boolean; storageKey?: string | null }) => void,
    ][],
    /** Waiters blocked on the no-Web-Locks lease (P2-3). */
    leaseWaiters: new Set<() => void>(),
    leaseListenerAdded: false,
    mirror: undefined as EventMirror | undefined,
    /** Opens the mirror, then recovers events the primary lost. Awaited by flush. */
    mirrorReady: Promise.resolve(),
  };

  // Mirror: open + restore-time recovery (05-sdks.md: the mirror is used to
  // recover when localStorage lost records). Runs under the session lock so
  // it cannot interleave with a concurrent track().
  state.mirrorReady = (async () => {
    try {
      state.mirror = await browser.mirror;
      if (state.mirror) {
        await guardSession(() => recoverFromMirror(storage, state.mirror));
      }
    } catch {
      /* degraded: primary-only */
    }
  })();

  // Restore: reconcile the stored record against the highest queued seq per
  // session (a kill between session save and enqueue leaves a gap).
  const stored = loadRecord(storage);
  if (stored) {
    const queuedMax = maxQueuedSeq(storage, stored.session_id);
    if (queuedMax > stored.seq) {
      saveRecord(storage, { ...stored, seq: queuedMax });
    }
  }

  const anonId = ensureAnonId(storage, browser.now);

  // -----------------------------------------------------------------
  // Heartbeat: only while visible and interaction within 5 min, checked
  // BEFORE expiry handling inside track() so it never starts a session.
  // -----------------------------------------------------------------
  function startHeartbeat(): void {
    if (heartbeatMs <= 0 || state.heartbeatTimer) return;
    state.heartbeatTimer = setInterval(() => {
      if (state.closed || !state.consented) return;
      if (browser.visibility && !browser.visibility()) return;
      if (browser.now() - state.lastInteraction > INTERACTION_WINDOW_MS) return;
      void sdk.track("heartbeat");
    }, heartbeatMs);
    // A background cadence must not hold a host event loop open.
    if (typeof state.heartbeatTimer === "object" && "unref" in state.heartbeatTimer) {
      (state.heartbeatTimer as { unref: () => void }).unref();
    }
  }

  function startInteractionTracking(): void {
    if (typeof document !== "undefined") {
      const mark = () => (state.lastInteraction = browser.now());
      const events = ["pointerdown", "keydown", "scroll"] as const;
      for (const type of events) {
        document.addEventListener(type, mark, { passive: true, capture: true });
        state.interactionListeners.push(() =>
          document.removeEventListener(type, mark, { capture: true }),
        );
      }
      return;
    }
    // Non-DOM runtimes (React Native): the host fires a lifecycle
    // "interaction" event when the user is active (the RN wrapper forwards
    // track() calls and touch hooks to it).
    const lc = browser.lifecycle;
    if (!lc) return;
    const mark = () => (state.lastInteraction = browser.now());
    lc.add("interaction", mark);
    state.lifecycleListeners.push(["interaction", mark]);
  }

  // -----------------------------------------------------------------
  // Unload flush + bfcache resume (05-sdks.md §"Durable queue" table):
  // pagehide / hidden → immediate flush (keepalive is set in transport);
  // pageshow {persisted} → resume the queue without minting a new session.
  // -----------------------------------------------------------------
  function attachLifecycle(): void {
    const lc = browser.lifecycle;
    if (!lc) return;
    const onHide = () => void flush();
    const onShow = (ev?: { persisted?: boolean }) => {
      if (ev?.persisted) scheduleFlush();
    };
    const onVisibility = () => {
      if (browser.visibility && !browser.visibility()) onHide();
    };
    lc.add("pagehide", onHide);
    lc.add("visibilitychange", onVisibility);
    lc.add("pageshow", onShow);
    // Push, not replace: startInteractionTracking already registered the
    // "interaction" mark on this list.
    state.lifecycleListeners.push(
      ["pagehide", onHide],
      ["visibilitychange", onVisibility],
      ["pageshow", onShow],
    );
  }

  // -----------------------------------------------------------------
  // Event minting
  // -----------------------------------------------------------------
  // Within one millisecond the UUIDv7 random bits decide storage order, so
  // mints carry a monotonic counter in the first two random bytes: events
  // minted in the same track() call keep enqueue order (session_start →
  // page_view → event).
  let lastMintMs = -1;
  let mintCounter = 0;
  function mintRandom(): Uint8Array {
    const ms = browser.now();
    if (ms === lastMintMs) mintCounter += 1;
    else {
      lastMintMs = ms;
      mintCounter = 0;
    }
    const r = randomBytes();
    r[0] = (mintCounter >> 8) & 0xff;
    r[1] = mintCounter & 0xff;
    return r;
  }

  function mintEvent(
    rec: SessionRecord,
    name: string,
    props: Record<string, unknown> | undefined,
    url: string | null,
    userId: string | null,
  ): Record<string, unknown> {
    const seq = rec.seq + 1;
    rec.seq = seq;
    rec.count += 1;
    // Per-instance time strictly increasing: max(now, last event + 1).
    const tMs = Math.max(browser.now(), rec.last_event_at + 1);
    rec.last_event_at = tMs;
    // Engagement (non-heartbeat) events move last_seen — expiry follows
    // engagement, never heartbeats (D16).
    if (name !== "heartbeat") rec.last_seen_at = tMs;
    const eventId = uuidv7(browser.now(), mintRandom());
    const event: Record<string, unknown> = {
      event_id: eventId,
      session_id: rec.session_id,
      seq,
      time: new Date(tMs).toISOString(),
      name,
      props: props ?? {},
    };
    if (url !== null) event.url = url;
    if (userId !== null) event.user_id = userId;
    return event;
  }

  // -----------------------------------------------------------------
  // Session guard: Web Locks when available; otherwise the lease-based
  // leader election (P2-3). Waiters wake early on `storage` events for the
  // lease key, then poll until the small budget expires — an election that
  // still fails proceeds anyway (D17: a raced cold start yields two short
  // counted sessions, never a lost event).
  // -----------------------------------------------------------------
  function leaseSubscribe(wake: () => void): () => void {
    state.leaseWaiters.add(wake);
    if (!state.leaseListenerAdded && browser.lifecycle) {
      state.leaseListenerAdded = true;
      const onStorage = (ev?: { storageKey?: string | null }) => {
        if (!ev?.storageKey || ev.storageKey === LEASE_KEY) {
          for (const w of state.leaseWaiters) w();
        }
      };
      browser.lifecycle.add("storage", onStorage);
      state.lifecycleListeners.push(["storage", onStorage]);
    }
    return () => state.leaseWaiters.delete(wake);
  }

  function guardSession<T>(fn: () => Promise<T>): Promise<T> {
    // `locks: null` (tests) forces the lease; absent locks fall back to the
    // real navigator.locks when the runtime provides one (Node ≥18 does).
    if (browser.locks) return browser.locks.request(LOCK_NAME, fn);
    return withLease(
      storage,
      browser.now,
      { ...browser.lease, subscribe: leaseSubscribe },
      fn,
    );
  }

  /**
   * The whole locked emit path: load/rotate the session, mint, save the
   * record with FINAL counters, then persist (primary + mirror fallback).
   * Used by track() and by the pending-`sdk_dropped` report in flush().
   */
  async function emitLocked(
    name: string,
    props?: Record<string, unknown>,
  ): Promise<void> {
    await state.mirrorReady; // mirror open + recovery before any persistence
    await guardSession(async () => {
      const now = browser.now();
      const ctx = browser.page ? browser.page() : null;
      const userId = storage.get(USER_KEY);
      let rec = loadRecord(storage);
      const expired = rec ? expiryReason(rec, now) : null;

      // Heartbeat: never starts or extends a session (D16).
      if (name === "heartbeat" && (!rec || expired)) return;

      // The pending sdk_dropped report carries the cleared counter atomically
      // (same lock) — nothing dropped between read and report.
      let emitProps = props;
      if (name === "sdk_dropped") {
        const n = takeDropped(storage);
        if (n === 0) return;
        emitProps = { dropped: n, reason: "quota", ...props };
      }

      let extra: Record<string, unknown>[] = [];

      if (!rec || expired) {
        // New session: session_start then page_view; a resume after expiry
        // emits page_view {resumed:true} so every session has engagement.
        rec = {
          session_id: newSessionId(now),
          started_at: now,
          last_seen_at: now,
          last_event_at: now,
          seq: 0,
          count: 0,
          anon_id: anonId,
        };
        const resumed = expired === "idle";
        const startProps = ctx ? { ...ctx.utm } : {};
        extra = [mintEvent(rec, "session_start", startProps, null, userId ?? null)];
        if (name === "page_view") {
          extra.push(
            mintEvent(
              rec,
              "page_view",
              { ...(resumed ? { resumed: true } : {}), ...emitProps },
              ctx?.url ?? null,
              userId ?? null,
            ),
          );
        } else {
          extra.push(
            mintEvent(
              rec,
              "page_view",
              resumed ? { resumed: true } : {},
              ctx?.url ?? null,
              userId ?? null,
            ),
          );
          extra.push(mintEvent(rec, name, emitProps, ctx?.url ?? null, userId ?? null));
        }
      } else {
        // Heartbeats never extend the session: last_seen_at does not move.
        // (mintEvent advances last_event_at and, for engagement, last_seen.)
        extra = [mintEvent(rec, name, emitProps, ctx?.url ?? null, userId ?? null)];
      }

      // Save the record with FINAL counters BEFORE enqueuing anything: a kill
      // between the writes leaves a harmless seq gap, never a durable event
      // the restored record does not know about (05-sdks.md §Durable queue).
      saveRecord(storage, rec);

      // Durability: persist every minted event before the caller resolves.
      // Quota overflow drops victims (counted in sa:dropped) and, if the
      // primary still refuses, AWAITS the mirror write.
      for (const event of extra) {
        await enqueueResilient(storage, state.mirror, event as never);
      }
    });
  }

  async function track(name: string, props?: Record<string, unknown>): Promise<void> {
    if (state.closed || !state.consented) return;
    await emitLocked(name, props);
    scheduleFlush();
  }

  // -----------------------------------------------------------------
  // Flush loop
  // -----------------------------------------------------------------
  async function flush(): Promise<void> {
    if (state.flushing) return state.flushing;
    let maxEvents = MAX_EVENTS_PER_REQUEST;
    let attempt = 0;
    const run = async (): Promise<void> => {
      await state.mirrorReady; // mirror open + restore recovery happens first
      // Report accumulated quota drops before sending: the report goes
      // through the normal emit path, so normal expiry rules apply and a
      // stale session is not resurrected just to carry it (D10 rule 2).
      if (state.closed || !state.consented) return;
      if (Number(storage.get(DROPPED_KEY) ?? "0") > 0) {
        await emitLocked("sdk_dropped");
      }
      while (!state.closed && state.consented) {
        const events = readQueue(storage);
        if (events.length === 0) return;
        const plan = selectBatch(events, MAX_BATCH_BYTES, maxEvents);
        if (plan.events.length === 0) return; // defensive; selectBatch always takes one
        const body = envelope(plan.events, anonId, new Date(browser.now()).toISOString());
        const ids = plan.events.map((e) => e.event_id);
        const result = await sendBatch(
          options.endpoint,
          body,
          options.apiKey,
          ids,
          browser.fetchFn,
        );
        await dequeueManyWithMirror(storage, state.mirror, result.acked);

        if (result.kind === "auth") {
          return; // 401/403: retain the queue; the next flush retries
        }

        if (result.kind === "too_large") {
          // 413: halve the batch. A single event that is still too large can
          // never be delivered — drop it and count it like a quota drop.
          if (ids.length <= 1) {
            await dequeueManyWithMirror(storage, state.mirror, ids);
            addDropped(storage, 1);
            maxEvents = MAX_EVENTS_PER_REQUEST;
            attempt = 0;
            continue;
          }
          maxEvents = Math.max(1, Math.floor(ids.length / 2));
          continue;
        }

        if (result.kind === "ok" || result.kind === "permanent") {
          if (result.acked.length === 0 && result.retained.length > 0) {
            // The server reported nothing for these ids (pathological or a
            // lossy response). Treat it like a transient failure so the
            // attempt cap bounds the loop.
            attempt += 1;
            if (attempt >= MAX_FLUSH_ATTEMPTS) return;
            await sleep(jitterDelay(Math.min(state.retryDelay, RETRY_MAX_MS), jitterFn));
            state.retryDelay = Math.min(state.retryDelay * 2, RETRY_MAX_MS);
            continue;
          }
          state.retryDelay = retryBase;
          attempt = 0;
          continue; // progress: drain the rest of the queue
        }

        // transient: honour Retry-After, else exponential backoff with jitter
        attempt += 1;
        if (attempt >= MAX_FLUSH_ATTEMPTS) return;
        await sleep(
          jitterDelay(
            result.retryAfterMs ?? Math.min(state.retryDelay, RETRY_MAX_MS),
            jitterFn,
          ),
        );
        state.retryDelay = Math.min(state.retryDelay * 2, RETRY_MAX_MS);
      }
    };
    state.flushing = run().finally(() => {
      state.flushing = null;
    });
    return state.flushing;
  }

  function scheduleFlush(): void {
    if (state.flushTimer || state.closed) return;
    state.flushTimer = setTimeout(() => {
      state.flushTimer = null;
      void flush();
    }, debounceMs);
    // Do not hold the event loop open for a debounce.
    if (typeof state.flushTimer === "object" && "unref" in state.flushTimer) {
      (state.flushTimer as { unref: () => void }).unref();
    }
  }

  function shutdown(): void {
    state.closed = true;
    if (state.flushTimer) clearTimeout(state.flushTimer);
    if (state.heartbeatTimer) clearInterval(state.heartbeatTimer);
    state.flushTimer = null;
    state.heartbeatTimer = null;
    for (const off of state.interactionListeners) off();
    state.interactionListeners = [];
    const lc = browser.lifecycle;
    for (const [type, cb] of state.lifecycleListeners) lc?.remove(type, cb);
    state.lifecycleListeners = [];
  }

  const sdk: AnalyticsSdk = {
    track,
    identify(userId) {
      storage.set(USER_KEY, userId);
    },
    setConsent(granted) {
      state.consented = granted;
      storage.set(CONSENT_KEY, granted ? "true" : "false");
      if (!granted) {
        // stop tracking, clear queue + session, persist the choice
        for (const key of storage.keys()) {
          if (key.startsWith("sa_ev:")) storage.remove(key);
        }
        clearRecord(storage);
        storage.remove(USER_KEY);
        storage.remove(DROPPED_KEY); // drops are part of the cleared state
        shutdown();
      }
    },
    flush,
    shutdown,
    __internal: {
      session: () => loadRecord(storage),
      queued: () => readQueue(storage),
      consented: () => state.consented,
      /** Pending-but-unreported quota drop count (testing only). */
      droppedPending: () => Number(storage.get(DROPPED_KEY) ?? "0"),
      /** Resolves once the mirror is open and restore recovery ran. */
      mirrorReady: () => state.mirrorReady,
    },
  };

  startHeartbeat();
  startInteractionTracking();
  attachLifecycle();
  return sdk;
}

// ---------------------------------------------------------------------------
// Environment defaults (browser globals)
// ---------------------------------------------------------------------------

function defaultStorage(): QueueStorage {
  if (typeof localStorage === "undefined") {
    throw new Error("localStorage is not available; pass options.browser.storage");
  }
  return {
    keys: () => Object.keys(localStorage),
    get: (k) => localStorage.getItem(k),
    set: (k, v) => localStorage.setItem(k, v),
    remove: (k) => localStorage.removeItem(k),
  };
}

function defaultLocks(): Browser["locks"] {
  // Node ≥18 implements real Web Locks (single process), so tests and the
  // browser share one code path; old browsers (Safari < 15.4) have no
  // navigator.locks and fall through to the lease-based election (P2-3).
  if (typeof navigator === "undefined") return undefined;
  const locks = (navigator as { locks?: LockManagerLike | undefined }).locks;
  return locks && typeof locks.request === "function" ? locks : undefined;
}

function defaultFetch(): typeof fetch {
  if (typeof fetch === "undefined") {
    throw new Error("fetch is not available; pass options.browser.fetchFn");
  }
  return fetch;
}

function defaultPage(): Browser["page"] {
  if (typeof window === "undefined" || typeof document === "undefined") return undefined;
  const win = { location: window.location, document: { referrer: document.referrer } };
  return () => pageContext(win as never);
}

function defaultVisibility(): Browser["visibility"] {
  if (typeof document === "undefined") return undefined;
  return () => document.visibilityState === "visible";
}

function defaultLifecycle(): Browser["lifecycle"] {
  if (typeof window === "undefined" || typeof document === "undefined") return undefined;
  const handlers = new Map<string, (e: Event) => void>();
  return {
    add(type, cb) {
      const handler = (e: Event) => {
        if (type === "storage") cb({ storageKey: (e as StorageEvent).key });
        else cb({ persisted: (e as PageTransitionEvent).persisted });
      };
      handlers.set(type, handler);
      if (type === "visibilitychange") document.addEventListener(type, handler);
      else window.addEventListener(type, handler);
    },
    remove(type) {
      const handler = handlers.get(type);
      if (!handler) return;
      handlers.delete(type);
      if (type === "visibilitychange") document.removeEventListener(type, handler);
      else window.removeEventListener(type, handler);
    },
  };
}

function ensureAnonId(storage: QueueStorage, now: () => number): string {
  const existing = storage.get(ANON_KEY);
  if (existing) return existing;
  const id = uuidv7(now(), randomBytes());
  storage.set(ANON_KEY, id);
  return id;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Full jitter: uniform in [base/2, base). */
function jitterDelay(base: number, jitter: () => number): number {
  return Math.round(base / 2 + jitter() * (base / 2));
}

// Re-exports for the demo page and tests.
export { SESSION_KEY, ANON_KEY, CONSENT_KEY, USER_KEY } from "./session.js";
export {
  EVENT_PREFIX,
  readQueue,
  queueDepth,
  dropForQuota,
  isQuotaError,
  addDropped,
  takeDropped,
} from "./queue.js";
export { openMirror } from "./mirror.js";
export type { EventMirror } from "./mirror.js";
export { expiryReason } from "./session.js";
export { loadRecord } from "./session.js";
export { DROPPED_KEY } from "./queue.js";
export type { SessionRecord } from "./session.js";
export type { QueuedEvent } from "./queue.js";
export type { QueueStorage } from "./queue.js";
export type { PageContext } from "./context.js";
