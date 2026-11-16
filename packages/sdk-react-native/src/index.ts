// React Native SDK (P2-4, docs/plan/05-sdks.md §"React Native SDK").
// Wraps the web core (D35): the core already owns every session/queue/retry/
// quota semantic the shared conformance suite checks; this module adapts it to
// RN — MMKV storage, AppState lifecycle, deep links — and adds the RN-only
// machinery: the init state machine, the generation token, early-call
// buffering, size-triggered flush and the persisted deep-link expiry.
//
// State machine (Ref 6): uninitialized → initializing → ready →
// shutting_down → uninitialized. init() is idempotent.
//
// Generation token (Ref 6 T18): every async unit (an init() awaiting
// getInitialURL, the buffered replay) captures the generation and re-checks it
// after every await. shutdown() and re-init bump it, so a stale continuation
// can never mutate the new instance.

import {
  init as initWeb,
  SESSION_KEY,
  loadRecord,
  readQueue,
  DROPPED_KEY,
  type AnalyticsSdk,
  type PageContext,
  type QueueStorage,
  type SessionRecord,
} from "@session-analytics/sdk-web";

export type RnState = "uninitialized" | "initializing" | "ready" | "shutting_down";

/** Minimal MMKV shape (react-native-mmkv). */
export interface MmkvLike {
  getString(key: string): string | undefined;
  set(key: string, value: string): void;
  delete(key: string): void;
  contains(key: string): boolean;
  getAllKeys(): string[];
}

/** Minimal AppState shape (react-native). */
export interface AppStateLike {
  currentState: string; // "active" | "background" | …
  addEventListener(type: "change", cb: (state: string) => void): () => void;
}

/** Deep-link source: initial URL + cold/warm url events. */
export interface DeepLinkSource {
  getInitialURL(): Promise<string | null>;
  addEventListener(type: "url", cb: (url: string) => void): () => void;
}

export interface RnInitOptions {
  apiKey: string;
  endpoint: string;
  /** The MMKV instance; events are durable before track() resolves. */
  storage: MmkvLike;
  /** AppState; backgrounding flushes immediately and stops heartbeats. */
  appState?: AppStateLike;
  deepLinks?: DeepLinkSource;
  fetchFn?: typeof fetch;
  now?: () => number;
  /** Current screen path, used as the event `url` (no deep link pending). */
  screen?: () => string | null;
  /** Heartbeat cadence; 0 disables. Default 30 s. */
  heartbeatMs?: number;
  /** Flush cadence. Default 5 s (Ref 6). */
  flushMs?: number;
  retryBaseMs?: number;
  /**
   * Headless instances (push handlers, background fetch) never track: every
   * method is a no-op and nothing is written (Ref 6 §"Headless work").
   */
  headless?: boolean;
}

export interface RnSdk {
  track(name: string, props?: Record<string, unknown>): Promise<void>;
  identify(userId: string): void;
  setConsent(granted: boolean): void;
  flush(): Promise<void>;
  /** Stop timers/listeners; bump the generation so stale async work dies. */
  shutdown(): void;
  /** Handle a deep link manually (a DeepLinkSource is wired automatically). */
  onDeepLink(url: string): void;
  state(): RnState;
  __internal: {
    session(): SessionRecord | null;
    queued(): { event_id: string }[];
    droppedPending(): number;
    /** The wrapped web core (introspection/tests). */
    core(): AnalyticsSdk | null;
  };
}

/** Up to 200 early track() calls are buffered until init() completes (Ref 6). */
const BUFFER_LIMIT = 200;
/** D10 rule 1: the queue reaching 100 events triggers a flush. */
const SIZE_TRIGGER = 100;

/** Parse utm parameters and the path out of a deep-link URL. */
export function parseDeepLink(raw: string): {
  url: string | null;
  utm: Record<string, string>;
} {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return { url: null, utm: {} };
  }
  const utm: Record<string, string> = {};
  for (const [k, v] of url.searchParams) {
    if (k.startsWith("utm_") && v) utm[k] = v;
  }
  return { url: url.pathname || "/", utm };
}

export function initRn(options: RnInitOptions): RnSdk {
  const clock = { now: options.now ?? Date.now };
  // Replay clock: buffered early calls carry their ORIGINAL timestamps, so
  // during replay now() is pinned to the buffered moment.
  let pinnedNow: number | null = null;
  const now = () => pinnedNow ?? clock.now();

  const storage: QueueStorage = {
    keys: () => options.storage.getAllKeys(),
    get: (k) => options.storage.getString(k) ?? null,
    set: (k, v) => options.storage.set(k, v),
    remove: (k) => options.storage.delete(k),
  };

  let state: RnState = "uninitialized";
  let generation = 0;
  let core: AnalyticsSdk | null = null;
  let offs: (() => void)[] = [];
  let pendingCampaign: { url: string | null; utm: Record<string, string> } | null = null;
  let pendingIdentify: string | null = null;
  /** Resolves when the current init lands; early track() calls chain on it. */
  let ready: Promise<void> = Promise.resolve();
  /** Resolves the current `ready` gate (also on shutdown, so no caller wedges). */
  let unblockReady: () => void = () => {};
  /** Serializes the replay of early calls so pinned timestamps don't interleave. */
  let replayTail: Promise<void> = Promise.resolve();
  let bufferedCount = 0;

  const noop = async () => {};

  if (options.headless) {
    // Headless instance: nothing is written, nothing is sent, init is a no-op.
    return {
      track: noop,
      identify: () => {},
      setConsent: () => {},
      flush: noop,
      shutdown: () => {},
      onDeepLink: () => {},
      state: () => "uninitialized",
      __internal: {
        session: () => null,
        queued: () => [],
        droppedPending: () => 0,
        core: () => null,
      },
    };
  }

  /**
   * Lifecycle source fed by AppState and the wrapper's interaction marks. The
   * web core maps visibilitychange-to-hidden onto an immediate flush.
   */
  const lifecycle = {
    listeners: new Map<string, (ev?: { persisted?: boolean }) => void>(),
    add(type: string, cb: (ev?: { persisted?: boolean }) => void) {
      this.listeners.set(type, cb);
    },
    remove(type: string) {
      this.listeners.delete(type);
    },
    fire(type: string, ev?: { persisted?: boolean }) {
      this.listeners.get(type)?.(ev);
    },
  };

  function page(): PageContext {
    if (pendingCampaign) {
      return { url: pendingCampaign.url, referrer: null, utm: pendingCampaign.utm };
    }
    return { url: options.screen?.() ?? null, referrer: null, utm: {} };
  }

  /**
   * D10 rule 3: a deep link FORCES expiry, persisted immediately — a process
   * kill between the deep link and the next event cannot resurrect the old
   * session. The campaign becomes the next session's attribution and is
   * cleared after one emit, so a stale URL never leaks (T18).
   */
  function onDeepLink(raw: string): void {
    const g = generation; // capture: a shutdown mid-parse must not apply it
    const { url, utm } = parseDeepLink(raw);
    if (g !== generation) return;
    options.storage.delete(SESSION_KEY); // persisted forced expiry
    pendingCampaign = { url, utm };
  }

  async function runInit(g: number): Promise<void> {
    // The canonical race (Ref 6 T18): init() awaits getInitialURL; shutdown()
    // or a re-init while it is pending must make this continuation dead.
    let initialUrl: string | null = null;
    if (options.deepLinks) {
      initialUrl = await options.deepLinks.getInitialURL().catch(() => null);
      if (g !== generation) return; // stale init: never touch the new state
    }

    pendingCampaign = null;
    core = initWeb({
      apiKey: options.apiKey,
      endpoint: options.endpoint,
      heartbeatMs: options.heartbeatMs ?? 30_000,
      flushDebounceMs: options.flushMs ?? 5_000,
      retryBaseMs: options.retryBaseMs,
      browser: {
        storage,
        locks: null, // no Web Locks on RN; the lease mutex is single-context
        fetchFn: options.fetchFn,
        now,
        page,
        visibility: () => !options.appState || options.appState.currentState === "active",
        lifecycle,
      },
    });

    offs.forEach((off) => off());
    offs = [];
    if (options.appState) {
      // Backgrounding flushes immediately (the core maps a hidden
      // visibilitychange onto a flush) and stops heartbeats; foregrounding
      // resumes them.
      const off = options.appState.addEventListener("change", (s) => {
        if (g !== generation) return;
        lifecycle.fire("visibilitychange");
        if (s !== "active") void core?.flush();
      });
      offs.push(off);
    }
    if (options.deepLinks) {
      const off = options.deepLinks.addEventListener("url", (u) => onDeepLink(u));
      offs.push(off);
      if (initialUrl) onDeepLink(initialUrl);
    }
    state = "ready";
    // D10 rule 4: an identify() before init completes is persisted now.
    if (pendingIdentify) {
      core.identify(pendingIdentify);
      pendingIdentify = null;
    }
    // Early track() calls chained on `ready` run from here on; each pins
    // now() to its original capture time.
  }

  function init(): Promise<void> {
    if (state === "initializing" || state === "ready") return ready; // idempotent
    generation += 1; // invalidate every prior async unit of this instance
    bufferedCount = 0;
    state = "initializing";
    // The gate: early track()/flush() chain on it. runInit()'s end AND
    // shutdown() resolve it, so a stuck getInitialURL can never wedge callers.
    ready = new Promise<void>((resolve) => {
      unblockReady = resolve;
    });
    replayTail = ready; // early replays wait for the gate, in submission order
    void runInit(generation)
      .catch(() => {
        if (state === "initializing") state = "uninitialized";
      })
      .then(unblockReady);
    return ready;
  }

  async function trackThroughCore(
    name: string,
    props?: Record<string, unknown>,
  ): Promise<void> {
    if (!core) return;
    const campaign = pendingCampaign;
    if (name !== "heartbeat") {
      // Interaction proxy: a user-driven event marks recent activity for the
      // heartbeat eligibility window.
      lifecycle.fire("interaction");
    }
    try {
      await core.track(name, props);
    } finally {
      if (campaign) pendingCampaign = null; // one emit consumes the campaign
    }
  }

  async function track(name: string, props?: Record<string, unknown>): Promise<void> {
    if (!core || state !== "ready") {
      // Early call: buffer it (up to 200) with its ORIGINAL timestamp, then
      // resolve only once the event is durably queued after init.
      if (bufferedCount >= BUFFER_LIMIT) return;
      bufferedCount += 1;
      const at = clock.now();
      const g = generation;
      // Serialize the replay: each call pins now() for exactly its own event,
      // so concurrent replays must not interleave the shared pinnedNow.
      const p = replayTail.then(async () => {
        if (g !== generation || state !== "ready" || !core) return;
        pinnedNow = at;
        try {
          await trackThroughCore(name, props);
        } finally {
          pinnedNow = null;
        }
      });
      replayTail = p;
      return p;
    }
    await trackThroughCore(name, props);
    // D10 rule 1: the size trigger flushes at 100 queued events.
    if (core.__internal.queued().length >= SIZE_TRIGGER) void core.flush();
  }

  function identify(userId: string): void {
    if (!core || state !== "ready") {
      pendingIdentify = userId; // D10 rule 4: applied as soon as init lands
      return;
    }
    core.identify(userId);
  }

  function setConsent(granted: boolean): void {
    core?.setConsent(granted);
  }

  async function flush(): Promise<void> {
    if (state === "shutting_down") return;
    const g = generation;
    await ready; // an early flush lands after init, not as a silent no-op
    if (g !== generation || state !== "ready" || !core) return;
    await core.flush();
  }

  function shutdown(): void {
    generation += 1; // every in-flight continuation of this instance dies
    state = "shutting_down";
    unblockReady(); // early track()/flush() chains must not wedge on it
    for (const off of offs) off();
    offs = [];
    core?.shutdown();
    core = null;
    state = "uninitialized";
  }

  void init(); // kick off; track() chains on it with original timestamps

  return {
    track,
    identify,
    setConsent,
    flush,
    shutdown,
    onDeepLink,
    state: () => state,
    __internal: {
      // Pre-ready reads go straight to storage: after a kill the new init is
      // still landing, but the durable state must be inspectable immediately.
      session: () => core?.__internal.session() ?? loadRecord(storage),
      queued: () => core?.__internal.queued() ?? readQueue(storage),
      droppedPending: () =>
        core?.__internal.droppedPending() ?? Number(storage.get(DROPPED_KEY) ?? "0"),
      core: () => core,
    },
  };
}
