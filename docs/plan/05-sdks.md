# 05 — SDKs

One ingestion contract, three delivery clients. The **server SDK**
(`@session-analytics/sdk-server`) is the primary path (Notion Rev 5 §19): the
application backend owns the session and emits authoritative events. The web and React
Native SDKs are **secondary** enrichment clients; they share the same payload and outcome
handling, and differ in runtime, session ownership and durability.

## Shared behaviour (what "correct" means for any SDK)

| Rule                 | Requirement                                                                                                                                                                                                                 |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Session start        | Client SDKs: `session_start` then `page_view`; a resume after expiry emits `page_view {resumed:true}`. Server SDK: `session_start` only when bootstrap returns `emitted_events === 0`, with the persisted attribution props |
| Continue             | any event within 30 min of the last one; each engagement event moves `last_seen` forward                                                                                                                                    |
| Local expiry         | idle > 30 min, age > 24 h, or 5,000 events → new session ID                                                                                                                                                                 |
| Ordering             | Client SDKs: per-session `seq` increments and per-instance time is strictly increasing (`max(now, last+1)`). Server SDK: `seq` is **omitted** (D31) and ordering falls back to `(time, event_id)`                           |
| Time                 | Client events are skew-corrected by the API; server/webhook events keep the producer's time (D32). `received_at` is always stored                                                                                           |
| Heartbeat            | every 30 s, **only** while visible/foreground and interaction within 5 min; checked **before** expiry handling, so it never starts or extends a session ([DEVIATIONS.md](DEVIATIONS.md) D16)                                |
| Durability           | Client SDKs: event is persisted before `track()` resolves. Server SDK: in-memory buffer; use a transactional outbox and call `track()` after commit                                                                         |
| Retry                | only transient failures retry; per-event rejections are permanent and removed                                                                                                                                               |
| Overflow             | Drop heartbeats first, then oldest; count and report in `sdk_dropped`                                                                                                                                                       |
| Consent              | `setConsent(false)` stops tracking, clears queue + session, persists the choice (client SDKs)                                                                                                                               |
| No headless sessions | Client SDKs: push handlers, background fetch and headless tasks must not call `track()`                                                                                                                                     |
| Provenance           | Every event carries `source`; a public client key may claim only `client` (D28)                                                                                                                                             |

## Server SDK (`packages/sdk-server`)

```ts
import { ServerAnalytics } from "@session-analytics/sdk-server";

const analytics = new ServerAnalytics({
  endpoint: "https://ingest.example.com",
  apiKey: process.env.ANALYTICS_SERVER_KEY, // kind = 'server'
});

const session = await analytics.startSession({ userId, properties: { device, utm } });
// propagate session.context as X-Analytics-Session on the backend's own API

analytics.track("order_placed", { props: { order_id, amount } });
analytics.track("payment_succeeded", {
  // sessionless webhook
  sessionId: null,
  source: "webhook",
  eventId: providerEventId, // producer idempotency key
  props: { provider: "stripe" },
});
await analytics.flush();
```

| Concern         | Behaviour                                                                                                                                                                                                                                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Bootstrap       | `startSession()` → `POST /v1/sessions`; stores the active session and its `emitted_events`. `validateSession(context)` checks a propagated context                                                                                                                       |
| Session binding | `track()` defaults to the active bootstrapped session; `sessionId: null` makes the event sessionless. A session-bound event without an `anonId` is an SDK error before any network call                                                                                  |
| `session_start` | Emitted automatically once, only when the session has no stored events yet, carrying the bootstrap `properties` as attribution                                                                                                                                           |
| `seq`           | Never minted (D31): a backend cannot know the browser's next sequence number in the same session                                                                                                                                                                         |
| Event ids       | UUIDv7, with a same-millisecond counter so ids stay in mint order. Pass `eventId` when the producer has its own idempotency key; the DB deduplicates re-delivery                                                                                                         |
| Batching        | Up to 500 events and 256 KB per request; `flush()` drains the buffer                                                                                                                                                                                                     |
| Retry matrix    | 200 → dequeue accepted ∪ rejected; 400 → drop the batch; 401/403 → retain and stop; 413 → halve (a single event that still exceeds the server limit is dropped and counted for `sdk_dropped`); 429 → honour `Retry-After`; 5xx/network → exponential backoff with jitter |

The retry matrix is shared between both SDKs (P2-2): at most **8 attempts per
`flush()` call** (`attempt >= MAX_FLUSH_ATTEMPTS`), then the queue is left for
the next call; a 200 that acknowledges nothing is bounded exactly like a
transient failure. Backoff is **full jitter** — uniform in `[base/2, base)` —
from a base of 1 s (`retryBaseMs` option; `browser.jitter` overrides the jitter
source in tests), doubling to a 30 s ceiling; a successful send resets the
schedule to the base, and a server `Retry-After` overrides the local schedule
for that one attempt.
| Shutdown | `shutdown()` stops the timer and performs a final drain; `track()` afterwards throws `closed` |
| Observability | `pending()`, `session()`, `authError()`, and an optional structured `logger` |

The SDK does not import `@session-analytics/shared` at runtime (types and constants are
duplicated), so it builds before the shared package and ships with no runtime
dependencies. G1B runs it against the real ingest app and Postgres:
`services/ingest/test/server-sdk-e2e.test.mjs`.

## Web SDK (`packages/sdk-web`) — secondary

Blueprint section 5 is the contract, but the PDF's snippet is incomplete: `enqueue()` is
referenced and never defined, there is no durable queue, no quota handling, no unload
path and no consent API. The implementation fills those gaps ([DEVIATIONS.md](DEVIATIONS.md) D10).

### Modules

| File           | Responsibility                                                                        |
| -------------- | ------------------------------------------------------------------------------------- |
| `session.ts`   | Stored session record, `withLock()` over Web Locks, expiry, `emit()`                  |
| `queue.ts`     | Durable queue: `localStorage` primary, IndexedDB mirror, quota errors → `sdk_dropped` |
| `transport.ts` | `fetch` with `keepalive`, batch selection, per-status handling, backoff with jitter   |
| `context.ts`   | URL, referrer, UTM, device hints                                                      |
| `index.ts`     | `init`, `track`, `identify`, `setConsent`, `flush`                                    |

### Durable queue

One record per event, `sa_ev:<event_id>`, written **before** `track()` resolves.
`localStorage` is synchronous and good enough for ~5 MB; an IndexedDB mirror is written
asynchronously and is used to recover when `localStorage` throws `QuotaExceededError`.
When the primary throws, `track()` awaits the mirror write before resolving, otherwise
"persisted before resolve" would be false on the fallback path.

`emit()` saves the session record **before** enqueuing the event. In the reverse order, a
kill between the two writes would leave a durable event whose `seq`/`lastAt` the restored
record does not know about; saving first means a crash can only leave a harmless gap in
`seq`. On restore the SDK also reconciles counters against the highest queued `seq` per
session.

| Situation                    | Behaviour                                                                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Quota exceeded on write      | Drop heartbeats first, then oldest events; count in `sdk_dropped`; retry the mirror                                                                                                                    |
| Page unload                  | `visibilitychange`/`pagehide` → `flush({keepalive:true})`; batches ≤ 60 KB so `keepalive` is honoured                                                                                                  |
| Tab restored from bfcache    | Resume via `pageshow` (persisted) without minting a new session                                                                                                                                        |
| Two tabs                     | One stored session record under `navigator.locks.request('sa_session', …)`, so `seq` and time are shared                                                                                               |
| No Web Locks (Safari < 15.4) | Unlocked fallback with a storage-event leader election; if a race still produces a second session, it is counted as a short separate session, not silently merged ([DEVIATIONS.md](DEVIATIONS.md) D17) |

### Multi-tab correctness

`lastAt` and the counters live in the stored record, not in tab memory, so two tabs
cannot emit the same time or the same `seq`. Every mutation happens inside one function,
inside the lock.

## React Native SDK (`packages/sdk-react-native`) — secondary

Blueprint section 6 is close to complete; the plan keeps its structure and fixes four
gaps ([DEVIATIONS.md](DEVIATIONS.md) D10):

1. **Flush cadence includes the size trigger.** `put()` calls `flush()` when the queue
   reaches 100, not only on the 5 s interval.
2. **`sdk_dropped` is reported even when the session is gone.** The PDF guards the report
   with `&& sess`, so drops after an idle expiry would never be reported; report them on
   the next flush through `run()`, so normal expiry rules apply and a stale session is
   not resurrected just to carry the drop event.
3. **`onDeepLink` persists the forced expiry** (`save()`), so a process kill between the
   deep link and the next event cannot resurrect the old session.
4. **`identify()` is persisted** and restored in `init()`, so events emitted after a cold
   start carry the `user_id` instead of `null`.

### State machine

```
uninitialized ──init()──► initializing ──► ready ──shutdown()──► shutting_down ──► uninitialized
                              │                                              ▲
                              └────────── shutdown() raced with init ────────┘
```

- `track()` before `ready` is buffered (≤ 200) with its original timestamp.
- `init()` is idempotent while ready and returns the cached promise; after `shutdown()`
  the promise is cleared, so a second `init()` with new credentials is honoured.
- `shutdown()` bumps `gen`, removes listeners/timers, drains `flushP`, ignores results
  from a newer generation, and resets every field so `init()` can be called again.
- Restore order on `init()`: anon ID → session → dropped counters → queue (sorted by key)
  → deep link → `ready` → replay buffered calls. Nothing can be overwritten.

### Durability statement (exact, as in the PDF)

Durable means: MMKV wrote the event to a memory-mapped file, which survives the app being
killed. It does **not** survive the device losing power before the OS flushes. That is the
guarantee we document, no more.

## Conformance suite (both SDKs)

A shared test suite (`packages/shared/test/conformance`) with a fake clock and a fake
transport. Both SDKs must pass the same cases:

| Case                                  | Assertion                                                                                           |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Idle expiry at 31 min                 | New session ID, `session_start` + `page_view`, `seq` restarts at 1                                  |
| Gap at 29 min                         | Same session                                                                                        |
| 24 h age                              | New session even with continuous events                                                             |
| 5,000 events                          | Rotation, no event lost                                                                             |
| Heartbeat while hidden                | Nothing emitted, and no session is created                                                          |
| Heartbeat after 6 min idle            | Nothing emitted, session not extended                                                               |
| Batch with one invalid event          | Valid events stored, invalid removed, counts identical                                              |
| Replay the same batch twice           | Identical stored set, no duplicates                                                                 |
| Kill mid-flush                        | After restart, queue contains everything unacknowledged                                             |
| Kill between session save and enqueue | Restored counters are reconciled against the highest queued `seq`; no duplicate `seq` after restart |
| 401 then 200                          | Queue preserved, then drained                                                                       |
| 400                                   | Batch dropped and counted once, no loop                                                             |
| Overflow                              | Heartbeats dropped first, then oldest, `sdk_dropped` counts match                                   |
| Consent off                           | Queue and session cleared, nothing sent                                                             |
| `init()` twice                        | Idempotent, one transport loop                                                                      |

## Packaging

- `sdk-web`: ESM + UMD bundle, no runtime dependencies, `sideEffects: false`, size budget
  12 KB gzipped. The shared package contributes types plus a small generated validator,
  not zod (zod alone exceeds the budget).
- `sdk-react-native`: peer deps `react-native-mmkv`, `@react-native-community/netinfo`,
  `react-native-device-info`, `react-native-get-random-values` (must be imported first),
  `uuidv7`. The `index.js` wiring awaits `init()` before
  `AppRegistry.registerComponent`.

## API key handling

The key ships in the bundle/binary. It is public and **write-only**: `app_ingest` holds
only `EXECUTE` on `ingest_batch()`, which writes the rows of the tenant resolved from the
key and returns per-event outcomes — the key can never read events or sessions, not even
its own tenant's. The dashboard uses separate credentials. Origin/bundle-ID allow-lists
are hints, not security boundaries; CORS is not a boundary either.
