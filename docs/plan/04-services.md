# 04 — Services (Express)

Two services, one shared package. TypeScript, Node 24, Express 5, `pg` (node-postgres)
through PgBouncer in transaction mode.

```
packages/shared  →  contract types, zod schemas, canonical JSON + payload_hash,
                    event category table, error codes, session bootstrap wire types
packages/sdk-server → application-backend client: bootstrap/resume, track, flush
packages/sdk-web →  (secondary) browser client: durable queue, session, transport
services/ingest  →  POST /v1/events, POST /v1/sessions[/validate]  (write path)
services/dashboard → GET /v1/...       (read path)
```

## Shared package (`packages/shared`)

| Export | Purpose |
| --- | --- |
| `EventSchema` (zod) | Single definition of the wire contract for the API; SDKs consume the generated types, and the web bundle uses a tiny generated validator rather than zod |
| `CATEGORY` map | `engagement \| lifecycle \| system` per reserved name; everything else is `e` |
| `canonicalJson(value)` | Deterministic serialisation: sorted keys, no whitespace, undefined dropped |
| `payloadHash(event)` | `sha256(canonicalJson({session_id, name, url, props, time, seq}))` using the **producer's original** time, computed before any correction. `user_id`, `anon_id`, `source` and `cat` are deliberately excluded, so a retry after `identify()` keeps the stored event's first-wins attribution, and re-delivering the same event from another SDK does not become a conflict; state this in the SDK docs |
| `SCHEMA_VERSION` | `1`; accept current and previous |
| `rejectReason` enum | `invalid_uuid, missing_anon_id, too_old, too_new, bad_name, props_too_large, schema_unsupported, session_capped, conflicting_duplicate` |
| `SOURCES` / `SOURCES_BY_KEY_KIND` | `client \| server \| webhook` per event, and the sources each API-key kind may submit ([DEVIATIONS.md](DEVIATIONS.md) D28) |
| `SessionStartRequestSchema` / `SessionValidateRequestSchema` | Wire contract for bootstrap/resume and context validation ([DEVIATIONS.md](DEVIATIONS.md) D30) |

`canonicalJson` is non-negotiable: `JSON.stringify` is key-order dependent, so a retry
built from a re-parsed object would hash differently and be rejected as a conflict
([DEVIATIONS.md](DEVIATIONS.md) D7).

SDKs import **types** only; the browser runtime validator is a small generated function
(or zod is externalised), because zod alone exceeds the 12 KB web budget
([05-sdks.md](05-sdks.md)).

## Ingest API

### Request lifecycle

1. **Authenticate**: `X-API-Key` → `sha256` → `api_keys` lookup (cached, 60 s TTL,
   invalidated on revoke in the same instance) → `tenant_id` **and** key kind
   (`ingest` = client, `server` = backend, `dashboard` never ingests). Revocation is
   therefore effective within 60 s across instances; that is what the customer-facing key
   policy states. A tenant ID in the body is ignored.
2. **Validate** the envelope (`schema_version`, optional `anon_id`, `sent_at`, `events`
   ≤ 500, body ≤ 1 MB) and each event (UUIDs, name pattern, props ≤ 4 KB / 20 keys /
   depth 2, time window). `session_id` is optional: machine-originated events are
   sessionless; a session-bound event without an `anon_id` (per event, else envelope) is
   rejected as `missing_anon_id` ([DEVIATIONS.md](DEVIATIONS.md) D29). Validation
   failures never reach Postgres.
3. **Authorize provenance**: `source` defaults to `client`. A client key may submit only
   `client`; a server key may submit `client`, `server` or `webhook`; a violation is
   `403 source_not_allowed` for the whole batch ([DEVIATIONS.md](DEVIATIONS.md) D28).
4. **Time**: `source = client` is skew-corrected with
   `t' = t + (server_received_at - sent_at)` and `t'` is validated. `server`/`webhook`
   keep the producer's `time` as observed (a webhook must not be shifted by delivery
   delay); the same 24 h / 5 min window applies. `received_at` is always stored
   ([DEVIATIONS.md](DEVIATIONS.md) D32).
5. **Hash** each event with `payloadHash` (original fields), then remove in-batch
   duplicates: same hash → keep one; different hash → keep the first by request order,
   reject the rest as `conflicting_duplicate`. Survivors get `ord`.
6. **Enrich**: classify `cat`; derive `device` and `country` from UA/IP (or
   `props.platform` for mobile); attach `utm` from the `session_start` props.
7. **Buffer** and flush.

### Session bootstrap / resume

Bootstrap is a separate operation from ingestion and creates **no analytics rows**: a
session becomes visible in `sessions_active` only when its first event arrives
([DEVIATIONS.md](DEVIATIONS.md) D30). Both routes require a `server` key.

| Method | Path | Body | Response |
| --- | --- | --- | --- |
| `POST` | `/v1/sessions` | `{ anon_id?, user_id?, properties? }` | `{ session_id, anon_id, user_id, started_at, last_event_at, expires_at, resumed, emitted_events, properties, context }` |
| `POST` | `/v1/sessions/validate` | `{ context }` | `{ valid, reason?, session_id?, anon_id?, user_id?, expires_at?, emitted_events?, properties? }` |

- `context` is an opaque HMAC token `v1.<session_id>.<exp_seconds>.<sig>` bound to the
  tenant. A raw session id is a correlation identifier, not a credential; the signature
  is what makes validation meaningful. Propagate it as `X-Analytics-Session` on the
  application backend's own requests.
- Resume rules: idle > 30 min (`last_event_at`), age > 24 h (`started_at`), or a
  different known `user_id` → rotate. Concurrent bootstraps for one visitor serialize on
  an advisory lock and return one session.
- `emitted_events` counts stored events for the session, so a server SDK emits
  `session_start` only when it is `0`.
- Validation checks the signature first, then the **live** idle clock in the database,
  so an idled-out session is rejected without reissuing tokens per request.

### Buffering and response semantics

The important design point: **an HTTP response is only written after the batch that
contains its events commits.** That is what makes "acknowledge only after commit" true.

```
buffer: Array<{ event, waiter: {resolve, reject} }>
flush():  take up to `MAX_BATCH` (500) items; one `ingest_batch()` call; map outcomes
          back to waiters; resolve 200 per request with its own per-event outcomes;
          on DB error, reject every waiter in the batch → 503
```

Rules that are not optional:

- A request is enqueued atomically and its waiter is resolved only when **all** of its
  events have outcomes. If `MAX_BATCH` splits a request across two flushes, the waiter
  aggregates both results before replying.
- Backpressure does not un-buffer: events already in the buffer stay there (dedupe makes
  the client's retry safe) and the request that trips `BUFFER_MAX` gets the 503.

| Rule | Value |
| --- | --- |
| Flush trigger | buffer size ≥ 500 events; the 1 s timer is the ceiling; adaptive idle flush (`FLUSH_IDLE_MS`, default 50 ms) when no flush is in flight |
| Concurrency | At most **one** `ingest_batch()` in flight; the timer does not stack flushes |
| Idle latency | ~50 ms typically, ≤ ~1 s with a stuck flush. Documented; dashboards tolerate it |
| Batch mixing | One call may contain several tenants (the definer function writes cross-tenant); tenant comes from each event row. One slow guard or bad row blocks every tenant in that flush — accepted head-of-line risk, measured in P4-6 |
| Backpressure | If the buffer exceeds `BUFFER_MAX` (default 5,000), respond 503 immediately so SDKs back off |
| Retry-After | On 503, set `Retry-After: 5` |

Per-request outcome assembly: a request's events are matched by `event_id` against
`stored`, `dups`, `capped`, `conflicts`; anything rejected at validation is merged in and
**every submitted ID appears in exactly one of `accepted` / `rejected`**. An ID the
function did not report is treated as an accepted duplicate (the defensive half of the
cross-session race fix).

### Endpoints

| Method | Path | Notes |
| --- | --- | --- |
| `POST` | `/v1/events` | Contract below |
| `POST` | `/v1/sessions` | Bootstrap/resume; `server` key only, no analytics rows |
| `POST` | `/v1/sessions/validate` | Validate a propagated `X-Analytics-Session` context; `server` key only |
| `GET` | `/healthz` | Liveness, no DB |
| `GET` | `/readyz` | DB round-trip through PgBouncer |
| `GET` | `/metrics` | Prometheus text, internal only: private port or IP allow-list, never routed by the public LB |

```jsonc
// 200
{ "accepted": ["018f2c…a1", "018f2c…a2"],
  "rejected": [ { "event_id": "018f2c…a3", "reason": "too_old" },
                { "event_id": "018f2c…a4", "reason": "session_capped" } ] }
```

| Status | Meaning | SDK |
| --- | --- | --- |
| 200 | Per-event outcomes (an all-rejected batch is still 200) | Remove IDs in either list |
| 400 | Malformed (SDK bug) | Drop batch, count `bad_request`, do not loop |
| 401/403 | Key invalid/revoked; 403 also `source_not_allowed` / `server_key_required` | Keep queue, retry in 10 min; an authenticated producer cannot fix its key kind, so a 403 source violation is permanent |
| 413 | Body too large | Halve batch, retry; if one event still exceeds 1 MB, reject that event permanently |
| 429/5xx/network | Transient | Exponential backoff, honour `Retry-After` |

### Abuse protection (P4, wired as middleware from P1)

Per-key, per-session and per-IP rate limits; per-tenant daily quota. Per-session and per-IP
limits use an in-process token bucket (the LB is consistent-hash, so one visitor maps to
one instance). Per-key and per-tenant limits are **not** enforceable in-process — the
effective limit is N× the configured value — so they are enforced in Postgres (a counter
updated by `ingest_batch()`) or explicitly documented as lenient until that exists. Plus
event-name cardinality alerts, two active keys per tenant, and an `origin`/bundle-ID
allow-list as a hint only.

### Config

`DATABASE_URL` (→ PgBouncer), `PORT`, `FLUSH_INTERVAL_MS=1000`, `FLUSH_IDLE_MS=50`,
`MAX_BATCH=500`, `BUFFER_MAX=5000`, `SESSION_CONTEXT_SECRET` (≥16 chars; bootstrap
routes stay unmounted when unset), `LOG_LEVEL`, `OTEL_EXPORTER_OTLP_ENDPOINT`. No
secrets in the image.

## Dashboard API

Read-only, tenant-scoped, runs as `app_dashboard` (no `BYPASSRLS`), and every request
runs `SELECT set_config('app.tenant_id', $1, true)` (transaction-local, no string
interpolation) inside its transaction. Endpoints map 1:1 to the blueprint's example
queries so the SQL is reviewable in one place:

| Endpoint | Source | Blueprint example |
| --- | --- | --- |
| `GET /v1/sessions/:id/replay` | `sessions` + `events` | Example 1 |
| `GET /v1/metrics/hourly?from&to` | `session_stats_15m` ∪ live slice | Example 2 |
| `GET /v1/metrics/daily?from&to&tz` | `session_stats_15m` / `identity_15m`, summed into local days | Example 3 |
| `GET /v1/entry-pages?from&to` | `sessions` + `events` | Example 4 |
| `POST /v1/funnels` (steps in body) | `events`, ordered sequence | Example 5 |
| `GET /v1/live` | `sessions_active` with `touch_bucket` range + 5-minute `last_seen_at`, excluding sessions that already have history | Example 6 |

Rules that hold for every endpoint:

1. Never read raw `events` for a headline metric (only replay, entry-page conversion and
   funnels, which are drill-downs and are documented as such).
2. Every history query filters on `home_day` as well as `started_at`, so Timescale prunes
   chunks.
3. `tz` comes from `tenant_settings` unless the caller is allowed to override; day
   boundaries use `time_bucket('1 day', bucket, $tz)`.
4. Visitor and user counts are returned with `"approximate": true` (HLL, ~1.6% error).
5. p95 target under 500 ms with ingest at peak; measured in P4.

## Observability

| Layer | Signals |
| --- | --- |
| Ingest | buffer depth, flush p95, 5xx rate, rejections by reason, `dedupe_conflict`, `sdk_dropped` totals, in-flight DB calls |
| DB | commit latency incl. standby, WAL MB/s, lock waits on `session_guard`/`sessions_active`, replication lag |
| Jobs | `timescaledb_information.job_stats` failures, archive backlog, reconcile drift warnings, dedupe/guard row counts and oldest age |
| Data quality | events without a guard row, sessions with zero engagement, late-partial rate, dedupe rows without events (I6) |

Structured JSON logs with `tenant_id`, `batch_size`, `outcome_counts`, `duration_ms`.
Trace context propagated from the SDK (`traceparent` is optional in v1; add in P2).
