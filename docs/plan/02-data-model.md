# 02 — Data model

Forward-only migrations under `db/migrations/`. Every table, index and policy below is
created by one of these files; nothing is created by hand.

## Migration list (P0 scope, extended later)

| File                        | Creates                                                                                                                   | Phase |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ----- |
| `0001_extensions.sql`       | `timescaledb`, `timescaledb_toolkit`, `pg_stat_statements` (where available)                                              | P0    |
| `0002_tenancy.sql`          | `tenants`, `api_keys`, `tenant_settings`, `identity_map`, and the five service roles                                      | P0    |
| `0003_events.sql`           | `events` hypertable, indexes, compression + retention policies                                                            | P0    |
| `0004_idempotency.sql`      | `event_dedupe` + age index                                                                                                | P0    |
| `0005_session_guard.sql`    | `session_guard` + age index                                                                                               | P0    |
| `0006_sessions_active.sql`  | `sessions_active` + touch index                                                                                           | P0    |
| `0007_sessions_history.sql` | `sessions_history` hypertable, indexes, compression + retention policies, `sessions` view                                 | P0    |
| `0008_session_stats.sql`    | `session_stats_15m` hypertable                                                                                            | P0    |
| `0009_functions.sql`        | `app_tenant()`, `parse_batch()`, `ingest_batch()` (`SECURITY DEFINER`, owned by `app_writer`) plus grants                 | P0    |
| `0010_jobs.sql`             | `archive_sessions`, `rebuild_session_stats`, `cleanup_idempotency`, `reconcile_session_stats` procedures                  | P0    |
| `0011_rls.sql`              | RLS enable/force + policies for the roles created in `0002`                                                               | P4    |
| `0012_caggs.sql`            | `events_15m`, `identity_15m`, refresh policies, backfill calls (**created in P3-1**, see "Continuous aggregates")         | P3    |
| `0013_daily.sql`            | `session_stats_daily`, `identity_daily`, `rollup_daily`                                                                   | P3    |
| `0014_schedule.sql`         | `add_job` registrations for the jobs that exist in the phase (staging/production only; the test harness never applies it) | P0/P3 |

Rationale for the split: P0 must be runnable and testable on its own, so tenancy,
event, identity, session and statistics tables arrive before RLS and rollups. RLS is
added in P4 because the ingest path goes through a definer function owned by a writer role
and the dashboard role does not exist until P3.

## Tables

### `tenants`, `api_keys`, `tenant_settings` (new DDL — blueprint references only)

The PDF resolves the tenant from the API key and stores a timezone per tenant but never
shows the DDL. Minimum shape:

```sql
CREATE TABLE tenants (
  tenant_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE api_keys (
  key_hash    bytea PRIMARY KEY,            -- sha256 of the bearer key, never the key
  tenant_id   uuid NOT NULL REFERENCES tenants,
  kind        text NOT NULL CHECK (kind IN ('ingest','server','dashboard')),
  origin_hint text,                          -- allow-list hint only, not a boundary
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz);

CREATE TABLE tenant_settings (
  tenant_id uuid PRIMARY KEY REFERENCES tenants,
  tz        text NOT NULL DEFAULT 'UTC');
```

Up to two active ingest keys per tenant (rotation) and at most one dashboard key, enforced by
the provisioning script rather than by DDL. A `server` key is a backend credential: it may
bootstrap/resume sessions and emit `server`/`webhook`-sourced events, which a public
`ingest` (client) key cannot ([DEVIATIONS.md](DEVIATIONS.md) D28). Revocation is by
`revoked_at` and takes effect within the 60 s auth cache TTL
([04-services.md](04-services.md)), not instantly across instances.

### `events` (hypertable on `time`)

As in the PDF, with these notes:

- `event_id` is `uuid NOT NULL`; `session_id` and `anon_id` are **nullable** because
  machine-originated events (webhooks, background jobs) are sessionless (D29).
  `anon_id` is required whenever `session_id` is present (rejection `missing_anon_id`).
  `seq integer` stays nullable and its ordering is defined as **`NULLS LAST`**
  (Postgres default for `ASC`), so `(time, seq, event_id)` is a total order even for
  events without `seq`.
- Rev 5 provenance (D28/D32): `source text NOT NULL` (`client | server | webhook`) and
  `received_at timestamptz NOT NULL` (the analytics receipt time). `time` remains the
  producer's occurrence time, corrected only for client sources.
- Index `events_session (tenant_id, session_id, time, seq, event_id)` — unchanged;
  `events_source (tenant_id, source, time DESC)` for provenance review.
- Compression `segmentby tenant_id`, `orderby session_id, time`; 7-day compression,
  90-day retention. Chunk interval starts at **1 day**, and the operational rule is 1 day
  up to ~200 events/s and 6 hours near 1,000 events/s (operational knob, not a migration).

### `event_dedupe` (plain table, global key)

```sql
CREATE TABLE event_dedupe (
  tenant_id    uuid        NOT NULL,
  event_id     uuid        NOT NULL,
  payload_hash text        NOT NULL,
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id))
WITH (autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX event_dedupe_age ON event_dedupe (received_at);
```

**Invariant D1:** a dedupe row exists **only for an event that was stored**. This is the
fix for the blueprint's silent-loss bug ([DEVIATIONS.md](DEVIATIONS.md) D1). Consequence:
`count(event_dedupe)` ≈ `count(events within 3 days)` and the table can be monitored as
such.

Retention: delete by `received_at < now() - 3 days`, bounded batches (200k/run, 1/min).
3 days = 24 h event age limit + 24 h max session length + 1 day margin.

### `session_guard` (identity, lock, cap)

```sql
PRIMARY KEY (tenant_id, session_id)
home_day     timestamptz NOT NULL   -- date_trunc('day', now(),'UTC') at first write, immutable
total_events integer     NOT NULL DEFAULT 0
created_at   timestamptz NOT NULL DEFAULT now()
fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02
INDEX session_guard_age (created_at)
```

Immutable `created_at` and `home_day` keep the frequent `total_events` update heap-only.
Cleanup at `created_at < now() - 3 days`.

### `sessions_active` (live summary)

Columns per PDF. `touch_bucket = floor(epoch(now())/600)` — 10-minute granularity, so
"archived after 40 minutes without a write" is really **40–50 minutes**. The PDF says
"roughly 40 minutes" in some places and "about 40" in others; we document the exact
range and keep `-5` buckets in the archive cutoff ([DEVIATIONS.md](DEVIATIONS.md) D8).
A late event for a closed session creates a row here even though `sessions_history`
already holds that session; readers identify it by the existence of the history row, not
by a flag ([DEVIATIONS.md](DEVIATIONS.md) D18).

### `sessions_history` (hypertable on `home_day`)

Same columns, `PRIMARY KEY (tenant_id, session_id, home_day)`. Because `home_day` comes
from the guard and never changes, a late partial always merges into the same row.
Indexes: `sessions_hist_started (tenant_id, started_at DESC)`, partial
`sessions_hist_user (tenant_id, user_id, started_at DESC) WHERE user_id IS NOT NULL`
(also the deletion join), and `sessions_active_touch (touch_bucket, tenant_id)` on the
live table.

Read view:

```sql
CREATE VIEW sessions WITH (security_invoker = true) AS ...
```

`security_invoker = true` is **required**: without it the view owner's privileges apply
and RLS on the base tables is bypassed ([DEVIATIONS.md](DEVIATIONS.md) D4). The active
half also excludes rows whose session already has a history row, so a late partial does
not appear twice ([DEVIATIONS.md](DEVIATIONS.md) D18).

### `session_stats_15m` (authoritative for sessions/metrics)

Columns per PDF, with one change: `total_duration_s` becomes `numeric(20,6)` (microsecond
precision, the precision of `extract(epoch …)`) and every contribution is stored
unrounded. Rounding at display time only. A scale of 3 would round each contribution and
reintroduce the drift D2 exists to remove ([DEVIATIONS.md](DEVIATIONS.md) D2).

### Continuous aggregates `events_15m` / `identity_15m` (P3-1, `0012_caggs.sql`)

Real-time CAGGs (`timescaledb.materialized_only = false`) over the 15-minute buckets,
exposed to the dashboard only through the tenant-bound views D4 requires (RLS cannot be
carried by a CAGG). Design pinned by the spike on the production image (Timescale
2.32.0 / toolkit 1.26.0, T24):

| Column (group: `tenant_id`, `bucket`)      | Why                                                                                                                                          |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `events_15m.events`, `engaged_events`      | exact counts; `engaged_events` filters `cat = 'e'`                                                                                            |
| `events_15m.sessions`                      | `count(DISTINCT session_id)` — supported inside a CAGG on the pinned version, so session counts stay exact                                     |
| `identity_15m.anon_hll`                    | `hyperloglog(4096, anon_id)` state; finalized on read with `distinct_count(rollup(...))`                                                       |
| `identity_15m.users_hll`                   | `hyperloglog(4096, user_id) FILTER (WHERE user_id IS NOT NULL)` — without the filter, NULL user_ids corrupt the sketch (spike-verified)        |

Three consequences worth writing down:

1. **HLL states, not counts.** `distinct_count(rollup(hyperloglog(...)))` cannot be nested
   inside a CAGG definition ("aggregate function calls cannot be nested"). The CAGG
   materializes the raw sketch; finalization happens at read time and composes upward:
   the daily rollup (P3-2) combines per-bucket states into a per-day sketch, which T24c
   proves equals one sketch over the whole range. Small cardinalities are exact and
   deterministic; the ~1.6% label applies at scale (04-services.md rule 4).
2. **Real-time reads a watermark, not the world.** `materialized_only=false` serves the
   union of materialized buckets and raw data **above** the refresh watermark. A late
   insert into an already-materialized bucket is invisible until the next refresh (≤ 15
   min by the policy); the newest partial bucket is kept out of the materialization
   (`end_offset = one bucket`) so it is always live. This is the freshness contract P3-7
   formalizes, and the reason the daily backfill in P3-2 refreshes explicit ranges.
3. **Backfill is a refresh, not a copy.** The migration calls
   `CALL refresh_continuous_aggregate(<cagg>, NULL, NULL)` — a no-op on a fresh install,
   the documented backfill on an upgrade (whatever the 90-day raw retention can serve).
   Refresh + retention policies are registered in the same file under the bootstrap
   LOGIN role (same rule as 0003/0008); the test harness deletes `policy_*` jobs to stay
   hermetic, and T24e reads them in that window.

### `identity_map` (new DDL)

```sql
CREATE TABLE identity_map (
  tenant_id uuid NOT NULL,
  anon_id   text NOT NULL,
  user_id   text NOT NULL,
  first_seen timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, anon_id, user_id));
CREATE INDEX identity_map_user ON identity_map (tenant_id, user_id);
```

Written inside `ingest_batch()` (`INSERT … ON CONFLICT DO NOTHING` over the stored rows),
so it commits with the event rather than in a second statement. Powers resolved-people
counts and the deletion workflow; metrics stay separately labelled. Sessionless events
are excluded: there is no visitor to stitch (D29).

### `session_bootstrap` (Rev 5 lifecycle record)

```sql
CREATE TABLE session_bootstrap (
  tenant_id     uuid        NOT NULL,
  session_id    uuid        NOT NULL,
  anon_id       text        NOT NULL,
  user_id       text,
  started_at    timestamptz NOT NULL,
  last_event_at timestamptz NOT NULL,   -- advanced by ingest_batch only
  props         jsonb       NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, session_id));
CREATE INDEX session_bootstrap_anon ON session_bootstrap (tenant_id, anon_id, started_at DESC);
CREATE INDEX session_bootstrap_age  ON session_bootstrap (last_event_at);
```

Backs `POST /v1/sessions`: a session exists here **before** it has any events, so
bootstrap creates no analytics rows (D30). Resume reads the idle clock but never moves
it; only `ingest_batch()` advances `last_event_at`. `cleanup_idempotency` prunes rows
older than 3 days, bounded like the dedupe/guard deletes.

### Daily tables (P3)

`session_stats_daily (tenant_id, local_day)` with `sessions`, `bounced`,
`total_duration_s numeric(20,6)`, `heartbeat_s`, `events`; `identity_daily` with rolled HLLs.
Rebuilt two to four days behind, three nights in a row, so partial local days never land.

## Row-level security (P4)

Roles are created by `0002_tenancy.sql` (P0) because the ingest function is owned by
`app_writer`; this section only enables and forces policies in P4.

- Roles (see D20/D21 in [DEVIATIONS.md](DEVIATIONS.md)): `app_owner` owns the schema and
  runs migrations (LOGIN, `pg_hba.conf`-restricted — Timescale background tasks refuse
  NOLOGIN owners); `app_writer` owns
  `ingest_batch()` and has `BYPASSRLS`, `INSERT` plus `SELECT` (for `RETURNING`) on
  `events`/`event_dedupe`/`identity_map`, `SELECT/UPDATE` on `session_guard` and
  `SELECT/INSERT/UPDATE` on `sessions_active`, but no login; `app_ingest` is
  `NOSUPERUSER NOBYPASSRLS` with `EXECUTE` on `ingest_batch()` only; `app_dashboard` is
  `NOSUPERUSER NOBYPASSRLS` with `SELECT` on views/rollups; `app_jobs` is LOGIN (D20)
  with `BYPASSRLS` and runs the scheduler. The function is `SECURITY DEFINER` with
  `SET search_path = pg_catalog, public`, so tenant resolution and every write happen in
  one reviewed routine and a leaked ingest key cannot read tenant data.
- `app.tenant_id` is set with `SET LOCAL` inside the API transaction.
- Use a fail-closed helper so a missing setting raises instead of matching nothing:

```sql
CREATE FUNCTION app_tenant() RETURNS uuid LANGUAGE sql STABLE AS
$$ SELECT current_setting('app.tenant_id', false)::uuid $$;  -- raises if unset
```

Policies are `FOR ALL USING (tenant_id = app_tenant()) WITH CHECK (tenant_id = app_tenant())`
— the `WITH CHECK` half is what stops cross-tenant inserts.

- Tables to protect: `events`, `sessions_active`, `sessions_history`,
  `session_stats_15m`, `session_stats_daily`, `identity_daily`, `identity_map`. The PDF's
  example names `session_stats_hourly`, which does not exist.
- Continuous aggregates (`events_15m`, `identity_15m`) are materialized views and cannot
  carry RLS. They are exposed only through `security_invoker` views (or a
  `security_barrier` function) that force `tenant_id = app_tenant()`; the dashboard role
  gets no direct grant. P3-1 verifies this on the pinned version.
- `FORCE ROW LEVEL SECURITY` on every hypertable; Timescale propagates the setting to
  chunks, so verify rather than writing chunk policies by hand (P4-1).

## Invariants and monitors (each has a test)

| ID  | Invariant                                     | Query shape                                                                                                                                                              |
| --- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| I1  | No duplicate event                            | `SELECT ... FROM events GROUP BY tenant_id, event_id HAVING count(*) > 1` → 0 rows                                                                                       |
| I2  | Cap never exceeded                            | `session_guard.total_events <= 5000` → 0 rows                                                                                                                            |
| I3  | Guard matches stored events                   | `guard.total_events = count(events)` for `event.time >= guard.created_at - INTERVAL '24 hours'`                                                                          |
| I4  | One history row per session                   | `GROUP BY tenant_id, session_id HAVING count(*) > 1` → 0 rows                                                                                                            |
| I5  | Statistics equal a rebuild                    | snapshot the range, `rebuild_session_stats(tenant, from, to)`, compare every column with `EXCEPT ALL` in both directions (no scratch schema)                             |
| I6  | Every dedupe row has a stored event           | `event_dedupe LEFT JOIN events` → 0 orphans (D1)                                                                                                                         |
| I7  | Sessions without engagement are exceptional   | `sessions` where `event_count = 0` is monitored and alerted on, not enforced: a dropped `page_view` leaves a lifecycle-only session, which is why `ec_0` exists          |
| I8  | No orphan events (integrity check, not an FK) | `events` with `time >= now() - INTERVAL '3 days'` **and `session_id IS NOT NULL`** and no `session_guard` row → 0 rows (sessionless events have no guard by design, D29) |
| I9  | Cross-tenant isolation                        | `app_dashboard` sees 0 rows of tenant B when `app.tenant_id` = tenant A                                                                                                  |

I6 and I9 do not exist in the PDF; they are the two additions that catch the defects we
are fixing.

## Open items for the P0 reviewer

1. Chunk interval: 1 day is the default; confirm against measured row width before the
   first benchmark.
2. `session_guard` cleanup uses `ctid`-based bounded deletes like the PDF's dedupe cleanup;
   confirm HOT ratio stays above 95% after a week of synthetic load.
3. ~~Whether `events` needs a `received_at` column for operational forensics.~~ Resolved
   in Rev 5 (D32): `received_at` is a first-class column from migration `0015`
   (`source` too); there were no production rows when it was added.
