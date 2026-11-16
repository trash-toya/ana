# 07 — Operations

## Topology (Compose, local + CI + staging)

```
web/RN SDK ──► LB (consistent hash on anon_id) ──► ingest (Express, stateless)
                                                    │
                                              PgBouncer (transaction pooling)
                                                    │
                       Postgres 17 + TimescaleDB + toolkit (primary)
                                    │
                              synchronous standby        async replica (dashboards)
```

| Service        | Image                                                                                  | Notes                                                                   |
| -------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `db`           | `timescale/timescaledb-ha:pg17` (verify tag; fall back to the newest available `pg17`) | Must include `timescaledb_toolkit`; assert it in a healthcheck          |
| `pgbouncer`    | `edoburu/pgbouncer`                                                                    | `pool_mode=transaction`, `max_client_conn=1000`, `default_pool_size=20` |
| `ingest`       | local build                                                                            | 4–8 DB connections per instance                                         |
| `dashboard`    | local build                                                                            | Separate role, separate pool                                            |
| `standby` (P4) | same image, `pg_basebackup` + `primary_conninfo`                                       | For the T11 drill                                                       |
| `lb` (P4)      | HAProxy or nginx                                                                       | Hash on `anon_id` header/field                                          |

Verification in P0, before any other work: `CREATE EXTENSION timescaledb_toolkit;` and a
`hyperloglog` round-trip, because `identity_15m` depends on it. If the image lacks the
toolkit, stop and re-plan — the blueprint's visitor/user metrics need it.

## Roles

| Role            | Attributes                                                                                                                                                                                                                                                                                                                                                                           | Used by                 |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------- |
| `app_owner`     | owns schema, runs migrations; LOGIN (D20 — Timescale background tasks refuse NOLOGIN owners), restricted by `pg_hba.conf` to the migration host                                                                                                                                                                                                                                      | migrations, policy jobs |
| `app_writer`    | owns `ingest_batch()`; `BYPASSRLS`; `INSERT, SELECT` on `events`/`event_dedupe`/`identity_map` (SELECT for `RETURNING`), `SELECT/UPDATE` on `session_guard`, `SELECT/INSERT/UPDATE` on `sessions_active`; no login                                                                                                                                                                   | the definer function    |
| `app_ingest`    | `NOSUPERUSER NOBYPASSRLS`; `EXECUTE` on `ingest_batch()` only, no table grants                                                                                                                                                                                                                                                                                                       | ingest service          |
| `app_dashboard` | `NOSUPERUSER NOBYPASSRLS`; `SELECT` on views/rollups/history; no `EXECUTE` on rebuild                                                                                                                                                                                                                                                                                                | dashboard service       |
| `app_jobs`      | `LOGIN` (D20), `BYPASSRLS`; `EXECUTE` on job procedures; `SELECT` on every table it touches; `DELETE` on `sessions_active`/dedupe/guard and on `session_stats_15m` (archive's zero-bucket cleanup); `UPDATE` on `sessions_active` (`SELECT … FOR UPDATE` needs it) plus `INSERT/UPDATE` on `sessions_history`/`session_stats_15m`; restricted by `pg_hba.conf` to the scheduler host | Timescale scheduler     |

`ingest_batch()` is `SECURITY DEFINER`, owned by `app_writer`, with
`SET search_path = pg_catalog, public`, so tenant resolution and every write happen in one
reviewed routine and the ingest service holds only an `EXECUTE` grant. `app_dashboard`
must never be the owner, and `FORCE ROW LEVEL SECURITY` is required or the owner bypasses
policies. Test I9 asserts this from the dashboard role. `app_jobs` is the only role that
crosses tenants by design; `0014_schedule.sql` is applied as `app_jobs` and registers
every job as that role, which is also what keeps the scheduler working after a failover.

## PgBouncer notes

- Transaction pooling is compatible with `SET LOCAL app.tenant_id` (must be inside an
  explicit transaction — the dashboard API does this).
- `ingest_batch()` is one transaction, so it is a single server connection for its
  duration; size `default_pool_size` from `ingest instances × connections_per_instance`
  plus the dashboard pool. The `default_pool_size=20` in the topology table is a floor,
  not a default.
- Disable **named** prepared statements unless PgBouncer ≥ 1.21 with
  `max_prepared_statements`; `node-postgres` uses unnamed/extended-protocol statements,
  which transaction pooling supports.
- A database or PgBouncer restart terminates idle server connections with `FATAL 57P01`.
  Every long-lived `pg.Pool` must have an `error` listener: without one, Node treats the
  event as unhandled and the process exits. The listener logs and lets the pool discard
  the broken client and reconnect; requests see 503 only while the database is actually
  unavailable.
- Advisory locks (`7001`) are transaction-scoped, so pooling does not break them.

## Retention (single table of truth)

| Tier                                                 | Resolution         | Kept           | Mechanism                          |
| ---------------------------------------------------- | ------------------ | -------------- | ---------------------------------- |
| `events`                                             | per event          | 90 d           | `add_retention_policy`             |
| `sessions_history`                                   | per session        | 90 d           | `add_retention_policy`             |
| `sessions_active`                                    | live               | ~40–50 min     | `archive_sessions` every minute    |
| `session_guard`, `event_dedupe`, `session_bootstrap` | identity/lifecycle | 3 d by row age | `cleanup_idempotency` every minute |
| `session_stats_15m`, `events_15m`, `identity_15m`    | 15 min UTC         | 90 d           | retention policies                 |
| `session_stats_daily`, `identity_daily`              | tenant-local day   | 5 y            | none (upserted)                    |
| Monthly (optional)                                   | month              | indefinitely   | yearly job, not in P0–P4           |

Compression: 7-day delay on `events` and `sessions_history`, `segmentby tenant_id`.
Late events and merges therefore only ever touch uncompressed chunks.

## Failure model

| Failure                         | Behaviour                                    | Mitigation                                  |
| ------------------------------- | -------------------------------------------- | ------------------------------------------- |
| Postgres unavailable            | 503, nothing acknowledged                    | SDK keeps events durably; retries           |
| Duplicate delivery (any day)    | One event row                                | Global dedupe key under the guard lock      |
| Conflicting duplicate           | First wins, conflict rejected + counted      | `dedupe_conflict` metric                    |
| Partly invalid batch            | Per-event outcomes                           | Nothing else lost                           |
| Wrong device clock              | Corrected via `sent_at`                      | Only corrected time validated               |
| Runaway session                 | Capped at 5,000                              | Guard counter + rate limits                 |
| Late event for archived session | Partial active row, merged with exact deltas | `home_day` identity                         |
| Archive races ingest            | Session written while archiving              | `SKIP LOCKED` + re-checked `touch_bucket`   |
| Guard-lock contention           | Waits on one row                             | Sticky routing, one in-flight batch per SDK |
| Job stalls                      | Active/guard/dedupe grow                     | Alerts on backlog and table size            |
| Failover                        | Primary lost                                 | Sync standby, fencing, drills               |

Replication: one synchronous standby (`synchronous_commit = on`,
`synchronous_standby_names`), plus an async replica for dashboards. RPO 0 for
acknowledged batches; RTO target 2 minutes via Patroni-style leader election and a
virtual endpoint PgBouncer follows. Set `dns_max_ttl` (or an equivalent re-resolution)
low enough that the pooler notices the promotion, or the target is fiction. **Fencing is
mandatory**: the old primary must be
unable to accept writes before promotion, or acknowledged writes can be discarded.
Async-only is an accepted alternative only if the weakened guarantee is documented to
customers.

Timescale jobs live in the catalog and replicate, but the scheduler must be configured on
the standby; after every failover drill, verify archive/cleanup/rollup resumed with no
backlog.

## Alerting thresholds (initial, tune after P4)

| Alert                    | Threshold                                                        |
| ------------------------ | ---------------------------------------------------------------- |
| Ingest 5xx rate          | > 0.5% for 5 min                                                 |
| Flush p95                | > 200 ms for 10 min                                              |
| Buffer depth             | > 50% of `BUFFER_MAX` for 1 min                                  |
| Archive backlog          | oldest `sessions_active` row > 100 min (2× the 50-minute window) |
| Job failures             | any failure in `job_stats`                                       |
| Dedupe/guard oldest row  | > 3.5 days (cleanup falling behind)                              |
| Replication lag          | > 5 s                                                            |
| Reconcile drift          | any warning                                                      |
| I6 orphans               | > 0 (would mean the cap/dedupe fix regressed)                    |
| Zero-engagement sessions | Unexpected rise; I7 is a monitor, not an invariant               |

## Runbooks (`ops/runbooks/`)

1. `ingest-backlog.md` — 5xx burst, buffer overflow, DB saturation.
2. `archive-stall.md` — `sessions_active` growth, lock waits, SKIP LOCKED behaviour.
3. `failover.md` — drill procedure, fencing check, job verification, measured RPO/RTO.
4. `deletion-request.md` — the seven-step workflow below.
5. `rollback.md` — forward-only migrations, so rollback means restore + replay, not down.

## Deletion workflow (blueprint section 9, made executable)

| Step | Action                                                                                                                                                                                                                                                                                                               | Verification                                                                  |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| 1    | Resolve `user_id` and every `anon_id` via `identity_map`, per tenant                                                                                                                                                                                                                                                 | Dump the resolved set to the deletion record                                  |
| 2    | Collect the `session_id` set from `sessions_active` and `sessions_history` by tenant + person IDs, then delete those rows and the matching `session_guard` rows (guards carry no person identifiers)                                                                                                                 | Batched; guard count matches the collected set                                |
| 3    | Raw events: preferred path is keyed HMACs for `anon_id`/`user_id` plus deleting the `identity_map` row (needs legal sign-off as anonymisation). If events are deleted instead (the alternative path), resolve their `event_id`s first and delete the matching `event_dedupe` rows, which carry no person identifiers | Chunk decompression measured, batched; I6 stays at 0 after the run            |
| 4    | `rebuild_session_stats(tenant, from, to)`; rebuild `session_stats_daily` for covered local days                                                                                                                                                                                                                      | No stale bucket remains (T8)                                                  |
| 5    | Refresh `events_15m` and `identity_15m` over the range; rebuild daily sketches still inside 90 days                                                                                                                                                                                                                  | Old daily sketches cannot be rebuilt and are treated as anonymised aggregates |
| 6    | Write a deletion record (timestamp, scope, operator)                                                                                                                                                                                                                                                                 | Record exists; backups age out on their own                                   |
| 7    | Prevent: props allow-list per event name                                                                                                                                                                                                                                                                             | Validation test                                                               |

Step 3's HMAC option is a day-1 schema decision, not a retroactive one: it changes what
`events.anon_id`/`user_id` contain. If legal sign-off is expected, take it before P0-3
rather than at P4-8.

## Sizing

Planning figures from the blueprint (about 400 B per event, ~8x event compression, ~5x
history, ~8 events/session, ~130 B dedupe incl. indexes, ~90 B guard, ~50 min live
window, +40% for indexes and bloat):

| Events/s | Events/day | Sessions/day | 90 d total (incl. +40%) |
| -------- | ---------- | ------------ | ----------------------- |
| 100      | 8.6 M      | 1.1 M        | ~101 GB                 |
| 500      | 43.2 M     | 5.4 M        | ~507 GB                 |
| 1,000    | 86.4 M     | 10.8 M       | ~1,013 GB               |
| 3,000    | 259.2 M    | 32.4 M       | ~3,040 GB               |

These are **targets to verify**, not measurements. The newest chunk plus indexes should
fit in about a quarter of RAM; chunk interval is 1 day up to ~200 events/s and ~6 hours
near 1,000 events/s.

## Scaling path (P5, each item gated on a trigger)

| Step | Move                                                                                                                                                    | Trigger                                                  |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| 1    | Vertical scale, smaller chunks                                                                                                                          | Newest chunk no longer fits in memory                    |
| 2    | Read replica for dashboards                                                                                                                             | Dashboard queries compete with ingest                    |
| 3    | Dedicated instance for a dominant tenant                                                                                                                | One tenant dominates volume                              |
| 4    | Rotate `event_dedupe` into day-generation tables (`TRUNCATE` the oldest; race-free because writers hold the guard lock). Alternative: hash partitioning | Dedupe churn/size dominates, roughly > 1,500 events/s    |
| 5    | Durable queue (`pgmq` first)                                                                                                                            | Multiple consumers, outage buffering, stronger guarantee |
| 6    | Column store for raw events (keep sessions + rollups in Postgres)                                                                                       | Billion-row ad-hoc scans dominate                        |
