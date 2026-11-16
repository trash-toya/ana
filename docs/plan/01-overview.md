# 01 — Overview

## What we are building

A session analytics pipeline where the **database is the only stateful component**:
Postgres 17 with TimescaleDB and timescaledb_toolkit. Clients assign session and event
IDs, the ingest API validates and batches, one database function (`ingest_batch()`)
applies identity, deduplication, the hard session cap and all writes in a single
transaction, and dashboards read pre-aggregated rollups plus a small live slice.

Delivery contract: **at-least-once from the SDK, exactly-once effect in the database**
for events the SDK held durably, within 24 hours. Explicitly _not_ suitable for billing
or audit events.

## Non-goals (deliberate, per blueprint section 1)

| Not built                  | Why it is safe                                                         | Reconsider when                                                        |
| -------------------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Kafka / Redpanda           | Ingest buffer holds only unacknowledged events; retries are idempotent | Multiple independent consumers, or ingest must survive long DB outages |
| Redis session cache        | `sessions_active` is ~40–50 minutes of sessions; upserts are cheap     | Sub-millisecond session lookups become a product requirement           |
| Server-side session timers | Expiry is derived from `last_seen_at`; archiving is housekeeping       | "Session ended" webhooks are required                                  |
| Foreign keys on events     | Adds a lookup to every insert; append-only data                        | Never on the hot path — periodic integrity checks instead              |
| Column store (ClickHouse)  | Rollups answer dashboard questions                                     | Ad-hoc scans over billions of wide rows dominate                       |

## Hard decisions for this build

1. **Express (Node 24, TypeScript)** for both services. The ingest API is I/O-bound and
   the interesting work is the buffering state machine, not raw request parsing.
   Validation code is shared with the TypeScript SDKs via a workspace package, which is
   the main reason not to use Go here.
2. **One Compose file, pinned images.** `timescale/timescaledb-ha` (PG + Timescale +
   toolkit), PgBouncer, and optionally a second Postgres for the failover drill. Every
   SQL file and every test runs against this, locally and in CI — the blueprint's own
   caveat is that its SQL was never executed.
3. **Blueprint SQL is a starting point, not gospel.** Defects found during review are
   fixed and listed in [DEVIATIONS.md](DEVIATIONS.md), each with a covering test. The
   most important one: `ingest_batch()` must not create an `event_dedupe` row for an
   event it then rejects as `session_capped`, otherwise the retry is reported as an
   accepted duplicate and the event is lost silently.
4. **No server-side `uuidv7()` dependency.** Client IDs are client-generated; internal
   rows use `gen_random_uuid()`. This keeps the Postgres version choice open (17 now,
   18 later) instead of pinning it for a convenience function.
5. **Rollups are derived but authoritative for their metrics.** `session_stats_15m` is
   the single source of truth for sessions, engagement events, duration and bounce — it is
   always recomputed from `sessions_history`, never typed by hand; HLL sketches are
   accelerators and are labelled approximate wherever shown.

## Repository layout

```
.
├── blueprint_v4.pdf
├── docs/plan/                  # this plan set + benchmarks/
├── db/
│   ├── migrations/             # 0001..00NN, forward-only, applied in order
│   ├── sql/functions/          # ingest_batch, parse_batch, app_tenant, helpers
│   ├── sql/jobs/               # archive, rebuild, cleanup, reconcile, rollup_daily
│   └── test/                   # fixtures, invariant queries, T1–T11 harness hooks, README
├── services/
│   ├── ingest/                 # Express: POST /v1/events, buffer, outcomes
│   └── dashboard/              # Express: rollups + live slice + history
├── packages/
│   ├── shared/                 # contract types, zod schemas, canonical JSON + hashing
│   ├── sdk-web/                # browser SDK
│   └── sdk-react-native/       # RN SDK
├── ops/
│   ├── docker-compose.yml
│   ├── pgbouncer/
│   ├── loadtest/
│   └── runbooks/
└── package.json                # npm workspaces
```

## Phase map

| Phase  | Theme                        | Exit criterion (short form)                                     |
| ------ | ---------------------------- | --------------------------------------------------------------- |
| **P0** | Database core                | T1–T9 green; invariants hold; baseline benchmarks recorded      |
| P1     | Ingest API + minimal web SDK | Events and sessions visible end-to-end in SQL                   |
| P2     | Durable clients              | T10 green; replay after kill is loss-free and count-identical   |
| P3     | Rollups and dashboards       | Dashboards read rollups + live slice only                       |
| P4     | Production hardening         | T11 green; zero cross-tenant reads; runbook + deletion workflow |
| P5     | Optional scaling             | Only on a stated trigger; separately approved                   |

Detail, task IDs and dependencies: [08-execution.md](08-execution.md).

## Risks carried into P0

| Risk                                                                                     | Impact                                        | Mitigation / decision point                                                                                                                                                                                                                                     |
| ---------------------------------------------------------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hyperloglog` inside a continuous aggregate may need a version-specific combine function | `identity_15m` may not be buildable as a CAGG | P0 spike on the pinned image; fallback is a per-bucket job writing rolled-up HLLs, or a plain table maintained by the ingest path                                                                                                                               |
| RLS + compression + CAGGs support varies by Timescale version                            | Tenant isolation could silently not apply     | Verify on the pinned version; fallback is tenant-bound views or security-barrier functions with the same cross-tenant test                                                                                                                                      |
| Express buffering couples HTTP response latency to the 1-second flush window             | p95 latency ~ flush interval under light load | Single in-flight `ingest_batch()` plus an adaptive idle flush (`FLUSH_IDLE_MS`, 50 ms), so a lone event does not wait out the 1 s timer; documented target is p95 under 200 ms at peak, and idle latency is stated as ~50 ms ([04-services.md](04-services.md)) |
| `event_dedupe` churn at high rates                                                       | Bloat, index pressure                         | Cap-fix keeps dedupe rows == stored events within 3 days; monitor size/age; P5 item 4 (generation tables) if > ~1,500 events/s                                                                                                                                  |
| Synchronous standby raises commit latency                                                | Ingest p95                                    | Measure in P0 (WAL MB/s benchmark); it is a hardware decision, not a code decision                                                                                                                                                                              |

## Definition of done (whole project)

1. Every invariant in [02-data-model.md](02-data-model.md) is enforced by a test that
   runs in CI against a real TimescaleDB, not a mock; every monitor there has an alert.
2. `docs/plan/DEVIATIONS.md` has a row for every difference from the PDF and each row
   names its covering test.
3. Dashboards never read raw `events` for a headline metric.
4. A deletion request for one person completes the seven-step workflow and leaves zero
   stale buckets.
5. A failover drill under load loses no acknowledged batch and resumes all jobs.
