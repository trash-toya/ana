# Session-Based Analytics on Postgres + TimescaleDB

A session analytics pipeline where **the database is the only stateful component**.
Clients assign session and event IDs; the ingest API validates and buffers; a single
database function (`ingest_batch()`) applies identity, deduplication, the hard session
cap and every write in one transaction; dashboards read pre-aggregated rollups plus a
small live slice — never raw events for headline metrics.

Delivery contract: **at-least-once from the SDK, exactly-once effect in the database**
for events the SDK held durably, within 24 hours. Deliberately **not** suitable for
billing or audit events.

## Status

| Phase | Theme                        | State                                                     |
| ----- | ---------------------------- | --------------------------------------------------------- |
| P0    | Database core                | ✅ closed (gates G0/G1B)                                  |
| P1    | Ingest API + minimal web SDK | ✅ closed (G1)                                            |
| P2    | Durable clients              | ✅ closed (G2)                                            |
| P3    | Rollups and dashboards       | ✅ closed (G3)                                            |
| P4    | Production hardening         | 🚧 in progress — roles/RLS and the CI isolation gate done |
| P5    | Optional scaling             | ⏸ not approved; each item gated on a stated trigger       |

The full plan, decisions and evidence live in [`docs/plan/`](docs/plan/README.md);
every intentional difference from the source blueprint is recorded with its covering
test in [`docs/plan/DEVIATIONS.md`](docs/plan/DEVIATIONS.md).

## Architecture

```
web / RN clients ──► POST /v1/events ──► ingest service (Express, :3001)
                                              │  buffer → batched call
                                              ▼
                                   ingest_batch(jsonb)  ── SECURITY DEFINER, one txn
                                              │  identity, dedupe, cap, writes
                                              ▼
                 Postgres 17 + TimescaleDB + toolkit (the only stateful component)
                   events (hypertable) → events_15m / identity_15m (CAGGs)
                   sessions_active / sessions_history → session_stats_15m → daily
                                              ▲
dashboard API (Express, :3002) ── rollups + live slice ──┘ (tenant-bound views under RLS)
```

- **Write path:** quota lives in `session_guard.total_events` under a row lock; capped
  events never claim a dedupe row, so retries stay stable.
- **Read path:** `session_stats_15m` is authoritative for sessions/metrics; HLL
  sketches are accelerators and are labelled approximate wherever shown.
- **Isolation:** RLS with a `tenant_id = app_tenant()` policy pair on every
  RLS-capable table; where the pinned Timescale version refuses RLS (compressed
  hypertables, CAGG materialization), tenant-bound views with a fail-closed predicate
  and no service grants on the base relations (`docs/plan/DEVIATIONS.md` D36/D37).

## Repository layout

```
db/
  migrations/       forward-only SQL (0001..0017), applied in order
  sql/functions/    ingest_batch, parse_batch, app_tenant, helpers
  sql/jobs/         archive, rebuild, cleanup, reconcile, rollup_daily
  test/             T1–T11 harness, invariants, property/concurrency/RLS suites
services/
  ingest/           Express ingest API (buffering, validation, auth, outcomes)
  dashboard/        Express dashboard API (rollups, live slice, history, freshness)
packages/
  shared/           contract types, zod schemas, canonical JSON + hashing
  sdk-web/          browser SDK (durable queue, multi-tab, retry matrix)
  sdk-react-native/ RN SDK (wraps the web core)
  sdk-server/       server SDK (session bootstrap/resume)
ops/
  docker-compose.yml  pinned TimescaleDB + PgBouncer
  loadtest/           P0/P3 benchmarks
  runbooks/           operational runbooks
demo/                 end-to-end demo (seeded tenant + browser page)
docs/plan/            executable plan set, benchmarks, deviations
```

## Quick start

Prerequisites: Node 24, pnpm 12 (`corepack enable`), Docker.

```bash
pnpm install
pnpm db:up            # TimescaleDB (pg17) + PgBouncer via ops/docker-compose.yml
pnpm db:migrate       # apply db/migrations in order (idempotent; SA_DB picks the db)
pnpm test             # build + full suite against real databases
pnpm demo             # seed a tenant, start the ingest API and a browser demo
```

`pnpm db:ping` checks the container. The default connection URL is
`postgresql://postgres:postgres@127.0.0.1:5432/postgres`; every test file creates its
own hermetic database from the migrations and drops it afterwards.

### Running the services

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
SESSION_CONTEXT_SECRET=<secret> pnpm -F @session-analytics/ingest start   # :3001
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/postgres \
  pnpm -F @session-analytics/dashboard start                              # :3002
```

Both services are stateless and expect PgBouncer (transaction mode) in front of
Postgres in production. Auth is by API key (`X-API-Key`): ingest keys are accepted only
by the ingest service, dashboard keys only by the dashboard; the key resolves to a
tenant inside the database (`resolve_api_key()`), never from the request.

| Variable                              | Service   | Default                                  |
| ------------------------------------- | --------- | ---------------------------------------- |
| `DATABASE_URL`                        | both      | —                                        |
| `PORT`                                | both      | 3001 / 3002                              |
| `SESSION_CONTEXT_SECRET`              | ingest    | — (bootstrap/resume disabled without it) |
| `MAX_BATCH` / `BUFFER_MAX`            | ingest    | 500 / 5000                               |
| `FLUSH_IDLE_MS` / `FLUSH_INTERVAL_MS` | ingest    | 50 / 1000                                |
| `STATEMENT_TIMEOUT_MS`                | dashboard | 10000                                    |
| `AUTH_TTL_MS`                         | dashboard | 60000                                    |

## Testing

```bash
pnpm test                  # everything (256 tests, 45 files, real TimescaleDB)
pnpm test:db               # database suites: T1–T9, invariants, property, RLS
pnpm test:db:concurrency   # T1–T9 only
pnpm test:property         # randomized invariant test (short seed set)
pnpm lint && pnpm typecheck && pnpm format:check
```

Every invariant in the plan is enforced by a test that runs against the pinned image,
not a mock. The cross-tenant isolation suite (`db/test/rls`) is a named, fail-fast CI
step: the build fails if isolation regresses. CI runs lint → format → typecheck →
build → migrate (×2) → isolation gate → full suite on every push and PR
(`.github/workflows/ci.yml`).

## Documentation

- [`docs/plan/README.md`](docs/plan/README.md) — plan index and approval gates
- [`docs/plan/01-overview.md`](docs/plan/01-overview.md) — goals, non-goals, risks
- [`docs/plan/02-data-model.md`](docs/plan/02-data-model.md) — schema, retention, RLS, invariants
- [`docs/plan/03-write-path.md`](docs/plan/03-write-path.md) — `ingest_batch()` and the jobs
- [`docs/plan/04-services.md`](docs/plan/04-services.md) — both Express services and the dashboard API
- [`docs/plan/05-sdks.md`](docs/plan/05-sdks.md) — client contracts and the conformance suite
- [`docs/plan/06-testing.md`](docs/plan/06-testing.md) — harness, T1–T11, CI, load tests
- [`docs/plan/07-operations.md`](docs/plan/07-operations.md) — topology, roles, monitoring, runbooks
- [`docs/plan/08-execution.md`](docs/plan/08-execution.md) — phased backlog and evidence
- [`db/test/README.md`](db/test/README.md) — database test harness
