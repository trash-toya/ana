# 08 — Execution plan

Full system planned; **P0 is the only approved work until G0 passes.** Each phase below
is a self-contained, reviewable increment. Task IDs are stable — reference them in commits
and PR titles (`P0-3: session_guard`).

## Summary

| Phase   | Theme                                      | Tasks | Gate                       |
| ------- | ------------------------------------------ | ----- | -------------------------- |
| **P0**  | Database core                              | 10    | G0                         |
| P1      | Ingest API + minimal web SDK               | 6     | G1                         |
| **P1B** | **Server-first collection (Notion Rev 5)** | **5** | **G1B**                    |
| P2      | Durable clients (secondary)                | 6     | G2                         |
| P3      | Rollups and dashboards                     | 6     | G3                         |
| P4      | Production hardening                       | 8     | G4                         |
| P5      | Optional scaling                           | 6     | separate approval per item |

**Rev 5 ordering (2026-11-15):** the canonical path is server-first. The application
backend bootstraps/resumes a session, propagates an opaque session context, and emits
authoritative events through the server SDK or the authenticated HTTP API. Client SDKs
remain supported but are **secondary**; P2 is scheduling, not priority. P1B implements
the server-first contract; it is a prerequisite for P3 (dashboards need server events
and sessionless events to be modelled) but not for the already-finished P1 work.

---

## P1B — Server-first collection (Notion Rev 5)

**Goal:** one canonical ingestion pipeline that the application backend owns. Sessions can
be bootstrapped/resumed by the backend, events can arrive without a session, and every
event carries provenance and both occurrence and receipt times.

| ID    | Task                                                                                                                                                          | Depends | Done when                                                                                                        |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------- |
| P1B-1 | Contract: optional `session_id`/`anon_id`, per-event `source` (client/server/webhook), `received_at` in the prepared row, `missing_anon_id` rejection         | P1-1    | Shared contract tests; existing web SDK payloads still valid                                                     |
| P1B-2 | Schema + write path: nullable `session_id`/`anon_id`, `events.source`, `events.received_at`, `api_keys.kind = 'server'`, sessionless branch in `ingest_batch` | P1B-1   | DB test T12: sessionless events store without guard/cap/summary rows; I8 excludes them                           |
| P1B-3 | Session bootstrap/resume API: `POST /v1/sessions`, `POST /v1/sessions/validate`, HMAC context token, server credential only                                   | P1B-2   | Idle (30 min) and age (24 h) rotation, concurrent bootstrap returns one session, tampered/expired token rejected |
| P1B-4 | `@session-analytics/sdk-server`: session-bound and sessionless track, batching, retries, per-event outcomes, session helpers                                  | P1B-3   | Unit tests + e2e through the real ingest app into Postgres                                                       |
| P1B-5 | Webhook/server-to-server adapter: `source: 'webhook'`, producer idempotency, duplicate delivery stores once                                                   | P1B-4   | Same `event_id` delivered twice → one stored event, both responses `accepted`                                    |

**G1B:** a backend can bootstrap a session, emit a session-bound order event and a
sessionless webhook event through the server SDK, and the SQL shows both with correct
`source`, `session_id` (NULL for the webhook), `time` and `received_at`; a replayed webhook
does not duplicate the event.

**Explicitly not in P1B:** transactional outbox implementation (documented integration
pattern + the SDK's `track()` contract), client SDK changes beyond keeping the existing
payload valid, and dashboards over server events (P3).

---

## P0 — Database core (Week 0)

**Goal:** prove the transactional design on a real TimescaleDB before writing any client.

| ID    | Task                                                                                                                         | Depends    | Done when                                                                                                |
| ----- | ---------------------------------------------------------------------------------------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------------------- |
| P0-1  | Repo scaffold: npm workspaces, TS config, lint/format, `docs/plan/benchmarks/`                                               | —          | `npm ci && npm run lint && npm run typecheck` clean                                                      |
| P0-2  | Compose: `timescaledb-ha` (pg17) + PgBouncer; healthcheck asserting `timescaledb_toolkit` and a `hyperloglog` round-trip     | P0-1       | `docker compose up -d && npm run db:ping` green; toolkit risk resolved                                   |
| P0-3  | Migrations `0001`–`0008`: extensions, tenancy, events, dedupe, guard, active, history+view, stats                            | P0-2       | Migrations apply cleanly from empty DB; second run is a no-op                                            |
| P0-4  | `app_tenant()`, `parse_batch()`, `ingest_batch()` per [03-write-path.md](03-write-path.md)                                   | P0-3       | Function compiles; retry and out-of-order fixtures pass (the cap-across-archiving fixture moves to P0-5) |
| P0-5  | Jobs: `archive_sessions`, `rebuild_session_stats`, `cleanup_idempotency`, `reconcile_session_stats` plus `0014_schedule.sql` | P0-4       | Procedures run; the schedule file registers them in staging; cap-across-archiving fixture passes         |
| P0-6  | Test harness: per-file database, N pools, fixture builders, invariant queries                                                | P0-3       | `npm run test:db` runs one trivial test end-to-end                                                       |
| P0-7  | T1–T5 (cap, late archive, cross-midnight dedupe, conflicts, attribution order)                                               | P0-4, P0-6 | All green, 3 consecutive runs, no flakes                                                                 |
| P0-8  | T6–T9 (archive race, stats drift, deletion rebuild, kill mid-transaction)                                                    | P0-5, P0-7 | All green; I5 and I6 assertions green                                                                    |
| P0-9  | Property test: ingest→archive vs load→rebuild byte-identical                                                                 | P0-8       | 30-minute nightly-budget run, zero drift                                                                 |
| P0-10 | Baseline benchmarks: sustained, burst, hot session, WAL, HOT ratio, archive duration; record in `docs/plan/benchmarks/p0.md` | P0-8       | Numbers recorded; chunk interval and pool sizes confirmed or changed                                     |

**G0 exit criteria (all must hold):**

1. T1–T9 pass three consecutive times against the pinned image.
2. I1–I8 hold after every test (I9 is a P4 role test); I6 is the new one and must be
   explicitly reported.
3. Worked examples from the PDF reproduce exactly.
4. `reconcile_session_stats` reports zero drift over a ≥51 h simulated window (so the
   26–50 h comparison range is fully populated).
5. Benchmarks recorded; any deviation from blueprint assumptions written into
   `benchmarks/p0.md` with a decision.
6. `DEVIATIONS.md` coverage by G0: D1, D2, D3, D5, D6, D7, D8, D9, D12. Later phases
   own the rest — D13 → P3-4, D4 → P4-1 (I9), D14 → P3-3, D15/D19 → P4-8, D16 → P2-5,
   D17 → P2-3, D10 → P2-1/P2-5, D18 → T2/P3-3. D11 is a decision with no test.

**Explicitly out of scope in P0:** RLS policies, CAGGs, daily rollups, any service, any SDK.

---

## P1 — Ingest API + minimal web SDK

| ID   | Task                                                                                         | Depends    | Done when                                                                 |
| ---- | -------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------- |
| P1-1 | `packages/shared`: contract schemas, category map, canonical JSON, `payloadHash`             | G0         | Unit tests, including key-order independence                              |
| P1-2 | Ingest API skeleton: Express app, key auth, `/healthz`, `/readyz`, `/metrics`                | G0         | Auth and health verified with supertest                                   |
| P1-3 | Validation, clock-skew correction, classification, in-batch dedupe                           | P1-1, P1-2 | Rejection reasons cover the full enum; 400 vs 200 behaviour correct       |
| P1-4 | Buffer + flush + outcome assembly (one in-flight call, adaptive idle flush, 503 on overflow) | P1-3       | Buffering tests with fake timers; "every ID in exactly one list" property |
| P1-5 | Minimal web SDK: `init`, `track`, `identify`, session record + Web Lock, plain fetch         | P1-1       | Events visible end-to-end in SQL in the demo page                         |
| P1-6 | Demo harness: static page + Compose profile + seeded tenant/key                              | P1-4, P1-5 | `npm run demo` shows events and sessions in SQL                           |

**G1:** end-to-end events and sessions visible in SQL; per-event outcome contract
honoured; no acknowledged batch lost on a forced 503.

---

## P2 — Durable clients

| ID   | Task                                                                                                              | Depends    | Done when                                            |
| ---- | ----------------------------------------------------------------------------------------------------------------- | ---------- | ---------------------------------------------------- |
| P2-1 | Web durable queue (localStorage + IndexedDB mirror), quota handling, unload flush                                 | G1         | Overflow and unload cases from the conformance suite |
| P2-2 | Web retry matrix (401/403, 413, 429, 5xx, network), backoff with jitter                                           | P1-5, P2-1 | All rows of the contract table tested                |
| P2-3 | Web multi-tab sharing and no-Web-Locks fallback                                                                   | P2-1       | Two-context test: no duplicate time/seq              |
| P2-4 | RN SDK: MMKV queue, state machine, generation token, deep links, background flush                                 | G1         | Conformance suite green on RN                        |
| P2-5 | RN fixes: size-triggered flush, persisted `identify`, `sdk_dropped` without a session, persisted deep-link expiry | P2-4       | Deviations D10 and D16 have tests                    |
| P2-6 | T10: shutdown mid-flight, re-init, one invalid event in a batch                                                   | P2-4       | Counts identical before/after                        |

**G2:** replayed batches after an app kill and mid-batch invalid events produce identical
session counts; nothing acknowledged is lost.

> **Verified 2026-11-16 (P2 complete).** Evidence — the shared conformance suite
> (`packages/shared/test/conformance`, 16 cases) runs green on BOTH the web SDK and the
> RN SDK: "replay after a lost response: identical stored set, no duplicates",
> "kill mid-flush: after restart the queue holds everything unacknowledged", "batch with
> one invalid event: valid stored, invalid removed, counts identical", plus the RN-only
> persistence cases in `packages/sdk-react-native/test/rn.test.mjs` (T10 shutdown
> mid-flight with counts identical before/after, deep-link expiry surviving a kill).
> Full suite at the time of the gate: 207 tests / 34 files, lint + typecheck clean.

---

## P3 — Rollups and dashboards

| ID   | Task                                                                                         | Depends | Done when                                                                |
| ---- | -------------------------------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------ |
| P3-1 | `events_15m`, `identity_15m` CAGGs + refresh policies + backfill + `materialized_only=false` | G2      | Refresh under 5 min; toolkit HLL works in the CAGG on the pinned version |
| P3-2 | `session_stats_daily`, `identity_daily`, `rollup_daily` (days 2–4, three nights)             | P3-1    | Partial local days never written; UTC+5:30 and UTC+5:45 correct          |
| P3-3 | Dashboard API: hourly, daily, live endpoints with `tenant_settings.tz`                       | P3-1    | Matches the blueprint's example output shape                             |
| P3-4 | Drill-down endpoints: replay, entry pages, funnels with `(time, seq, event_id)` ordering     | P3-3    | Funnel tie-break case from the PDF passes                                |

Both rows landed together (`services/dashboard`, one service): auth (dashboard-key
only, 60 s cache), the six endpoints per the shapes section in 04-services.md, and
21 tests (auth 7, endpoints 14). Done-when evidence: shapes pinned by
`endpoints.test.mjs` (hourly live-slice labelling + D18 exclusion, daily closed/open
merge with `Asia/Kolkata` grouping and HLL identity `approximate: true`, funnel
tie-break with same-timestamp/NULL-seq events, replay storage-order, live
5-min + D18, tenant scoping 404s, READ-ONLY transactions + invariants). The funnel
tie-break is the documented `(time, seq NULLS LAST, event_id)` storage order — the
PDF itself is not in-repo (see the shapes note in 04-services.md).
| P3-5 | Reconcile + retention verification (90 d / 5 y tiers) | P3-2 | Nightly reconcile zero drift; retention drops whole chunks |

P3-5 evidence (T26, `db/test/rollups/`): a late partial at now-30 h merges as exact
deltas and the nightly reconcile stays silent (T26a); at now-60 h — always inside the
rollup window — `events_15m` equals the raw aggregate, `session_stats_daily` /
`identity_daily` equal an independent recompute from `sessions_history` / `events`,
and a deliberately corrupted daily row is rewritten by the next `rollup_daily` run
(T26b). Retention: the five 90 d policies are registered on
`events`/`sessions_history`/`session_stats_15m`/`events_15m`/`identity_15m` and the
daily tables carry none (T26c); `drop_chunks` (a function on 2.32, spiked) drops
whole 1-day chunks with the 90-day boundary day retained, and the 5 y daily tier
survives raw retention (T26d).
| P3-6 | Dashboard performance pass | P3-4 | p95 under 500 ms with synthetic peak ingest |

P3-6 evidence: `pnpm run bench:p3` (ops/loadtest/p3-bench.mjs), results in
benchmarks/p3.md + p3-results.json — worst p95 across all endpoints, steady and
under 200 ev/s background ingest, is **379.3 ms** (`POST /v1/funnels`); every
rollup-backed endpoint is under 9 ms p95. Finding: the funnel's recursive CTE is
the only non-trivial read (~300 ms at 10 k events); a `(tenant_id, name, time)`
index or pre-materialized step counts is the P4-scale candidate — no change now.
| P3-7 | Freshness gate (Notion §22): scheduled aggregation keeps dashboards within 1 h under load and after a backlog; publish last-updated/stale state | P3-2 | Backlog replay within the 1 h bound; freshness exposed to dashboards |

P3-7 evidence: `0016_freshness.sql` adds `cagg_watermark_ts(view)` (wraps
`_timescaledb_functions.cagg_watermark`, normalizes the never-refreshed placeholder
BC timestamp to NULL). T27 (`db/test/rollups/t27_freshness.test.mjs`): a 90-minute
backlog (six 15-min buckets) is replayed by ONE refresh in well under the 1-hour gate
with the watermark advancing past every backlog bucket and zero drift (T27a); the
gate bounds materialization only — an event right now is fully visible above a
2-hour-old watermark (T27b, T24d semantics); a never-refreshed CAGG reads NULL, not
a BC timestamp (T27c). The dashboard service exposes `GET /v1/freshness`
(services/dashboard/test/freshness.test.mjs: empty/fresh/degraded/stale/worst-of +
auth). Measured watermark semantics: empty windows never advance the watermark, so
`behind_ms` is the **aggregation debt against data** (tenant's latest completed
bucket end − watermark, clamped at 0) rather than `now − watermark` — a quiet but
fully-materialized tenant reads `fresh`; thresholds: fresh ≤ 2×cadence, degraded ≤
gate (1 h), stale beyond (04-services.md §Response shapes).
| P3-8 | Session-cap contract decision (Notion §22): keep synchronous guard enforcement while deferring only summary maintenance, or make the cap analytical — documented + tested before any async session association | G1B | Written decision + tests; the hard 5,000 cap is never silently weakened |

P3-8 evidence: the decision is written in [03-write-path.md §"Session-cap contract
decision"] — keep synchronous guard enforcement (`room = 5000 - guard.total_events`
under the row lock `ingest_batch` already takes), defer only summary maintenance, and
never make the cap analytical. T28 pins it: the cap holds at exactly 5,000 with every
summary absent (T28a, plus capped retries staying `session_capped`); both
out-of-contract writer shapes trip an invariant loudly — events without a guard claim
diverge guard/events (I3), a guard bumped to match exceeds the counter cap (I2)
(T28b); after the deferred maintenance pass converges, I3 closes and history carries
`total_events = 5000` (T28c).

**G3:** dashboards read only rollups + live slice; bounce rate non-zero and plausible;
approximate labels applied to HLL metrics.

**G3 status: PASSED (P3 close-out).** Evidence: `services/dashboard/test/g3_gate.test.mjs`
— headline metrics (hourly/daily) are behavioral-proven to read the rollups + live
slice only: with 7 raw events stored and no archived stats yet, the hours/days
arrays are empty (no raw-event fallback) while the live slice reports the active
sessions; after `forceArchive` + one CAGG refresh the same range is served entirely
from `session_stats_15m` with bounce_rate = 0.6 matching the seeded 3/5 pattern,
archived sessions leave the live slice (D18), and `live: true` labelling holds both
before and after. Bounce plausibility bounds and the `approximate: true` HLL labels
are additionally pinned by the P3-3 endpoint tests. P3 rows P3-1…P3-8 are closed
(commits b816d7b, b20d92b, 3c73904, 71ca8a7, 629f74b, 99512bd, c16142f).

---

## P4 — Production hardening

| ID   | Task                                                                                                                                                                                                                                                                                                                            | Depends    | Done when                                                                |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------ |
| P4-1 | Roles and RLS: enable/force, `app_tenant()` policies with `WITH CHECK`, `security_invoker` view                                                                                                                                                                                                                                 | G3         | I9 green from `app_dashboard`; `app_ingest` holds no table grants at all |
| P4-2 | Cross-tenant automated test wired into CI                                                                                                                                                                                                                                                                                       | P4-1       | Build fails if isolation regresses                                       |
| P4-3 | Abuse protection: limits, rate limits, key rotation, cardinality alerts                                                                                                                                                                                                                                                         | P4-1       | Documented limits enforced with tests                                    |
| P4-4 | Synchronous standby + Patroni-style failover + fencing                                                                                                                                                                                                                                                                          | G3         | T11 passes; RPO 0 measured                                               |
| P4-5 | LB with consistent hash on `anon_id`                                                                                                                                                                                                                                                                                            | P4-4       | Same visitor lands on one instance; lock waits drop                      |
| P4-6 | Load test at 2x peak, 1 hour                                                                                                                                                                                                                                                                                                    | P4-3, P4-5 | p95 flush under 200 ms; error rate under 0.5%                            |
| P4-7 | Alerts, dashboards, runbooks (5 files), on-call checklist                                                                                                                                                                                                                                                                       | P4-6       | Every alert has a runbook link and a trigger test                        |
| P4-8 | Deletion workflow end-to-end incl. HMAC option and legal sign-off note                                                                                                                                                                                                                                                          | P4-2, P3-5 | T8 plus a full seven-step dry run recorded                               |
| P4-9 | Server-SDK bootstrap gates (Notion Ref 1 §5): N concurrent resumes of one expired session yield exactly one successor; resume never extends past 24 h; capped session rotated; rollback atomic; cross-tenant/forged tokens rejected. Write-once `successor_session_id` only if the D30 advisory-lock design proves insufficient | G1B        | The five Ref 1 tests pass against the built bootstrap path               |

**G4:** T11 green, zero cross-tenant reads, 2x peak sustained, runbooks and deletion
workflow reviewed.

P4-1 evidence (D36/D37): `0011_rls.sql` enables+forces RLS with the
`tenant_isolation`/`owner_maintenance` policy pair on every tenant table that exists by
then (the 0002 tenancy tables transferred to `app_owner` first); `0017_rls_access.sql`
covers the 0013/0015 tables, rebuilds the `sessions` view with tenant predicates on both
arms, adds the tenant-bound views (`events_tenant`/`history_tenant` security_invoker,
`events_15m_tenant`/`identity_15m_tenant` owner-context wrappers) and `resolve_api_key()`,
and grants app_dashboard SELECT on exactly the read surfaces. Both services read keys
through `resolve_api_key()` (zero table grants for `app_ingest`), and every raw-table read
in `services/dashboard/src/queries.ts` goes through a tenant-bound view. Three
pinned-image RLS refusals are documented in D37 (compressed hypertables, CAGG
materialization hypertables, `security_invoker` on CAGG views); isolation for those
surfaces is the fail-closed view predicate layer with no service grants on the base
relations. Done-when proof in `db/test/rls/t29_rls.test.mjs`: I9 holds from
`app_dashboard` (0 tenant-B rows across all ten granted surfaces, tenant-A rows still
visible), unset binding raises (fail-closed), `app_ingest` holds zero table privileges,
`app_dashboard` is SELECT-only with the write path and CAGG internals denied, a
cross-tenant insert from a granted scratch role is rejected by `WITH CHECK` while its
own tenant's insert is accepted, and `resolve_api_key` returns the right tenant/revoked
state with no row for an unknown hash. Full suite 256/256 (45 files).

P4-2 evidence: `.github/workflows/ci.yml` runs on every push/PR with the pinned
`timescale/timescaledb-ha:pg17` service container and `DATABASE_URL` matching the
harness default. The chain is lint → format:check → wait for Postgres → build →
typecheck (build-first: workspace imports resolve through `dist/*.d.ts`) →
`db:migrate` twice (idempotency, `_migrations` ledger asserted at 16) →
**cross-tenant isolation gate** (`pnpm exec vitest run db/test/rls`, fail-fast,
named step) → full `pnpm test` (2 workers under `CI=1` to stay inside the service
container's `max_connections=100`). The gate is also part of the full suite, so the
build fails if isolation regresses. New scripts: `test:db` (vitest run db/test, 21
files) and `db:migrate` (the migration runner CLI).

P4-5 (LB) can land before P4-4; the drill needs the LB, so treat the two as one increment
even though the IDs stay stable.

---

## P5 — Optional scaling (not approved by default)

Each item is its own decision with a stated trigger from
[07-operations.md](07-operations.md#scaling-path-p5-each-item-gated-on-a-trigger).

| ID   | Item                            | Trigger                               |
| ---- | ------------------------------- | ------------------------------------- |
| P5-1 | Vertical scale / smaller chunks | Newest chunk no longer fits in memory |
| P5-2 | Read replica for dashboards     | Query competition with ingest         |
| P5-3 | Per-tenant instance             | One tenant dominates                  |
| P5-4 | Dedupe generation tables        | > ~1,500 events/s or size dominates   |
| P5-5 | Durable queue (`pgmq`)          | Multiple consumers / outage buffering |
| P5-6 | Column store for raw events     | Billion-row ad-hoc scans              |

---

## Working agreement

- One PR per task ID; PR body links the task and the exit criterion it advances.
- DB changes ship with the test that fails without them.
- Any deviation from the PDF is added to `DEVIATIONS.md` in the same PR, and a task that
  changes a documented rule updates the owning doc in that PR too.
- A phase is closed only by the gate, not by task completion.
- No SDK work before G0: the blueprint is explicit that the concurrency suite is the gate,
  and that is also the cheapest way to discover a design flaw.
