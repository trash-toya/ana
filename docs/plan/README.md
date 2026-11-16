# Execution plan — Session-Based Analytics on Postgres + TimescaleDB

Source of truth: [`blueprint_v4.pdf`](../../blueprint_v4.pdf) (Revision 4, 30 pages).
These documents convert the blueprint into an executable, reviewable build plan.

## Decisions already made

| Decision | Choice | Recorded in |
| --- | --- | --- |
| Scope | Full system planned; **Week 0 executed and reviewed first** | [08-execution.md](08-execution.md) |
| API stack | **Express.js** (Node 24, TypeScript) for ingest and dashboard APIs | [04-services.md](04-services.md) |
| Database environment | **Docker Compose, `timescale/timescaledb-ha`**, plus PgBouncer | [07-operations.md](07-operations.md) |
| Blueprint defects | **Fix, with a documented deviations appendix** | [DEVIATIONS.md](DEVIATIONS.md) |

## Document map

| Doc | Contents | Audience |
| --- | --- | --- |
| [01-overview.md](01-overview.md) | Goals, non-goals, repo layout, phase map, definition of done, risks | Everyone |
| [02-data-model.md](02-data-model.md) | Tables, keys, indexes, retention, RLS, invariants, migration file list | DB reviewers |
| [03-write-path.md](03-write-path.md) | Corrected `ingest_batch()` algorithm, archive/rebuild/cleanup/rollup jobs, outcome mapping | DB + backend |
| [04-services.md](04-services.md) | Express ingest API (buffering, validation, auth, outcomes), dashboard API, shared package | Backend |
| [05-sdks.md](05-sdks.md) | Web SDK, React Native SDK, shared conformance suite | Client |
| [06-testing.md](06-testing.md) | Test harness, T1–T11, invariants, CI, load tests | QA / all |
| [07-operations.md](07-operations.md) | Compose topology, PgBouncer, roles, replication, monitoring, runbook, deletion workflow, sizing | Ops |
| [08-execution.md](08-execution.md) | Phased backlog P0–P5 with tasks, dependencies and exit criteria | Delivery |
| [DEVIATIONS.md](DEVIATIONS.md) | Every intentional difference from the PDF, with rationale and covering test | Reviewers |

## Approval gates

Execution starts only after this plan is approved. Each phase closes with a review
against its exit criteria in [08-execution.md](08-execution.md); a phase that fails its
criteria is not "mostly done", it is open.

| Gate | Phase | What the reviewer signs off |
| --- | --- | --- |
| G0 (next) | P0 — database core | Schema + `ingest_batch()` + jobs pass T1–T9 under parallel workers on the pinned image; first benchmark numbers recorded |
| G1 | P1 — ingest API + minimal web SDK | End-to-end events visible in SQL; per-event outcome contract honoured |
| G2 | P2 — durable clients | Replay after app kill and mid-batch invalid event produce identical session counts (T10) |
| G3 | P3 — rollups and dashboards | Dashboards read only rollups + live slice; bounce non-zero and plausible |
| G4 | P4 — production hardening | Failover drill (T11), zero cross-tenant reads, 2x peak load test, runbook and deletion workflow |
| G5 | P5 — optional scaling | Only if a trigger in section 12 fires; each item separately approved |

## Status

- [x] Plan drafted
- [x] Review findings addressed (2026-11-14)
- [ ] Plan approved
- [ ] P0 started
