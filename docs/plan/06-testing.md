# 06 — Testing

The blueprint's position is correct and we adopt it as a gate: **most remaining risk is
transactional, not infrastructural**, so the concurrency suite is built before any SDK
work and must pass before P1 starts.

## Harness

| Piece         | Choice                                                                                                                                                                                                                                                                                          | Why                                                  |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Runner        | `node --test` (Node 24)                                                                                                                                                                                                                                                                         | No extra dependency; same runtime as the services    |
| DB client     | `pg` with N independent `Pool`s                                                                                                                                                                                                                                                                 | Concurrency tests need real connections, not a mock  |
| Environment   | Compose from [07-operations.md](07-operations.md)                                                                                                                                                                                                                                               | Every test runs against pinned TimescaleDB           |
| Isolation     | One database per test file, created from migrations, dropped after                                                                                                                                                                                                                              | Parallel CI without cross-talk                       |
| Fixtures      | `db/test/fixtures/*.sql` + typed TS builders in `packages/shared/test`                                                                                                                                                                                                                          | Deterministic tenants, sessions, events              |
| Clock control | `SET`-based fake "now" is not possible for `now()`. Tests pre-create `session_guard` rows with the desired `home_day`, pass explicit event `time`s, and `UPDATE` `touch_bucket`/`created_at`/`received_at` where a job must see age; `ingest_batch()` overwrites `home_day` only for new guards | Avoids flakiness without a test-only clock parameter |

`db/test/README.md` documents how to run: `npm run test:db` (Compose up → migrate → run).
The harness applies every migration for the phase under test (`0001`–`0010` in P0) and
**not** `0014_schedule.sql`, then asserts that no job from `archive_sessions`,
`rebuild_session_stats`, `cleanup_idempotency`, `reconcile_session_stats` or `rollup_daily`
is scheduled. It also **deletes every Timescale compression/retention policy job**
(`policy_*`): tests write data months in the past, and a 90-day retention policy would
otherwise drop the chunks under the test mid-run (observed during P0). Test databases are
fully hermetic — no background job mutates state.

## Concurrency suite (T1–T11, T28)

Each test runs many workers in parallel, then asserts the invariants in section
[02-data-model.md](02-data-model.md).

| #   | Scenario                                                                                                                     | Expected result                                                                                                                                                               | Phase |
| --- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| T1  | 20 workers create the same new session at once, 500 distinct events each (10,000 total)                                      | Exactly 5,000 stored; `guard.total_events = 5000`; other 5,000 `session_capped`; one session row; **zero dedupe rows for capped events** (I6)                                 | P0    |
| T2  | Fill to 4,900, force archiving, send 200 late events                                                                         | 100 stored; one history row with `total_events = 5000`; stats session count unchanged; duration delta exact                                                                   | P0    |
| T3  | 10 workers send the same 500 events, repeating across a midnight boundary (clock faked by shifting `home_day`/`received_at`) | Each event stored once; no duplicates                                                                                                                                         | P0    |
| T4  | Same `event_id` twice in one request with different payloads; and across two requests                                        | Within one request the API keeps the first by request order; across requests the first commit wins. The other is `conflicting_duplicate`; conflict counter incremented        | P0    |
| T5  | Three batches with different `user_id`, `entry_url`, campaign data, in all six arrival orders                                | Identical final attribution in every order                                                                                                                                    | P0    |
| T6  | Continuous ingest on many sessions while `archive_sessions` runs every few seconds                                           | No lost events; no session archived while being written; no session twice in history                                                                                          | P0    |
| T7  | Random late partials merged into history                                                                                     | `session_stats_15m` equals a full rebuild: zero drift on **every** column                                                                                                     | P0    |
| T8  | Delete a tenant's sessions, then rebuild the range                                                                           | No stale bucket; zero sessions for the tenant in the range                                                                                                                    | P0    |
| T9  | Terminate the backend mid-transaction (`pg_terminate_backend`), repeatedly, at every statement boundary                      | No partial effects: events, dedupe, guard and session agree                                                                                                                   | P0    |
| T10 | RN SDK: `shutdown()` with a request in flight then `init()`; batch with one invalid event                                    | No mutation after shutdown; valid events stored and removed; nothing lost                                                                                                     | P2    |
| T11 | Failover drill under load                                                                                                    | No acknowledged batch lost; jobs resume; measured RPO/RTO within targets                                                                                                      | P4    |
| T28 | P3-8 cap contract: summaries deferred/deleted, out-of-contract writers, reconcile convergence                                | Cap still stops at exactly 5,000 from the guard alone; capped retries stay `session_capped`; both direct-writer shapes trip I3/I2 loudly; maintenance pass converges (I3 = 0) | P3    |

### T1 detail (the test that proves the cap fix)

T1 is also the regression test for the dedupe/cap ordering defect. Beyond the aggregate
assertions it checks:

```sql
-- no dedupe row may exist for an event that was not stored
SELECT count(*) FROM event_dedupe d
LEFT JOIN events e ON e.tenant_id = d.tenant_id AND e.event_id = d.event_id
WHERE e.event_id IS NULL;                                  -- must be 0

-- and a retried capped event must still be rejected, not "accepted as duplicate"
--   send the same capped event_id again -> reason must remain session_capped
```

Without the fix, the first query returns 5,000 and the retry is reported `accepted`.

### Invariant queries (after every test)

```sql
-- I1 no duplicate events
SELECT count(*) FROM (SELECT tenant_id, event_id FROM events
                      GROUP BY 1,2 HAVING count(*) > 1) x;                    -- 0

-- I2 cap never exceeded
SELECT count(*) FROM session_guard WHERE total_events > 5000;                 -- 0

-- I3 guard matches events (within the guard's lifetime)
SELECT count(*) FROM session_guard g
WHERE g.total_events <> (SELECT count(*) FROM events e
                         WHERE e.tenant_id = g.tenant_id
                           AND e.session_id = g.session_id
                           AND e.time >= g.created_at - INTERVAL '24 hours');  -- 0

-- I4 one history row per session
SELECT count(*) FROM (SELECT tenant_id, session_id FROM sessions_history
                      GROUP BY 1,2 HAVING count(*) > 1) x;                    -- 0

-- I6 dedupe is a ledger of stored events
SELECT count(*) FROM event_dedupe d
LEFT JOIN events e ON e.tenant_id = d.tenant_id AND e.event_id = d.event_id
WHERE e.event_id IS NULL;                                                     -- 0
```

I5 (statistics equal a rebuild) and I7–I9 are separate assertions with their own
fixtures: I5 needs a snapshot table, I7 is a monitor, and I9 needs the dashboard role.

## Property tests

A generator produces random event streams (out-of-order times, duplicates, late
partials, gaps around 30 min, cap boundaries) and runs them through two paths:

1. `ingest_batch()` → `archive_sessions()` → `session_stats_15m`
2. the same events loaded directly into `sessions_history` → `rebuild_session_stats()`

The two must be byte-identical, compared with a symmetric `EXCEPT ALL` over the full
`session_stats_15m` row set (via a snapshot table). The second path uses an independent
merge implementation — a plain TypeScript model of the session rules — not the archive
procedure itself, otherwise a bug in the merge is replicated in the oracle. This is the
strongest single check on the delta logic and is what makes I5 trustworthy at scale.

## Scenario tests from the PDF's worked examples

Each of these is a fixture with exact expected output — they are cheap and catch
regressions in the merge logic:

- retries (`e5` re-sent with different `sent_at` → still one row),
- out-of-order attribution (B then C → `user_456` at 10:03 wins),
- cap across archiving (4,900 + 200 → 100 stored, one history row).

## API and SDK tests

| Layer         | Approach                                                                                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Ingest API    | `supertest` against the Express app with a real DB; assert per-event outcomes, 400/401/413/429/503 paths, `Retry-After`, and the "every ID in exactly one list" property |
| Buffering     | Fake timers; assert one `ingest_batch()` per flush, no stacked flushes, 503 on buffer overflow                                                                           |
| Dashboard API | Two tenants; assert zero cross-tenant rows with `app_dashboard` (I9) and that queries never touch raw `events` for headline metrics                                      |
| SDKs          | The shared conformance suite in [05-sdks.md](05-sdks.md) plus T10 for RN                                                                                                 |

## CI

```
lint → typecheck → unit → db:migrate → T1–T9 → invariants → property (short seed set)
```

- `test:db` is a required check on every PR touching `db/`.
- The property test runs a shorter seed set on PRs and a longer one nightly.
- T11 is not in PR CI; it runs on the staging Compose stack during drills.

## Load tests (P4, targets from blueprint section 10)

| Test                  | Target                                                                      |
| --------------------- | --------------------------------------------------------------------------- |
| Sustained             | 2x expected peak for 1 hour                                                 |
| Burst                 | 5x peak for 1 minute, p95 flush under 200 ms                                |
| One hot session       | Many writers; lock wait stays small with sticky routing                     |
| Dedupe churn          | Oldest row < 3.5 days and row count within ±10% over an hour at steady rate |
| WAL with sync standby | Within standby headroom; commit p95 acceptable                              |
| HOT ratio             | > 95% on `sessions_active` and `session_guard`                              |
| Archive duration      | Oldest `sessions_active` row < 100 min (2× the window)                      |
| CAGG refresh          | Well under the 5-minute schedule                                            |
| Dashboard p95         | Under 500 ms with ingest at peak                                            |

Results are recorded in `docs/plan/benchmarks/` (new file per run) so sizing claims stay
honest — the blueprint's capacity numbers are targets, not measurements.
