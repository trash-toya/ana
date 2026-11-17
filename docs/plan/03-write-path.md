# 03 — Write path and jobs

One function, `ingest_batch(jsonb)`, does identity, deduplication, cap and writes in one
transaction. Order matters and is the whole safety argument:

```
ensure guard rows  →  lock guard rows (sorted)  →  classify vs dedupe  →  rank + cap
→  claim dedupe for accepted only  →  insert events  →  guard counter  →  upsert active
→  return outcomes
```

Step 5 is where we differ from the PDF: the PDF claims dedupe rows for the whole batch
_before_ the cap, so a capped event leaves a dedupe row behind, and its retry is
reported as an accepted duplicate while no event row exists. That is silent data loss
([DEVIATIONS.md](DEVIATIONS.md) D1). Claiming only for accepted events makes
`event_dedupe` a ledger of stored events, so "duplicate" always means "already stored".

## `ingest_batch(p jsonb)`

Rev 5 (D29): a batch may also contain **sessionless** rows (`session_id IS NULL`).
They go through the same dedupe/claim path but skip the guard, rank and budget CTEs
(`loose`), and are excluded from `sessions_active`, `sessions_history`, `identity_map`
and the cap. The sketch below shows the session-bound path; the actual CTEs (including
`loose`) are in `db/sql/functions/ingest_batch.sql`.

```sql
CREATE FUNCTION ingest_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE r jsonb;
BEGIN
  -- 1. identity: every session in the batch has a guard row before any quota is computed
  INSERT INTO session_guard (tenant_id, session_id, home_day)
  SELECT DISTINCT tenant_id, session_id, date_trunc('day', now(), 'UTC')
  FROM parse_batch(p) ORDER BY 1, 2
  ON CONFLICT (tenant_id, session_id) DO NOTHING;

  -- 2. lock in a fixed order: the lock exists before the session does
  PERFORM 1 FROM session_guard g
  JOIN (SELECT DISTINCT tenant_id, session_id FROM parse_batch(p)) k
    USING (tenant_id, session_id)
  ORDER BY g.tenant_id, g.session_id
  FOR UPDATE OF g;

  WITH batch AS (            -- in-batch duplicates already removed by the API; ord breaks ties
    SELECT DISTINCT ON (tenant_id, event_id) *
    FROM parse_batch(p) ORDER BY tenant_id, event_id, ord),

  seen AS (                  -- read-only classification under the guard lock
    SELECT b.tenant_id, b.event_id, b.payload_hash AS new_hash, d.payload_hash AS old_hash
    FROM batch b
    JOIN event_dedupe d ON d.tenant_id = b.tenant_id AND d.event_id = b.event_id),

  dups AS (SELECT tenant_id, event_id FROM seen WHERE new_hash =  old_hash),
  conflicts AS (SELECT tenant_id, event_id FROM seen WHERE new_hash <> old_hash),

  cand AS (                  -- genuinely new events only
    SELECT b.* FROM batch b
    WHERE NOT EXISTS (SELECT 1 FROM seen s
                      WHERE s.tenant_id = b.tenant_id AND s.event_id = b.event_id)),

  ranked AS (
    SELECT c.*, row_number() OVER (PARTITION BY c.tenant_id, c.session_id
                                   ORDER BY c.time, c.seq, c.event_id) AS rn
    FROM cand c),

  budget AS (
    SELECT g.tenant_id, g.session_id, g.home_day,
           5000 - g.total_events AS room
    FROM session_guard g
    JOIN (SELECT DISTINCT tenant_id, session_id FROM batch) k USING (tenant_id, session_id)),

  accepted AS (SELECT r.*, b.home_day FROM ranked r JOIN budget b USING (tenant_id, session_id)
               WHERE r.rn <= b.room),
  capped   AS (SELECT r.tenant_id, r.event_id FROM ranked r JOIN budget b USING (tenant_id, session_id)
               WHERE r.rn >  b.room),

  claimed AS (               -- claim ONLY what will be stored
    INSERT INTO event_dedupe (tenant_id, event_id, payload_hash)
    SELECT tenant_id, event_id, payload_hash FROM accepted ORDER BY 1, 2
    ON CONFLICT (tenant_id, event_id) DO NOTHING
    RETURNING tenant_id, event_id),

  stored AS (
    SELECT a.* FROM accepted a
    JOIN claimed c USING (tenant_id, event_id)),

  lost AS (                  -- accepted but claimed by a concurrent txn: it IS stored, report it accepted
    SELECT a.tenant_id, a.event_id FROM accepted a
    WHERE NOT EXISTS (SELECT 1 FROM claimed c
                      WHERE c.tenant_id = a.tenant_id AND c.event_id = a.event_id)),

  ident AS (                 -- identity stitching commits with the event, not in a second statement
    INSERT INTO identity_map (tenant_id, anon_id, user_id)
    SELECT DISTINCT tenant_id, anon_id, user_id FROM stored WHERE user_id IS NOT NULL
    ON CONFLICT (tenant_id, anon_id, user_id) DO NOTHING
    RETURNING 1),

  ins AS (
    INSERT INTO events (time, tenant_id, session_id, event_id, cat, seq, anon_id, user_id,
                        name, url, props)
    SELECT time, tenant_id, session_id, event_id, cat, seq, anon_id, user_id, name, url, props
    FROM stored
    RETURNING tenant_id, event_id),

  g_up AS (                  -- cap counter tracks what was actually stored
    UPDATE session_guard g SET total_events = g.total_events + s.n
    FROM (SELECT tenant_id, session_id, count(*) AS n FROM stored GROUP BY 1, 2) s
    WHERE g.tenant_id = s.tenant_id AND g.session_id = s.session_id
    RETURNING 1),

  up AS (                    -- live summary, counters from stored rows only
    INSERT INTO sessions_active AS sa (tenant_id, session_id, home_day, anon_id, started_at,
        last_seen_at, event_count, total_events, heartbeats, touch_bucket,
        user_id, user_at, entry_url, entry_at, device, country, utm, attr_at)
    SELECT tenant_id, session_id, min(home_day), min(anon_id), min(time), max(time),
           count(*) FILTER (WHERE cat = 'e'), count(*),
           count(*) FILTER (WHERE name = 'heartbeat'),
           floor(extract(epoch FROM now()) / 600)::int,
           (array_agg(user_id ORDER BY time, seq, event_id)
              FILTER (WHERE user_id IS NOT NULL))[1],
           min(time) FILTER (WHERE user_id IS NOT NULL),
           (array_agg(url ORDER BY time, seq, event_id)
              FILTER (WHERE name = 'page_view'))[1],
           min(time) FILTER (WHERE name = 'page_view'),
           (array_agg(props->>'device'  ORDER BY time, seq, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           (array_agg(props->>'country' ORDER BY time, seq, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           (array_agg(props->'utm'      ORDER BY time, seq, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           min(time) FILTER (WHERE name = 'session_start')
    FROM stored GROUP BY tenant_id, session_id
    ORDER BY tenant_id, session_id
    ON CONFLICT (tenant_id, session_id) DO UPDATE SET
      started_at    = LEAST(sa.started_at, EXCLUDED.started_at),
      last_seen_at  = GREATEST(sa.last_seen_at, EXCLUDED.last_seen_at),
      event_count   = sa.event_count  + EXCLUDED.event_count,
      total_events  = sa.total_events + EXCLUDED.total_events,
      heartbeats    = sa.heartbeats   + EXCLUDED.heartbeats,
      touch_bucket  = EXCLUDED.touch_bucket,
      user_id   = CASE WHEN EXCLUDED.user_at  < COALESCE(sa.user_at,  'infinity') THEN EXCLUDED.user_id   ELSE sa.user_id   END,
      user_at   = LEAST(sa.user_at,  EXCLUDED.user_at),
      entry_url = CASE WHEN EXCLUDED.entry_at < COALESCE(sa.entry_at, 'infinity') THEN EXCLUDED.entry_url ELSE sa.entry_url END,
      entry_at  = LEAST(sa.entry_at, EXCLUDED.entry_at),
      device    = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity') THEN EXCLUDED.device    ELSE sa.device    END,
      country   = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity') THEN EXCLUDED.country   ELSE sa.country   END,
      utm       = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity') THEN EXCLUDED.utm       ELSE sa.utm       END,
      attr_at   = LEAST(sa.attr_at, EXCLUDED.attr_at)
    RETURNING 1)
  SELECT jsonb_build_object(
    'stored',    coalesce((SELECT jsonb_agg(event_id) FROM stored),  '[]'),
    'dups',      coalesce((SELECT jsonb_agg(event_id) FROM (
                   SELECT event_id FROM dups UNION ALL SELECT event_id FROM lost) d), '[]'),
    'capped',    coalesce((SELECT jsonb_agg(event_id) FROM capped),  '[]'),
    'conflicts', coalesce((SELECT jsonb_agg(event_id) FROM conflicts),'[]'))
  INTO r;

  RETURN r;
END $$;
```

### Why this is correct

| Property                          | Argument                                                                                                                                                                                                                                                                         |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cap is hard                       | Step 1 creates the guard, step 2 locks it, so concurrent creators of a new session serialize on one row and each reads the true `total_events`. Archived sessions keep their guard for 3 days, so the cap survives archiving                                                     |
| No deadlock                       | Every writer takes guard locks first, sorted by `(tenant_id, session_id)`, and only then touches `event_dedupe`, `events`, `sessions_active`                                                                                                                                     |
| Duplicate never consumes quota    | `cand` excludes everything already in `event_dedupe`, and quota is computed from `cand` only                                                                                                                                                                                     |
| Rejected event stays rejected     | capped/conflicting events never reach `event_dedupe`, so a retry is evaluated again and rejected again with the same reason                                                                                                                                                      |
| No double count                   | counters (`guard.total_events`, `sessions_active.*`) are derived from `stored`, which is `claimed` ∩ `accepted`                                                                                                                                                                  |
| Every submitted ID has an outcome | `stored` + `dups` + `lost` + `capped` + `conflicts` cover the batch; `lost` is a cross-session claim race, which is an accepted duplicate because the winning transaction stored the event. The API also maps any unlisted submitted ID to `accepted` as a defensive second gate |
| Retry is free                     | a retry lands in `dups` and is reported `accepted` — but only because the event really is stored                                                                                                                                                                                 |

### `parse_batch`

Unchanged from the PDF: `jsonb_to_recordset` into a typed row set including `ord`,
`payload_hash`, and the server-classified `cat`. `cat` is computed by the API, never by
SQL: the shared package owns the name→category table, and an `event_names` seed table
mirrors it for the registry (the seed is generated from the package, not typed twice).
`ingest_batch()` calls `parse_batch(p)` in three places; it is `IMMUTABLE` and cheap, but
the implementation materialises it once as `batch_sessions` so the lock order is visible
in one place.

### Outcome mapping (API side)

| Function output                 | Response                                                                      | SDK action                   | Counter                   |
| ------------------------------- | ----------------------------------------------------------------------------- | ---------------------------- | ------------------------- |
| `stored`                        | `accepted`                                                                    | remove from queue            | `ingest_stored`           |
| `dups`                          | `accepted` (idempotent; includes events that lost a cross-session claim race) | remove from queue            | `ingest_duplicate`        |
| `capped`                        | `rejected: session_capped`                                                    | remove from queue, permanent | `ingest_capped`           |
| `conflicts`                     | `rejected: conflicting_duplicate`                                             | remove from queue, permanent | `dedupe_conflict`         |
| validation failure (never sent) | `rejected: <reason>`                                                          | remove from queue, permanent | `ingest_rejected{reason}` |

## Jobs

All jobs are Timescale `add_job` procedures. **No `COMMIT` inside a procedure**; each run
is one bounded transaction. At-least-once execution is safe because every job is
idempotent. The `add_job` calls live in `0014_schedule.sql`, which is applied as
`app_jobs`; migrations `0001`–`0013` only define the procedures, so test databases start
with no custom jobs scheduled.

| Job                       | Interval  | Bound                   | Idempotency argument                                                                                                                                                                                                                                                                                                                                                |
| ------------------------- | --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `archive_sessions`        | 1 min     | 50,000 sessions         | Delete-and-merge in one statement; `FOR UPDATE SKIP LOCKED` + re-checked `touch_bucket`; selection ordered by `touch_bucket, tenant_id, session_id` for reproducible runs; candidates selected by `ctid` (a `USING` self-join lets the planner re-run the locking scan per row — measured 34 s for 7,000 rows vs 0.13 s with the ctid form; see `benchmarks/p0.md`) |
| `rebuild_session_stats`   | on demand | whole range             | Deletes the range first, then recomputes from `sessions_history`                                                                                                                                                                                                                                                                                                    |
| `cleanup_idempotency`     | 1 min     | 200k dedupe, 100k guard | Deletes by age index only                                                                                                                                                                                                                                                                                                                                           |
| `reconcile_session_stats` | 1 day     | 26–50 h ago             | Compares before/after; logs drift, never "fixes" silently                                                                                                                                                                                                                                                                                                           |
| `rollup_daily` (P3)       | 1 day     | days 2–4 old            | Upsert by local day, rewritten three nights                                                                                                                                                                                                                                                                                                                         |

### `archive_sessions`

As in the PDF, with two changes:

1. **Exact deltas with additive duration.** `contrib` emits `+new` and `-old` rows with
   exact per-session seconds; the upsert sums them into `numeric` totals. This makes
   `rebuild_session_stats` and the delta path agree exactly, which is what invariant I5
   asserts ([DEVIATIONS.md](DEVIATIONS.md) D2).
2. **Advisory lock `7001`** is taken by both `archive_sessions` and
   `rebuild_session_stats`, so a rebuild never interleaves with deltas.

Cutoff: `cut = floor(epoch(now())/600)::int - 5` → sessions whose last write was 40–50
minutes ago. The batch of 50,000 has headroom: one minute of backlog at 1,000 events/s is
~7,500 sessions. A partial row created by a late event gets `touch_bucket = now()`, so it
waits a full window before merging even though `last_seen_at` is old; that is deliberate,
and it is why readers exclude active rows that already have history
([DEVIATIONS.md](DEVIATIONS.md) D18).

### `rebuild_session_stats(p_tenant, p_from, p_to)`

Deletes the aligned bucket range first (so a bucket whose sessions were all deleted
disappears), then recomputes from `sessions_history` with
`home_day >= date_trunc('day', v_from - INTERVAL '1 day', 'UTC')` for chunk pruning.

Used by: the nightly reconcile, the deletion workflow, and test I5.

### `reconcile_session_stats`

Compares the full row (sessions, all `ec_*`, duration, heartbeats, events) for
`now()-50h .. now()-26h`, not just the session count. Any nonzero delta is a bug signal,
not a maintenance step ([DEVIATIONS.md](DEVIATIONS.md) D12).

## Session-cap contract decision (P3-8, Notion §22)

**Decision: keep synchronous guard enforcement; defer only summary maintenance. The
cap is never analytical.** Decided in P3, before any async session association work in
P4; pinned by T28 (`db/test/concurrency/t28_cap_contract.test.mjs`).

The hard 5,000-event cap is a **contract with three parties**: the SDK (rotates the
session locally at 5,000 events, [05-sdks.md](05-sdks.md) — the server cap is the
backstop, not the primary mechanism), the storage model (bounds per-session row
counts and write amplification), and the rejection API (`session_capped`, a stable
verdict that idempotent retries keep receiving, D1/D9). Those properties only hold if
the quota decision is made atomically with the event insert — which is what
`ingest_batch` does: `room = 5000 - guard.total_events` read under the guard row lock
the identity step already takes. There is no extra lock, no extra round trip, and the
measured cost is negligible ([benchmarks/p0.md](benchmarks/p0.md)).

Why not the alternative (an analytical cap — count events per session after the fact
and cap at reconciliation time): an analytical cap admits over-cap windows between
insert and rebuild; its verdict is not stable (a retry's answer can change while
summaries lag); it is O(rows) per decision; and it can be **silently weakened** by an
async writer that inserts first and lets maintenance converge later. Rejected.

Rules every future writer (P4 async session association, backfill tools, deletion
repair) must hold, with the invariant that makes each auditable:

1. **Single quota authority.** Quota comes from `session_guard.total_events` under the
   row lock, inside the same transaction that inserts the event rows. The sanctioned
   path is `ingest_batch()`. _(I3: guard matches events.)_
2. **Summaries are deferrable, the guard is not.** `sessions_active`,
   `sessions_history`, `session_stats_15m` and the daily rollups are rebuildable and
   may lag — they are never inputs to the cap, and deleting or deferring them does
   not change a quota decision. _(T28a: cap holds at exactly 5,000 with every summary
   absent; T28c: a later maintenance pass converges them, I3 closes.)_
3. **No direct event-row writers.** Any path that writes `events` rows for a session
   without claiming room is a bug that must fail loudly in CI and alerts, never a
   tolerated state. _(T28b: both writer shapes trip an invariant — events-without-guard
   diverges (I3); guard-bumped-to-match exceeds the cap (I2).)_
4. **Rejections stay stable.** Once `session_capped`, retries return `session_capped`
   forever; capped events leave no dedupe claim (D1/D9). _(T28a retry assertion.)_
5. **The cap never moves silently.** Raising or removing the 5,000 limit is a schema +
   contract change (SDK rotation, docs, tests) — not a config knob.

## Failure semantics

| Event                                         | What the client sees                                                                                                                                 | What the database shows                                           |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Transaction rolls back                        | 503 for every event in the batch; SDK keeps all                                                                                                      | Nothing (atomic)                                                  |
| Crash mid-transaction                         | Same as rollback                                                                                                                                     | Nothing; connection dies, transaction aborts                      |
| Concurrent identical retries                  | Both get `accepted`; one is `dups`                                                                                                                   | One event row                                                     |
| Concurrent conflicting writes to one event_id | The first commit wins; the other is `conflicting_duplicate` (within one request the API keeps the first by request order; `ord` is the SQL fallback) | One event row, one hash                                           |
| Cap reached mid-batch                         | Overflow events `session_capped`                                                                                                                     | `guard.total_events = 5000` exactly                               |
| Late event for archived session               | `accepted`                                                                                                                                           | Partial active row + guard; next archive merges with exact deltas |

## Deliverables in P0

1. `db/sql/functions/ingest_batch.sql` with the statement above plus `parse_batch` and
   `app_tenant()`; migration `0009` sources these files and `0010` sources the job files,
   so there is one copy of each. `ingest_batch()` is `SECURITY DEFINER`, owned by
   `app_writer`, with a pinned `search_path`.
2. Job procedures and `add_job` registrations.
3. A `db/test/sql/` harness with the invariant queries and one SQL scenario per worked
   example in the PDF (retries, out-of-order attribution, cap across archiving).
4. The T1–T9 parallel-worker suite ([06-testing.md](06-testing.md)).
