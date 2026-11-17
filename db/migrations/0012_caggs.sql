-- 0012_caggs.sql — the two real-time continuous aggregates (P3-1).
-- Source: 02-data-model.md migration list, 04-services.md endpoint sources.
--
-- Design (spiked on the pinned image: Timescale 2.32.0 / toolkit 1.26.0):
--   * `count(DISTINCT ...)` IS supported inside a CAGG on this version, so
--     events_15m carries an EXACT distinct-session count per bucket.
--   * `distinct_count(rollup(hyperloglog(...)))` cannot be nested in the CAGG
--     definition ("aggregate function calls cannot be nested"); the CAGG
--     materializes the HLL STATE and the finalization happens on read:
--     `SELECT distinct_count(rollup(users_hll)) FROM identity_15m ...`.
--     HLL over small cardininalities is exact and deterministic; the ~1.6%
--     error label applies at scale (04-services.md rule 4: "approximate").
--   * user_id is nullable: the users sketch must FILTER NULLs, or anonymous
--     traffic would corrupt the user counts (verified in the P0 spike).
--   * materialized_only=false: dashboards read the union of the materialized
--     buckets and the raw hypertable, so the newest (never-yet-refreshed)
--     bucket is always live. This is the freshness story until P3-7 formalizes it.
--
-- RLS (0011, P4) cannot be applied to CAGGs; they are exposed only through
-- security_invoker views with a forced tenant predicate (D4).
SET ROLE app_owner;

CREATE MATERIALIZED VIEW events_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT tenant_id,
       time_bucket(INTERVAL '15 minutes', time) AS bucket,
       count(*)                                 AS events,
       count(*) FILTER (WHERE cat = 'e')        AS engaged_events,
       count(DISTINCT session_id)               AS sessions
FROM events
GROUP BY tenant_id, time_bucket(INTERVAL '15 minutes', time)
WITH NO DATA;

CREATE MATERIALIZED VIEW identity_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT tenant_id,
       time_bucket(INTERVAL '15 minutes', time) AS bucket,
       -- D4 note: the sketches are exposed through tenant-bound views only.
       hyperloglog(4096, anon_id)                                    AS anon_hll,
       hyperloglog(4096, user_id) FILTER (WHERE user_id IS NOT NULL) AS users_hll
FROM events
GROUP BY tenant_id, time_bucket(INTERVAL '15 minutes', time)
WITH NO DATA;

-- Backfill: refresh the full history. On a fresh install this is a no-op over
-- empty hypertables; on an upgrade with 90 days of raw data it materializes
-- everything the retention window can still serve. NULL, NULL = the whole range.
CALL refresh_continuous_aggregate('events_15m', NULL, NULL);
CALL refresh_continuous_aggregate('identity_15m', NULL, NULL);

RESET ROLE;
-- Policy jobs must be owned by a LOGIN role (Timescale requirement; same rule
-- as 0003/0008). The test harness deletes policy_* jobs after migrating, so
-- test databases stay hermetic; production gets the 15-minute refresh cadence.
SELECT add_retention_policy('events_15m', INTERVAL '90 days');
SELECT add_retention_policy('identity_15m', INTERVAL '90 days');
-- The newest partial bucket stays out of the materialization (end_offset = one
-- bucket): refreshing a bucket that is still receiving writes is wasted work,
-- and the real-time layer serves it anyway. start_offset = 5 days: the nightly
-- rollup_daily consumes local days 2–4 old, and late events can land there —
-- a 2-day window would leave those sketches stale forever (see 0013).
SELECT add_continuous_aggregate_policy(
  'events_15m',
  start_offset      => INTERVAL '5 days',
  end_offset        => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '15 minutes');
SELECT add_continuous_aggregate_policy(
  'identity_15m',
  start_offset      => INTERVAL '5 days',
  end_offset        => INTERVAL '15 minutes',
  schedule_interval => INTERVAL '15 minutes');
