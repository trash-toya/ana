-- 0013_daily.sql — the daily rollup tables and the nightly job (P3-2).
-- Source: 02-data-model.md "Daily tables", 03-write-path.md jobs table.
--
-- session_stats_daily is a pure function of session_stats_15m (which is itself
-- a pure function of sessions_history — D2/G3 chain); identity_daily rolls the
-- identity_15m HLL STATES up by local day, so multi-day windows keep composing
-- (T24c property). Finalization stays on read: distinct_count(rollup(...)).
--
-- Bounce definition (01-overview.md: session_stats_15m is authoritative for
-- "sessions, engagement events, duration and bounce"): a session is bounced
-- when it produced <= 1 engagement event (ec_0 + ec_1). The rate is
-- bounced / sessions, displayed not stored.
--
-- Days 2–4 old only (local days [current_date-4, current_date-1)): a local day
-- is rewritten on three consecutive nights before it is final, so late
-- partials (the reconcile window is 26–50 h) settle before the oldest raw
-- data ages out. Yesterday and today are NEVER written: their local day is
-- still open, and the live slice serves them (G3: rollups + live slice).
SET ROLE app_owner;

CREATE TABLE session_stats_daily (
  tenant_id        uuid           NOT NULL,
  local_day        date           NOT NULL,
  sessions         bigint         NOT NULL,
  bounced          bigint         NOT NULL,  -- <= 1 engagement event (ec_0 + ec_1)
  total_duration_s numeric(20, 6) NOT NULL,  -- exact sum of the 15m numerics (D2)
  heartbeat_s      bigint         NOT NULL,
  events           bigint         NOT NULL,
  PRIMARY KEY (tenant_id, local_day)
);

CREATE TABLE identity_daily (
  tenant_id uuid        NOT NULL,
  local_day date        NOT NULL,
  anon_hll  hyperloglog,             -- rolled state; NULL when the day had no events
  users_hll hyperloglog,             -- NULL when no event carried a user_id
  PRIMARY KEY (tenant_id, local_day)
);

CREATE OR REPLACE PROCEDURE rollup_daily(job_id int, config jsonb)
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
  -- Local days [current_date-4, current_date-1): D-4, D-3, D-2 rewritten each
  -- night; D-1 and D never (still open locally — "partial days never written").
  -- Day cast idiom: time_bucket(..., tz) returns the local-midnight TIMESTAMPTZ;
  -- a bare ::date re-interprets it in the SESSION timezone and undoes the tz
  -- shift (found by T25a). `AT TIME ZONE tz` first, then ::date.
  v_from date := current_date - 4;    -- inclusive
  v_to   date := current_date - 1;    -- exclusive
  -- UTC prune window. A local day D under the widest real-world offsets
  -- (+14 Kiribati, -12 Etc/GMT+12) spans UTC [D-14h, D+1+12h); ±14 h around
  -- the date bounds prunes every bucket the exact per-tenant expression below
  -- could pick. The expression filter is authoritative; the window only
  -- bounds the scan.
  v_utc_from timestamptz := v_from::timestamp AT TIME ZONE 'UTC' - INTERVAL '14 hours';
  v_utc_to   timestamptz := v_to::timestamp AT TIME ZONE 'UTC' + INTERVAL '14 hours';
BEGIN
  -- Same advisory lock as rebuild_session_stats / archive_sessions (7001):
  -- never read a 15-minute range mid-rebuild, and never run two rollups at once.
  PERFORM pg_advisory_xact_lock(7001);
  -- Late events can land in 2–4-day-old buckets (client clocks, retries).
  -- Staleness is handled by the REFRESH POLICY, not here: 0012 sets its
  -- start_offset to 5 days (covering the rollup window), so any late arrival
  -- is re-materialized within the 15-minute cadence and rollup_daily always
  -- reads fresh sketches. (refresh_continuous_aggregate() is a procedure and
  -- cannot be nested inside this one — verified against the pinned version.)

  DELETE FROM session_stats_daily WHERE local_day >= v_from AND local_day < v_to;
  DELETE FROM identity_daily      WHERE local_day >= v_from AND local_day < v_to;

  INSERT INTO session_stats_daily
    (tenant_id, local_day, sessions, bounced, total_duration_s, heartbeat_s, events)
  SELECT s.tenant_id,
         (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date,
         sum(s.sessions),
         sum(s.ec_0 + s.ec_1),
         sum(s.total_duration_s),
         sum(s.heartbeat_s),
         sum(s.events)
    FROM session_stats_15m s
    LEFT JOIN tenant_settings tz ON tz.tenant_id = s.tenant_id
   WHERE s.bucket >= v_utc_from
     AND s.bucket < v_utc_to
     AND (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date >= v_from
     AND (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date < v_to
   GROUP BY 1, 2;

  INSERT INTO identity_daily (tenant_id, local_day, anon_hll, users_hll)
  SELECT s.tenant_id,
         (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date,
         rollup(s.anon_hll),
         rollup(s.users_hll)
    FROM identity_15m s
    LEFT JOIN tenant_settings tz ON tz.tenant_id = s.tenant_id
   WHERE s.bucket >= v_utc_from
     AND s.bucket < v_utc_to
     AND (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date >= v_from
     AND (time_bucket(INTERVAL '1 day', s.bucket, COALESCE(tz.tz, 'UTC'))
            AT TIME ZONE COALESCE(tz.tz, 'UTC'))::date < v_to
   GROUP BY 1, 2;
END
$$;
