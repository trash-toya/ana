-- archive_sessions(job_id int, config jsonb) — moves sessions whose last server write
-- is 40–50 minutes old (touch_bucket is 10-minute granularity) out of sessions_active,
-- merges them into sessions_history and applies exact +new/-old deltas to
-- session_stats_15m, in ONE statement inside one transaction.
--
-- Differences from the blueprint (documented in 03-write-path.md):
--   * selection ordered by (touch_bucket, tenant_id, session_id) for reproducible runs
--   * total_duration_s is numeric(20,6) and nothing is rounded (D2), so the delta path
--     and rebuild_session_stats agree exactly (I5)
CREATE OR REPLACE PROCEDURE archive_sessions(job_id int, config jsonb)
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
  n   bigint;
  cut int := floor(extract(epoch FROM now()) / 600)::int - 5;  -- last write 40+ min ago
BEGIN
  PERFORM pg_advisory_xact_lock(7001);        -- serialise with rebuild_session_stats

  -- Buckets this run contributes to (for the zero-row cleanup after the big statement).
  CREATE TEMP TABLE archive_touched ON COMMIT DROP AS
    SELECT NULL::uuid AS tenant_id, NULL::timestamptz AS bucket WHERE false;

  -- The candidates are selected BY CTID, not joined: with a USING self-join the planner
  -- re-runs the FOR UPDATE scan per candidate row (measured 34 s for 7,000 rows) and
  -- even MATERIALIZED leaves a 49M-comparison join filter. ctid + TID scan: ~20 ms.
  -- The locking read still re-checks touch_bucket after locking (EPQ), so a concurrent
  -- ingest that bumps touch_bucket drops the row from this run.
  WITH moved AS (
    DELETE FROM sessions_active
     WHERE ctid IN (
       SELECT ctid FROM sessions_active
        WHERE touch_bucket <= cut
        ORDER BY touch_bucket, tenant_id, session_id
        LIMIT 50000
          FOR UPDATE SKIP LOCKED)
   RETURNING *),
  old AS (                                    -- history rows these sessions merge into
    SELECT o.* FROM sessions_history o JOIN moved m
      ON o.tenant_id = m.tenant_id AND o.session_id = m.session_id
     AND o.home_day = m.home_day),
  merged AS (
    SELECT m.tenant_id, m.session_id, m.home_day, m.anon_id,
           LEAST(o.started_at, m.started_at)          AS started_at,
           GREATEST(o.last_seen_at, m.last_seen_at)   AS last_seen_at,
           COALESCE(o.event_count, 0) + m.event_count  AS event_count,
           COALESCE(o.total_events, 0) + m.total_events AS total_events,
           COALESCE(o.heartbeats, 0)   + m.heartbeats   AS heartbeats,
           CASE WHEN m.user_at  < COALESCE(o.user_at,  'infinity') THEN m.user_id
                ELSE o.user_id   END AS user_id,
           LEAST(o.user_at,  m.user_at)  AS user_at,
           CASE WHEN m.entry_at < COALESCE(o.entry_at, 'infinity') THEN m.entry_url
                ELSE o.entry_url END AS entry_url,
           LEAST(o.entry_at, m.entry_at) AS entry_at,
           CASE WHEN m.attr_at  < COALESCE(o.attr_at,  'infinity') THEN m.device
                ELSE o.device   END AS device,
           CASE WHEN m.attr_at  < COALESCE(o.attr_at,  'infinity') THEN m.country
                ELSE o.country  END AS country,
           CASE WHEN m.attr_at  < COALESCE(o.attr_at,  'infinity') THEN m.utm
                ELSE o.utm      END AS utm,
           LEAST(o.attr_at,  m.attr_at)  AS attr_at
      FROM moved m LEFT JOIN sessions_history o
        ON o.tenant_id = m.tenant_id AND o.session_id = m.session_id
       AND o.home_day = m.home_day),
  hist AS (
    INSERT INTO sessions_history
      (tenant_id, session_id, home_day, anon_id, started_at, last_seen_at,
       event_count, total_events, heartbeats, user_id, user_at, entry_url, entry_at,
       device, country, utm, attr_at)
    SELECT tenant_id, session_id, home_day, anon_id, started_at, last_seen_at,
           event_count, total_events, heartbeats, user_id, user_at, entry_url, entry_at,
           device, country, utm, attr_at
      FROM merged
      ON CONFLICT (tenant_id, session_id, home_day) DO UPDATE SET
        started_at    = EXCLUDED.started_at,
        last_seen_at  = EXCLUDED.last_seen_at,
        event_count   = EXCLUDED.event_count,
        total_events  = EXCLUDED.total_events,
        heartbeats    = EXCLUDED.heartbeats,
        user_id       = EXCLUDED.user_id,
        user_at       = EXCLUDED.user_at,
        entry_url     = EXCLUDED.entry_url,
        entry_at      = EXCLUDED.entry_at,
        device        = EXCLUDED.device,
        country       = EXCLUDED.country,
        utm           = EXCLUDED.utm,
        attr_at       = EXCLUDED.attr_at
   RETURNING 1),
  contrib AS (                                -- +new version, -old version: exact deltas
    SELECT tenant_id, time_bucket('15 minutes', started_at) AS bucket, 1 AS sg,
           event_count, heartbeats,
           extract(epoch FROM last_seen_at - started_at) AS dur
      FROM merged
    UNION ALL
    SELECT tenant_id, time_bucket('15 minutes', started_at), -1, event_count, heartbeats,
           extract(epoch FROM last_seen_at - started_at)
      FROM old),
  fill AS (INSERT INTO archive_touched
             SELECT DISTINCT tenant_id, bucket FROM contrib RETURNING 1),

  st AS (
    INSERT INTO session_stats_15m AS t
      (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
       total_duration_s, heartbeat_s, events)
    SELECT tenant_id, bucket, sum(sg),
           coalesce(sum(sg) FILTER (WHERE event_count = 0), 0),
           coalesce(sum(sg) FILTER (WHERE event_count = 1), 0),
           coalesce(sum(sg) FILTER (WHERE event_count = 2), 0),
           coalesce(sum(sg) FILTER (WHERE event_count >= 3), 0),
           sum(sg * dur),                    -- numeric(20,6), never rounded (D2)
           sum(sg * heartbeats) * 30,
           sum(sg * event_count)
      FROM contrib
     GROUP BY 1, 2
      ON CONFLICT (tenant_id, bucket) DO UPDATE SET
        sessions         = t.sessions         + EXCLUDED.sessions,
        ec_0             = t.ec_0             + EXCLUDED.ec_0,
        ec_1             = t.ec_1             + EXCLUDED.ec_1,
        ec_2             = t.ec_2             + EXCLUDED.ec_2,
        ec_3p            = t.ec_3p            + EXCLUDED.ec_3p,
        total_duration_s = t.total_duration_s + EXCLUDED.total_duration_s,
        heartbeat_s      = t.heartbeat_s      + EXCLUDED.heartbeat_s,
        events           = t.events           + EXCLUDED.events
   RETURNING 1)
  SELECT count(*) INTO n FROM moved;

  -- A merge can move a session to a different bucket (a late event moved started_at):
  -- the -old delta can zero out the source bucket entirely. A rebuild would not have
  -- that row at all (I5 compares row SETS), so drop any all-zero row this run touched.
  DELETE FROM session_stats_15m t
   USING archive_touched k
    WHERE t.tenant_id = k.tenant_id AND t.bucket = k.bucket
      AND t.sessions = 0 AND t.ec_0 = 0 AND t.ec_1 = 0 AND t.ec_2 = 0
      AND t.ec_3p = 0 AND t.total_duration_s = 0 AND t.heartbeat_s = 0
      AND t.events = 0;

  -- n is logged via job_stats failure accounting only if this raises; keeping the
  -- procedure silent keeps runs comparable in benchmarks.
END
$$;
