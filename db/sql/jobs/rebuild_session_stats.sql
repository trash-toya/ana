-- rebuild_session_stats(p_tenant, p_from, p_to) — statistics are a pure function of
-- sessions_history, so any range can be rebuilt. The rebuild replaces the whole range:
-- buckets whose sessions were all deleted disappear instead of lingering. Takes the
-- same advisory lock as archive_sessions, so a rebuild never interleaves with deltas.
-- Used by: the nightly reconcile, the deletion workflow, and test I5.
CREATE OR REPLACE PROCEDURE rebuild_session_stats(
  p_tenant uuid, p_from timestamptz, p_to timestamptz)
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
  v_from timestamptz := time_bucket('15 minutes', p_from);  -- align both ends to buckets
  v_to   timestamptz := time_bucket('15 minutes', p_to);
BEGIN
  PERFORM pg_advisory_xact_lock(7001);

  DELETE FROM session_stats_15m
   WHERE bucket >= v_from AND bucket < v_to
     AND (p_tenant IS NULL OR tenant_id = p_tenant);

  INSERT INTO session_stats_15m
    (tenant_id, bucket, sessions, ec_0, ec_1, ec_2, ec_3p,
     total_duration_s, heartbeat_s, events)
  SELECT tenant_id, time_bucket('15 minutes', started_at), count(*),
         count(*) FILTER (WHERE event_count = 0),
         count(*) FILTER (WHERE event_count = 1),
         count(*) FILTER (WHERE event_count = 2),
         count(*) FILTER (WHERE event_count >= 3),
         sum(extract(epoch FROM last_seen_at - started_at)),   -- numeric, unrounded (D2)
         sum(heartbeats) * 30,
         sum(event_count)
    FROM sessions_history
   WHERE home_day >= date_trunc('day', v_from - INTERVAL '1 day', 'UTC')  -- chunk pruning
     AND started_at >= v_from AND started_at < v_to
     AND (p_tenant IS NULL OR tenant_id = p_tenant)
   GROUP BY 1, 2;
END
$$;
