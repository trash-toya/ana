-- reconcile_session_stats(job_id int, config jsonb) — nightly safety net, NOT the
-- correctness mechanism. With exact deltas it should change nothing; any drift is a
-- bug signal (D12: compares the FULL row — sessions, ec_0..ec_3p, duration,
-- heartbeats, events — not just the session count).
CREATE OR REPLACE PROCEDURE reconcile_session_stats(job_id int, config jsonb)
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
  before_diff bigint;
  after_diff  bigint;
BEGIN
  -- Full-row snapshot of the range before the rebuild.
  CREATE TEMP TABLE reconcile_before ON COMMIT DROP AS
    SELECT * FROM session_stats_15m
     WHERE bucket >= now() - INTERVAL '50 hours'
       AND bucket <  now() - INTERVAL '26 hours';

  CALL rebuild_session_stats(NULL::uuid,
                             now() - INTERVAL '50 hours',
                             now() - INTERVAL '26 hours');

  -- Symmetric full-row comparison (EXCEPT ALL in both directions). Any row that
  -- differs in any column, any extra or missing row, is drift.
  SELECT count(*) INTO before_diff FROM (
    (SELECT * FROM reconcile_before
      EXCEPT ALL
     SELECT * FROM session_stats_15m
      WHERE bucket >= now() - INTERVAL '50 hours'
        AND bucket <  now() - INTERVAL '26 hours')
    UNION ALL
    (SELECT * FROM session_stats_15m
      WHERE bucket >= now() - INTERVAL '50 hours'
        AND bucket <  now() - INTERVAL '26 hours'
      EXCEPT ALL
     SELECT * FROM reconcile_before)) d;

  IF before_diff > 0 THEN
    RAISE WARNING 'session stats drift: % row(s) differ over the 26-50h window',
      before_diff;
  END IF;
END
$$;
