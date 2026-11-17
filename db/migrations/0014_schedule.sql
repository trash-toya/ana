-- 0014_schedule.sql — Timescale add_job registrations. STAGING/PRODUCTION ONLY:
-- the test harness never applies this file and asserts that none of the jobs below is
-- scheduled (06-testing.md). This pinned Timescale version's add_job() has no owner
-- argument; the job owner is the role that calls it, so the registrations run under
-- SET ROLE app_jobs. That keeps the scheduler working after a failover and gives every
-- job the intended privilege set.
SET ROLE app_jobs;

DO $$
BEGIN
  PERFORM add_job('archive_sessions',        INTERVAL '1 minute');
  PERFORM add_job('cleanup_idempotency',     INTERVAL '1 minute');
  PERFORM add_job('reconcile_session_stats', INTERVAL '1 day');
  PERFORM add_job('rollup_daily',            INTERVAL '1 day');
END
$$;

RESET ROLE;
