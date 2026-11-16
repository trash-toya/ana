-- 0010_jobs.sql — job PROCEDURES only. The add_job registrations live in
-- 0014_schedule.sql (applied in staging/production; the test harness never applies it,
-- so test databases start with no custom jobs scheduled). Objects are created as
-- app_owner; grants are applied as the bootstrap role (see 0009).
SET ROLE app_owner;

\i db/sql/jobs/archive_sessions.sql
\i db/sql/jobs/rebuild_session_stats.sql
\i db/sql/jobs/cleanup_idempotency.sql
\i db/sql/jobs/reconcile_session_stats.sql

RESET ROLE;

-- app_jobs runs the scheduler. SELECT on every table it touches, DELETE on the
-- churned ones, INSERT/UPDATE on the derived ones (07-operations.md).
GRANT EXECUTE ON PROCEDURE archive_sessions(int, jsonb)            TO app_jobs;
GRANT EXECUTE ON PROCEDURE rebuild_session_stats(uuid, timestamptz, timestamptz) TO app_jobs;
GRANT EXECUTE ON PROCEDURE cleanup_idempotency(int, jsonb)         TO app_jobs;
GRANT EXECUTE ON PROCEDURE reconcile_session_stats(int, jsonb)     TO app_jobs;

GRANT SELECT, DELETE, UPDATE ON sessions_active TO app_jobs;
-- UPDATE on sessions_active: SELECT ... FOR UPDATE SKIP LOCKED in archive_sessions
-- requires it.
GRANT SELECT, DELETE ON event_dedupe, session_guard TO app_jobs;
GRANT SELECT, INSERT, UPDATE ON sessions_history TO app_jobs;
GRANT SELECT, INSERT, UPDATE, DELETE ON session_stats_15m TO app_jobs;
-- DELETE on session_stats_15m: archive_sessions removes buckets that the -old delta
-- zeroed out (a rebuild would not have that row at all).
