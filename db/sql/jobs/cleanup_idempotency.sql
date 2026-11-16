-- cleanup_idempotency(job_id int, config jsonb) — bounded deletes by age index only.
-- 3 days = 24 h event age limit + 24 h max session length + 1 day margin.
-- Both deletes are bounded so a run never becomes an unbounded transaction.
CREATE OR REPLACE PROCEDURE cleanup_idempotency(job_id int, config jsonb)
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
  DELETE FROM event_dedupe
   WHERE ctid IN (SELECT ctid FROM event_dedupe
                   WHERE received_at < now() - INTERVAL '3 days'
                   ORDER BY received_at
                   LIMIT 200000);

  DELETE FROM session_guard
   WHERE ctid IN (SELECT ctid FROM session_guard
                   WHERE created_at < now() - INTERVAL '3 days'
                   ORDER BY created_at
                   LIMIT 100000);

  -- Bootstrap lifecycle records: 24 h resume window + margin. Bounded like the
  -- rest; a session whose events still exist keeps its analytics rows.
  DELETE FROM session_bootstrap
   WHERE ctid IN (SELECT ctid FROM session_bootstrap
                   WHERE last_event_at < now() - INTERVAL '3 days'
                   ORDER BY last_event_at
                   LIMIT 100000);
END
$$;
