-- I4: one history row per session
SELECT count(*) AS n FROM (
  SELECT tenant_id, session_id FROM sessions_history
   GROUP BY 1, 2 HAVING count(*) > 1) x;
