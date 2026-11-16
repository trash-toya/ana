-- I8: no orphan events inside the guard window (integrity check, not an FK)
SELECT count(*) AS n
  FROM events e
 WHERE e.time >= now() - INTERVAL '3 days'
   AND NOT EXISTS (SELECT 1 FROM session_guard g
                    WHERE g.tenant_id = e.tenant_id
                      AND g.session_id = e.session_id);
