-- I3: guard matches stored events (within the guard's lifetime)
SELECT count(*) AS n FROM session_guard g
 WHERE g.total_events <> (SELECT count(*) FROM events e
                           WHERE e.tenant_id = g.tenant_id
                             AND e.session_id = g.session_id
                             AND e.time >= g.created_at - INTERVAL '24 hours');
