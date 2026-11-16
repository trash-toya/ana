-- I6: dedupe is a ledger of stored events — zero orphans
SELECT count(*) AS n
  FROM event_dedupe d
  LEFT JOIN events e
    ON e.tenant_id = d.tenant_id AND e.event_id = d.event_id
 WHERE e.event_id IS NULL;
