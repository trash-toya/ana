-- I10 — tenant event-name cardinality (P4-3 abuse monitor).
--
-- Event names are a low-cardinality dimension by design: the SDK sends a small,
-- stable set. A tenant whose distinct-name count explodes is either emitting
-- unbounded machine-generated names (a client bug that bloats every rollup) or
-- probing the pipeline. This is a MONITOR, not enforcement: alert on any row,
-- page on sustained growth (07-operations.md, ops/runbooks/abuse-protection.md).
--
-- Window: one hour of receipt time (the raw table's recent chunks only), so the
-- query is cheap enough to run every minute. Threshold: 100 distinct names per
-- tenant per hour — a generous ceiling that no documented SDK produces.
SELECT tenant_id,
       count(DISTINCT name)::int AS event_names
  FROM events
 WHERE received_at >= now() - interval '1 hour'
 GROUP BY tenant_id
HAVING count(DISTINCT name) > 100
 ORDER BY event_names DESC, tenant_id;
