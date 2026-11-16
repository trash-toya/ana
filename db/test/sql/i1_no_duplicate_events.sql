-- Invariant queries (06-testing.md). Each returns a single count that must be 0.
-- The harness asserts these after every concurrency test.

-- I1: no duplicate event
SELECT count(*) AS n FROM (
  SELECT tenant_id, event_id FROM events
   GROUP BY 1, 2 HAVING count(*) > 1) x;
