-- ingest_batch(p jsonb) — the one function that applies identity, deduplication, the
-- hard 5,000-event cap and every write in one transaction.
--
-- Safety argument (03-write-path.md):
--   1. ensure guard rows            (identity exists before quota is computed)
--   2. lock guard rows, sorted      (concurrent creators serialize; no deadlock)
--   3. classify vs dedupe           (dups / conflicts / genuinely new)
--   4. rank + cap                   (rn <= room per session, ordered time,seq,event_id)
--   5. claim dedupe ONLY for stored (D1: no dedupe row for capped/conflicting events)
--   6. write events + counters      (derived from `stored` only)
--   7. upsert sessions_active       (first-wins attribution, additive counters)
--
-- Rev 5 (D29): events with `session_id IS NULL` are machine-originated. They
-- have no guard, no cap, no session summary and no identity stitching, but they
-- still go through the same dedupe + insert path. Everything session-scoped
-- filters them out instead of inventing a session.
--
-- SECURITY DEFINER, owned by app_writer, pinned search_path: the ingest role holds
-- only EXECUTE and cannot read tenant data (07-operations.md).
CREATE OR REPLACE FUNCTION ingest_batch(p jsonb) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
  r jsonb;
BEGIN
  -- 0. materialize the batch once; ordered so the lock order is visible in one place
  CREATE TEMP TABLE batch_sessions ON COMMIT DROP AS
    SELECT * FROM parse_batch(p)
   ORDER BY tenant_id, session_id, ord;

  -- 1. identity: every session in the batch has a guard row before any quota is computed
  INSERT INTO session_guard (tenant_id, session_id, home_day)
  SELECT DISTINCT tenant_id, session_id, date_trunc('day', now(), 'UTC')
    FROM batch_sessions
   WHERE session_id IS NOT NULL
   ORDER BY 1, 2
    ON CONFLICT (tenant_id, session_id) DO NOTHING;

  -- 2. lock in a fixed order: the lock exists before the session does
  PERFORM 1
    FROM session_guard g
    JOIN (SELECT DISTINCT tenant_id, session_id FROM batch_sessions
           WHERE session_id IS NOT NULL) k
      USING (tenant_id, session_id)
   ORDER BY g.tenant_id, g.session_id
     FOR UPDATE OF g;

  WITH batch AS (           -- in-batch duplicates already removed by the API; ord breaks ties
    SELECT DISTINCT ON (tenant_id, event_id) *
      FROM batch_sessions
     ORDER BY tenant_id, event_id, ord),

  seen AS (                 -- read-only classification under the guard lock
    SELECT b.tenant_id, b.event_id, b.payload_hash AS new_hash, d.payload_hash AS old_hash
      FROM batch b
      JOIN event_dedupe d
        ON d.tenant_id = b.tenant_id AND d.event_id = b.event_id),

  dups      AS (SELECT tenant_id, event_id FROM seen WHERE new_hash =  old_hash),
  conflicts AS (SELECT tenant_id, event_id FROM seen WHERE new_hash <> old_hash),

  cand AS (                 -- genuinely new events only; quota computed from cand
    SELECT b.* FROM batch b
     WHERE NOT EXISTS (SELECT 1 FROM seen s
                        WHERE s.tenant_id = b.tenant_id AND s.event_id = b.event_id)),

  ranked AS (               -- session-bound events only: the cap is a session rule
    SELECT c.*,
           row_number() OVER (PARTITION BY c.tenant_id, c.session_id
                              ORDER BY c.time, c.seq NULLS LAST, c.event_id) AS rn
      FROM cand c
     WHERE c.session_id IS NOT NULL),

  budget AS (
    SELECT g.tenant_id, g.session_id, g.home_day, 5000 - g.total_events AS room
      FROM session_guard g
      JOIN (SELECT DISTINCT tenant_id, session_id FROM batch
             WHERE session_id IS NOT NULL) k
        USING (tenant_id, session_id)),

  accepted AS (
    SELECT r.*, b.home_day FROM ranked r JOIN budget b USING (tenant_id, session_id)
     WHERE r.rn <= b.room),
  capped AS (
    SELECT r.tenant_id, r.event_id FROM ranked r JOIN budget b USING (tenant_id, session_id)
     WHERE r.rn >  b.room),

  loose AS (                -- sessionless: no cap, no home_day, dedupe still applies
    SELECT c.*, NULL::bigint AS rn, NULL::timestamptz AS home_day
      FROM cand c
     WHERE c.session_id IS NULL),

  accepted_all AS (
    SELECT * FROM accepted UNION ALL SELECT * FROM loose),

  claimed AS (              -- claim ONLY what will be stored (D1)
    INSERT INTO event_dedupe (tenant_id, event_id, payload_hash)
    SELECT tenant_id, event_id, payload_hash FROM accepted_all ORDER BY 1, 2
      ON CONFLICT (tenant_id, event_id) DO NOTHING
   RETURNING tenant_id, event_id),

  stored AS (
    SELECT a.* FROM accepted_all a JOIN claimed c USING (tenant_id, event_id)),

  lost AS (                 -- accepted but claimed by a concurrent txn: it IS stored,
    SELECT a.tenant_id, a.event_id FROM accepted_all a   -- report it as a duplicate
     WHERE NOT EXISTS (SELECT 1 FROM claimed c
                        WHERE c.tenant_id = a.tenant_id AND c.event_id = a.event_id)),

  ident AS (                -- identity stitching commits with the event, not later
    INSERT INTO identity_map (tenant_id, anon_id, user_id)
    SELECT DISTINCT tenant_id, anon_id, user_id FROM stored
     WHERE user_id IS NOT NULL AND anon_id IS NOT NULL
      ON CONFLICT (tenant_id, anon_id, user_id) DO NOTHING
   RETURNING 1),

  ins AS (
    INSERT INTO events (time, tenant_id, session_id, event_id, cat, seq, anon_id,
                        user_id, name, url, source, received_at, props)
    SELECT time, tenant_id, session_id, event_id, cat, seq, anon_id, user_id,
           name, url, source, received_at, props
      FROM stored
   RETURNING tenant_id, event_id),

  g_up AS (                 -- cap counter tracks what was actually stored
    UPDATE session_guard g
       SET total_events = g.total_events + s.n
      FROM (SELECT tenant_id, session_id, count(*) AS n
              FROM stored WHERE session_id IS NOT NULL GROUP BY 1, 2) s
     WHERE g.tenant_id = s.tenant_id AND g.session_id = s.session_id
   RETURNING 1),

  bs_up AS (                -- the bootstrap idle clock advances on real events only
    UPDATE session_bootstrap b
       SET last_event_at = GREATEST(b.last_event_at, s.max_time)
      FROM (SELECT tenant_id, session_id, max(time) AS max_time
              FROM stored WHERE session_id IS NOT NULL GROUP BY 1, 2) s
     WHERE b.tenant_id = s.tenant_id AND b.session_id = s.session_id
   RETURNING 1),

  up AS (                   -- live summary, counters from stored rows only
    INSERT INTO sessions_active AS sa
      (tenant_id, session_id, home_day, anon_id, started_at, last_seen_at,
       event_count, total_events, heartbeats, touch_bucket,
       user_id, user_at, entry_url, entry_at, device, country, utm, attr_at)
    SELECT tenant_id, session_id, min(home_day), min(anon_id),
           min(time), max(time),
           count(*) FILTER (WHERE cat = 'e'), count(*),
           count(*) FILTER (WHERE name = 'heartbeat'),
           floor(extract(epoch FROM now()) / 600)::int,
           (array_agg(user_id ORDER BY time, seq NULLS LAST, event_id)
              FILTER (WHERE user_id IS NOT NULL))[1],
           min(time) FILTER (WHERE user_id IS NOT NULL),
           (array_agg(url ORDER BY time, seq NULLS LAST, event_id)
              FILTER (WHERE name = 'page_view'))[1],
           min(time) FILTER (WHERE name = 'page_view'),
           (array_agg(props->>'device'  ORDER BY time, seq NULLS LAST, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           (array_agg(props->>'country' ORDER BY time, seq NULLS LAST, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           (array_agg(props->'utm'      ORDER BY time, seq NULLS LAST, event_id)
              FILTER (WHERE name = 'session_start'))[1],
           min(time) FILTER (WHERE name = 'session_start')
      FROM stored
     WHERE session_id IS NOT NULL
     GROUP BY tenant_id, session_id
     ORDER BY tenant_id, session_id
      ON CONFLICT (tenant_id, session_id) DO UPDATE SET
        started_at    = LEAST(sa.started_at, EXCLUDED.started_at),
        last_seen_at  = GREATEST(sa.last_seen_at, EXCLUDED.last_seen_at),
        event_count   = sa.event_count  + EXCLUDED.event_count,
        total_events  = sa.total_events + EXCLUDED.total_events,
        heartbeats    = sa.heartbeats   + EXCLUDED.heartbeats,
        touch_bucket  = EXCLUDED.touch_bucket,
        user_id   = CASE WHEN EXCLUDED.user_at  < COALESCE(sa.user_at,  'infinity')
                         THEN EXCLUDED.user_id   ELSE sa.user_id   END,
        user_at   = LEAST(sa.user_at,  EXCLUDED.user_at),
        entry_url = CASE WHEN EXCLUDED.entry_at < COALESCE(sa.entry_at, 'infinity')
                         THEN EXCLUDED.entry_url ELSE sa.entry_url END,
        entry_at  = LEAST(sa.entry_at, EXCLUDED.entry_at),
        device    = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity')
                         THEN EXCLUDED.device    ELSE sa.device    END,
        country   = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity')
                         THEN EXCLUDED.country   ELSE sa.country   END,
        utm       = CASE WHEN EXCLUDED.attr_at  < COALESCE(sa.attr_at,  'infinity')
                         THEN EXCLUDED.utm       ELSE sa.utm       END,
        attr_at   = LEAST(sa.attr_at, EXCLUDED.attr_at)
   RETURNING 1)

  SELECT jsonb_build_object(
           'stored',    coalesce((SELECT jsonb_agg(event_id) FROM stored),    '[]'::jsonb),
           'dups',      coalesce((SELECT jsonb_agg(event_id) FROM (
                          SELECT event_id FROM dups
                          UNION ALL
                          SELECT event_id FROM lost) d), '[]'::jsonb),
           'capped',    coalesce((SELECT jsonb_agg(event_id) FROM capped),    '[]'::jsonb),
           'conflicts', coalesce((SELECT jsonb_agg(event_id) FROM conflicts), '[]'::jsonb))
    INTO r;

  RETURN r;
END
$$;

COMMENT ON FUNCTION ingest_batch(jsonb) IS
  'One transaction: guard, lock, classify, cap, claim dedupe, write events, session. '
  'Sessionless events skip the cap/summary but keep dedupe. '
  'Returns stored/dups/capped/conflicts as event_id arrays.';
