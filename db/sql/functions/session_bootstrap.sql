-- session_bootstrap.sql — the bootstrap/resume endpoint's only database access
-- (Notion Rev 5 §19.2, D30). SECURITY DEFINER, owned by app_writer: the ingest
-- role holds EXECUTE only, exactly like ingest_batch.
--
-- A bootstrapped session has NO events yet, so nothing is written to
-- sessions_active/history here. `session_bootstrap` is the lifecycle record;
-- `session_guard` is created at bootstrap because "the lock exists before the
-- session does" (same invariant as ingest_batch), and it carries the stored
-- event count the server SDK uses to decide whether to emit session_start.
--
-- Resume is NOT human activity: it reads the idle clock but never moves it
-- (§19.4). Only ingest_batch advances `last_event_at`.

CREATE OR REPLACE FUNCTION bootstrap_session(p jsonb) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
  v_tenant  uuid := (p->>'tenant_id')::uuid;
  v_anon    text := p->>'anon_id';
  v_user    text := NULLIF(p->>'user_id', '');
  v_props   jsonb := COALESCE(p->'properties', '{}'::jsonb);
  v_now     timestamptz := COALESCE((p->>'now')::timestamptz, now());
  v_row     session_bootstrap%ROWTYPE;
  v_total   integer := 0;
  v_resumed boolean := false;
BEGIN
  IF v_tenant IS NULL OR v_anon IS NULL THEN
    RAISE EXCEPTION 'bootstrap_session: tenant_id and anon_id are required';
  END IF;

  -- Concurrent bootstraps for one visitor serialize here, so a race returns
  -- one session instead of creating several (§19.2).
  PERFORM pg_advisory_xact_lock(hashtextextended(v_tenant::text || ':' || v_anon, 0));

  SELECT * INTO v_row
    FROM session_bootstrap b
   WHERE b.tenant_id = v_tenant
     AND b.anon_id = v_anon
     AND b.started_at    > v_now - INTERVAL '24 hours'
     AND b.last_event_at > v_now - INTERVAL '30 minutes'
     -- Anonymous → signed-in keeps the session; a different known user rotates.
     AND (b.user_id IS NULL OR v_user IS NULL OR b.user_id = v_user)
   ORDER BY b.started_at DESC
   LIMIT 1;

  IF FOUND THEN
    v_resumed := true;
    IF v_row.user_id IS NULL AND v_user IS NOT NULL THEN
      UPDATE session_bootstrap
         SET user_id = v_user
       WHERE tenant_id = v_row.tenant_id AND session_id = v_row.session_id
       RETURNING * INTO v_row;
    END IF;
  ELSE
    v_row.tenant_id := v_tenant;
    v_row.session_id := gen_random_uuid();
    v_row.anon_id := v_anon;
    v_row.user_id := v_user;
    v_row.started_at := v_now;
    v_row.last_event_at := v_now;
    v_row.props := COALESCE(v_props, '{}'::jsonb);
    INSERT INTO session_bootstrap (tenant_id, session_id, anon_id, user_id,
                                   started_at, last_event_at, props)
    VALUES (v_row.tenant_id, v_row.session_id, v_row.anon_id, v_row.user_id,
            v_row.started_at, v_row.last_event_at, v_row.props)
      ON CONFLICT (tenant_id, session_id) DO NOTHING;
    INSERT INTO session_guard (tenant_id, session_id, home_day)
    VALUES (v_tenant, v_row.session_id, date_trunc('day', v_now, 'UTC'))
      ON CONFLICT (tenant_id, session_id) DO NOTHING;
  END IF;

  SELECT g.total_events INTO v_total
    FROM session_guard g
   WHERE g.tenant_id = v_row.tenant_id AND g.session_id = v_row.session_id;
  v_total := COALESCE(v_total, 0);

  RETURN jsonb_build_object(
    'session_id',     v_row.session_id,
    'anon_id',        v_row.anon_id,
    'user_id',        v_row.user_id,
    'started_at',     v_row.started_at,
    'last_event_at',  v_row.last_event_at,
    'expires_at',     LEAST(v_row.started_at + INTERVAL '24 hours',
                            v_row.last_event_at + INTERVAL '30 minutes'),
    'resumed',        v_resumed,
    'emitted_events', v_total,
    'properties',     v_row.props);
END
$$;

COMMENT ON FUNCTION bootstrap_session(jsonb) IS
  'Create or resume a session for a visitor under an advisory lock. No analytics rows; '
  'resume does not move the idle clock.';

-- Validation read for POST /v1/sessions/validate. Returns found=false for an
-- unknown session; the API decides expiry from `expires_at`.
CREATE OR REPLACE FUNCTION lookup_session(
  p_tenant  uuid,
  p_session uuid,
  p_now     timestamptz DEFAULT now()
) RETURNS jsonb
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  SELECT COALESCE((
    SELECT jsonb_build_object(
             'found',          true,
             'anon_id',        b.anon_id,
             'user_id',        b.user_id,
             'started_at',     b.started_at,
             'last_event_at',  b.last_event_at,
             'expires_at',     LEAST(b.started_at + INTERVAL '24 hours',
                                     b.last_event_at + INTERVAL '30 minutes'),
             'emitted_events', COALESCE(g.total_events, 0),
             'properties',     b.props)
      FROM session_bootstrap b
      LEFT JOIN session_guard g USING (tenant_id, session_id)
     WHERE b.tenant_id = p_tenant
       AND b.session_id = p_session
  ), '{"found": false}'::jsonb)
$$;

COMMENT ON FUNCTION lookup_session(uuid, uuid, timestamptz) IS
  'Session validation read for a propagated context: identity, expiry and stored event count.';
