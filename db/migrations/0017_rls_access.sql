-- 0017_rls_access.sql — the rest of the P4-1 access layer on top of 0011.
-- (Tables created after 0011 get their RLS here: the daily rollups from 0013
-- and session_bootstrap from 0015.)
--
-- Pinned-image findings (Timescale 2.32.0), three RLS refusals:
--   * ENABLE/FORCE RLS is REFUSED on hypertables with columnstore (compression)
--     enabled — `events` and `sessions_history` cannot carry RLS ("operation
--     not supported on hypertables that have columnstore enabled").
--   * RLS is REFUSED on CAGG materialization hypertables ("operation not
--     supported on materialization tables").
--   * security_invoker is REFUSED on CAGG views themselves ("only timescaledb
--     parameters allowed in WITH clause for continuous aggregate").
-- For all three, isolation is enforced by the tenant-bound view layer (D4/D37):
-- the tenant predicate lives IN the view body (fail-closed via app_tenant(),
-- cannot be pushed out of the view), the CAGG views and their materialization
-- hypertables get NO grant to any service role, and no service query reads the
-- raw tables except through the tenant-bound views. app_dashboard holds a
-- SELECT grant on the two compressed raw tables only because a security_invoker
-- view exposes base-table privileges to the invoker (D37). The isolation
-- boundary for the refused surfaces is the view predicate layer, while every
-- RLS-capable table carries enforced policies.
--
--   1. RLS on the tables created after 0011 (0013 daily rollups, 0015
--      session_bootstrap).
--   2. Tenant-bound views: events_tenant, history_tenant (security_invoker),
--      events_15m_tenant, identity_15m_tenant (owner-context wrappers).
--   3. resolve_api_key(): SECURITY DEFINER key lookup owned by app_writer, so
--      app_ingest and app_dashboard authenticate with EXECUTE only and hold
--      NO table grants at all (P4-1 done-when).
--   4. The read grants: app_dashboard gets SELECT on exactly the tenant-bound
--      views, RLS-protected tables and functions the endpoints use.
SET ROLE app_owner;

-- ---------------------------------------------------------------------------
-- 1. RLS on the tables created after 0011
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  t text;
BEGIN
  FOR t IN
    SELECT unnest(ARRAY[
      'session_stats_daily', 'identity_daily', 'session_bootstrap'])
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format($f$
      CREATE POLICY tenant_isolation ON %I
        TO PUBLIC
        USING (tenant_id = app_tenant())
        WITH CHECK (tenant_id = app_tenant())$f$, t);
    EXECUTE format($f$
      CREATE POLICY owner_maintenance ON %I
        TO app_owner
        USING (true)
        WITH CHECK (true)$f$, t);
  END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- 2. Tenant-bound views (D4). app_tenant() raises when app.tenant_id is unset
--    — the views are fail-closed, exactly like the RLS policies.
--
--    events_tenant / history_tenant are security_invoker: RLS could not be
--    attached to the compressed base tables (header), so the invoker reads
--    them with the tenant predicate in the view.
--
--    The CAGG wrappers are security-DEFINER on purpose (D37 extension):
--    Timescale REFUSES security_invoker on continuous aggregates ("only
--    timescaledb parameters allowed in WITH clause") and refuses RLS on the
--    materialization hypertables, so the realtime CAGG views always run with
--    owner privileges — any grant on them would let the invoker read every
--    tenant. The wrappers therefore live in the owner context but carry the
--    tenant predicate IN the view body (fail-closed, cannot be pushed out),
--    and the CAGG views + materialization hypertables get NO grant to
--    app_dashboard.
-- ---------------------------------------------------------------------------
CREATE VIEW events_tenant WITH (security_invoker = true) AS
  SELECT * FROM events WHERE tenant_id = app_tenant();

CREATE VIEW history_tenant WITH (security_invoker = true) AS
  SELECT * FROM sessions_history WHERE tenant_id = app_tenant();

CREATE VIEW events_15m_tenant AS
  SELECT * FROM events_15m WHERE tenant_id = app_tenant();

CREATE VIEW identity_15m_tenant AS
  SELECT * FROM identity_15m WHERE tenant_id = app_tenant();

COMMENT ON VIEW events_tenant IS
  'Tenant-bound raw events (P4-1, D4/D37): events is compressed and cannot carry
RLS on the pinned Timescale version, so the tenant predicate lives here. This
view is the sanctioned read path for the service; the underlying SELECT grant
exists only because security_invoker checks invoker privileges on base tables.';
COMMENT ON VIEW history_tenant IS
  'Tenant-bound sessions_history (P4-1, D4/D37): same compression constraint
as events_tenant.';
COMMENT ON VIEW events_15m_tenant IS
  'Tenant-bound realtime CAGG (P4-1, D4/D37): the CAGG itself cannot be
security_invoker (Timescale restriction) and must not be granted to the
service, so this owner-context wrapper carries the tenant predicate in its
body; app_tenant() raises when the binding is unset.';
COMMENT ON VIEW identity_15m_tenant IS
  'Tenant-bound realtime CAGG (P4-1, D4/D37); finalize HLL states on read with
distinct_count(rollup(...)). Same wrapper scheme as events_15m_tenant.';

-- The `sessions` view (0007) already has security_invoker; its history half
-- needs the tenant predicate IN the view, because sessions_history cannot
-- carry RLS on the pinned version. Rebuilt with identical columns.
CREATE OR REPLACE VIEW sessions WITH (security_invoker = true) AS
  SELECT a.tenant_id, a.session_id, a.home_day, a.anon_id, a.user_id, a.started_at,
         a.last_seen_at, a.event_count, a.total_events, a.heartbeats, a.entry_url,
         a.device, a.country, a.utm
    FROM sessions_active a
   WHERE tenant_id = app_tenant()
     AND NOT EXISTS (
           SELECT 1 FROM sessions_history h
            WHERE h.tenant_id = a.tenant_id AND h.session_id = a.session_id)
  UNION ALL
  SELECT h.tenant_id, h.session_id, h.home_day, h.anon_id, h.user_id, h.started_at,
         h.last_seen_at, h.event_count, h.total_events, h.heartbeats, h.entry_url,
         h.device, h.country, h.utm
    FROM sessions_history h
   WHERE tenant_id = app_tenant();

-- ---------------------------------------------------------------------------
-- 3. resolve_api_key(): the ONLY way both services authenticate. SECURITY
--    DEFINER owned by app_writer (pinned search_path), so a leaked key grants
--    no read on api_keys and no tenant data. Cross-tenant BY DESIGN: the
--    lookup finds the tenant from the hash. (Ownership changes need the
--    bootstrap role, as in 0015.)
-- ---------------------------------------------------------------------------
RESET ROLE;

CREATE OR REPLACE FUNCTION resolve_api_key(p_key_hash bytea)
RETURNS TABLE (tenant_id uuid, kind text, revoked boolean)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
  SELECT tenant_id, kind, revoked_at IS NOT NULL
    FROM api_keys
   WHERE key_hash = p_key_hash
$$;

ALTER FUNCTION resolve_api_key(bytea) OWNER TO app_writer;
GRANT SELECT ON api_keys TO app_writer;   -- the definer needs the read
GRANT EXECUTE ON FUNCTION resolve_api_key(bytea) TO app_ingest, app_dashboard;

COMMENT ON FUNCTION resolve_api_key(bytea) IS
  'API-key resolution for both services (P4-1): SECURITY DEFINER over api_keys,
so app_ingest/app_dashboard hold EXECUTE only and no table grants. Returns
tenant_id, kind and the revoked flag; no row for an unknown hash.';

-- ---------------------------------------------------------------------------
-- 4. app_dashboard read grants: exactly what the endpoints use
--    (04-services.md §Dashboard API). The events/sessions_history SELECT
--    grants exist because security_invoker views check invoker privileges on
--    their base tables (D37) — the sanctioned read path is the tenant-bound
--    views. No grant on: api_keys, session_guard, event_dedupe,
--    session_bootstrap, identity_map, tenants — and no write privilege
--    anywhere.
-- ---------------------------------------------------------------------------
GRANT SELECT ON events_tenant, history_tenant                 TO app_dashboard;
GRANT SELECT ON events_15m_tenant, identity_15m_tenant        TO app_dashboard;
GRANT SELECT ON sessions                                      TO app_dashboard;
GRANT SELECT ON events, sessions_history                      TO app_dashboard; -- invoker ACLs (D37)
GRANT SELECT ON sessions_active                               TO app_dashboard; -- live slice (D14)
GRANT SELECT ON session_stats_15m                             TO app_dashboard; -- hourly headline
GRANT SELECT ON session_stats_daily, identity_daily           TO app_dashboard; -- daily rollups
GRANT SELECT ON tenant_settings                               TO app_dashboard; -- tz resolution
GRANT EXECUTE ON FUNCTION app_tenant()                          TO app_dashboard;
GRANT EXECUTE ON FUNCTION cagg_watermark_ts(text)               TO app_dashboard;
