-- 0011_rls.sql — row level security for the tenant tables (P4-1). Every
-- tenant-scoped table gets RLS ENABLED + FORCED with two policies:
--
--   tenant_isolation  TO PUBLIC  USING/WITH CHECK (tenant_id = app_tenant())
--     The fail-closed resolver raises when app.tenant_id is unset, so a
--     missing tenant binding aborts instead of matching nothing. The
--     WITH CHECK half stops cross-tenant inserts.
--   owner_maintenance TO app_owner USING/WITH CHECK (true)
--     Timescale background workers (CAGG refresh, compression, retention)
--     run as the table owner (app_owner) with no tenant context; forced RLS
--     with only the tenant policy would make app_tenant() raise inside them.
--     app_owner is the migration/maintenance principal (pg_hba-restricted)
--     and already owns the schema, so this bypass adds no new capability
--     (DEVIATIONS.md D36). The isolation boundary is between tenants, via
--     app_ingest/app_dashboard, both NOSUPERUSER NOBYPASSRLS.
--
-- The bypass role set is therefore exactly: postgres (superuser), app_owner
-- (owner policy), app_writer/app_jobs (BYPASSRLS). Writers that must see
-- cross-tenant data all sit in that set by design.
--
-- EXCLUDED (pinned-image finding, Timescale 2.32.0): `events` and
-- `sessions_history` are compressed (columnstore) hypertables, and Timescale
-- REFUSES RLS changes on them ("operation not supported on hypertables that
-- have columnstore enabled"). Their isolation is enforced by the access layer
-- in 0017: no app role holds a raw grant, and app_dashboard reads them only
-- through the tenant-bound security_invoker views (D4).
--
-- api_keys is NOT tenant-RLS-protected: key authentication finds the tenant
-- FROM the key hash, cross-tenant by definition. It is protected by grants —
-- no app role holds table grants on it; both services resolve keys through
-- resolve_api_key() (0017), a SECURITY DEFINER function owned by app_writer.
-- The 0002 tenancy tables were created by the bootstrap role; P4-1 makes
-- app_owner the single owner of the schema (RLS requires ownership). The
-- transfer itself needs the current owner, so it runs before SET ROLE.
ALTER TABLE tenants         OWNER TO app_owner;
ALTER TABLE api_keys        OWNER TO app_owner;
ALTER TABLE tenant_settings OWNER TO app_owner;
ALTER TABLE identity_map    OWNER TO app_owner;

SET ROLE app_owner;

DO $$
DECLARE
  t text;
BEGIN
  FOR t IN
    SELECT unnest(ARRAY[
      'sessions_active', 'session_guard', 'event_dedupe',
      'session_stats_15m', 'identity_map', 'tenants', 'tenant_settings'])
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

COMMENT ON POLICY tenant_isolation ON sessions_active IS
  'Tenant isolation (P4-1, I9): rows are visible/writable only with the
matching app.tenant_id; unset binding raises (fail-closed). Applied to every
tenant table (0011 for the early tables, 0017 for 0013/0015 tables and the
CAGG materialization hypertables).';
COMMENT ON POLICY owner_maintenance ON sessions_active IS
  'Owner bypass for Timescale background workers that run without a tenant
context (CAGG refresh, compression, retention). See 0011 header / D36.';

RESET ROLE;
