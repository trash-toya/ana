-- app_tenant() — fail-closed tenant resolver for RLS policies (P4) and any
-- tenant-scoped SQL. Raises if app.tenant_id is unset instead of matching nothing.
-- Set with SELECT set_config('app.tenant_id', $1, true) inside an explicit
-- transaction (transaction pooling compatible; 07-operations.md).
CREATE OR REPLACE FUNCTION app_tenant() RETURNS uuid
LANGUAGE sql STABLE
SET search_path = pg_catalog, public
AS $$
  SELECT current_setting('app.tenant_id', false)::uuid
$$;

COMMENT ON FUNCTION app_tenant() IS
  'Tenant from app.tenant_id; raises if unset (fail-closed). Transaction-local.';
