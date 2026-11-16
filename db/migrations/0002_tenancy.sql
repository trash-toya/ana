-- 0002_tenancy.sql — tenants, api_keys, tenant_settings, identity_map and the five
-- service roles. Applied by the bootstrap role (role creation needs it).
-- From 0003 on, migrations run under SET ROLE app_owner (see db/test/README.md).

CREATE TABLE tenants (
  tenant_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE api_keys (
  key_hash    bytea PRIMARY KEY,            -- sha256 of the bearer key, never the key
  tenant_id   uuid NOT NULL REFERENCES tenants,
  kind        text NOT NULL CHECK (kind IN ('ingest', 'dashboard')),
  origin_hint text,                         -- allow-list hint only, not a boundary
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz);
CREATE INDEX api_keys_tenant ON api_keys (tenant_id, kind) WHERE revoked_at IS NULL;

CREATE TABLE tenant_settings (
  tenant_id uuid PRIMARY KEY REFERENCES tenants,
  tz        text NOT NULL DEFAULT 'UTC');

-- Written inside ingest_batch() (INSERT ... ON CONFLICT DO NOTHING over stored rows),
-- so it commits with the event (02-data-model.md). Powers resolved-people counts and
-- the deletion workflow.
CREATE TABLE identity_map (
  tenant_id  uuid NOT NULL,
  anon_id    text NOT NULL,
  user_id    text NOT NULL,
  first_seen timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, anon_id, user_id));
CREATE INDEX identity_map_user ON identity_map (tenant_id, user_id);

-- Roles (07-operations.md, with D20): app_owner and app_jobs are LOGIN because
-- Timescale background tasks (policies, the scheduler) refuse NOLOGIN owners
-- ("Hypertable owner must have LOGIN permission to run background tasks"). Production
-- restricts these logins via pg_hba.conf to the migration/scheduler host.
-- app_writer stays NOLOGIN: it only owns the definer function.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner') THEN
    CREATE ROLE app_owner LOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_writer') THEN
    CREATE ROLE app_writer NOLOGIN BYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ingest') THEN
    CREATE ROLE app_ingest LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_dashboard') THEN
    CREATE ROLE app_dashboard LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_jobs') THEN
    CREATE ROLE app_jobs LOGIN BYPASSRLS;
  END IF;
END
$$;

-- app_owner owns the schema; PG 15+ no longer grants CREATE on public to everyone.
ALTER SCHEMA public OWNER TO app_owner;

-- Key policy: up to two active ingest keys and at most one dashboard key per tenant,
-- enforced by the provisioning script, not DDL (02-data-model.md).
