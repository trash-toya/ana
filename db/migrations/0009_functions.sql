-- 0009_functions.sql — the write-path functions. Sources db/sql/functions/*.sql so
-- there is exactly one copy of each. Objects are created as app_owner; the
-- SECURITY DEFINER ownership and the grants are applied as the bootstrap role,
-- because PostgreSQL 16+ requires the grantor to be able to SET ROLE to the grantee.
SET ROLE app_owner;

\i db/sql/functions/app_tenant.sql
\i db/sql/functions/parse_batch.sql
\i db/sql/functions/ingest_batch.sql

RESET ROLE;

-- SECURITY DEFINER: owned by app_writer (BYPASSRLS, no login), pinned search_path.
ALTER FUNCTION ingest_batch(jsonb) OWNER TO app_writer;

-- The ingest service holds only EXECUTE; it has no table privileges at all, so a
-- leaked ingest key cannot read tenant data (07-operations.md).
GRANT EXECUTE ON FUNCTION ingest_batch(jsonb) TO app_ingest;

-- parse_batch and app_tenant run inside ingest_batch as app_writer.
GRANT EXECUTE ON FUNCTION app_tenant()       TO app_writer;
GRANT EXECUTE ON FUNCTION parse_batch(jsonb) TO app_writer;

-- The definer role needs every privilege the function body uses. SELECT on the
-- written tables is required for INSERT ... RETURNING / ON CONFLICT DO UPDATE.
GRANT INSERT, SELECT ON events, event_dedupe, identity_map TO app_writer;
GRANT INSERT, SELECT, UPDATE ON session_guard   TO app_writer;
GRANT INSERT, SELECT, UPDATE ON sessions_active TO app_writer;
