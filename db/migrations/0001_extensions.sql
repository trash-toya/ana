-- 0001_extensions.sql — extensions. Applied by the bootstrap role (superuser),
-- because CREATE EXTENSION for these needs it. Pinned-image gate: the healthcheck
-- also asserts timescaledb_toolkit (ops/docker-compose.yml, db:ping).
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- pg_stat_statements is loadable only if the image has it in shared_preload_libraries;
-- it is an operational nicety, not a P0 dependency (07-operations.md benchmarking).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements'
  ) THEN
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_stat_statements unavailable on this image; skipping';
END
$$;
