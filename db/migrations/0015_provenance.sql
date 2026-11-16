-- 0015_provenance.sql — Rev 5 server-first collection (D28–D32).
--
-- 1. Events carry provenance (`source`) and the analytics receipt time
--    (`received_at`) separately from the occurrence time (`time`).
-- 2. `session_id`/`anon_id` become nullable: machine-originated events
--    (background jobs, webhooks) have no human session (D29).
-- 3. `session_bootstrap` backs the bootstrap/resume endpoint: a session exists
--    there before it has any events, so bootstrap creates no analytics rows.
-- 4. API keys gain the `server` kind; only it may create sessions or emit
--    server/webhook-sourced events (D28).
SET ROLE app_owner;

-- --- events -----------------------------------------------------------------
-- Occurrence time stays `time`; receipt time is new. Timescale refuses a
-- non-constant default (now()) on a columnstore hypertable, so add it nullable,
-- backfill from the occurrence time, then enforce NOT NULL. The write path
-- always supplies received_at; there is no column default by design.
-- Every step is re-runnable: a migration that fails halfway must be safe to
-- retry (this file replaces parse_batch below, which can fail on its own).
ALTER TABLE events ALTER COLUMN session_id DROP NOT NULL;
ALTER TABLE events ALTER COLUMN anon_id    DROP NOT NULL;
ALTER TABLE events ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'client';
ALTER TABLE events ADD COLUMN IF NOT EXISTS received_at timestamptz;
UPDATE events SET received_at = time WHERE received_at IS NULL;
ALTER TABLE events ALTER COLUMN received_at SET NOT NULL;
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_source_check;
ALTER TABLE events ADD CONSTRAINT events_source_check
  CHECK (source IN ('client', 'server', 'webhook'));
-- Server-originated event review ("what did the backend emit for this tenant?").
CREATE INDEX IF NOT EXISTS events_source ON events (tenant_id, source, time DESC);

-- --- session bootstrap/resume ----------------------------------------------
CREATE TABLE IF NOT EXISTS session_bootstrap (
  tenant_id     uuid        NOT NULL,
  session_id    uuid        NOT NULL,
  anon_id       text        NOT NULL,
  user_id       text,
  started_at    timestamptz NOT NULL,
  last_event_at timestamptz NOT NULL,   -- advanced by ingest_batch only
  props         jsonb       NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, session_id)
);
-- Resume lookup: latest session for a visitor.
CREATE INDEX IF NOT EXISTS session_bootstrap_anon ON session_bootstrap (tenant_id, anon_id, started_at DESC);
-- Bounded age cleanup (cleanup_idempotency).
CREATE INDEX IF NOT EXISTS session_bootstrap_age ON session_bootstrap (last_event_at);

-- --- api_keys ---------------------------------------------------------------
-- api_keys is owned by the bootstrap role (created in 0002), so this DDL runs
-- after RESET ROLE below.

RESET ROLE;
ALTER TABLE api_keys DROP CONSTRAINT IF EXISTS api_keys_kind_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_kind_check
  CHECK (kind IN ('ingest', 'server', 'dashboard'));

-- --- write path -------------------------------------------------------------
-- ingest_batch is owned by app_writer after 0009, but app_writer has no schema
-- privileges (it owns only the function). Drop it as the bootstrap role,
-- recreate it as app_owner (which owns schema public), then transfer ownership
-- below. It is recreated in the same migration run.
RESET ROLE;
DROP FUNCTION IF EXISTS ingest_batch(jsonb);

SET ROLE app_owner;

-- parse_batch is owned by app_owner and its OUT row type changes, which
-- CREATE OR REPLACE cannot do. Drop and recreate (no production rows yet).
DROP FUNCTION IF EXISTS parse_batch(jsonb);
\i db/sql/functions/parse_batch.sql
-- ingest_batch(): sessionless branch + provenance columns + bootstrap clock.
\i db/sql/functions/ingest_batch.sql
-- bootstrap_session() / lookup_session(): the only way in for app_ingest.
\i db/sql/functions/session_bootstrap.sql

RESET ROLE;

ALTER FUNCTION ingest_batch(jsonb)                OWNER TO app_writer;
ALTER FUNCTION bootstrap_session(jsonb)           OWNER TO app_writer;
ALTER FUNCTION lookup_session(uuid, uuid, timestamptz) OWNER TO app_writer;

-- parse_batch was dropped and recreated above, and ingest_batch was dropped
-- before being recreated: restore both grants.
GRANT EXECUTE ON FUNCTION parse_batch(jsonb)                      TO app_writer;
GRANT EXECUTE ON FUNCTION ingest_batch(jsonb)                     TO app_ingest;
GRANT EXECUTE ON FUNCTION bootstrap_session(jsonb)                TO app_ingest;
GRANT EXECUTE ON FUNCTION lookup_session(uuid, uuid, timestamptz) TO app_ingest;

GRANT INSERT, SELECT, UPDATE ON session_bootstrap TO app_writer;
-- The ingest service still holds EXECUTE only: no direct table access.
GRANT SELECT, DELETE ON session_bootstrap TO app_jobs;
