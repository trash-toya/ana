-- 0003_events.sql — the event hypertable. DDL per blueprint section 4, with the
-- documented notes from 02-data-model.md (seq NULLS LAST, 1-day chunks to start).
SET ROLE app_owner;

CREATE TABLE events (
  time       timestamptz NOT NULL,          -- skew-corrected by the ingest API
  tenant_id  uuid        NOT NULL,
  session_id uuid        NOT NULL,
  event_id   uuid        NOT NULL,
  cat        char(1)     NOT NULL,          -- e engagement, l lifecycle, s system
  seq        integer,                       -- nullable; ordering is NULLS LAST (ASC default)
  anon_id    text        NOT NULL,
  user_id    text,
  name       text        NOT NULL,
  url        text,
  props      jsonb       NOT NULL DEFAULT '{}'
);
SELECT create_hypertable('events', 'time', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX events_session ON events (tenant_id, session_id, time, seq NULLS LAST, event_id);

-- Policy jobs must be owned by a LOGIN role (Timescale requirement: "Hypertable owner
-- must have LOGIN permission to run background tasks"). app_owner is NOLOGIN by design,
-- so compression/retention policies are registered under the bootstrap role. They are
-- operational config, not schema (07-operations.md keeps the distinction).
RESET ROLE;
ALTER TABLE events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'tenant_id',
  timescaledb.compress_orderby   = 'session_id, time');
SELECT add_compression_policy('events', INTERVAL '7 days');
SELECT add_retention_policy('events', INTERVAL '90 days');
