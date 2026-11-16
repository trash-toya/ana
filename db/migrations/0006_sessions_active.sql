-- 0006_sessions_active.sql — the live summary. Rows live 40–50 minutes (touch_bucket
-- is 10-minute granularity; see D8). A late event for a closed session creates a row
-- here too; readers exclude rows whose session already has history (D18).
SET ROLE app_owner;

CREATE TABLE sessions_active (
  tenant_id    uuid        NOT NULL,
  session_id   uuid        NOT NULL,
  home_day     timestamptz NOT NULL,       -- copied from session_guard
  anon_id      text        NOT NULL,
  started_at   timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  event_count  integer     NOT NULL DEFAULT 0,  -- engagement events in this segment
  total_events integer     NOT NULL DEFAULT 0,  -- all events in this segment
  heartbeats   integer     NOT NULL DEFAULT 0,
  touch_bucket integer     NOT NULL,            -- floor(epoch / 600) of the last server write
  user_id      text,
  user_at      timestamptz,                     -- earliest event that carried a user_id
  entry_url    text,
  entry_at     timestamptz,                     -- earliest page_view
  device       text,
  country      text,
  utm          jsonb,
  attr_at      timestamptz,                     -- earliest session_start
  PRIMARY KEY (tenant_id, session_id)
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX sessions_active_touch ON sessions_active (touch_bucket, tenant_id);

RESET ROLE;
