-- 0007_sessions_history.sql — closed sessions, partitioned by the guard's immutable
-- home_day so a late event always lands on the same history identity.
SET ROLE app_owner;

CREATE TABLE sessions_history (
  tenant_id    uuid        NOT NULL,
  session_id   uuid        NOT NULL,
  home_day     timestamptz NOT NULL,       -- stable partition key
  anon_id      text        NOT NULL,
  started_at   timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL,
  event_count  integer     NOT NULL,
  total_events integer     NOT NULL,
  heartbeats   integer     NOT NULL,
  user_id      text,
  user_at      timestamptz,
  entry_url    text,
  entry_at     timestamptz,
  device       text,
  country      text,
  utm          jsonb,
  attr_at      timestamptz,
  PRIMARY KEY (tenant_id, session_id, home_day)
);
SELECT create_hypertable('sessions_history', 'home_day', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX sessions_hist_started ON sessions_history (tenant_id, started_at DESC);
CREATE INDEX sessions_hist_user ON sessions_history (tenant_id, user_id, started_at DESC)
  WHERE user_id IS NOT NULL;

-- Policy jobs registered under the bootstrap role (LOGIN requirement, see 0003).
RESET ROLE;
ALTER TABLE sessions_history SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'tenant_id',
  timescaledb.compress_orderby   = 'started_at');
SELECT add_compression_policy('sessions_history', INTERVAL '7 days');
SELECT add_retention_policy('sessions_history', INTERVAL '90 days');

-- Read-side view over both tables (owned by app_owner).
SET ROLE app_owner;
-- security_invoker is REQUIRED: without it the view owner's privileges apply and RLS on
-- the base tables is bypassed (D4). The active half excludes rows whose session already
-- has a history row, so a late partial does not appear twice (D18).
CREATE VIEW sessions WITH (security_invoker = true) AS
  SELECT a.tenant_id, a.session_id, a.home_day, a.anon_id, a.user_id, a.started_at,
         a.last_seen_at, a.event_count, a.total_events, a.heartbeats, a.entry_url,
         a.device, a.country, a.utm
    FROM sessions_active a
   WHERE NOT EXISTS (
           SELECT 1 FROM sessions_history h
            WHERE h.tenant_id = a.tenant_id AND h.session_id = a.session_id)
  UNION ALL
  SELECT h.tenant_id, h.session_id, h.home_day, h.anon_id, h.user_id, h.started_at,
         h.last_seen_at, h.event_count, h.total_events, h.heartbeats, h.entry_url,
         h.device, h.country, h.utm
    FROM sessions_history h;

RESET ROLE;
