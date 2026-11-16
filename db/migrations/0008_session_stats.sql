-- 0008_session_stats.sql — the exact, incrementally maintained answer for closed
-- sessions. Invariant: the table always equals the aggregate of sessions_history (I5).
-- D2: total_duration_s is numeric(20,6) — the precision of extract(epoch ...) — and
-- contributions are never rounded before display; bigint + per-group rounding made the
-- +new/-old deltas non-additive.
SET ROLE app_owner;

CREATE TABLE session_stats_15m (
  tenant_id        uuid           NOT NULL,
  bucket           timestamptz    NOT NULL,  -- time_bucket('15 minutes', started_at)
  sessions         bigint         NOT NULL,
  ec_0             bigint         NOT NULL,  -- sessions with 0 engagement events
  ec_1             bigint         NOT NULL,  -- exactly 1
  ec_2             bigint         NOT NULL,  -- exactly 2
  ec_3p            bigint         NOT NULL,  -- 3 or more
  total_duration_s numeric(20, 6) NOT NULL,  -- elapsed: last_seen_at - started_at
  heartbeat_s      bigint         NOT NULL,  -- heartbeats * 30: rough observed presence
  events           bigint         NOT NULL,  -- engagement events
  PRIMARY KEY (tenant_id, bucket)
);
SELECT create_hypertable('session_stats_15m', 'bucket', chunk_time_interval => INTERVAL '7 days');

-- Policy jobs registered under the bootstrap role (LOGIN requirement, see 0003).
RESET ROLE;
SELECT add_retention_policy('session_stats_15m', INTERVAL '90 days');
