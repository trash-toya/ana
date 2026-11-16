-- 0005_session_guard.sql — session identity, lock and cap. Created by the first
-- accepted batch of a session; the counter lives here, independent of archiving.
SET ROLE app_owner;

CREATE TABLE session_guard (
  tenant_id    uuid        NOT NULL,
  session_id   uuid        NOT NULL,
  home_day     timestamptz NOT NULL,       -- server UTC day of first write; never changes
  total_events integer     NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, session_id)
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX session_guard_age ON session_guard (created_at);  -- immutable -> HOT updates

RESET ROLE;
