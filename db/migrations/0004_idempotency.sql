-- 0004_idempotency.sql — global idempotency ledger. Key is (tenant_id, event_id) and
-- nothing else (D1: a row exists only for an event that was stored).
SET ROLE app_owner;

CREATE TABLE event_dedupe (
  tenant_id    uuid        NOT NULL,
  event_id     uuid        NOT NULL,
  payload_hash text        NOT NULL,
  received_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, event_id)
) WITH (autovacuum_vacuum_scale_factor = 0.02);
CREATE INDEX event_dedupe_age ON event_dedupe (received_at);

-- Retention: 3 days = 24 h event age limit + 24 h max session length + 1 day margin.
-- Deleted by cleanup_idempotency (bounded batches), not a Timescale retention policy,
-- because it must stay exactly in step with session_guard (02-data-model.md).

RESET ROLE;