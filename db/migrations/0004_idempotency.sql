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

-- Retention: 90 days, matching the raw-event retention window and the producer
-- replay horizon (Notion §21): a replay inside 90 days must still get an
-- idempotent outcome. Deleted by cleanup_idempotency (bounded batches), not a
-- Timescale retention policy, because the horizon must stay explicit and
-- testable next to the 3-day session-lifecycle cleanup in the same job.

RESET ROLE;