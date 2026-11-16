-- parse_batch(p jsonb) — deserialize the API-produced batch into a typed row set.
-- Pure: no SQL tables touched, no side effects. The API owns enrichment and
-- classification (cat comes from the shared package, never from SQL; 03-write-path.md).
-- Fields per event:
--   tenant_id, session_id (NULL for machine-originated events, D29),
--   event_id, name, cat ('e'|'l'|'s'), time (occurrence, corrected per source),
--   received_at (analytics receipt time, D32), seq (nullable), anon_id
--   (nullable only when session_id is NULL), user_id (nullable), url (nullable),
--   source ('client'|'server'|'webhook'), props,
--   payload_hash (canonical JSON hash of producer-original fields), ord (request order).
CREATE OR REPLACE FUNCTION parse_batch(p jsonb)
RETURNS TABLE (
  tenant_id    uuid,
  session_id   uuid,
  event_id     uuid,
  name         text,
  cat          char(1),
  "time"       timestamptz,
  received_at  timestamptz,
  seq          integer,
  anon_id      text,
  user_id      text,
  url          text,
  source       text,
  props        jsonb,
  payload_hash text,
  ord          integer
)
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog, public
AS $$
  SELECT e.tenant_id, e.session_id, e.event_id, e.name,
         e.cat::char(1), e.time, e.received_at, e.seq, e.anon_id, e.user_id,
         e.url, COALESCE(e.source, 'client'), COALESCE(e.props, '{}'::jsonb),
         e.payload_hash, e.ord
    FROM jsonb_to_recordset(p) AS e(
      tenant_id    uuid,
      session_id   uuid,
      event_id     uuid,
      name         text,
      cat          text,
      time         timestamptz,
      received_at  timestamptz,
      seq          integer,
      anon_id      text,
      user_id      text,
      url          text,
      source       text,
      props        jsonb,
      payload_hash text,
      ord          integer)
$$;

COMMENT ON FUNCTION parse_batch(jsonb) IS
  'Typed deserialization of an ingest batch. IMMUTABLE; called once per batch.';
