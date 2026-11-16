-- Smoke test of the P0 write path (run by scripts/smoke.mjs too).
SELECT ingest_batch($j$[
  {"tenant_id":"11111111-1111-4111-8111-111111111111","session_id":"22222222-2222-4222-8222-222222222222","event_id":"33333333-3333-4333-8333-333333333331","name":"session_start","cat":"l","time":"2026-06-10T06:00:00Z","seq":1,"anon_id":"a1","user_id":null,"url":null,"props":{"utm":{"source":"x"},"device":"desktop","country":"DE"},"payload_hash":"h1","ord":0},
  {"tenant_id":"11111111-1111-4111-8111-111111111111","session_id":"22222222-2222-4222-8222-222222222222","event_id":"33333333-3333-4333-8333-333333333332","name":"page_view","cat":"e","time":"2026-06-10T06:00:30Z","seq":2,"anon_id":"a1","user_id":"u1","url":"/home","props":{},"payload_hash":"h2","ord":1}
]$j$::jsonb);

SELECT count(*) AS events FROM events;
SELECT total_events, home_day FROM session_guard;
SELECT user_id, entry_url, utm, device FROM sessions_active;
SELECT user_id, anon_id FROM identity_map;

-- retry of the same event_id with same hash -> dup
SELECT ingest_batch($j$[
  {"tenant_id":"11111111-1111-4111-8111-111111111111","session_id":"22222222-2222-4222-8222-222222222222","event_id":"33333333-3333-4333-8333-333333333332","name":"page_view","cat":"e","time":"2026-06-10T06:00:30Z","seq":2,"anon_id":"a1","user_id":"u1","url":"/home","props":{},"payload_hash":"h2","ord":0}
]$j$::jsonb);

UPDATE sessions_active SET touch_bucket = 0;
CALL archive_sessions(NULL, NULL);
SELECT tenant_id, bucket, sessions, ec_1, total_duration_s, events FROM session_stats_15m;
SELECT count(*) AS hist FROM sessions_history;
SELECT count(*) AS active_left FROM sessions_active;

CALL rebuild_session_stats('11111111-1111-4111-8111-111111111111'::uuid, '2026-06-01', '2026-07-01');
SELECT count(*) AS rows_rebuilt FROM session_stats_15m;
SELECT count(*) AS orphan_dedupe FROM event_dedupe d LEFT JOIN events e USING (tenant_id, event_id) WHERE e.event_id IS NULL;
