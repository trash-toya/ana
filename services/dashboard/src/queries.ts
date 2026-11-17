// The dashboard's SQL, in one reviewable place (04-services.md §Dashboard API):
// each function maps 1:1 to an endpoint and to the blueprint's example query.
//
// Rules that hold for every query here:
//   1. Raw `events` is read ONLY by drill-downs (replay, funnels, and the
//      entry-page conversion read); headline metrics come from the rollups.
//   2. History reads filter `home_day` AND `started_at` so Timescale prunes
//      chunks (sessions_history is chunked by home_day).
//   3. Day boundaries use `time_bucket('1 day', ts, $tz) AT TIME ZONE $tz` —
//      the same idiom rollup_daily was corrected to by T25a: a bare ::date on
//      the bucket re-interprets it in the session timezone and undoes the shift.
//   4. Visitor/user counts finalize HLL state at read time
//      (`distinct_count(rollup(...))`), COALESCEd to 0 for empty days, and are
//      labelled `approximate` in the response (~1.6% at scale).
//   5. Live reads exclude active rows whose session already has history
//      (D18 anti-join) so a late partial is never counted twice.

import type { TenantQuery } from "./db.js";

// Every query resolves the tenant through app_tenant() — the fail-closed
// helper from 0009 (raises if app.tenant_id is unset) — never through a
// caller-supplied parameter. db.ts#withTenant sets app.tenant_id
// transaction-locally via set_config(..., true) before any of these run, so
// a request without a bound tenant cannot read anything at all. RLS policies
// (0011, P4) will additionally enforce this on the database side.

// ---------------------------------------------------------------------------
// GET /v1/sessions/:id/replay
// ---------------------------------------------------------------------------

/** Point lookup first: home_day from the guard drives chunk pruning below. */
export function replayHomeDay(sessionId: string) {
  return (q: TenantQuery) =>
    q<{ home_day: string }>(
      `SELECT home_day
         FROM session_guard
        WHERE tenant_id = app_tenant() AND session_id = $1`,
      [sessionId],
    );
}

/**
 * The session row: history first (the guard's home_day prunes the hypertable),
 * falling back to the active segment (D18: both never coexist for one session).
 * Reads the concrete tables — the `sessions` view would scan both hypertables
 * for a point lookup without the home_day predicate.
 */
export function replaySession(sessionId: string, homeDay: string) {
  return (q: TenantQuery) =>
    q<{
      session_id: string;
      anon_id: string;
      user_id: string | null;
      started_at: string;
      last_seen_at: string;
      event_count: number;
      total_events: number;
      heartbeats: number;
      entry_url: string | null;
      device: string | null;
      country: string | null;
      utm: unknown;
    }>(
      `SELECT session_id, anon_id, user_id, started_at, last_seen_at,
              event_count, total_events, heartbeats, entry_url, device, country, utm
         FROM sessions_history
        WHERE tenant_id = app_tenant() AND session_id = $1 AND home_day = $2
        UNION ALL
       SELECT session_id, anon_id, user_id, started_at, last_seen_at,
              event_count, total_events, heartbeats, entry_url, device, country, utm
         FROM sessions_active
        WHERE tenant_id = app_tenant() AND session_id = $1 AND home_day = $2
          AND NOT EXISTS (SELECT 1
                            FROM sessions_history h
                           WHERE h.tenant_id = app_tenant() AND h.session_id = $1)`,
      [sessionId, homeDay],
    );
}

/**
 * The event stream for one session (drill-down — the raw-events exception).
 * The time window keeps the 1-day-chunk scan bounded: an event outside
 * started_at − 5 min … last_seen_at + 5 min cannot belong to this segment
 * (server clock; the ingest API skew-corrects `time`).
 */
export function replayEvents(sessionId: string, startedAt: string, lastSeenAt: string) {
  return (q: TenantQuery) =>
    q<{
      event_id: string;
      time: string;
      seq: number | null;
      cat: string;
      name: string;
      url: string | null;
      props: unknown;
    }>(
      `SELECT event_id, time, seq, cat, name, url, props
         FROM events
        WHERE tenant_id = app_tenant()
          AND session_id = $1
          AND time >= $2::timestamptz - INTERVAL '5 minutes'
          AND time <= $3::timestamptz + INTERVAL '5 minutes'
        ORDER BY time, seq NULLS LAST, event_id`,
      [sessionId, startedAt, lastSeenAt],
    );
}

// ---------------------------------------------------------------------------
// GET /v1/metrics/hourly?from&to
// ---------------------------------------------------------------------------

/**
 * Headline hourly metrics from session_stats_15m only (rule 1). The CAGG is
 * real-time (`materialized_only=false`), so the current bucket is included;
 * closed-session drift above the refresh watermark is ≤ 15 min (T24d) —
 * documented, not corrected here.
 */
export function hourlyStats(from: string, to: string) {
  return (q: TenantQuery) =>
    q<{
      hour: string;
      sessions: string;
      bounced: string;
      total_duration_s: string;
      heartbeat_s: string;
      events: string;
    }>(
      `SELECT time_bucket('1 hour', bucket) AS hour,
              sum(sessions)        AS sessions,
              sum(ec_0 + ec_1)     AS bounced,
              sum(total_duration_s) AS total_duration_s,
              sum(heartbeat_s)     AS heartbeat_s,
              sum(events)          AS events
         FROM session_stats_15m
        WHERE tenant_id = app_tenant()
          AND bucket >= $1::timestamptz
          AND bucket <  $2::timestamptz
        GROUP BY 1 ORDER BY 1`,
      [from, to],
    );
}

/**
 * The live slice (Example 2 + D14/D18): rows seen in the last 5 minutes,
 * touch_bucket range bound for the index, anti-joined against history so a
 * late partial is not counted twice.
 */
export function liveSlice() {
  return (q: TenantQuery) =>
    q<{ sessions: string; events: string; engaged_events: string }>(
      `SELECT count(*)                      AS sessions,
              COALESCE(sum(a.total_events), 0) AS events,
              COALESCE(sum(a.event_count), 0)  AS engaged_events
         FROM sessions_active a
        WHERE a.tenant_id = app_tenant()
          AND a.last_seen_at > now() - INTERVAL '5 minutes'
          AND a.touch_bucket >= floor(extract(epoch FROM now()) / 600)::int - 1
          AND NOT EXISTS (SELECT 1 FROM sessions_history h
                           WHERE h.tenant_id = a.tenant_id
                             AND h.session_id = a.session_id)`,
    );
}

// ---------------------------------------------------------------------------
// GET /v1/metrics/daily?from&to&tz
// ---------------------------------------------------------------------------

/**
 * Closed days from the nightly rollup (session_stats_daily wins where a row
 * exists: it is the authoritative day-level store, rewritten three nights per
 * day). Days it has not written (yesterday/today, older than the rollup
 * window within the 90-day retention) fall through to the 15m aggregate.
 */
export function dailyClosed(fromDay: string, toDay: string) {
  return (q: TenantQuery) =>
    q<{
      day: string;
      sessions: string;
      bounced: string;
      total_duration_s: string;
      heartbeat_s: string;
      events: string;
    }>(
      `SELECT local_day::text AS day,
              sessions, bounced, total_duration_s, heartbeat_s, events
         FROM session_stats_daily
        WHERE tenant_id = app_tenant()
          AND local_day >= $1::date
          AND local_day <= $2::date
        ORDER BY local_day`,
      [fromDay, toDay],
    );
}

/**
 * Open (or not-yet-rolled) days from the real-time 15m CAGG, grouped into
 * local days with the tenant tz (rule 3's cast idiom). The caller excludes
 * days already served from session_stats_daily.
 */
export function dailyFrom15m(
  from: string,
  to: string,
  tz: string,
  excludeDays: string[],
) {
  return (q: TenantQuery) =>
    q<{
      day: string;
      sessions: string;
      bounced: string;
      total_duration_s: string;
      heartbeat_s: string;
      events: string;
    }>(
      `SELECT (time_bucket('1 day', bucket, $3) AT TIME ZONE $3)::date::text AS day,
              sum(sessions)         AS sessions,
              sum(ec_0 + ec_1)      AS bounced,
              sum(total_duration_s) AS total_duration_s,
              sum(heartbeat_s)      AS heartbeat_s,
              sum(events)           AS events
         FROM session_stats_15m
        WHERE tenant_id = app_tenant()
          AND bucket >= $1::timestamptz
          AND bucket <  $2::timestamptz
          AND (time_bucket('1 day', bucket, $3) AT TIME ZONE $3)::date::text
                <> ALL($4::text[])
        GROUP BY 1 ORDER BY 1`,
      [from, to, tz, excludeDays],
    );
}

/** Closed days' identity: finalize the stored HLL states at read time. */
export function identityClosed(fromDay: string, toDay: string) {
  return (q: TenantQuery) =>
    q<{ day: string; visitors: string | null; users: string | null }>(
      `SELECT local_day::text AS day,
              distinct_count(rollup(anon_hll))  AS visitors,
              distinct_count(rollup(users_hll)) AS users
         FROM identity_daily
        WHERE tenant_id = app_tenant()
          AND local_day >= $1::date
          AND local_day <= $2::date
        GROUP BY local_day
        ORDER BY local_day`,
      [fromDay, toDay],
    );
}

/** Open days' identity: roll the 15m sketches into the local day, then finalize. */
export function identityFrom15m(
  from: string,
  to: string,
  tz: string,
  excludeDays: string[],
) {
  return (q: TenantQuery) =>
    q<{ day: string; visitors: string | null; users: string | null }>(
      `SELECT (time_bucket('1 day', bucket, $3) AT TIME ZONE $3)::date::text AS day,
              distinct_count(rollup(anon_hll))  AS visitors,
              distinct_count(rollup(users_hll)) AS users
         FROM identity_15m
        WHERE tenant_id = app_tenant()
          AND bucket >= $1::timestamptz
          AND bucket <  $2::timestamptz
          AND (time_bucket('1 day', bucket, $3) AT TIME ZONE $3)::date::text
                <> ALL($4::text[])
        GROUP BY 1
        ORDER BY 1`,
      [from, to, tz, excludeDays],
    );
}

// ---------------------------------------------------------------------------
// GET /v1/entry-pages?from&to
// ---------------------------------------------------------------------------

/**
 * Entry-page conversion (Example 4; a documented drill-down, so the `sessions`
 * view is allowed here — the view itself carries the D18 anti-join). The
 * home_day bound is a superset of the started_at window across ±1 local day
 * (a session's home_day is its start day in the tenant tz); started_at is
 * the exact filter, so pruning stays correct.
 */
export function entryPages(from: string, to: string, tz: string) {
  return (q: TenantQuery) =>
    q<{
      entry_url: string;
      sessions: string;
      bounced: string;
      avg_duration_s: string | null;
    }>(
      `SELECT entry_url,
              count(*)                             AS sessions,
              count(*) FILTER (WHERE event_count <= 1) AS bounced,
              avg(extract(epoch FROM last_seen_at - started_at)) AS avg_duration_s
         FROM sessions
        WHERE tenant_id = app_tenant()
          AND home_day >= time_bucket('1 day', $1::timestamptz, $3) - INTERVAL '1 day'
          AND home_day <= time_bucket('1 day', $2::timestamptz, $3) + INTERVAL '1 day'
          AND started_at >= $1::timestamptz
          AND started_at <  $2::timestamptz
          AND entry_url IS NOT NULL
        GROUP BY entry_url
        ORDER BY sessions DESC
        LIMIT 20`,
      [from, to, tz],
    );
}

// ---------------------------------------------------------------------------
// POST /v1/funnels
// ---------------------------------------------------------------------------

/**
 * Ordered funnel over raw `events` (Example 5; a documented drill-down).
 * One walk per session: match step 1's earliest event, then the earliest
 * later event for step 2, and so on — the tie-break is exactly the storage
 * order (time, seq NULLS LAST, event_id), which the recursive CTE's `ord`
 * row number encodes. Steps must be engagement events (cat = 'e').
 */
export function funnel(from: string, to: string, steps: string[]) {
  const stepArray = steps;
  return (q: TenantQuery) =>
    q<{ step: string; sessions: string }>(
      `WITH RECURSIVE ev AS (
          SELECT e.session_id, e.name,
                 row_number() OVER (ORDER BY e.time, e.seq NULLS LAST, e.event_id) AS ord
            FROM events e
           WHERE e.tenant_id = app_tenant()
             AND e.cat = 'e'
             AND e.time >= $1::timestamptz
             AND e.time <  $2::timestamptz
             AND e.name = ANY($3::text[])
       ),
       walk AS (
          SELECT session_id, 1 AS step, ord
            FROM (
              SELECT DISTINCT ON (session_id) session_id, ord
                FROM ev
               WHERE name = ($3::text[])[1]
               ORDER BY session_id, ord
            ) first_step
          UNION ALL
          SELECT w.session_id, w.step + 1, nxt.ord
            FROM walk w
            CROSS JOIN LATERAL (
                 SELECT ord
                   FROM ev
                  WHERE ev.session_id = w.session_id
                    AND ev.ord > w.ord
                    AND ev.name = ($3::text[])[w.step + 1]
                  ORDER BY ord
                  LIMIT 1
               ) nxt
       )
       SELECT step::text AS step, count(DISTINCT session_id) AS sessions
         FROM walk
        GROUP BY step
        ORDER BY step`,
      [from, to, stepArray],
    );
}

// ---------------------------------------------------------------------------
// GET /v1/freshness
// ---------------------------------------------------------------------------

/**
 * The freshness gate (Notion §22, P3-7): each CAGG's materialization watermark
 * plus the tenant's own data high-water mark. Empirically (pinned image), the
 * watermark only advances over buckets that contain data — empty windows leave
 * it at the last materialized data bucket — so "now - watermark" would read a
 * quiet tenant as stale while its dashboards are exact. The published
 * `behind_ms` is therefore the AGGREGATION DEBT relative to data: the tenant's
 * latest completed 15-min bucket end minus the watermark, clamped at 0. The
 * gate is that the scheduled aggregation keeps that debt within 1 h under load
 * and after a backlog; buckets above the watermark are served live (T24d).
 */
export function freshness() {
  return (q: TenantQuery) =>
    q<{
      events_watermark: string | null;
      identity_watermark: string | null;
      has_events: boolean;
      data_bucket_end: string | null;
    }>(
      `SELECT cagg_watermark_ts('events_15m')   AS events_watermark,
              cagg_watermark_ts('identity_15m') AS identity_watermark,
              EXISTS (SELECT 1 FROM events)      AS has_events,
              (SELECT time_bucket('15 minutes', max(time)) + INTERVAL '15 minutes'
                 FROM events)                    AS data_bucket_end`,
    );
}

// ---------------------------------------------------------------------------
// GET /v1/live
// ---------------------------------------------------------------------------

/**
 * The live roster (Example 6, D14): last seen within 5 minutes, touch_bucket
 * index bound, D18 anti-join. Rows are labelled `live: true` so a dashboard
 * never presents the slice as a session count for a larger window.
 */
export function liveRoster() {
  return (q: TenantQuery) =>
    q<{
      session_id: string;
      anon_id: string;
      user_id: string | null;
      started_at: string;
      last_seen_at: string;
      event_count: number;
      total_events: number;
      entry_url: string | null;
      device: string | null;
      country: string | null;
      utm: unknown;
    }>(
      `SELECT a.session_id, a.anon_id, a.user_id, a.started_at, a.last_seen_at,
              a.event_count, a.total_events, a.entry_url, a.device, a.country, a.utm
         FROM sessions_active a
        WHERE a.tenant_id = app_tenant()
          AND a.last_seen_at > now() - INTERVAL '5 minutes'
          AND a.touch_bucket >= floor(extract(epoch FROM now()) / 600)::int - 1
          AND NOT EXISTS (SELECT 1 FROM sessions_history h
                           WHERE h.tenant_id = a.tenant_id
                             AND h.session_id = a.session_id)
        ORDER BY a.last_seen_at DESC`,
    );
}
