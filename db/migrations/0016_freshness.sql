-- 0016_freshness.sql — the freshness gate (P3-7). Exposes each continuous
-- aggregate's materialization watermark as a plain timestamptz so the dashboard
-- API never touches Timescale internals directly.
--
-- Watermark semantics (T24d): buckets < the watermark are served from the
-- materialization; everything above it is served live from the raw hypertable
-- (materialized_only=false). The refresh policy (0012: 15-min cadence,
-- end_offset = one bucket) keeps the watermark within ~15 min of now; a backlog
-- is replayed within the 1-hour gate (measured in T27). A never-refreshed CAGG
-- reports a placeholder BC timestamp — normalized to NULL here so callers can
-- distinguish "empty install" from "stale".
SET ROLE app_owner;

CREATE OR REPLACE FUNCTION cagg_watermark_ts(p_view text) RETURNS timestamptz
LANGUAGE sql STABLE
SET search_path = pg_catalog, _timescaledb_functions, _timescaledb_catalog,
                   timescaledb_information
AS $$
  SELECT CASE
           WHEN w IS NULL OR w < '2000-01-01'::timestamptz THEN NULL
           ELSE w
         END
    FROM (
      SELECT _timescaledb_functions.to_timestamp(
               _timescaledb_functions.cagg_watermark(h.id)) AS w
        FROM _timescaledb_catalog.hypertable h
        JOIN timescaledb_information.continuous_aggregates ca
          ON ca.materialization_hypertable_name = h.table_name
       WHERE ca.view_name = p_view
    ) x
$$;

COMMENT ON FUNCTION cagg_watermark_ts(text) IS
  'Materialization watermark of a continuous aggregate (exclusive upper bound
of materialized buckets); NULL when the CAGG has never been refreshed. The
freshness gate (Notion §22) bounds this within 1 h of now under load.';

RESET ROLE;
