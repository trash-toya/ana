// Dashboard service configuration (04-services.md §Dashboard API + §Config
// pattern from the ingest service). No secrets in the image; everything here
// is tunables. The dashboard is read-only: no buffer, no batch knobs.

export interface DashboardConfig {
  port: number;
  /** Per-request statement timeout (dashboard queries must stay interactive). */
  statementTimeoutMs: number;
  /** Key-cache TTL; 60 s per the key policy (same as ingest). */
  authTtlMs: number;
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): DashboardConfig {
  return {
    port: Number(env.PORT ?? 3002),
    statementTimeoutMs: Number(env.STATEMENT_TIMEOUT_MS ?? 10_000),
    authTtlMs: Number(env.AUTH_TTL_MS ?? 60_000),
  };
}
