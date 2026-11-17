// Tenant-scoped query execution (04-services.md §Dashboard API): every request
// runs inside a READ ONLY transaction that sets `app.tenant_id`
// transaction-locally via set_config(..., true) — no string interpolation.
// RLS policies land in 0011 (P4); until then every SQL statement in
// queries.ts also carries an explicit tenant_id predicate, so the tenant
// binding is fail-safe today and enforced in depth tomorrow.

import type { Pool, QueryConfig, QueryResult, QueryResultRow } from "pg";

export type TenantQuery = <R extends QueryResultRow>(
  text: string | QueryConfig,
  values?: unknown[],
) => Promise<QueryResult<R>>;

export async function withTenant<T>(
  pool: Pool,
  tenantId: string,
  fn: (q: TenantQuery) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN READ ONLY");
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenantId]);
    const result = await fn((text, values) =>
      client.query(text as string, values as unknown[]),
    );
    await client.query("COMMIT");
    return result;
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // connection is released either way
    }
    throw err;
  } finally {
    client.release();
  }
}
