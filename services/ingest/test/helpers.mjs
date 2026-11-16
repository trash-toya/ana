// Seeding helper for ingest service tests: one tenant + three keys
// (active ingest, dashboard-kind, revoked ingest).
import { createHash, randomUUID } from "node:crypto";

export async function seedTenantWithKeys(pool) {
  const tenantId = (
    await pool.query(
      "INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id",
      ["ingest-test"],
    )
  ).rows[0].tenant_id;

  async function addKey(kind, { revoked = false } = {}) {
    const raw = `sk_${randomUUID()}`;
    const hash = createHash("sha256").update(raw, "utf8").digest();
    await pool.query(
      `INSERT INTO api_keys (key_hash, tenant_id, kind, revoked_at)
       VALUES ($1, $2, $3, $4)`,
      [hash, tenantId, kind, revoked ? new Date() : null],
    );
    return raw;
  }

  return {
    tenantId,
    ingestKey: await addKey("ingest"),
    serverKey: await addKey("server"),
    dashboardKey: await addKey("dashboard"),
    revokedKey: await addKey("ingest", { revoked: true }),
  };
}

export function deadPool(pg) {
  // Never-connecting pool for liveness/no-DB tests. Port 9 (discard) refuses
  // instantly in this environment.
  return new pg.Pool({
    connectionString: "postgresql://postgres:postgres@127.0.0.1:9/nope",
    connectionTimeoutMillis: 2000,
  });
}
