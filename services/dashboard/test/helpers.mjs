// Seeding helper for dashboard service tests: one tenant, three keys
// (dashboard-kind active, dashboard-kind revoked, ingest-kind for the
// wrong-kind path). Mirrors services/ingest/test/helpers.mjs.
import { createHash, randomUUID } from "node:crypto";

export async function seedTenantWithKeys(pool) {
  const tenantId = (
    await pool.query("INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id", [
      "dashboard-test",
    ])
  ).rows[0].tenant_id;

  async function addKey(kind, { revoked = false } = {}) {
    const raw = `dk_${randomUUID()}`;
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
    dashboardKey: await addKey("dashboard"),
    revokedKey: await addKey("dashboard", { revoked: true }),
    ingestKey: await addKey("ingest"),
  };
}
