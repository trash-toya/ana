// Demo seed: apply migrations to the compose "postgres" database (idempotent
// via the _migrations ledger; 0014_schedule is staging-only) and create/reuse
// the demo tenant with a fresh ingest key. Prints the key for run.mjs to pass
// to the page via /demo/config.
//
// Run directly: node demo/seed.mjs

import crypto from "node:crypto";
import path from "node:path";
import { pathToFileURL } from "node:url";
import pg from "pg";
import { applyMigrations } from "../db/test/scripts/migrate.mjs";

const COMPOSE_URL =
  process.env.DEMO_DB_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";
const TENANT_NAME = "Demo Tenant";

export async function seed({ log = console.log } = {}) {
  // 1. Schema: idempotent — the second run is a no-op (ledger table).
  await applyMigrations("postgres");
  log("[seed] migrations applied");

  // 2. Demo tenant + fresh ingest key (only write access exists anywhere).
  const pool = new pg.Pool({ connectionString: COMPOSE_URL });
  try {
    const tenant =
      (await pool.query("SELECT tenant_id FROM tenants WHERE name = $1", [TENANT_NAME]))
        .rows[0]?.tenant_id ??
      (
        await pool.query("INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id", [
          TENANT_NAME,
        ])
      ).rows[0].tenant_id;

    // Rotate: revoke stale demo keys so leaked copies stop working.
    await pool.query(
      "UPDATE api_keys SET revoked_at = now() WHERE tenant_id = $1 AND kind IN ('ingest','server') AND origin_hint = 'demo' AND revoked_at IS NULL",
      [tenant],
    );

    async function mint(kind) {
      const raw = `sk_demo_${kind}_${crypto.randomBytes(24).toString("base64url")}`;
      const hash = crypto.createHash("sha256").update(raw).digest();
      await pool.query(
        "INSERT INTO api_keys (key_hash, tenant_id, kind, origin_hint) VALUES ($1, $2, $3, 'demo')",
        [hash, tenant, kind],
      );
      return raw;
    }

    const apiKey = await mint("ingest");
    const serverKey = await mint("server");
    log("[seed] demo tenant ready, fresh ingest + server keys minted");
    return { tenantId: tenant, apiKey, serverKey };
  } finally {
    await pool.end();
  }
}

// Direct execution: print the key prominently for manual use.
if (import.meta.url === pathToFileURL(path.resolve(process.argv[1] ?? "")).href) {
  const { apiKey } = await seed();
  console.log(`\n  DEMO INGEST KEY: ${apiKey}\n`);
}
