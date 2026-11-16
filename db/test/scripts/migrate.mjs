// Migration runner. Applies db/migrations/*.sql in order, skipping 0014 unless asked.
// Conventions (db/test/README.md):
//   * 0001 and 0002 run as the bootstrap role (extensions + role creation need it);
//   * every later migration sets SET ROLE app_owner itself and RESETs at the end;
//   * \i paths are resolved relative to the REPO ROOT (psql is spawned with cwd=root).
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const execFileP = promisify(execFile);
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const MIGRATIONS = path.join(ROOT, "db/migrations");

export const BOOTSTRAP_URL =
  process.env.DATABASE_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";

const SCHEDULE_FILE = "0014_schedule.sql";

export async function psqlFile(url, file, { cwd = ROOT } = {}) {
  await execFileP("psql", [url, "-v", "ON_ERROR_STOP=1", "-q", "-f", file], {
    cwd,
    maxBuffer: 32 * 1024 * 1024,
  });
}

function quoteIdent(name) {
  if (!/^[a-z_][a-z0-9_]*$/.test(name)) {
    throw new Error(`unsupported database name: ${name}`);
  }
  return name;
}

function urlForDb(dbName) {
  const u = new URL(BOOTSTRAP_URL);
  u.pathname = `/${dbName}`;
  return u.toString();
}

// The bootstrap connection always goes to the maintenance DB on the same host, even
// when DATABASE_URL itself names a database that is about to be created.
function adminUrl() {
  const u = new URL(BOOTSTRAP_URL);
  u.pathname = "/postgres";
  return u.toString();
}

async function ensureDatabase(dbName) {
  const client = new pg.Client({ connectionString: adminUrl() });
  await client.connect();
  try {
    const exists = await client.query("SELECT 1 FROM pg_database WHERE datname = $1", [
      dbName,
    ]);
    if (exists.rowCount === 0) {
      await client.query(`CREATE DATABASE ${quoteIdent(dbName)}`);
    }
  } finally {
    await client.end();
  }
}

/**
 * Apply all migrations to `dbName`. Files are applied once, in lexicographic order.
 * `withSchedule` adds 0014_schedule.sql (staging / production only; the test harness
 * never applies it — 06-testing.md).
 */
export async function applyMigrations(dbName, { withSchedule = false } = {}) {
  await ensureDatabase(dbName);
  const url = urlForDb(dbName);
  const files = fs
    .readdirSync(MIGRATIONS)
    .filter((f) => f.endsWith(".sql"))
    .sort();

  // Ledger for idempotent re-runs (P0-3: "second run is a no-op"). Created by the
  // bootstrap role; only the migration runner owns it.
  const ledger = new pg.Client({ connectionString: url });
  await ledger.connect();
  let applied;
  try {
    await ledger.query(
      "CREATE TABLE IF NOT EXISTS _migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())",
    );
    applied = new Set(
      (await ledger.query("SELECT name FROM _migrations")).rows.map((r) => r.name),
    );
  } finally {
    await ledger.end();
  }

  for (const file of files) {
    if (file === SCHEDULE_FILE && !withSchedule) continue;
    if (applied.has(file)) continue;
    await psqlFile(url, path.join(MIGRATIONS, file));
    const client = new pg.Client({ connectionString: url });
    await client.connect();
    try {
      await client.query("INSERT INTO _migrations (name) VALUES ($1)", [file]);
    } finally {
      await client.end();
    }
  }
  return url;
}

// CLI: npm run db:migrate [-- --with-schedule]
if (import.meta.url === `file://${process.argv[1]}`) {
  const withSchedule = process.argv.includes("--with-schedule");
  const dbName = process.env.SA_DB ?? "postgres";
  await applyMigrations(dbName, { withSchedule });
  console.log(
    `migrations applied to ${dbName}${withSchedule ? " (+0014 schedule)" : ""}`,
  );
}
