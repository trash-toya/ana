// Dashboard service entry point. DATABASE_URL points at PgBouncer (transaction
// mode) in production; tests point it straight at a test database.

import pg from "pg";
import { createApp } from "./app.js";
import { configFromEnv } from "./config.js";

const config = configFromEnv();
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  console.error(JSON.stringify({ msg: "DATABASE_URL is required" }));
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: 10, // transaction-mode pooling: few connections go a long way
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  statement_timeout: config.statementTimeoutMs,
});

// Database restarts and PgBouncer failovers terminate idle clients with a
// FATAL (57P01); log it — pg discards the broken client and reconnects.
pool.on("error", (err: Error & { code?: string }) => {
  console.error(
    JSON.stringify({ msg: "db_pool_error", error: err.message, code: err.code }),
  );
});

const app = createApp({ pool });
const server = app.listen(config.port, () => {
  console.log(JSON.stringify({ msg: "listening", port: config.port }));
});

async function shutdown(signal: string): Promise<void> {
  console.log(JSON.stringify({ msg: "shutting_down", signal }));
  // Read-only service: stop accepting, wait for in-flight requests, release.
  const drain = new Promise((resolve) => server.close(resolve));
  const cap = new Promise((resolve) => setTimeout(resolve, 10_000).unref());
  await Promise.race([drain, cap]);
  await pool.end();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
