// Ingest service entry point. DATABASE_URL points at PgBouncer (transaction
// mode) in production; tests point it straight at a test database.

import pg from "pg";
import { createApp } from "./app.js";
import { configFromEnv } from "./config.js";
import { Flusher } from "./flusher.js";
import { SessionService } from "./sessions.js";

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
  statement_timeout: 10_000,
});

// Database restarts and PgBouncer failovers terminate idle clients with a
// FATAL (57P01). An unhandled pool 'error' event would crash the process (and
// take every in-flight request with it); log it — pg discards the broken
// client and the pool reconnects on the next query.
pool.on("error", (err: Error & { code?: string }) => {
  console.error(JSON.stringify({ msg: "db_pool_error", error: err.message, code: err.code }));
});

// Bootstrap/resume is optional: without a secret the routes stay unmounted and
// the service is ingestion-only. Never invent a secret in production.
const contextSecret = process.env.SESSION_CONTEXT_SECRET;
const sessions = contextSecret ? new SessionService({ pool, secret: contextSecret }) : undefined;
if (!sessions) {
  console.error(
    JSON.stringify({ msg: "session_bootstrap_disabled", reason: "SESSION_CONTEXT_SECRET is not set" }),
  );
}

const flusher = new Flusher({ pool, ...config });
const app = createApp({ pool, flusher, sessions });
const server = app.listen(config.port, () => {
  console.log(JSON.stringify({ msg: "listening", port: config.port }));
});

async function shutdown(signal: string): Promise<void> {
  console.log(JSON.stringify({ msg: "shutting_down", signal }));
  // Stop accepting, drain the buffer (acknowledged only after commit), then
  // release the pool. Hard cap if the DB hangs.
  server.close();
  const drain = flusher.shutdown();
  const cap = new Promise((resolve) => setTimeout(resolve, 10_000).unref());
  await Promise.race([drain, cap]);
  await pool.end();
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
