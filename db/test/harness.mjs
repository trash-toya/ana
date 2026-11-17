// Test harness (06-testing.md): one database per test file, created from migrations,
// dropped after; parallel CI without cross-talk.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { applyMigrations, BOOTSTRAP_URL } from "./scripts/migrate.mjs";

const SQL_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "sql");

export { BOOTSTRAP_URL };

export function newDbName(label = "t") {
  return `sa_test_${label}_${randomUUID().replaceAll("-", "").slice(0, 12)}`;
}

export async function createTestDb(name) {
  const client = new pg.Client({ connectionString: BOOTSTRAP_URL });
  await client.connect();
  try {
    await client.query(`CREATE DATABASE ${name}`);
  } finally {
    await client.end();
  }
  const url = await applyMigrations(name);
  await removePolicyJobs(url);
  return url;
}

/**
 * Drop every Timescale compression/retention policy job. Tests write data months in
 * the past, so the 90-day retention policy would otherwise drop the chunks under the
 * test mid-run (observed: stats/history tables randomly emptied). Test databases are
 * hermetic: no background job mutates state (06-testing.md).
 */
export async function removePolicyJobs(url) {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    const jobs = await client.query(
      `SELECT job_id FROM timescaledb_information.jobs
        WHERE proc_name LIKE 'policy_%' OR proc_name = 'policy_compression'`,
    );
    for (const row of jobs.rows) {
      await client.query("SELECT delete_job($1)", [row.job_id]);
    }
  } finally {
    await client.end();
  }
}

export async function dropTestDb(name) {
  const client = new pg.Client({ connectionString: BOOTSTRAP_URL });
  await client.connect();
  const terminateBackends = () =>
    client.query(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
        WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [name],
    );
  try {
    // Terminate leftovers, then drop. Safe on an already-gone database.
    await terminateBackends();
    // Timescale's launcher can respawn a background worker (scheduler /
    // telemetry reporter) in the gap between the terminate and the DROP, so a
    // single pass is racy (error 55006 "being accessed by other users").
    // Retry: terminate whoever reconnected, wait, drop again.
    for (let attempt = 0; ; attempt++) {
      try {
        await client.query(`DROP DATABASE IF EXISTS ${name}`);
        return;
      } catch (err) {
        if (err.code !== "55006" || attempt >= 20) throw err;
        await terminateBackends();
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    }
  } finally {
    await client.end();
  }
}

/**
 * Create a test database, run `fn`, always drop the database afterwards.
 */
export async function withTestDb(label, fn) {
  const name = newDbName(label);
  const url = await createTestDb(name);
  const pool = new pg.Pool({ connectionString: url, max: 40 });
  try {
    return await fn({ pool, url, dbName: name });
  } finally {
    await pool.end();
    await dropTestDb(name);
  }
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

/**
 * A recent anchor for event times. Fixed-date fixtures (e.g. 2026-11-15) rot:
 * invariant I3 only counts events within `session_guard.created_at - 24 h` and
 * I8 within `now() - 3 days`, so an old hardcoded date eventually falls outside
 * the window and the invariant reports a phantom violation. Anchoring to
 * wall-clock minus a couple of hours keeps every asserted event inside both
 * windows regardless of when the suite runs.
 */
export function recentBase(hoursAgo = 2) {
  return Date.now() - hoursAgo * 3_600_000;
}

export function sha(parts) {
  // Deterministic stand-in for payloadHash: any distinct string per payload works;
  // the canonical-JSON contract lives in packages/shared (P1).
  let h = 5381;
  const s = JSON.stringify(parts);
  for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
  return `h${h.toString(16).padStart(8, "0")}`;
}

export function event({
  tenantId,
  sessionId,
  eventId,
  seq,
  time,
  name = "page_view",
  cat = "e",
  url = "/p",
  props = {},
  anonId = "a1",
  userId = null,
  ord,
  source = "client",
  receivedAt,
}) {
  const iso = typeof time === "string" ? time : time.toISOString();
  return {
    tenant_id: tenantId,
    session_id: sessionId,
    event_id: eventId,
    name,
    cat,
    time: iso,
    // Rev 5 (D32): the write path always supplies the receipt time.
    received_at: receivedAt ?? iso,
    seq,
    anon_id: anonId,
    user_id: userId,
    url,
    source,
    props,
    payload_hash: sha([sessionId, name, url, props, time, seq]),
    ord: ord ?? 0,
  };
}

/** Call ingest_batch with a batch of events on one client. Returns the parsed result. */
export async function ingest(client, events) {
  const res = await client.query("SELECT ingest_batch($1) AS r", [
    JSON.stringify(events),
  ]);
  return res.rows[0]?.r ?? { stored: [], dups: [], capped: [], conflicts: [] };
}

/** Force-archive: age every touch_bucket, then run the job directly. */
export async function forceArchive(pool, { minutes = 50 } = {}) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `UPDATE sessions_active
          SET touch_bucket = floor(extract(epoch FROM now()) / 600)::int - $1
          WHERE touch_bucket > floor(extract(epoch FROM now()) / 600)::int - $1`,
      [minutes],
    );
    await client.query("CALL archive_sessions(NULL, NULL)");
    await client.query("COMMIT");
  } finally {
    client.release();
  }
}

export function readInvariant(name) {
  return fs.readFileSync(path.join(SQL_DIR, `${name}.sql`), "utf8");
}

/** Run an invariant query and assert the (single) scalar is 0. */
export async function assertInvariant(client, name) {
  const rows = await client.query(readInvariant(name));
  const scalar = Number(Object.values(rows.rows[0] ?? { n: 0 })[0]);
  if (scalar !== 0) {
    throw new Error(`invariant ${name} violated: ${scalar}`);
  }
  return scalar;
}

/** Assert no custom job is scheduled (the harness never applies 0014). */
export async function assertNoCustomJobs(client) {
  const res = await client.query(
    `SELECT count(*) AS n FROM timescaledb_information.jobs
      WHERE proc_name IN ('archive_sessions', 'rebuild_session_stats',
                          'cleanup_idempotency', 'reconcile_session_stats',
                          'rollup_daily')`,
  );
  if (Number(res.rows[0].n) !== 0) {
    throw new Error("custom jobs are scheduled in a test database (0014 leaked)");
  }
}
