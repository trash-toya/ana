#!/usr/bin/env node
// T11 — failover drill under load (P4-4).
//
// What this proves, against the real pinned image on this machine:
//   * every batch the ingest path acknowledged before fencing is present on the
//     promoted standby (RPO = 0 with synchronous_commit=on),
//   * the write endpoint is usable again after fencing + promotion + the
//     PgBouncer repoint, within the RTO budget (measured, printed),
//   * Timescale jobs exist and execute on the promoted node.
//
// Sequence: primary + streaming standby, migrations (with the 0014 schedule),
// synchronous replication enabled, PgBouncer in front, load through PgBouncer,
// then: fence (SIGKILL primary) → pg_promote() on the standby → recreate
// PgBouncer with DB_HOST=standby → verify acknowledged IDs → run a job.
//
// This is the drill, not the production controller: Patroni/etcd do leader
// election and fencing in production (07-operations.md, D39). The drill performs
// the same fencing-first order explicitly so the guarantee is testable.
//
//   node ops/ha/t11-drill.mjs            # setup, drill, teardown
//   node ops/ha/t11-drill.mjs --keep     # leave the stack running afterwards
//
// It is not part of PR CI (06-testing.md): it needs Docker and takes minutes.

import { execFileSync, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import pg from "pg";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const COMPOSE = ["-f", "ops/ha/docker-compose.yml"];
const PRIMARY_PORT = 55432;
const STANDBY_PORT = 55433;
const PGBOUNCER_PORT = 56432;
const WORKERS = 8;
const WARMUP_MS = 2_000;
const POST_RTO_MS = 2_000;
const RTO_BUDGET_MS = 120_000;

const keep = process.argv.includes("--keep");

const url = (port) => `postgresql://postgres:postgres@127.0.0.1:${port}/postgres`;
const nowMs = () => Number(process.hrtime.bigint() / 1_000_000n);

function log(msg) {
  console.log(`[t11] ${msg}`);
}

function compose(args, env = {}) {
  return execFileSync("docker", ["compose", ...COMPOSE, ...args], {
    cwd: ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...env },
  }).trim();
}

function containerId(service) {
  return compose(["ps", "-q", service]);
}

async function withClient(port, fn, { attempts = 1, delayMs = 500 } = {}) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const client = new pg.Client({
      connectionString: url(port),
      connectionTimeoutMillis: 2_000,
      statement_timeout: 10_000,
    });
    try {
      await client.connect();
      return await fn(client);
    } catch (err) {
      lastErr = err;
      if (i + 1 < attempts) await sleep(delayMs);
    } finally {
      await client.end().catch(() => {});
    }
  }
  throw lastErr;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(label, fn, { timeoutMs = 90_000, stepMs = 250 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (err) {
      lastErr = err;
    }
    await sleep(stepMs);
  }
  throw new Error(
    `timed out waiting for ${label}${lastErr ? `: ${lastErr.message}` : ""}`,
  );
}

/** One row in the shape ingest_batch() consumes (03-write-path.md). */
function eventRow({ tenantId, sessionId, eventId, seq }) {
  const time = new Date().toISOString();
  return {
    tenant_id: tenantId,
    session_id: sessionId,
    event_id: eventId,
    name: "drill_click",
    cat: "e",
    time,
    received_at: time,
    seq,
    anon_id: "drill-anon",
    user_id: null,
    url: "/drill",
    source: "client",
    props: {},
    payload_hash: `h-${eventId}`,
    ord: 0,
  };
}

async function main() {
  if (spawnSync("docker", ["info"], { stdio: "ignore" }).status !== 0) {
    console.error(
      "[t11] Docker is not available; this drill needs it (not a PR CI test).",
    );
    process.exit(1);
  }

  log("resetting the HA stack");
  compose(["down", "-v", "--remove-orphans"]);
  compose(["up", "-d", "--wait", "primary"]);

  log("applying migrations (with the 0014 schedule)");
  const migrated = spawnSync("pnpm", ["db:migrate", "--", "--with-schedule"], {
    cwd: ROOT,
    encoding: "utf8",
    env: { ...process.env, DATABASE_URL: url(PRIMARY_PORT) },
  });
  if (migrated.status !== 0) throw new Error(`migrations failed:\n${migrated.stderr}`);

  log("starting the standby (pg_basebackup + primary_conninfo)");
  compose(["up", "-d", "--wait", "standby"]);

  await waitFor("a streaming standby", () =>
    withClient(PRIMARY_PORT, async (c) => {
      const { rows } = await c.query(
        "SELECT count(*)::int AS n FROM pg_stat_replication WHERE state = 'streaming'",
      );
      return rows[0].n >= 1;
    }),
  );

  log("enabling synchronous replication");
  await withClient(PRIMARY_PORT, (c) =>
    c
      .query("ALTER SYSTEM SET synchronous_standby_names = '*'")
      .then(() => c.query("SELECT pg_reload_conf()")),
  );
  await waitFor("sync_state = sync", () =>
    withClient(PRIMARY_PORT, async (c) => {
      const { rows } = await c.query(
        "SELECT count(*)::int AS n FROM pg_stat_replication WHERE sync_state = 'sync'",
      );
      return rows[0].n >= 1;
    }),
  );

  log("starting PgBouncer (the virtual endpoint)");
  compose(["up", "-d", "pgbouncer"]);
  await waitFor(
    "PgBouncer",
    () => withClient(PGBOUNCER_PORT, (c) => c.query("SELECT 1")),
    {
      timeoutMs: 60_000,
    },
  );

  const tenantId = randomUUID();
  await withClient(PRIMARY_PORT, (c) =>
    c.query("INSERT INTO tenants (name) VALUES ($1)", [`t11-${tenantId.slice(0, 8)}`]),
  );

  // ---- load ---------------------------------------------------------------
  const pool = new pg.Pool({
    connectionString: url(PGBOUNCER_PORT),
    max: WORKERS,
    connectionTimeoutMillis: 2_000,
    statement_timeout: 5_000,
    idleTimeoutMillis: 500,
  });
  let stop = false;
  let errors = 0;
  const acked = [];

  async function worker() {
    const own = [];
    let sessionId = randomUUID();
    let seq = 0;
    let inSession = 0;
    while (!stop) {
      if (inSession >= 1_000) {
        // Stay well inside the 5,000 per-session cap.
        sessionId = randomUUID();
        seq = 0;
        inSession = 0;
      }
      const eventId = randomUUID();
      try {
        const { rows } = await pool.query("SELECT ingest_batch($1) AS r", [
          JSON.stringify([eventRow({ tenantId, sessionId, eventId, seq })]),
        ]);
        const result = rows[0].r;
        if (result.stored.includes(eventId) || result.dups.includes(eventId)) {
          own.push(eventId);
        }
        seq++;
        inSession++;
      } catch {
        if (stop) break;
        errors++;
        await sleep(100);
      }
    }
    return own;
  }

  log(`load: ${WORKERS} writers through PgBouncer for ${WARMUP_MS} ms`);
  const workers = Array.from({ length: WORKERS }, () => worker());
  await sleep(WARMUP_MS);

  // ---- fence + promote ----------------------------------------------------
  const primaryId = containerId("primary");
  log(`fencing the primary (SIGKILL ${primaryId.slice(0, 12)})`);
  const t0 = nowMs();
  execFileSync("docker", ["kill", primaryId], { stdio: "ignore" });

  log("promoting the standby");
  await waitFor(
    "the standby to leave recovery",
    () =>
      withClient(
        STANDBY_PORT,
        async (c) => {
          await c.query("SELECT pg_promote(true, 30)");
          const { rows } = await c.query("SELECT pg_is_in_recovery() AS ro");
          return rows[0].ro === false;
        },
        { attempts: 1 },
      ),
    { timeoutMs: 60_000, stepMs: 500 },
  );

  log("repointing PgBouncer at the promoted standby");
  // The promoted node inherits `synchronous_standby_names='*'` from the primary.
  // With no standby left, every commit would wait for one and fail after
  // wal_sender_timeout. A failover controller clears the list on promotion
  // (Patroni does this); the drill does it explicitly before traffic resumes.
  await withClient(STANDBY_PORT, async (c) => {
    await c.query("ALTER SYSTEM RESET synchronous_standby_names");
    await c.query("SELECT pg_reload_conf()");
  });
  compose(["up", "-d", "--no-deps", "--force-recreate", "pgbouncer"], {
    DB_HOST: "standby",
  });
  await waitFor(
    "a writable endpoint through PgBouncer",
    () =>
      withClient(PGBOUNCER_PORT, async (c) => {
        const { rows } = await c.query("SELECT pg_is_in_recovery() AS ro");
        return rows[0].ro === false;
      }),
    { timeoutMs: RTO_BUDGET_MS },
  );
  const rtoMs = nowMs() - t0;

  await sleep(POST_RTO_MS);
  stop = true;
  const perWorker = await Promise.all(workers);
  for (const list of perWorker) acked.push(...list);
  await pool.end().catch(() => {});

  // ---- verify -------------------------------------------------------------
  const missing = [];
  for (let i = 0; i < acked.length; i += 1_000) {
    const chunk = acked.slice(i, i + 1_000);
    const { rows } = await withClient(STANDBY_PORT, (c) =>
      c.query(
        "SELECT event_id::text AS id FROM events WHERE event_id = ANY($1::uuid[])",
        [chunk],
      ),
    );
    const present = new Set(rows.map((r) => r.id));
    for (const id of chunk) if (!present.has(id)) missing.push(id);
  }

  const jobs = await withClient(STANDBY_PORT, async (c) => {
    const { rows } = await c.query(
      `SELECT count(*)::int AS n FROM timescaledb_information.jobs
        WHERE proc_name IN ('archive_sessions', 'cleanup_idempotency',
                            'reconcile_session_stats', 'rollup_daily')`,
    );
    await c.query("CALL archive_sessions(NULL, NULL)");
    return rows[0].n;
  });

  const report = {
    ackedEvents: acked.length,
    missingAfterPromotion: missing.length,
    rpo: missing.length === 0 ? 0 : missing.length,
    rtoMs,
    rtoBudgetMs: RTO_BUDGET_MS,
    loadErrorsDuringFailover: errors,
    customJobsPresent: jobs,
    jobRanAfterPromotion: true,
  };
  console.log(`[t11] result ${JSON.stringify(report)}`);

  if (!keep) {
    log("tearing the stack down");
    compose(["down", "-v", "--remove-orphans"]);
  } else {
    log("stack left running (primary is fenced; standby is the writable node)");
  }

  if (missing.length > 0) {
    console.error(
      `[t11] FAIL: ${missing.length} acknowledged events are missing (RPO > 0)`,
    );
    process.exit(1);
  }
  if (jobs < 4) {
    console.error(`[t11] FAIL: only ${jobs} Timescale jobs present on the promoted node`);
    process.exit(1);
  }
  if (rtoMs > RTO_BUDGET_MS) {
    console.error(`[t11] FAIL: RTO ${rtoMs} ms exceeds ${RTO_BUDGET_MS} ms`);
    process.exit(1);
  }
  log("PASS: no acknowledged batch lost; endpoint writable within budget");
}

main().catch(async (err) => {
  console.error(`[t11] FAIL: ${err.stack ?? err}`);
  if (!keep) {
    try {
      compose(["down", "-v", "--remove-orphans"]);
    } catch {
      // best effort
    }
  }
  process.exit(1);
});
