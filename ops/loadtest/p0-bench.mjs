// P0-10 baseline benchmarks (08-execution.md):
//   sustained ingest, burst ingest, hot-session ingest, WAL bytes/s,
//   HOT-update ratio on the churned tables, archive duration.
//
// Usage: npm run bench:p0
//   BENCH_SECONDS   sustained window (default 30)
//   BENCH_RATE      sustained target events/s (default 400)
//   BENCH_DB        database name (default sa_bench_<ts>, dropped afterwards)
//
// The database is created from migrations and dropped at the end. No 0014 schedule,
// no policy jobs (same hermetic setup as the tests — see db/test/README.md).
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import pg from "pg";
import {
  createTestDb,
  dropTestDb,
  removePolicyJobs,
  event,
} from "../../db/test/harness.mjs";

const execFileP = promisify(execFile);
const SECONDS = Number(process.env.BENCH_SECONDS ?? 30);
const RATE = Number(process.env.BENCH_RATE ?? 400);
const BATCH = 500; // matches the API's MAX_BATCH
const WORKERS = 8; // matches host cores
const BOOTSTRAP =
  process.env.DATABASE_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";

const tenantA = crypto.randomUUID();
const results = {};

const pct = (arr, p) => {
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const r1 = (x) => Math.round(x * 10) / 10;
const MB = (b) => Math.round(b / (1024 * 1024));

// ---------------------------------------------------------------- DB lifecycle
const dbName = `sa_bench_${Date.now() % 100000000}`;
const url = await createTestDb(dbName);
await removePolicyJobs(url);

const pool = new pg.Pool({ connectionString: url, max: WORKERS + 2 });
const tenant = async () => tenantA;

// Register the tenant (provisioning step; api_keys are P1).
{
  const c = await pool.connect();
  await c.query("INSERT INTO tenants (tenant_id, name) VALUES ($1, 'bench')", [tenantA]);
  c.release();
}

// One ingest call: BATCH events across `sessions` distinct sessions.
function makeBatch(sessionIds, t0Ms, i) {
  const out = [];
  for (let j = 0; j < BATCH; j++) {
    const sessionId = sessionIds[(i * BATCH + j) % sessionIds.length];
    const t = new Date(t0Ms + ((i * BATCH + j) % 3600) * 1000);
    out.push(
      event({
        tenantId: tenantA,
        sessionId,
        eventId: crypto.randomUUID(),
        seq: 1 + ((i * BATCH + j) % 3600),
        time: t,
        name: "page_view",
        cat: "e",
      }),
    );
  }
  return out;
}

async function runSustained(label, sessionIds, rate, seconds) {
  const latencies = [];
  let stored = 0;
  let batches = 0;
  const startedAt = Date.now();

  await Promise.all(
    Array.from({ length: WORKERS }, async (_, w) => {
      let i = 0;
      let myBatches = 0;
      while (Date.now() - startedAt < seconds * 1000) {
        const events = makeBatch(sessionIds, Date.now() - 3600_000, w * 1000 + i);
        const t = performance.now();
        const client = await pool.connect();
        try {
          const res = await client.query("SELECT ingest_batch($1) AS r", [
            JSON.stringify(events),
          ]);
          stored += res.rows[0].r.stored.length;
        } finally {
          client.release();
        }
        latencies.push(performance.now() - t);
        batches++;
        i++;
        myBatches++;
        // Closed-loop pacing: worker w must finish myBatches batches by now.
        const targetMs = (myBatches * BATCH * WORKERS * 1000) / rate;
        const elapsed = Date.now() - startedAt;
        if (elapsed < targetMs) {
          await new Promise((r) => setTimeout(r, targetMs - elapsed));
        }
      }
    }),
  );

  const wall = seconds;
  results[label] = {
    stored,
    achieved_eps: Math.round(stored / wall),
    target_eps: rate,
    p50_ms: r1(pct(latencies, 50)),
    p95_ms: r1(pct(latencies, 95)),
    p99_ms: r1(pct(latencies, 99)),
    max_ms: r1(Math.max(...latencies)),
    calls: batches,
  };
}

// WAL + HOT snapshot helper
async function statsSnapshot() {
  const c = await pool.connect();
  try {
    const wal = await c.query(
      "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0') AS b",
    );
    const tables = await c.query(`
      SELECT relname, n_tup_ins, n_tup_upd, n_tup_hot_upd
        FROM pg_stat_user_tables
       WHERE relname IN ('sessions_active', 'session_guard', 'event_dedupe')`);
    return {
      wal_bytes: Number(wal.rows[0].b),
      tables: Object.fromEntries(
        tables.rows.map((r) => [
          r.relname,
          {
            ins: Number(r.n_tup_ins),
            upd: Number(r.n_tup_upd),
            hot: Number(r.n_tup_hot_upd),
          },
        ]),
      ),
    };
  } finally {
    c.release();
  }
}

// ------------------------------------------------------------------ 1. sustained
console.log(`bench: sustained ${RATE} ev/s for ${SECONDS}s across ${WORKERS} workers…`);
const sessSustained = Array.from({ length: 2000 }, () => crypto.randomUUID());
const wal0 = (await statsSnapshot()).wal_bytes;
const tSustained = Date.now();
await runSustained("sustained", sessSustained, RATE, SECONDS);
const wal1 = (await statsSnapshot()).wal_bytes;
results.sustained.wal_mb_per_s = r1((wal1 - wal0) / (1024 * 1024) / SECONDS);
results.sustained.wal_bytes_per_event = Math.round(
  (wal1 - wal0) / results.sustained.stored,
);
console.log("  sustained:", JSON.stringify(results.sustained));

// --------------------------------------------------------------------- 2. burst
console.log("bench: burst (max rate, 2s wall, fresh sessions)…");
{
  const sessBurst = Array.from({ length: 5000 }, () => crypto.randomUUID());
  const latencies = [];
  let stored = 0;
  const stop = Date.now() + 2000;
  await Promise.all(
    Array.from({ length: WORKERS }, async (_, w) => {
      let i = 0;
      while (Date.now() < stop) {
        const events = makeBatch(sessBurst, Date.now() - 3600_000, w * 5000 + i);
        const t = performance.now();
        const client = await pool.connect();
        try {
          const res = await client.query("SELECT ingest_batch($1) AS r", [
            JSON.stringify(events),
          ]);
          stored += res.rows[0].r.stored.length;
        } finally {
          client.release();
        }
        latencies.push(performance.now() - t);
        i++;
      }
    }),
  );
  results.burst = {
    stored,
    achieved_eps: Math.round(stored / 2),
    p50_ms: r1(pct(latencies, 50)),
    p95_ms: r1(pct(latencies, 95)),
    max_ms: r1(Math.max(...latencies)),
  };
  console.log("  burst:", JSON.stringify(results.burst));
}

// ---------------------------------------------------------------- 3. hot session
console.log("bench: hot session (100 writers, one session, to the cap)…");
{
  // Fresh session per run is capped at 5,000 events; time the storm.
  const sessionId = crypto.randomUUID();
  const latencies = [];
  let stored = 0;
  const workers = Array.from({ length: 20 }, async (_, w) => {
    const client = await pool.connect();
    try {
      for (let i = 0; i < (500 / 500) * 25; i++) {
        // 25 batches of 20 events each
        const events = Array.from({ length: 20 }, (_, j) =>
          event({
            tenantId: tenantA,
            sessionId,
            eventId: crypto.randomUUID(),
            seq: w * 500 + i * 20 + j + 1,
            time: new Date(Date.now() - 3600_000 + (w * 500 + i * 20 + j) * 1000),
            name: "page_view",
            cat: "e",
          }),
        );
        const t = performance.now();
        const res = await client.query("SELECT ingest_batch($1) AS r", [
          JSON.stringify(events),
        ]);
        stored += res.rows[0].r.stored.length;
        latencies.push(performance.now() - t);
      }
    } finally {
      client.release();
    }
  });
  const t0 = Date.now();
  await Promise.all(workers);
  const wall = (Date.now() - t0) / 1000;
  results.hot_session = {
    stored,
    wall_s: r1(wall),
    achieved_eps: Math.round(stored / wall),
    p50_ms: r1(pct(latencies, 50)),
    p95_ms: r1(pct(latencies, 95)),
  };
  console.log("  hot_session:", JSON.stringify(results.hot_session));
}

// ------------------------------------------------------------- 4. archive duration
console.log("bench: archive duration (age all sessions, run job)…");
{
  const c = await pool.connect();
  try {
    const t = performance.now();
    await c.query("BEGIN");
    await c.query(
      `UPDATE sessions_active
          SET touch_bucket = floor(extract(epoch FROM now()) / 600)::int - 6
        WHERE touch_bucket > floor(extract(epoch FROM now()) / 600)::int - 6`,
    );
    await c.query("CALL archive_sessions(NULL, NULL)");
    await c.query("COMMIT");
    const ms = performance.now() - t;
    const hist = await c.query("SELECT count(*) AS n FROM sessions_history");
    const active = await c.query("SELECT count(*) AS n FROM sessions_active");
    results.archive = {
      moved: Number(hist.rows[0].n),
      active_left: Number(active.rows[0].n),
      duration_ms: r1(ms),
      per_1k_rows_ms: r1((ms / Math.max(1, Number(hist.rows[0].n))) * 1000),
    };
    console.log("  archive:", JSON.stringify(results.archive));
  } finally {
    c.release();
  }
}

// ------------------------------------------------------------- final table stats
const finalStats = await statsSnapshot();
results.hot_tables = finalStats.tables;

await pool.end();
await dropTestDb(dbName);

console.log("\n=== P0 benchmark results ===");
console.log(JSON.stringify(results, null, 2));
execFileP("mkdir", ["-p", "docs/plan/benchmarks"])
  .then(() =>
    execFileP("tee", ["docs/plan/benchmarks/p0-results.json"], {
      input: JSON.stringify(results, null, 2),
    }),
  )
  .catch(() => {});
