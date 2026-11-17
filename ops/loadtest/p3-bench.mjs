// P3-6 dashboard performance benchmark (08-execution.md P3-6): p95 under 500 ms
// with synthetic peak ingest.
//
// Usage: pnpm run bench:p3
//   BENCH_RATE      background ingest rate, events/s (default 200)
//   BENCH_SECONDS   background load window (default 20)
//   BENCH_SESSIONS  synthetic sessions seeded (default 2000)
//
// Scenario A (steady state): ~2000 sessions / ~10k events ingested through the
// real ingest_batch(), archived, CAGGs refreshed, nightly rollup run; then each
// dashboard endpoint is exercised 50x over HTTP (the real service).
// Scenario B (under load): the same endpoints re-measured while background
// ingest runs at BENCH_RATE into fresh sessions — the "with ingest at peak"
// condition at dashboard scale.
//
// Database created from migrations, dropped afterwards; no schedule/policy jobs
// (same hermetic setup as the tests and p0-bench).
import { createHash, randomUUID } from "node:crypto";
import pg from "pg";
import request from "supertest";
import {
  createTestDb,
  dropTestDb,
  event,
  ingest,
  removePolicyJobs,
} from "../../db/test/harness.mjs";
import { createApp } from "../../services/dashboard/dist/app.js";

const SESSIONS = Number(process.env.BENCH_SESSIONS ?? 2000);
const RATE = Number(process.env.BENCH_RATE ?? 200);
const SECONDS = Number(process.env.BENCH_SECONDS ?? 20);
const ITER = 50;
const HOUR = 3_600_000;

const pct = (arr, p) => {
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const r1 = (x) => Math.round(x * 10) / 10;

const results = {
  scenario_a_steady: {},
  scenario_b_under_load: {},
  config: {
    sessions: SESSIONS,
    rate: RATE,
    seconds: SECONDS,
    iterations: ITER,
  },
};

// ---------------------------------------------------------------- DB lifecycle
const dbName = `sa_bench_p3_${Date.now() % 100000000}`;
const url = await createTestDb(dbName);
await removePolicyJobs(url);
const pool = new pg.Pool({ connectionString: url, max: 12 });

const tenant = randomUUID();
const secondTenant = randomUUID();
const rawKey = `dk_${randomUUID()}`;
const ingestKeyHash = createHash("sha256").update(rawKey, "utf8").digest();

// ---------------------------------------------------------------- seed data
console.log(`seeding ${SESSIONS} sessions …`);
{
  const c = await pool.connect();
  try {
    await c.query(
      "INSERT INTO tenants (tenant_id, name) VALUES ($1, 'bench-p3'), ($2, 'bench-p3-b')",
      [tenant, secondTenant],
    );
    await c.query(
      "INSERT INTO tenant_settings (tenant_id, tz) VALUES ($1, 'Asia/Kolkata')",
      [tenant],
    );
    await c.query(
      `INSERT INTO api_keys (key_hash, tenant_id, kind) VALUES ($1, $2, 'dashboard')`,
      [ingestKeyHash, tenant],
    );
  } finally {
    c.release();
  }

  // 2000 sessions × ~5 events across the trailing 72 h, through the real
  // ingest_batch (500-event batches). Second tenant adds cross-tenant volume
  // the dashboard queries must scan past.
  const totalBatches = Math.ceil((SESSIONS * 5) / 500);
  const sessionIds = Array.from({ length: SESSIONS }, () => randomUUID());
  for (let b = 0; b < totalBatches; b++) {
    const evs = [];
    const count = Math.min(500, SESSIONS * 5 - b * 500);
    for (let j = 0; j < count; j++) {
      const n = b * 500 + j;
      const sessionId = sessionIds[n % SESSIONS];
      const t = Date.now() - (72 * HOUR * (n % SESSIONS)) / SESSIONS + (n % 300) * 1000;
      evs.push(
        event({
          tenantId: n % 10 === 0 ? secondTenant : tenant,
          sessionId,
          eventId: randomUUID(),
          seq: (n % 5) + 1,
          time: new Date(t).toISOString(),
          name: ["page_view", "click", "page_view", "click", "checkout"][n % 5],
          anonId: `anon_${n % 400}`,
          userId: n % 3 === 0 ? `user_${n % 700}` : null,
        }),
      );
    }
    const r = await ingest(c, evs);
    if (r.stored.length !== evs.length) throw new Error(`seed batch ${b} incomplete`);
  }
  console.log(`  ingested ${SESSIONS * 5} events in ${totalBatches} batches`);

  // Archive everything, materialize the CAGGs, run the nightly rollup once.
  await c.query(
    `UPDATE sessions_active
        SET touch_bucket = floor(extract(epoch FROM now()) / 600)::int - 60`,
  );
  const t0 = Date.now();
  await c.query("CALL archive_sessions(NULL, NULL)");
  results.archive_ms = Date.now() - t0;
  await c.query(
    `CALL refresh_continuous_aggregate('events_15m', NULL, now() - INTERVAL '15 minutes')`,
  );
  await c.query(
    `CALL refresh_continuous_aggregate('identity_15m', NULL, now() - INTERVAL '15 minutes')`,
  );
  await c.query("CALL rollup_daily($1, $2)", [0, "{}"]);

  // A live roster: 60 active sessions with fresh touch buckets.
  for (let i = 0; i < 60; i++) {
    await c.query(
      `INSERT INTO sessions_active (tenant_id, session_id, home_day, anon_id, started_at,
                                    last_seen_at, event_count, total_events, touch_bucket,
                                    entry_url, device, country)
       VALUES ($1, $2, date_trunc('day', now()), $3, now() - interval '5 minutes',
               now() - ($4 * interval '1 second'),
               2, 3, floor(extract(epoch FROM now()) / 600)::int, '/live', 'mobile', 'DE')`,
      [tenant, randomUUID(), `live_${i}`, i],
    );
  }
}

// ---------------------------------------------------------------- the service
const app = createApp({ pool });
const auth = { "X-API-Key": rawKey };
const from = new Date(Date.now() - 96 * HOUR).toISOString();
const to = new Date().toISOString();
const replayIds = await (async () => {
  const c = await pool.connect();
  try {
    const r = await c.query(
      `SELECT session_id FROM sessions_history WHERE tenant_id = $1 LIMIT 10`,
      [tenant],
    );
    return r.rows.map((r) => r.session_id);
  } finally {
    c.release();
  }
})();

async function measure(name, fn, group = "scenario_a_steady", iterations = ITER) {
  const latencies = [];
  let failures = 0;
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    const ok = await fn(i);
    latencies.push(performance.now() - t0);
    if (!ok) failures++;
  }
  latencies.sort((a, b) => a - b);
  const row = {
    p50: r1(pct(latencies, 50)),
    p95: r1(pct(latencies, 95)),
    p99: r1(pct(latencies, 99)),
    max: r1(latencies[latencies.length - 1]),
    failures,
  };
  results[group][name] = row;
  console.log(
    `  ${name.padEnd(28)} p50 ${row.p50}  p95 ${row.p95}  p99 ${row.p99}  max ${row.max}${failures ? `  FAILURES ${failures}` : ""}`,
  );
  return row;
}

console.log(`scenario A: steady state, ${ITER} iterations per endpoint`);
await measure("replay", (i) =>
  request(app)
    .get(`/v1/sessions/${replayIds[i % replayIds.length]}/replay`)
    .set(auth)
    .then((r) => r.status === 200),
);
await measure("hourly", () =>
  request(app)
    .get(`/v1/metrics/hourly?from=${from}&to=${to}`)
    .set(auth)
    .then((r) => r.status === 200),
);
await measure("daily", () =>
  request(app)
    .get(`/v1/metrics/daily?from=${from}&to=${to}`)
    .set(auth)
    .then((r) => r.status === 200),
);
await measure("entry_pages", () =>
  request(app)
    .get(`/v1/entry-pages?from=${from}&to=${to}`)
    .set(auth)
    .then((r) => r.status === 200),
);
await measure("funnel_3_steps", () =>
  request(app)
    .post("/v1/funnels")
    .set(auth)
    .send({ from, to, steps: ["page_view", "click", "checkout"] })
    .then((r) => r.status === 200),
);
await measure("live", () =>
  request(app)
    .get("/v1/live")
    .set(auth)
    .then((r) => r.status === 200),
);

// ---------------------------------------------------------------- under load
console.log(`scenario B: background ingest at ${RATE} ev/s for ${SECONDS}s`);
let stopIngest = false;
const ingestErrors = [];
async function ingestWorker(workerId) {
  const c = await pool.connect();
  try {
    let seq = 0;
    while (!stopIngest) {
      const t0 = Date.now();
      const batch = [];
      for (let j = 0; j < 500; j++) {
        const sessionId = randomUUID();
        batch.push(
          event({
            tenantId: secondTenant,
            sessionId,
            eventId: randomUUID(),
            seq: 1,
            time: new Date().toISOString(),
            name: "page_view",
            anonId: `load_${workerId}`,
          }),
        );
        seq++;
      }
      await ingest(c, batch);
      const spent = Date.now() - t0;
      const wait = Math.max(0, (500 / RATE) * 1000 - spent);
      if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    }
  } catch (err) {
    ingestErrors.push(String(err));
  } finally {
    c.release();
  }
}
const workers = Array.from({ length: 4 }, (_, w) => ingestWorker(w));
await new Promise((r) => setTimeout(r, 2000)); // let the load spin up

await measure(
  "under_load_hourly",
  () =>
    request(app)
      .get(`/v1/metrics/hourly?from=${from}&to=${to}`)
      .set(auth)
      .then((r) => r.status === 200),
  "scenario_b_under_load",
);
await measure(
  "under_load_live",
  () =>
    request(app)
      .get("/v1/live")
      .set(auth)
      .then((r) => r.status === 200),
  "scenario_b_under_load",
);
await measure(
  "under_load_daily",
  () =>
    request(app)
      .get(`/v1/metrics/daily?from=${from}&to=${to}`)
      .set(auth)
      .then((r) => r.status === 200),
  "scenario_b_under_load",
);
await measure(
  "under_load_funnel",
  () =>
    request(app)
      .post("/v1/funnels")
      .set(auth)
      .send({ from, to, steps: ["page_view", "click", "checkout"] })
      .then((r) => r.status === 200),
  "scenario_b_under_load",
);

stopIngest = true;
await Promise.allSettled(workers);
if (ingestErrors.length) {
  console.error(`background ingest errors: ${ingestErrors[0]}`);
  process.exit(1);
}

// ---------------------------------------------------------------- verdict
const targets = Object.entries(results)
  .filter(([k]) => k.startsWith("scenario"))
  .flatMap(([, v]) => Object.values(v))
  .filter((row) => row && typeof row === "object" && "p95" in row);
const worst = Math.max(...targets.map((t) => t.p95));
results.verdict = {
  worst_p95_ms: worst,
  target_ms: 500,
  pass: worst < 500 && targets.every((t) => t.failures === 0),
};
console.log(
  `\nworst p95: ${worst} ms — ${results.verdict.pass ? "PASS (< 500 ms)" : "FAIL"}`,
);

await pool.end();
await dropTestDb(dbName);

const { writeFileSync } = await import("node:fs");
writeFileSync(
  new URL("../../docs/plan/benchmarks/p3-results.json", import.meta.url),
  JSON.stringify(results, null, 2),
);
console.log("results → docs/plan/benchmarks/p3-results.json");
process.exit(results.verdict.pass ? 0 : 1);
