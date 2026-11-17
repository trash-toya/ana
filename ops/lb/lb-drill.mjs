#!/usr/bin/env node
// P4-5 — sticky ingest routing drill.
//
// Proves the deployed shape: with `balance hdr(X-Analytics-Route)` +
// `hash-type consistent`, every request carrying one visitor's key lands on one
// ingest instance regardless of how many instances exist. The same load through
// a roundrobin frontend is the control.
//
// It also samples Postgres for session_guard lock waits during a hot-session
// burst through each frontend and reports them (informational: the guarantee is
// locality — one instance owns a visitor's session — not a latency SLA).
//
//   node ops/lb/lb-drill.mjs [--keep]
//
// Needs: the base database up (`pnpm db:up`), Docker for HAProxy. Not in CI.

import { execFileSync, spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import pg from "pg";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DB_NAME = "sa_lb_drill";
const BASE_DB = "postgresql://postgres:postgres@127.0.0.1:5432";
const PORT_A = 3101;
const PORT_B = 3102;
const HASH_PORT = 18080;
const CONTROL_PORT = 18081;
const KEYS = 32;
const REQUESTS_PER_KEY = 5;
const keep = process.argv.includes("--keep");

const dbUrl = `${BASE_DB}/${DB_NAME}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (m) => console.log(`[lb] ${m}`);

function run(cmd, args, env = {}) {
  return execFileSync(cmd, args, {
    cwd: ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...env },
  }).trim();
}

async function waitFor(label, fn, { timeoutMs = 30_000, stepMs = 200 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      if (await fn()) return;
    } catch (err) {
      lastErr = err;
    }
    await sleep(stepMs);
  }
  throw new Error(
    `timed out waiting for ${label}${lastErr ? `: ${lastErr.message}` : ""}`,
  );
}

function event(anonId, sessionId) {
  return {
    event_id: randomUUID(),
    session_id: sessionId,
    time: new Date(Date.now() - 1_000).toISOString(),
    name: "lb_click",
    anon_id: anonId,
  };
}

function envelope(anonId, events) {
  return {
    schema_version: 1,
    anon_id: anonId,
    sent_at: new Date().toISOString(),
    events,
  };
}

async function post(port, apiKey, anonId, sessionId, extraHeaders = {}) {
  // One retry: HAProxy can close a keep-alive socket between requests, which
  // surfaces as "fetch failed" on reuse. The drill cares about routing, not
  // connection-pool behaviour.
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/v1/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": apiKey,
          "X-Analytics-Route": anonId,
          ...extraHeaders,
        },
        body: JSON.stringify(envelope(anonId, [event(anonId, sessionId)])),
      });
      return { status: res.status, instance: res.headers.get("x-ingest-instance") };
    } catch (err) {
      if (attempt >= 1) throw err;
      await sleep(50);
    }
  }
}

async function main() {
  if (spawnSync("docker", ["info"], { stdio: "ignore" }).status !== 0) {
    console.error("[lb] Docker is not available (HAProxy runs in a container).");
    process.exit(1);
  }
  run("pnpm", ["db:ping"]);

  log(`migrating ${DB_NAME}`);
  try {
    run("pnpm", ["db:migrate"], { SA_DB: DB_NAME, DATABASE_URL: BASE_DB });
  } catch (err) {
    throw new Error(`migration failed: ${err.stderr ?? err.message}`);
  }

  const admin = new pg.Client({ connectionString: `${BASE_DB}/postgres` });
  await admin.connect();
  await admin.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [DB_NAME],
  );
  await admin.end();

  const pool = new pg.Pool({ connectionString: dbUrl, max: 2 });
  const tenantId = (
    await pool.query("INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id", [
      `lb-${randomUUID().slice(0, 8)}`,
    ])
  ).rows[0].tenant_id;
  const apiKey = `sk_ingest_${randomUUID()}`;
  const { createHash } = await import("node:crypto");
  await pool.query(
    "INSERT INTO api_keys (key_hash, tenant_id, kind) VALUES ($1, $2, 'ingest')",
    [createHash("sha256").update(apiKey, "utf8").digest(), tenantId],
  );
  await pool.end();

  const env = {
    DATABASE_URL: dbUrl,
    // The drill is about routing, not limits: disabled to keep 429s out.
    RATE_LIMIT_IP_PER_SEC: "0",
    RATE_LIMIT_KEY_PER_SEC: "0",
    RATE_LIMIT_TENANT_PER_SEC: "0",
    RATE_LIMIT_SESSION_PER_SEC: "0",
    // Enough DB concurrency for session_guard lock waits to be observable.
    PG_POOL_MAX: "40",
  };
  const servers = [
    { port: PORT_A, id: "ingest-a" },
    { port: PORT_B, id: "ingest-b" },
  ].map(({ port, id }) =>
    spawn("node", ["services/ingest/dist/server.js"], {
      cwd: ROOT,
      env: { ...process.env, ...env, PORT: String(port), INSTANCE_ID: id },
      stdio: ["ignore", "ignore", "inherit"],
    }),
  );

  let haproxyUp = false;
  try {
    for (const port of [PORT_A, PORT_B]) {
      await waitFor(`ingest :${port}`, async () => {
        const res = await fetch(`http://127.0.0.1:${port}/readyz`);
        return res.ok;
      });
    }

    log(`starting HAProxy (${HASH_PORT} hash, ${CONTROL_PORT} roundrobin)`);
    run("docker", ["compose", "-f", "ops/lb/docker-compose.yml", "up", "-d"]);
    haproxyUp = true;
    await waitFor("HAProxy", async () => {
      const res = await fetch(`http://127.0.0.1:${HASH_PORT}/healthz`);
      return res.ok;
    });

    // ---- affinity ---------------------------------------------------------
    log(
      `affinity: ${KEYS} visitors x ${REQUESTS_PER_KEY} requests through :${HASH_PORT}`,
    );
    const anons = Array.from({ length: KEYS }, () => `anon-${randomUUID()}`);
    const perKey = new Map();
    let unexpected = 0;
    for (const anon of anons) {
      const seen = new Set();
      const session = randomUUID();
      for (let i = 0; i < REQUESTS_PER_KEY; i++) {
        const r = await post(HASH_PORT, apiKey, anon, session);
        if (r.status !== 200) unexpected++;
        if (r.instance) seen.add(r.instance);
      }
      perKey.set(anon, seen);
    }
    const multi = [...perKey.values()].filter((s) => s.size > 1).length;
    const instances = new Set([...perKey.values()].flatMap((s) => [...s]));

    // ---- control: the same shape without affinity -------------------------
    log(`control: ${KEYS} visitors through :${CONTROL_PORT} (roundrobin)`);
    let controlMulti = 0;
    for (const anon of anons.slice(0, 8)) {
      const seen = new Set();
      const session = randomUUID();
      for (let i = 0; i < 10; i++) {
        const r = await post(CONTROL_PORT, apiKey, anon, session);
        if (r.instance) seen.add(r.instance);
      }
      if (seen.size > 1) controlMulti++;
    }

    // ---- lock-wait samples (informational) --------------------------------
    async function lockSamples(port, label) {
      const watcher = new pg.Client({ connectionString: dbUrl });
      await watcher.connect();
      let samples = 0;
      let waits = 0;
      let done = false;
      const session = randomUUID();
      const blast = Array.from({ length: 120 }, () =>
        post(port, apiKey, "hot-visitor", session),
      );
      const sampling = (async () => {
        while (!done) {
          const { rows } = await watcher.query(
            `SELECT count(*)::int AS n FROM pg_stat_activity
              WHERE datname = $1 AND wait_event_type = 'Lock'`,
            [DB_NAME],
          );
          samples++;
          if (rows[0].n > 0) waits++;
          await sleep(10);
        }
      })();
      const results = await Promise.allSettled(blast);
      done = true;
      await sampling;
      await watcher.end();
      const ok = results.filter(
        (r) => r.status === "fulfilled" && r.value.status === 200,
      ).length;
      log(`${label}: ${ok}/120 ok, lock-wait samples ${waits}/${samples}`);
      return { ok, waits, samples };
    }
    const hashLocks = await lockSamples(HASH_PORT, "hot session via hash");
    const controlLocks = await lockSamples(CONTROL_PORT, "hot session via roundrobin");

    const report = {
      keys: KEYS,
      requestsPerKey: REQUESTS_PER_KEY,
      visitorsSplitAcrossInstances: multi,
      instancesUsed: [...instances].sort(),
      requestFailures: unexpected,
      controlVisitorsSplit: controlMulti,
      controlSampleSize: 8,
      lockWaits: { hash: hashLocks, roundrobin: controlLocks },
    };
    console.log(`[lb] result ${JSON.stringify(report)}`);

    if (unexpected > 0) throw new Error(`${unexpected} requests failed through the LB`);
    if (multi > 0)
      throw new Error(`${multi}/${KEYS} visitors were split across instances`);
    if (instances.size < 2)
      throw new Error("only one backend received traffic; LB drill is void");
    if (controlMulti === 0) {
      throw new Error(
        "control (roundrobin) showed perfect affinity; HAProxy config is suspect",
      );
    }
    log("PASS: every visitor stayed on one instance; control without affinity did not");
  } finally {
    for (const s of servers) s.kill("SIGTERM");
    if (haproxyUp && !keep) {
      try {
        run("docker", ["compose", "-f", "ops/lb/docker-compose.yml", "down", "-v"]);
      } catch {
        // best effort
      }
    }
    if (!keep) {
      const admin = new pg.Client({ connectionString: `${BASE_DB}/postgres` });
      await admin.connect();
      await admin
        .query(
          `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE datname = $1 AND pid <> pg_backend_pid()`,
          [DB_NAME],
        )
        .catch(() => {});
      await admin.query(`DROP DATABASE IF EXISTS ${DB_NAME}`).catch(() => {});
      await admin.end();
    }
  }
}

main().catch((err) => {
  console.error("[lb] FAIL:", err, err?.cause ?? "");
  process.exit(1);
});
