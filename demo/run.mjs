// Demo orchestrator (P1-6): `pnpm demo` →
//   1. compose up db + pgbouncer, wait until healthy
//   2. seed (migrations + demo tenant/key)
//   3. spawn the ingest API on :3001 (via pgbouncer, like production)
//   4. wait for /readyz
//   5. spawn the demo server on :8081 and print the URL
// SIGINT tears everything down.

import { spawn, spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const COMPOSE = ["-f", "ops/docker-compose.yml"];

const log = (m) => console.log(`[demo] ${m}`);
const die = (m) => {
  console.error(`[demo] FATAL: ${m}`);
  process.exit(1);
};

function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { cwd: ROOT, encoding: "utf8", ...opts });
  if (r.status !== 0) die(`${cmd} ${args.join(" ")} failed:\n${r.stderr || r.stdout}`);
  return r;
}

// 1. DB stack.
log("starting compose db + pgbouncer…");
run("docker", ["compose", ...COMPOSE, "up", "-d", "db", "pgbouncer"]);
for (let i = 0; i < 60; i++) {
  const st = spawnSync(
    "docker",
    ["compose", ...COMPOSE, "ps", "db", "--format", "{{.Status}}"],
    {
      cwd: ROOT,
      encoding: "utf8",
    },
  ).stdout.trim();
  if (st.includes("healthy")) break;
  if (i === 59) die("db did not become healthy in 120 s");
  await new Promise((r) => setTimeout(r, 2_000));
}
log("db healthy");

// 2. Seed.
const { seed } = await import("./seed.mjs");
const { apiKey } = await seed({ log });

// 3. Ingest API (through pgbouncer, transaction mode).
const ingest = spawn("node", ["services/ingest/dist/server.js"], {
  cwd: ROOT,
  env: {
    ...process.env,
    PORT: "3001",
    DATABASE_URL: "postgresql://postgres:postgres@127.0.0.1:6432/postgres",
    // Demo-only: enables /v1/sessions* so the server-first flow is usable.
    // Production sets a real secret from the environment.
    SESSION_CONTEXT_SECRET:
      process.env.SESSION_CONTEXT_SECRET ?? "demo-only-session-context-secret",
  },
  stdio: ["ignore", "inherit", "inherit"],
});

// 4. Wait for readiness.
let ready = false;
for (let i = 0; i < 50; i++) {
  if (ingest.exitCode !== null) die("ingest exited during startup");
  try {
    const res = await fetch("http://127.0.0.1:3001/readyz");
    if (res.ok) {
      ready = true;
      break;
    }
  } catch {
    /* not up yet */
  }
  await new Promise((r) => setTimeout(r, 200));
}
if (!ready) die("ingest /readyz did not come up in 10 s");
log("ingest ready on :3001");

// 5. Demo server.
const demo = spawn("node", ["demo/server.mjs"], {
  cwd: ROOT,
  env: { ...process.env, DEMO_API_KEY: apiKey, DEMO_PORT: "8081" },
  stdio: ["ignore", "inherit", "inherit"],
});

const cleanup = () => {
  log("shutting down…");
  ingest.kill("SIGTERM");
  demo.kill("SIGTERM");
  setTimeout(() => process.exit(0), 500).unref();
};
process.on("SIGINT", cleanup);
process.on("SIGTERM", cleanup);

demo.on("exit", (code) => {
  log(`demo server exited (${code})`);
  ingest.kill("SIGTERM");
  process.exit(code ?? 0);
});
ingest.on("exit", (code) => {
  if (code) log(`ingest exited (${code})`);
});

log(`open http://localhost:8081  (ingest key rotated this run)`);
