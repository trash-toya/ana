// P0-2 gate: DB reachable, toolkit present, hyperloglog round-trip works.
// Usage: npm run db:ping  (uses DATABASE_URL or falls back to localhost:5432).
import { Client } from "pg";

const url =
  process.env.DATABASE_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";

const client = new Client({ connectionString: url, connectionTimeoutMillis: 5_000 });
await client.connect();

const checks = {
  version: "SELECT version()",
  toolkit: "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb_toolkit'",
  timescale: "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'",
  hll: "SELECT distinct_count(hyperloglog(4096, x)) FROM (VALUES (1),(1),(2)) v(x)",
};

const out = {};
for (const [name, sql] of Object.entries(checks)) {
  const res = await client.query(sql);
  out[name] = res.rows[0];
}
await client.end();

const hllOk = Number(out.hll.distinct_count) === 2;
const extOk = Number(out.toolkit.count) === 1 && Number(out.timescale.count) === 1;
console.log(JSON.stringify(out, null, 2));
if (!hllOk || !extOk) {
  console.error(
    !extOk
      ? "FATAL: TimescaleDB or timescaledb_toolkit missing. Stop and re-plan (07-operations.md)."
      : "FATAL: hyperloglog round-trip failed. Toolkit is broken on this image.",
  );
  process.exit(1);
}
console.log("db:ping ok — toolkit present, HLL round-trip passes");
