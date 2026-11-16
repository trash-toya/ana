// Demo server (P1-6): one same-origin process, so the browser never deals
// with CORS. Port 8080.
//
//   GET  /            → demo/index.html
//   GET  /sdk/*       → packages/sdk-web/dist/* (built ESM)
//   POST /v1/events   → proxy to the ingest API on :3001
//   GET  /demo/config  → { apiKey } for sdk init
//   GET  /demo/state  → SQL: active sessions, last events, counts
//
// The SQL panel connects as the compose superuser — fine for a demo; real
// readers use scoped roles (P4).

import http from "node:http";
import { createReadStream, existsSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "..");
const SDK_DIST = path.join(ROOT, "packages/sdk-web/dist");

const INGEST = process.env.DEMO_INGEST_URL ?? "http://127.0.0.1:3001";
const DB_URL =
  process.env.DEMO_DB_URL ?? "postgresql://postgres:postgres@127.0.0.1:5432/postgres";
const PORT = Number(process.env.DEMO_PORT ?? 8081); // 8080 is commonly taken (Dozzle etc.)

const pool = new pg.Pool({ connectionString: DB_URL, max: 2 });

// Postgres restarts and PgBouncer failovers terminate idle clients with a
// FATAL (57P01). Without this listener Node treats the pool's 'error' event as
// unhandled and exits; pg discards the broken client and reconnects on demand.
pool.on("error", (err) => {
  console.error(JSON.stringify({ msg: "demo_db_pool_error", error: err.message }));
});

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json",
};

async function readBody(req, limit = 2 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new Error("body too large");
    chunks.push(c);
  }
  return Buffer.concat(chunks);
}

async function state() {
  const [sessions, events, counts, tenant] = await Promise.all([
    pool.query(
      `SELECT session_id, anon_id, user_id, started_at, last_seen_at,
              event_count, total_events, heartbeats, entry_url
         FROM sessions_active
        ORDER BY last_seen_at DESC
        LIMIT 10`,
    ),
    pool.query(
      `SELECT time, session_id, event_id, cat, seq, user_id, name, url, props
         FROM events
        ORDER BY time DESC
        LIMIT 20`,
    ),
    pool.query(
      `SELECT (SELECT count(*) FROM events)              AS events,
              (SELECT count(DISTINCT session_id) FROM events) AS sessions,
              (SELECT count(*) FROM sessions_active)      AS active_rows`,
    ),
    pool.query(`SELECT count(*)::int AS tenants FROM tenants`),
  ]);
  return {
    tenants: tenant.rows[0].tenants,
    counts: counts.rows[0],
    sessions: sessions.rows,
    events: events.rows,
  };
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj, (_k, v) => (typeof v === "bigint" ? Number(v) : v));
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
  });
  res.end(body);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  const { pathname } = url;

  try {
    // Config: the freshly minted demo key (write-only, public).
    if (pathname === "/demo/config") {
      return sendJson(res, 200, {
        apiKey: process.env.DEMO_API_KEY ?? "",
        endpoint: "/v1/events",
      });
    }

    // Live SQL state for the panel.
    if (pathname === "/demo/state") {
      return sendJson(res, 200, await state());
    }

    // Same-origin ingest proxy. The ingest API sees 127.0.0.1, which is fine:
    // origin_hint is a hint, not a boundary.
    if (pathname === "/v1/events" && req.method === "POST") {
      const body = await readBody(req);
      const upstream = await fetch(`${INGEST}/v1/events`, {
        method: "POST",
        headers: {
          "content-type": req.headers["content-type"] ?? "application/json",
          "x-api-key": req.headers["x-api-key"] ?? "",
        },
        body,
      });
      const text = await upstream.text();
      const headers = {
        "content-type": upstream.headers.get("content-type") ?? "application/json",
      };
      const retryAfter = upstream.headers.get("retry-after");
      if (retryAfter !== null) headers["retry-after"] = retryAfter;
      res.writeHead(upstream.status, headers);
      return res.end(text);
    }

    // Static: demo/index.html at /, SDK dist under /sdk/.
    let file = null;
    if (pathname === "/" || pathname === "/index.html")
      file = path.join(HERE, "index.html");
    else if (pathname.startsWith("/sdk/")) {
      file = path.normalize(path.join(SDK_DIST, pathname.slice("/sdk/".length)));
      if (!file.startsWith(SDK_DIST)) return sendJson(res, 403, { error: "forbidden" });
    }
    if (file && existsSync(file) && statSync(file).isFile()) {
      res.writeHead(200, {
        "content-type": MIME[path.extname(file)] ?? "application/octet-stream",
      });
      return createReadStream(file).pipe(res);
    }

    sendJson(res, 404, { error: "not_found", path: pathname });
  } catch (err) {
    sendJson(res, 502, {
      error: "demo_server_error",
      message: String(err?.message ?? err),
    });
  }
});

server.listen(PORT, () => {
  console.log(JSON.stringify({ msg: "demo_server_listening", port: PORT }));
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    server.close();
    pool.end().finally(() => process.exit(0));
  });
}
