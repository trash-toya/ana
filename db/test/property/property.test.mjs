// Property test (P0-9): random event streams through two paths.
//   Path 1 (system under test): ingest_batch() → archive_sessions() → session_stats_15m
//   Path 2 (oracle):            the same events loaded directly into sessions_history by
//                               an independent TypeScript model of the session rules →
//                               rebuild_session_stats()
// The two session_stats_15m row sets must be byte-identical (every column). The oracle
// does NOT reuse the SQL merge machinery, so a merge bug in path 1 surfaces as a
// difference instead of being replicated.
import { test } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import pg from "pg";
import {
  withTestDb, createTestDb, dropTestDb, ingest, event, forceArchive,
} from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const SEEDS = Number(process.env.SA_PROPERTY_SEEDS ?? 3);

function mulberry32(seed) {
  return function () {
    seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const BASE = Date.UTC(2026, 5, 10, 6, 0, 0);
const HOME_DAY = new Date("2026-06-10T00:00:00Z"); // arrival day is not bucketed anyway

/** Generate a random scenario: batches in arrival order across sessions. */
function generateScenario(seed) {
  const rand = mulberry32(seed);
  const tenants = Array.from({ length: 2 }, () => randomUuid());
  const sessions = Array.from(
    { length: 4 + Math.floor(rand() * 6) },
    () => ({ tenantId: tenants[Math.floor(rand() * tenants.length)],
             sessionId: randomUuid() }),
  );

  const span = 4 * 3600_000;
  const events = new Map(); // event_id -> event, for verbatim duplicate resends
  const batches = [];

  const nBatches = 6 + Math.floor(rand() * 6);
  for (let b = 0; b < nBatches; b++) {
    const batch = [];
    const n = 5 + Math.floor(rand() * 40);
    for (let i = 0; i < n; i++) {
      const s = sessions[Math.floor(rand() * sessions.length)];
      // Later batches write some event times BEHIND already-archived activity, which
      // is what produces late partials.
      const offset = Math.max(
        Math.floor(rand() * span) - (b >= nBatches / 2 ? span * 0.4 : 0), 0);
      const time = new Date(BASE + offset);
      const roll = rand();
      const [name, cat] = roll < 0.55 ? ["page_view", "e"]
        : roll < 0.8 ? ["click", "e"]
        : roll < 0.92 ? ["heartbeat", "s"]
        : ["session_start", "l"];
      const userId = rand() < 0.3 ? `u${Math.floor(rand() * 3)}` : null;
      const eventId = randomUuid();
      const ev = event({
        tenantId: s.tenantId, sessionId: s.sessionId, eventId,
        seq: 1 + Math.floor(offset / 1000),
        time, name, cat,
        url: name === "page_view" ? `/p${Math.floor(rand() * 3)}` : null,
        props: name === "session_start"
          ? { utm: { source: `s${Math.floor(rand() * 2)}` }, device: "desktop",
              country: "DE" }
          : {},
        userId,
      });
      batch.push(ev);
      events.set(eventId, ev);
    }
    batches.push(batch);
  }

  // Verbatim duplicate resends (same hash) scattered into random batches.
  const dupSource = [...events.values()];
  for (let d = 0; d < 10; d++) {
    batches[Math.floor(rand() * batches.length)]
      .push({ ...dupSource[Math.floor(rand() * dupSource.length)] });
  }
  return batches;
}

/** Independent model of the session rules (blueprint section 3 / 03-write-path.md). */
function modelHistoryRows(scenario) {
  const bySession = new Map();
  for (const batch of scenario) {
    for (const e of batch) {
      const key = `${e.tenant_id}|${e.session_id}`;
      if (!bySession.has(key)) bySession.set(key, new Map());
      const seen = bySession.get(key);
      // Dedupe by (tenant, event_id): first hash wins; same-hash replay is a duplicate.
      if (!seen.has(e.event_id)) seen.set(e.event_id, e);
      // Conflicting same-event_id/different-hash is excluded by the generator.
    }
  }
  const rows = [];
  for (const [key, evs] of bySession) {
    const list = [...evs.values()].sort(
      (a, b) => a.time.localeCompare(b.time) || (a.seq ?? 0) - (b.seq ?? 0) ||
        a.event_id.localeCompare(b.event_id));
    const engaged = list.filter((e) => e.cat === "e");
    const firstUser = list.find((e) => e.user_id !== null) ?? null;
    const firstPageView = list.find((e) => e.name === "page_view") ?? null;
    const firstStart = list.find((e) => e.name === "session_start") ?? null;
    rows.push({
      tenant_id: key.split("|")[0],
      session_id: key.split("|")[1],
      home_day: HOME_DAY,
      anon_id: "a1",
      started_at: list[0].time,
      last_seen_at: list[list.length - 1].time,
      event_count: engaged.length,
      total_events: list.length,
      heartbeats: list.filter((e) => e.name === "heartbeat").length,
      user_id: firstUser?.user_id ?? null,
      user_at: firstUser?.time ?? null,
      entry_url: firstPageView?.url ?? null,
      entry_at: firstPageView?.time ?? null,
      device: firstStart?.props?.device ?? null,
      country: firstStart?.props?.country ?? null,
      utm: firstStart?.props?.utm ?? null,
      attr_at: firstStart?.time ?? null,
    });
  }
  return rows;
}

async function statsRows(url) {
  const c = new pg.Client({ connectionString: url });
  await c.connect();
  try {
    return await c.query("SELECT * FROM session_stats_15m");
  } finally {
    await c.end();
  }
}

const norm = (res) => res.rows.map((r) => ({
  tenant_id: r.tenant_id,
  bucket: new Date(r.bucket).getTime(),
  sessions: Number(r.sessions),
  ec_0: Number(r.ec_0), ec_1: Number(r.ec_1),
  ec_2: Number(r.ec_2), ec_3p: Number(r.ec_3p),
  total_duration_s: Number(r.total_duration_s),
  heartbeat_s: Number(r.heartbeat_s),
  events: Number(r.events),
})).sort((a, b) => a.tenant_id.localeCompare(b.tenant_id) || a.bucket - b.bucket);

test(`property: ingest→archive equals independent model→rebuild (${SEEDS} seeds)`,
  async () => {
    for (let seed = 1; seed <= SEEDS; seed++) {
      const scenario = generateScenario(seed);

      // Path 1: system under test. Archive mid-way and at the end (late partials).
      const got = await withTestDb(`prop1_s${seed}`, async ({ pool, url }) => {
        for (let i = 0; i < scenario.length; i++) {
          const client = await pool.connect();
          try {
            const r = await ingest(client, scenario[i]);
            assert.equal(r.conflicts.length, 0, `seed ${seed}: generator made no conflicts`);
          } finally {
            client.release();
          }
          if (i === Math.floor(scenario.length / 2)) await forceArchive(pool);
        }
        await forceArchive(pool);
        return norm(await statsRows(url));
      });

      // Path 2: independent oracle in its own database.
      const oracleDb = `sa_test_oracle_${randomUuid().replaceAll("-", "").slice(0, 10)}`;
      const oracleUrl = await createTestDb(oracleDb);
      try {
        const oc = new pg.Client({ connectionString: oracleUrl });
        await oc.connect();
        try {
          for (const r of modelHistoryRows(scenario)) {
            await oc.query(
              `INSERT INTO sessions_history
                 (tenant_id, session_id, home_day, anon_id, started_at, last_seen_at,
                  event_count, total_events, heartbeats, user_id, user_at,
                  entry_url, entry_at, device, country, utm, attr_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
              [r.tenant_id, r.session_id, r.home_day, r.anon_id, r.started_at,
               r.last_seen_at, r.event_count, r.total_events, r.heartbeats,
               r.user_id, r.user_at, r.entry_url, r.entry_at, r.device, r.country,
               r.utm ? JSON.stringify(r.utm) : null, r.attr_at],
            );
          }
          await oc.query(
            "CALL rebuild_session_stats(NULL::uuid, '2026-01-01', '2027-01-01')");
        } finally {
          await oc.end();
        }
        const want = norm(await statsRows(oracleUrl));
        assert.deepEqual(got, want,
          `seed ${seed}: stats byte-identical across both paths`);
      } finally {
        await dropTestDb(oracleDb);
      }
    }
  });
