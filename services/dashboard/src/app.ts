// GET /v1/* dashboard endpoints, /healthz (no DB), /readyz (DB round-trip).
// Read-only, tenant-scoped: auth resolves the tenant from a `dashboard` key,
// then every query runs in a READ ONLY transaction with app.tenant_id set
// (db.ts#withTenant) and resolves it fail-closed through app_tenant().

import express, {
  type ErrorRequestHandler,
  type Express,
  type Request,
  type RequestHandler,
  type Response,
} from "express";
import { z } from "zod";
import type { Pool } from "pg";
import { DashboardKeyAuth } from "./auth.js";
import { withTenant, type TenantQuery } from "./db.js";
import * as queries from "./queries.js";
import "./types.js";

const ISO_INSTANT = z
  .string()
  .min(10)
  .refine((s) => !Number.isNaN(Date.parse(s)), "not an ISO instant");

/** IANA zone name (or UTC); rejects arbitrary strings before they hit SQL. */
function isTimeZone(tz: string): boolean {
  try {
    new Intl.DateTimeFormat("en", { timeZone: tz });
    return true;
  } catch {
    return false;
  }
}

const TZ = z.string().max(64).refine(isTimeZone, "unknown time zone");

const HourlyQuery = z
  .object({ from: ISO_INSTANT, to: ISO_INSTANT })
  .refine((v) => Date.parse(v.from) < Date.parse(v.to), "from must precede to")
  .refine(
    (v) => Date.parse(v.to) - Date.parse(v.from) <= 92 * 86_400_000,
    "range exceeds 92 days (the 15m rollups' retention horizon)",
  );

const DailyQuery = z
  .object({ from: ISO_INSTANT, to: ISO_INSTANT, tz: TZ.optional() })
  .refine((v) => Date.parse(v.from) < Date.parse(v.to), "from must precede to")
  .refine(
    (v) => Date.parse(v.to) - Date.parse(v.from) <= 366 * 86_400_000,
    "range exceeds 366 days",
  );

const FunnelsBody = z
  .object({
    steps: z.array(z.string().min(1).max(100)).min(1).max(10),
    from: ISO_INSTANT,
    to: ISO_INSTANT,
  })
  .refine((v) => Date.parse(v.from) < Date.parse(v.to), "from must precede to")
  .refine(
    (v) => Date.parse(v.to) - Date.parse(v.from) <= 92 * 86_400_000,
    "range exceeds 92 days",
  );

export interface DashboardAppOptions {
  pool: Pool;
  auth?: DashboardKeyAuth;
}

const SESSION_ID = z.string().uuid();

export function createApp(options: DashboardAppOptions): Express {
  const auth = options.auth ?? new DashboardKeyAuth(options.pool);

  const app: Express = express();
  app.disable("x-powered-by");
  app.locals.pool = options.pool;

  // Funnels take a JSON body (steps in the request); 1 MB cap, same as ingest.
  app.use(express.json({ limit: "1mb" }));

  app.get("/healthz", (_req: Request, res: Response) => {
    res.status(200).json({ ok: true });
  });

  app.get("/readyz", async (_req: Request, res: Response) => {
    try {
      await options.pool.query("SELECT 1");
      res.status(200).json({ ok: true });
    } catch {
      res.status(503).json({ ok: false });
    }
  });

  // Every /v1 route requires a dashboard key.
  app.use("/v1", auth.middleware());

  app.get(
    "/v1/sessions/:id/replay",
    handle(async (req, q, res) => {
      const id = SESSION_ID.parse(req.params.id);
      const guard = (await queries.replayHomeDay(id)(q)).rows[0];
      if (!guard) {
        res.status(404).json({ error: "not_found" });
        return;
      }
      const sessionRows = (await queries.replaySession(id, guard.home_day)(q)).rows;
      const session = sessionRows[0];
      if (!session) {
        // Guard without a session row: the session was archived away with its
        // guard (retention tiers); treat as gone.
        res.status(404).json({ error: "not_found" });
        return;
      }
      const events = (
        await queries.replayEvents(id, session.started_at, session.last_seen_at)(q)
      ).rows;
      res.status(200).json({
        session: {
          session_id: session.session_id,
          anon_id: session.anon_id,
          user_id: session.user_id,
          started_at: session.started_at,
          last_seen_at: session.last_seen_at,
          event_count: session.event_count,
          total_events: session.total_events,
          heartbeats: session.heartbeats,
          entry_url: session.entry_url,
          device: session.device,
          country: session.country,
          utm: session.utm ?? null,
        },
        events: events.map((e) => ({
          event_id: e.event_id,
          time: e.time,
          seq: e.seq,
          cat: e.cat,
          name: e.name,
          url: e.url,
          props: e.props ?? null,
        })),
      });
    }),
  );

  app.get(
    "/v1/metrics/hourly",
    handle(async (req, q, res) => {
      const parsed = HourlyQuery.safeParse(req.query);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_query" });
        return;
      }
      const { from, to } = parsed.data;
      const stats = (await queries.hourlyStats(from, to)(q)).rows;
      const live = (await queries.liveSlice()(q)).rows[0];
      res.status(200).json({
        hours: stats.map((r) => ({
          hour: r.hour,
          sessions: Number(r.sessions),
          bounce_rate: rate(r.bounced, r.sessions),
          events: Number(r.events),
          total_duration_s: Number(r.total_duration_s),
          source: "stats_15m",
        })),
        // D14: the live slice is labelled as such, never folded silently into
        // the hours array; D18: already excluded closed sessions in SQL.
        live: {
          sessions: Number(live?.sessions ?? 0),
          events: Number(live?.events ?? 0),
          engaged_events: Number(live?.engaged_events ?? 0),
          live: true,
        },
      });
    }),
  );

  app.get(
    "/v1/metrics/daily",
    handle(async (req, q, res) => {
      const parsed = DailyQuery.safeParse(req.query);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_query" });
        return;
      }
      const { from, to } = parsed.data;
      const tz = parsed.data.tz ?? (await tenantTz(q));
      const fromDay = dayText(from, tz);
      const toDay = dayText(to, tz);

      // Closed days first; whatever they cover falls through to the 15m rollup.
      const closed = (await queries.dailyClosed(fromDay, toDay)(q)).rows;
      const closedDays = closed.map((r) => r.day);
      const open = (await queries.dailyFrom15m(from, to, tz, closedDays)(q)).rows;
      const identClosed = (await queries.identityClosed(fromDay, toDay)(q)).rows;
      const identOpen = (await queries.identityFrom15m(from, to, tz, closedDays)(q)).rows;

      const statsByDay = new Map<string, DailyStat>();
      for (const r of closed) {
        statsByDay.set(r.day, {
          ...r,
          source: "daily",
        });
      }
      for (const r of open) {
        // A day that the daily table covers is authoritative; 15m fills the rest.
        if (!statsByDay.has(r.day)) {
          statsByDay.set(r.day, { ...r, source: "15m" });
        }
      }
      const identityByDay = new Map<
        string,
        { visitors: string | null; users: string | null }
      >();
      for (const r of [...identClosed, ...identOpen]) {
        identityByDay.set(r.day, r);
      }

      const days = [...statsByDay.entries()]
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([day, s]) => {
          const ident = identityByDay.get(day);
          return {
            day,
            sessions: Number(s.sessions),
            bounce_rate: rate(s.bounced, s.sessions),
            events: Number(s.events),
            total_duration_s: Number(s.total_duration_s),
            visitors: Number(ident?.visitors ?? 0),
            users: Number(ident?.users ?? 0),
            // Rule 4: HLL-derived counts are labelled approximate wherever shown.
            approximate: true,
            source: s.source,
          };
        });
      res.status(200).json({ days, tz });
    }),
  );

  app.get(
    "/v1/entry-pages",
    handle(async (req, q, res) => {
      const parsed = DailyQuery.safeParse(req.query);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_query" });
        return;
      }
      const { from, to } = parsed.data;
      const tz = parsed.data.tz ?? (await tenantTz(q));
      const rows = (await queries.entryPages(from, to, tz)(q)).rows;
      res.status(200).json({
        entry_pages: rows.map((r) => ({
          entry_url: r.entry_url,
          sessions: Number(r.sessions),
          bounce_rate: rate(r.bounced, r.sessions),
          avg_duration_s: r.avg_duration_s === null ? null : Number(r.avg_duration_s),
        })),
      });
    }),
  );

  app.post(
    "/v1/funnels",
    handle(async (req, q, res) => {
      const parsed = FunnelsBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_body" });
        return;
      }
      const { from, to, steps } = parsed.data;
      const rows = (await queries.funnel(from, to, steps)(q)).rows;
      const byStep = new Map(rows.map((r) => [Number(r.step), Number(r.sessions)]));
      const first = byStep.get(1) ?? 0;
      res.status(200).json({
        steps: steps.map((name, i) => {
          const sessions = byStep.get(i + 1) ?? 0;
          return {
            step: i + 1,
            name,
            sessions,
            // Conversion of step 1 (0 when nothing reached the first step).
            conversion: first === 0 ? 0 : sessions / first,
          };
        }),
      });
    }),
  );

  app.get(
    "/v1/live",
    handle(async (_req, q, res) => {
      const rows = (await queries.liveRoster()(q)).rows;
      res.status(200).json({
        live: true,
        count: rows.length,
        sessions: rows.map((r) => ({
          session_id: r.session_id,
          anon_id: r.anon_id,
          user_id: r.user_id,
          started_at: r.started_at,
          last_seen_at: r.last_seen_at,
          event_count: r.event_count,
          total_events: r.total_events,
          entry_url: r.entry_url,
          device: r.device,
          country: r.country,
          utm: r.utm ?? null,
        })),
      });
    }),
  );

  app.use(notFound);
  app.use(errorHandler);
  return app;
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

interface DailyStat {
  day: string;
  sessions: string;
  bounced: string;
  total_duration_s: string;
  heartbeat_s: string;
  events: string;
  source: "daily" | "15m";
}

function rate(numerator: string | number, denominator: string | number): number | null {
  const d = Number(denominator);
  if (d === 0) return null;
  return Math.round((Number(numerator) / d) * 10_000) / 10_000;
}

/** The tenant's configured tz (tenant_settings); the caller's tz overrides it. */
async function tenantTz(q: TenantQuery): Promise<string> {
  const r = await q<{ tz: string }>(
    `SELECT tz FROM tenant_settings WHERE tenant_id = app_tenant()`,
  );
  return r.rows[0]?.tz ?? "UTC";
}

function dayText(instant: string, tz: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(instant));
}

/** Wraps a handler in withTenant and funnels errors to the error handler. */
function handle(
  fn: (req: Request, q: TenantQuery, res: Response) => Promise<void>,
): RequestHandler {
  return (req, res, next) => {
    const pool = req.app.locals.pool as Pool;
    withTenant(pool, req.tenantId as string, (q) => fn(req, q, res)).catch(next);
  };
}

function notFound(req: Request, res: Response): void {
  res.status(404).json({ error: "not_found", path: req.path });
}

// Same response contract as the ingest service: 400 malformed/invalid,
// 404 unmatched path, 500 everything else (zod rejections inside a handler
// or the error handler itself are 400s below).
export const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  if (err instanceof z.ZodError) {
    res.status(400).json({ error: "invalid_query" });
    return;
  }
  if (err?.type === "entity.parse.failed" || err instanceof SyntaxError) {
    res.status(400).json({ error: "bad_json" });
    return;
  }
  const status = typeof err?.status === "number" && err.status >= 400 ? err.status : 500;
  if (status >= 500) {
    console.error(
      JSON.stringify({ msg: "unhandled_error", error: String(err?.stack ?? err) }),
    );
  }
  res.status(status).json({ error: "internal_error" });
};
