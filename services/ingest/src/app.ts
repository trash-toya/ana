// POST /v1/events, /healthz (no DB), /readyz (DB round-trip), /metrics.
// The write path's validation/buffer/flush land in P1-3/P1-4 behind the
// `submit` seam; P1-2 wires auth, liveness, readiness and metrics around it.

import express, {
  type ErrorRequestHandler,
  type Express,
  type NextFunction,
  type Request,
  type RequestHandler,
  type Response,
} from "express";
import type { Pool } from "pg";
import {
  SOURCES_BY_KEY_KIND,
  SessionStartRequestSchema,
  SessionValidateRequestSchema,
} from "@session-analytics/shared";
import { ApiKeyAuth } from "./auth.js";
import { BufferFullError, Flusher } from "./flusher.js";
import {
  LIMIT_DEFAULTS,
  RateLimiter,
  sendRateLimited,
  type LimitConfig,
} from "./limits.js";
import { Metrics } from "./metrics.js";
import type { SessionService } from "./sessions.js";
import {
  EnvelopeError,
  assembleOutcomes,
  validateBatch,
  type IngestBatchResult,
  type ValidationOutcome,
} from "./validate.js";
import "./types.js";

/** Result of one ingest_batch() call (outcomes keyed by event_id). */
export type { IngestBatchResult } from "./validate.js";
export { BufferFullError, Flusher } from "./flusher.js";
export { SessionService } from "./sessions.js";

export interface SubmitRequest {
  tenantId: string;
  /** Validated + deduped batch (rows to store) and the per-event ledger. */
  outcome: ValidationOutcome;
  receivedAt: Date;
}

export type SubmitFn = (req: SubmitRequest) => Promise<IngestBatchResult>;

export interface IngestAppOptions {
  pool: Pool;
  auth?: ApiKeyAuth;
  metrics?: Metrics;
  /** Restrict /metrics to internal addresses (default true). */
  metricsLocalOnly?: boolean;
  /** Override the internal-address predicate for tests. */
  metricsAllowed?: (remoteAddress: string | undefined) => boolean;
  /** Buffer+flush pipeline; created from the pool when not provided. */
  flusher?: Flusher;
  /** Write path override (tests). Default: flusher.enqueue. */
  submit?: SubmitFn;
  /** Bootstrap/resume service. Omitted → `/v1/sessions*` is not mounted. */
  sessions?: SessionService;
  /** Abuse-protection buckets (04-services.md). `null` disables all scopes. */
  limits?: LimitConfig | null;
  /** Pre-built limiter (tests inject one with a fake clock). */
  limiter?: RateLimiter;
}

export function createApp(options: IngestAppOptions): Express {
  const metrics = options.metrics ?? new Metrics();
  const auth = options.auth ?? new ApiKeyAuth(options.pool);
  const flusher = options.flusher ?? new Flusher({ pool: options.pool, metrics });
  const submit: SubmitFn =
    options.submit ?? ((r) => flusher.enqueue(r.tenantId, r.outcome));
  const metricsLocalOnly = options.metricsLocalOnly ?? true;
  const metricsAllowed =
    options.metricsAllowed ?? ((addr: string | undefined) => isInternalAddress(addr));
  const limiter =
    options.limiter ??
    new RateLimiter(
      options.limits === null
        ? { ip: null, key: null, tenant: null, session: null }
        : (options.limits ?? LIMIT_DEFAULTS),
    );

  // Abuse protection scopes 2–3: per-key and per-tenant, after auth (the auth
  // middleware sets req.keyDigest / req.tenantId). They apply to the session
  // bootstrap routes too: those are server-key operations and equally abusable.
  const authGate: RequestHandler[] = [
    auth.middleware(),
    limiter.middleware("key", (req) => req.keyDigest, metrics),
    limiter.middleware("tenant", (req) => req.tenantId, metrics),
  ];

  const app: Express = express();
  app.disable("x-powered-by");

  app.use(countRequests(metrics));

  // Abuse protection scope 1: every /v1 request by peer address, before body
  // parsing and before auth — an unauthenticated flood is the cheapest attack.
  app.use(
    "/v1",
    limiter.middleware("ip", (req) => req.socket.remoteAddress ?? "unknown", metrics),
  );

  // Body cap: 1 MB (blueprint §Abuse protection). Over-large bodies are
  // rejected before any parsing; oversized-but-valid-JSON bodies surface as
  // entity.too.large from body-parser and map to 413 in the error handler.
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

  app.get("/metrics", (req: Request, res: Response) => {
    if (metricsLocalOnly && !metricsAllowed(req.socket.remoteAddress)) {
      res.status(403).json({ error: "forbidden" });
      return;
    }
    metrics.gauge("rate_limit_buckets", limiter.size());
    res.type("text/plain; version=0.0.4; charset=utf-8").send(metrics.render());
  });

  app.post("/v1/events", ...authGate, async (req, res) => {
    const receivedAt = new Date();
    const outcome = validateBatch(req.body, {
      tenantId: req.tenantId as string,
      receivedAt,
    });
    // D28: a client key is public; it may not forge server/webhook provenance.
    const allowed = SOURCES_BY_KEY_KIND[req.keyKind ?? "ingest"];
    const disallowed = outcome.rows.find((row) => !allowed.includes(row.source));
    if (disallowed) {
      res.status(403).json({ error: "source_not_allowed", source: disallowed.source });
      return;
    }
    // Abuse protection scope 4: charge each session the number of VALIDATED
    // events it carries (a flush can legitimately carry hundreds). Charged
    // all-or-nothing across the batch, and before anything reaches the buffer.
    const sessionCosts = new Map<string, number>();
    for (const row of outcome.rows) {
      if (row.session_id) {
        sessionCosts.set(row.session_id, (sessionCosts.get(row.session_id) ?? 0) + 1);
      }
    }
    const sessionDecision = limiter.checkCosts("session", sessionCosts);
    if (!sessionDecision.ok) {
      sendRateLimited(res, sessionDecision, metrics);
      return;
    }
    const dbResult: IngestBatchResult = outcome.rows.length
      ? await submit({ tenantId: req.tenantId as string, outcome, receivedAt })
      : { stored: [], dups: [], capped: [], conflicts: [] };
    res.status(200).json(assembleOutcomes(outcome, dbResult));
  });

  if (options.sessions) {
    const sessions = options.sessions;

    app.post("/v1/sessions", ...authGate, requireServerKey, async (req, res, next) => {
      const parsed = SessionStartRequestSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "malformed" });
        return;
      }
      try {
        const session = await sessions.start(req.tenantId as string, parsed.data);
        metrics.inc("session_bootstrap_total", { resumed: String(session.resumed) });
        res.status(200).json(session);
      } catch (err) {
        next(err);
      }
    });

    app.post(
      "/v1/sessions/validate",
      ...authGate,
      requireServerKey,
      async (req, res, next) => {
        const parsed = SessionValidateRequestSchema.safeParse(req.body);
        if (!parsed.success) {
          res.status(400).json({ error: "malformed" });
          return;
        }
        try {
          const result = await sessions.validate(
            req.tenantId as string,
            parsed.data.context,
          );
          metrics.inc("session_validation_total", { valid: String(result.valid) });
          res.status(200).json(result);
        } catch (err) {
          next(err);
        }
      },
    );
  }

  app.use(notFound);
  app.use(errorHandler);
  return app;
}

/** Bootstrap/resume is a private server operation (D30). */
const requireServerKey: RequestHandler = (req, res, next) => {
  if (req.keyKind !== "server") {
    res.status(403).json({ error: "server_key_required" });
    return;
  }
  next();
};

function notFound(req: Request, res: Response): void {
  res.status(404).json({ error: "not_found", path: req.path });
}

// Maps body-parser failures and unhandled route errors onto the response
// contract (04-services.md): 400 malformed, 413 too large, 503 transient,
// everything else 500.
export const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  if (err instanceof EnvelopeError) {
    res.status(400).json({ error: err.code });
    return;
  }
  if (err instanceof BufferFullError) {
    res.status(503).set("Retry-After", String(err.retryAfter)).json({ error: err.code });
    return;
  }
  if (err?.type === "entity.too.large") {
    res.status(413).json({ error: "body_too_large" });
    return;
  }
  if (err?.type === "entity.parse.failed" || err instanceof SyntaxError) {
    res.status(400).json({ error: "bad_json" });
    return;
  }
  const status = typeof err?.status === "number" && err.status >= 400 ? err.status : 500;
  if (status >= 500)
    console.error(
      JSON.stringify({ msg: "unhandled_error", error: String(err?.stack ?? err) }),
    );
  res.status(status).json({ error: "internal_error" });
};

function countRequests(metrics: Metrics): RequestHandler {
  return (req, res, next) => {
    res.on("finish", () => {
      metrics.inc("http_requests_total", {
        method: req.method,
        route: req.route?.path ?? "unmatched",
        status: String(res.statusCode),
      });
    });
    next();
  };
}

/**
 * Loopback + RFC 1918 (and 172.31 vs 172.16). /metrics is internal only —
 * private port or IP allow-list, never routed by the public LB.
 */
export function isInternalAddress(address: string | undefined): boolean {
  if (!address) return false;
  let a = address;
  if (a.startsWith("::ffff:")) a = a.slice("::ffff:".length);
  if (a === "::1") return true;
  const m = /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/.exec(a);
  if (!m) return false;
  const [o1, o2] = [Number(m[1]), Number(m[2])];
  if (o1 === 127 || o1 === 10) return true;
  if (o1 === 192 && o2 === 168) return true;
  if (o1 === 172 && o2 >= 16 && o2 <= 31) return true;
  return false;
}
