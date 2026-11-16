// Session bootstrap/resume (Notion Rev 5 §19.2, D30).
//
// Two operations, both separate from event ingestion:
//   start()    — create or resume a session for a visitor, return the signed
//                context the backend propagates as `X-Analytics-Session`.
//   validate() — check a propagated context on an application request.
//
// A raw session id is a correlation identifier, not a credential: the HMAC is
// what proves the context was issued by this analytics service for this tenant.
// The token's expiry is the hard session age; the *live* idle expiry comes from
// the database at validation time, so a session that idles out is rejected
// without reissuing tokens on every request.

import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import type { Pool } from "pg";
import {
  SESSION_IDLE_MS,
  SESSION_MAX_AGE_MS,
  type SessionDescriptor,
  type SessionStartRequest,
  type SessionValidation,
} from "@session-analytics/shared";

const TOKEN_VERSION = "v1";
/** Minimum secret length; anything shorter is a configuration error. */
export const MIN_CONTEXT_SECRET_LENGTH = 16;

interface BootstrapRow {
  session_id: string;
  anon_id: string;
  user_id: string | null;
  started_at: string;
  last_event_at: string;
  expires_at: string;
  resumed: boolean;
  emitted_events: number;
  properties: Record<string, unknown>;
}

interface LookupRow {
  found: boolean;
  anon_id?: string;
  user_id?: string | null;
  started_at?: string;
  last_event_at?: string;
  expires_at?: string;
  emitted_events?: number;
  properties?: Record<string, unknown>;
}

export interface SessionServiceOptions {
  pool: Pool;
  /** HMAC secret shared only with the analytics service. */
  secret: string;
  /** Injectable clock (tests). */
  now?: () => Date;
}

export class SessionService {
  private readonly pool: Pool;
  private readonly secret: string;
  private readonly now: () => Date;

  constructor(options: SessionServiceOptions) {
    if (!options.secret || options.secret.length < MIN_CONTEXT_SECRET_LENGTH) {
      throw new Error(
        `session context secret must be at least ${MIN_CONTEXT_SECRET_LENGTH} characters`,
      );
    }
    this.pool = options.pool;
    this.secret = options.secret;
    this.now = options.now ?? (() => new Date());
  }

  async start(tenantId: string, request: SessionStartRequest): Promise<SessionDescriptor> {
    const at = this.now();
    const params = {
      tenant_id: tenantId,
      anon_id: request.anon_id ?? randomUUID(),
      user_id: request.user_id ?? null,
      properties: request.properties ?? {},
      now: at.toISOString(),
    };
    const { rows } = await this.pool.query<{ r: BootstrapRow }>(
      "SELECT bootstrap_session($1) AS r",
      [JSON.stringify(params)],
    );
    const row = rows[0]?.r;
    if (!row) throw new Error("bootstrap_session returned no row");
    return {
      session_id: row.session_id,
      anon_id: row.anon_id,
      user_id: row.user_id,
      started_at: iso(row.started_at),
      last_event_at: iso(row.last_event_at),
      expires_at: iso(row.expires_at),
      resumed: row.resumed,
      emitted_events: row.emitted_events,
      properties: row.properties ?? {},
      // Hard age, not the idle expiry: the idle clock is checked in validate().
      context: this.sign(tenantId, row.session_id, Date.parse(row.started_at) + SESSION_MAX_AGE_MS),
    };
  }

  async validate(tenantId: string, context: string): Promise<SessionValidation> {
    const parsed = parseToken(context);
    if (!parsed) return { valid: false, reason: "malformed" };
    const expected = this.signature(tenantId, parsed.sessionId, parsed.exp);
    if (!safeEqual(expected, parsed.signature)) {
      return { valid: false, reason: "bad_signature" };
    }
    const now = this.now().getTime();
    if (parsed.exp * 1000 <= now) return { valid: false, reason: "expired" };

    const { rows } = await this.pool.query<{ r: LookupRow }>(
      "SELECT lookup_session($1, $2, $3) AS r",
      [tenantId, parsed.sessionId, new Date(now).toISOString()],
    );
    const row = rows[0]?.r;
    if (!row) throw new Error("lookup_session returned no row");
    if (!row.found) return { valid: false, reason: "unknown_session" };
    const expiresAt = iso(row.expires_at as string);
    if (Date.parse(expiresAt) <= now) {
      return { valid: false, reason: "expired", session_id: parsed.sessionId, expires_at: expiresAt };
    }
    return {
      valid: true,
      session_id: parsed.sessionId,
      anon_id: row.anon_id,
      user_id: row.user_id ?? null,
      expires_at: expiresAt,
      emitted_events: row.emitted_events ?? 0,
      properties: row.properties ?? {},
    };
  }

  /** `v1.<session_id>.<exp_epoch_s>.<hmac>` — self-describing, no key server. */
  private sign(tenantId: string, sessionId: string, expMs: number): string {
    const exp = Math.floor(expMs / 1000);
    return [TOKEN_VERSION, sessionId, String(exp), this.signature(tenantId, sessionId, exp)].join(".");
  }

  private signature(tenantId: string, sessionId: string, exp: number): string {
    return createHmac("sha256", this.secret)
      .update(`${tenantId}|${sessionId}|${exp}`)
      .digest("base64url");
  }
}

/** Idle window, exported for documentation/tests (mirrors the DB rule). */
export const IDLE_WINDOW_MS = SESSION_IDLE_MS;

interface ParsedToken {
  sessionId: string;
  exp: number;
  signature: string;
}

function parseToken(context: string): ParsedToken | null {
  const parts = context.split(".");
  if (parts.length !== 4 || parts[0] !== TOKEN_VERSION) return null;
  const sessionId = parts[1];
  const expRaw = parts[2];
  const signature = parts[3];
  if (!sessionId || !expRaw || !signature) return null;
  if (!UUID_RE.test(sessionId)) return null;
  const exp = Number(expRaw);
  if (!Number.isSafeInteger(exp) || exp <= 0) return null;
  if (!/^[A-Za-z0-9_-]{10,}$/.test(signature)) return null;
  return { sessionId, exp, signature };
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function safeEqual(a: string, b: string): boolean {
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}

/** Postgres may return a Date (pg) or an ISO string (jsonb). */
function iso(value: string | Date): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}
