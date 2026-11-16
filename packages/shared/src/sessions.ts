// Session bootstrap/resume contract (Notion Rev 5 §19.2, D30).
//
// `POST /v1/sessions` creates or resumes an analytics session for the
// application backend. It is a separate operation from event ingestion and it
// creates NO analytics rows: the session becomes visible in `sessions_active`
// only when its first event arrives. `POST /v1/sessions/validate` lets the
// backend check a propagated `X-Analytics-Session` context on each request.
//
// The context is opaque and HMAC-signed. A raw session id is a correlation
// identifier, not an authentication credential; validation is what makes the
// signature meaningful.

import { z } from "zod";

import { PropsSchema } from "./contract.js";

/** Idle-expiry window for resume (blueprint session rule). */
export const SESSION_IDLE_MS = 30 * 60 * 1000;
/** Hard session age; the bootstrap flow rotates after this. */
export const SESSION_MAX_AGE_MS = 24 * 60 * 60 * 1000;

/** Header the application backend propagates to its own first-party API. */
export const SESSION_CONTEXT_HEADER = "X-Analytics-Session";

export const SessionStartRequestSchema = z
  .object({
    /** Visitor correlation id. Generated when absent; reuse it to resume. */
    anon_id: z.string().min(1).optional(),
    /** Authenticated application user, from the backend (never a client claim). */
    user_id: z.string().min(1).nullable().optional(),
    /** Session_start attribution: device, country, utm, … (≤4 KB). */
    properties: PropsSchema.optional(),
  })
  .strict();

export type SessionStartRequest = z.infer<typeof SessionStartRequestSchema>;

export interface SessionDescriptor {
  session_id: string;
  anon_id: string;
  user_id: string | null;
  /** ISO timestamps. */
  started_at: string;
  last_event_at: string;
  /** min(started_at + 24 h, last_event_at + 30 min), evaluated at response time. */
  expires_at: string;
  /** false when a new session was created, true when an existing one resumed. */
  resumed: boolean;
  /** Stored events for this session so far (0 → the SDK emits session_start). */
  emitted_events: number;
  /** Persisted bootstrap attribution, rehydrated on resume. */
  properties: Record<string, unknown>;
  /** Opaque signed context to propagate as `X-Analytics-Session`. */
  context: string;
}

export const SessionValidateRequestSchema = z
  .object({ context: z.string().min(1) })
  .strict();

export type SessionValidateRequest = z.infer<typeof SessionValidateRequestSchema>;

export type SessionInvalidReason =
  | "malformed"
  | "bad_signature"
  | "expired"
  | "unknown_session";

export interface SessionValidation {
  valid: boolean;
  reason?: SessionInvalidReason;
  session_id?: string;
  anon_id?: string;
  user_id?: string | null;
  expires_at?: string;
  emitted_events?: number;
  properties?: Record<string, unknown>;
}
