// Request validation and batch preparation (04-services.md steps 2–4):
//
//   1. Envelope: schema_version (accept current+previous), optional anon_id,
//      sent_at, events ≤ 500. Failures here are malformed requests → 400.
//   2. Per event: shape violations (missing/extra/wrong-typed fields,
//      non-ISO time) are SDK bugs → 400. Value violations map onto the
//      per-event rejection enum → 200 entries:
//        invalid_uuid, missing_anon_id, bad_name, props_too_large
//   3. Time (D32): `source = client` is skew-corrected
//      (t' = t + (received_at − sent_at)) and t' is validated; `server` and
//      `webhook` keep the producer's time as-is (a webhook must not be shifted
//      by the webhook's own delay) and are validated against the same window:
//      older than 24 h → too_old, more than 5 min ahead → too_new.
//   4. payloadHash over the producer's ORIGINAL fields, then in-batch dedupe:
//      same event_id + same hash → exact duplicate (mirrors the first);
//      same event_id + different hash → keep first by request order, reject
//      the rest as conflicting_duplicate. Survivors get ord 0..n−1.
//   5. Classification: cat from the CATEGORY map (SQL never hard-codes names).

import {
  ACCEPTED_SCHEMA_VERSIONS,
  EnvelopeSchema,
  MAX_EVENTS_PER_REQUEST,
  EventSchema,
  PropsSchema,
  categorize,
  payloadHash,
  type RejectReason,
  type Source,
} from "@session-analytics/shared";
import { z } from "zod";

/** Rows handed to ingest_batch() via parse_batch (column order as there). */
export interface PreparedEvent {
  tenant_id: string;
  event_id: string;
  /** null for machine-originated events (D29). */
  session_id: string | null;
  name: string;
  cat: "e" | "l" | "s";
  /** Occurrence time (skew-corrected only for client-sourced events, D32). */
  time: string;
  /** Analytics receipt time — always server time. */
  received_at: string;
  seq: number | null;
  /** null only when session_id is null (missing_anon_id otherwise). */
  anon_id: string | null;
  user_id: string | null;
  url: string | null;
  source: Source;
  props: Record<string, unknown>;
  payload_hash: string;
  ord: number;
}

export interface RejectedEvent {
  event_id: string;
  reason: RejectReason;
}

export interface ValidationOutcome {
  rows: PreparedEvent[];
  /** Per-event validation rejections (200 responses, never 400). */
  rejected: RejectedEvent[];
  /** In-batch exact duplicates: this ID mirrors the outcome of `of`. */
  dups: { event_id: string; of: string }[];
}

/** Envelope-level failure → 400 (SDK bug; drop batch, do not loop). */
export class EnvelopeError extends Error {
  readonly status = 400;
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "EnvelopeError";
  }
}

export interface ValidateContext {
  tenantId: string;
  /** When the server received the request — the reference for both the skew
   *  correction and the corrected-time window. */
  receivedAt: Date;
}

/**
 * Envelope-level shape, with events left opaque: the envelope gate must not
 * fail (400) because of a per-event value violation that owns a dedicated
 * rejection reason. Event shape/value checks happen per event below.
 */
const EnvelopeShape = z
  .object({
    schema_version: EnvelopeSchema.shape.schema_version,
    anon_id: EnvelopeSchema.shape.anon_id,
    sent_at: EnvelopeSchema.shape.sent_at,
    events: z.array(z.unknown()).min(1).max(MAX_EVENTS_PER_REQUEST),
  })
  .strict();

const TOO_OLD_MS = 24 * 60 * 60 * 1000;
const TOO_NEW_MS = 5 * 60 * 1000;

/** Post-shape-check view of a wire event (required fields are strings). */
interface WireEvent {
  event_id: string;
  session_id?: string | null;
  time: string;
  name: string;
  seq?: number | null;
  /** Per-event visitor id; overrides the envelope (server batches mix visitors). */
  anon_id?: string | null;
  user_id?: string | null;
  url?: string | null;
  source?: Source;
  props?: Record<string, unknown>;
}

export function validateBatch(raw: unknown, ctx: ValidateContext): ValidationOutcome {
  assertEnvelope(raw);
  const envelope = EnvelopeShape.parse(raw);
  const received = ctx.receivedAt.getTime();
  const receivedIso = ctx.receivedAt.toISOString();
  const skewMs = received - Date.parse(envelope.sent_at);
  const anonId = envelope.anon_id ?? null;

  const rows: PreparedEvent[] = [];
  const rejected: RejectedEvent[] = [];
  const dups: { event_id: string; of: string }[] = [];
  const firstById = new Map<string, { hash: string }>();

  for (const rawEvent of envelope.events) {
    const shape = shapeFailure(rawEvent);
    if (shape) {
      throw new EnvelopeError("malformed", `event ${describe(rawEvent)}: ${shape}`);
    }
    // Shape passed: required fields are strings, optional fields are typed.
    const event = rawEvent as WireEvent;
    const sessionId = event.session_id ?? null;
    const source: Source = event.source ?? "client";
    // A per-event anon_id overrides the envelope: one server batch may mix
    // visitors and sessions, so the envelope cannot be the only source.
    const eventAnonId = event.anon_id !== undefined ? event.anon_id : anonId;
    // --- value-level checks, in the reason enum's order -------------------
    if (!isUuid(event.event_id) || (sessionId !== null && !isUuid(sessionId))) {
      rejected.push({ event_id: event.event_id, reason: "invalid_uuid" });
      continue;
    }
    // D29: only sessionless events may omit the visitor correlation id.
    if (sessionId !== null && (eventAnonId === null || eventAnonId === "")) {
      rejected.push({ event_id: event.event_id, reason: "missing_anon_id" });
      continue;
    }
    if (!EventSchema.shape.name.safeParse(event.name).success) {
      rejected.push({ event_id: event.event_id, reason: "bad_name" });
      continue;
    }
    if (!PropsSchema.safeParse(event.props ?? {}).success) {
      rejected.push({ event_id: event.event_id, reason: "props_too_large" });
      continue;
    }

    // --- time: source-dependent trust (D32) -------------------------------
    // Client timestamps are corrected; server/webhook timestamps are the
    // producer's observation and must not be shifted by delivery delay.
    const corrected =
      source === "client"
        ? new Date(Date.parse(event.time) + skewMs).toISOString()
        : new Date(event.time).toISOString();
    const ageMs = received - Date.parse(corrected);
    if (ageMs > TOO_OLD_MS) {
      rejected.push({ event_id: event.event_id, reason: "too_old" });
      continue;
    }
    if (ageMs < -TOO_NEW_MS) {
      rejected.push({ event_id: event.event_id, reason: "too_new" });
      continue;
    }

    // --- hash on the producer's ORIGINAL fields, then in-batch dedupe -----
    const hash = payloadHash({
      session_id: sessionId,
      name: event.name,
      url: event.url ?? null,
      props: event.props ?? {},
      time: event.time,
      seq: event.seq ?? null,
    });
    const first = firstById.get(event.event_id);
    if (first) {
      if (first.hash === hash) {
        // exact duplicate: reported however the first turns out
        dups.push({ event_id: event.event_id, of: event.event_id });
      } else {
        // different payload, same id: first by request order wins
        rejected.push({ event_id: event.event_id, reason: "conflicting_duplicate" });
      }
      continue;
    }
    firstById.set(event.event_id, { hash });

    rows.push({
      tenant_id: ctx.tenantId,
      event_id: event.event_id,
      session_id: sessionId,
      name: event.name,
      cat: categorize(event.name),
      time: corrected,
      received_at: receivedIso,
      seq: event.seq ?? null,
      anon_id: eventAnonId ?? null,
      user_id: event.user_id ?? null,
      url: event.url ?? null,
      source,
      props: event.props ?? {},
      payload_hash: hash,
      ord: rows.length,
    });
  }

  return { rows, rejected, dups };
}

// ---------------------------------------------------------------------------
// Outcome assembly: every submitted ID in exactly one list.
// ---------------------------------------------------------------------------

export interface IngestBatchResult {
  stored: string[];
  dups: string[];
  capped: string[];
  conflicts: string[];
}

export interface AssembledOutcomes {
  accepted: string[];
  rejected: RejectedEvent[];
}

/**
 * Match each submitted event_id against the validation ledger and the
 * ingest_batch() result (04-services.md): stored/newly-seen → accepted,
 * already-stored duplicates → accepted, capped → session_capped, conflicts →
 * conflicting_duplicate, validation rejections merged in, and an ID the
 * function did not report is treated as an accepted duplicate (the defensive
 * half of the cross-session race fix). In-batch exact duplicates mirror the
 * outcome of their first occurrence.
 */
export function assembleOutcomes(
  outcome: ValidationOutcome,
  result: IngestBatchResult,
): AssembledOutcomes {
  const stored = new Set(result.stored);
  const seen = new Set(result.dups);
  const capped = new Set(result.capped);
  const conflicts = new Set(result.conflicts);

  const accepted: string[] = [];
  const rejected: RejectedEvent[] = [];
  const reported = new Set<string>();

  // Resolution cache so repeat occurrences resolve consistently.
  const resolved = new Map<string, { accepted: boolean; reason?: RejectReason }>();

  const submitted = [
    ...outcome.rows.map((r) => r.event_id),
    ...outcome.rejected.map((r) => r.event_id),
    ...outcome.dups.map((d) => d.event_id),
  ];

  const resolve = (id: string): { accepted: boolean; reason?: RejectReason } => {
    const cached = resolved.get(id);
    if (cached) return cached;
    let value: { accepted: boolean; reason?: RejectReason };
    if (capped.has(id)) value = { accepted: false, reason: "session_capped" };
    else if (conflicts.has(id)) value = { accepted: false, reason: "conflicting_duplicate" };
    else if (stored.has(id) || seen.has(id)) value = { accepted: true };
    else value = { accepted: true }; // unreported by the function → accepted duplicate
    resolved.set(id, value);
    return value;
  };

  const push = (
    v: { accepted: boolean; reason?: RejectReason },
    id: string,
  ): void => {
    if (v.accepted) accepted.push(id);
    else rejected.push({ event_id: id, reason: v.reason as RejectReason });
    reported.add(id);
  };

  // One entry per ID: the first occurrence decides (row → function result,
  // validation rejection → its reason, exact dup → mirrors its first).
  // Later occurrences of the same ID (in-batch exact dups and
  // conflicting variants) are skipped here but kept in the ledger for
  // rejection metrics.
  for (const r of outcome.rows) {
    if (!reported.has(r.event_id)) push(resolve(r.event_id), r.event_id);
  }
  for (const r of outcome.rejected) {
    if (!reported.has(r.event_id)) {
      const v = { accepted: false, reason: r.reason };
      resolved.set(r.event_id, v);
      push(v, r.event_id);
    }
  }
  for (const d of outcome.dups) {
    if (!reported.has(d.event_id)) push(resolve(d.of), d.event_id);
  }

  // Contract invariant: every submitted ID in exactly one list. A violation
  // is a bug in this module, not in a client — recover deterministically by
  // force-placing leftovers in `accepted`.
  const distinctSubmitted = new Set(submitted);
  if (reported.size !== distinctSubmitted.size) {
    for (const id of distinctSubmitted) {
      if (!reported.has(id)) push({ accepted: true }, id);
    }
  }

  return { accepted, rejected };
}

// ---------------------------------------------------------------------------
// Envelope and shape checks
// ---------------------------------------------------------------------------

/** Envelope-level gate: unsupported schema_version and structurally invalid
 *  envelopes are 400s (SDK bug). Event value violations are NOT handled here —
 *  they belong to the per-event rejection reasons. */
function assertEnvelope(raw: unknown): void {
  const version = (raw as { schema_version?: unknown } | null)?.schema_version;
  if (typeof version === "number" && !ACCEPTED_SCHEMA_VERSIONS.includes(version)) {
    throw new EnvelopeError(
      "schema_unsupported",
      `schema_version ${version} not in ${ACCEPTED_SCHEMA_VERSIONS.join(", ")}`,
    );
  }
  if (!EnvelopeShape.safeParse(raw).success) {
    throw new EnvelopeError("malformed", "envelope failed schema validation");
  }
}

/**
 * Shape check: required fields present with the right primitive type,
 * optional fields correctly typed, time ISO-parseable, no unknown keys.
 * Anything failing here is an SDK bug → 400 (missing event_id, seq as a
 * string, url as a number, unknown field, non-ISO time, …). Value-level
 * failures (bad uuid, bad name, oversized props) are NOT shape failures —
 * they get their own per-event rejection reasons. Returns a short
 * description or null when the shape is sound.
 */
function shapeFailure(event: unknown): string | null {
  if (typeof event !== "object" || event === null) return "not an object";
  const e = event as Record<string, unknown>;
  for (const key of ["event_id", "time", "name"]) {
    if (typeof e[key] !== "string") return `field ${key} must be a string`;
  }
  // Rev 5: session_id is optional; null and absent both mean "sessionless".
  if ("session_id" in e && e.session_id !== null && typeof e.session_id !== "string") {
    return "field session_id must be string|null";
  }
  if ("seq" in e && e.seq !== null && typeof e.seq !== "number") {
    return "field seq must be number|null";
  }
  for (const key of ["anon_id", "user_id", "url"] as const) {
    if (key in e && e[key] !== null && typeof e[key] !== "string") {
      return `field ${key} must be string|null`;
    }
  }
  if ("source" in e && e.source !== "client" && e.source !== "server" && e.source !== "webhook") {
    return "field source must be client|server|webhook";
  }
  if ("props" in e && (e.props === null || typeof e.props !== "object" || Array.isArray(e.props))) {
    return "field props must be an object";
  }
  // unknown keys and non-ISO timestamps are structural failures
  const parsed = EventSchema.safeParse(e);
  if (!parsed.success) {
    for (const issue of parsed.error.issues) {
      if (issue.code === "unrecognized_keys") return `unknown field(s): ${issue.message}`;
      if (issue.path[0] === "time" && issue.code === "invalid_string") {
        return "field time is not an ISO 8601 timestamp";
      }
    }
  }
  return null;
}

function describe(event: unknown): string {
  const id = (event as { event_id?: unknown } | null)?.event_id;
  return typeof id === "string" ? id : "<no event_id>";
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}
