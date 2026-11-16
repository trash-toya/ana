// The wire contract: one definition shared by the ingest API and the SDKs
// (docs/plan/04-services.md, Notion Rev 5 §19). Validation failures never
// reach Postgres.
//
// Reason mapping for P1-3 (per-event rejections surface as 200 entries, never
// 400):
//   invalid_uuid      — event_id/session_id not a UUID
//   missing_anon_id   — an event with a session_id and no envelope anon_id
//   bad_name          — name fails NAME_PATTERN
//   props_too_large   — props size (>4 KB), key count (>20) or depth (>2)
//   too_old/too_new   — time outside the window (API layer, after the
//                       source-dependent correction/D32 — not encoded here)
//   schema_unsupported— envelope schema_version outside accepted versions
//
// Rev 5: `session_id` is optional (machine-originated events are sessionless,
// D29) and `source` records provenance (D28). `anon_id` is required whenever a
// session is present, because attribution and sessions_active depend on it.

import { z } from "zod";

/** Current wire schema version. Accept the current and the previous one. */
export const SCHEMA_VERSION = 1;

/** Versions the ingest API accepts (current and previous). */
export const ACCEPTED_SCHEMA_VERSIONS: readonly number[] = [1, 0];

/** Event provenance (Notion Rev 5 §19.4). Defaults to `client`. */
export const SOURCES = ["client", "server", "webhook"] as const;
export const SourceSchema = z.enum(SOURCES);
export type Source = (typeof SOURCES)[number];

/** Source allowed for each API-key kind (D28): client keys are public. */
export const SOURCES_BY_KEY_KIND: Readonly<
  Record<"ingest" | "server" | "dashboard", readonly Source[]>
> = {
  ingest: ["client"],
  server: SOURCES,
  dashboard: [],
};

/** Per-event rejection reasons; the response contract's only vocabulary. */
export const REJECT_REASONS = [
  "invalid_uuid",
  "missing_anon_id",
  "too_old",
  "too_new",
  "bad_name",
  "props_too_large",
  "schema_unsupported",
  "session_capped",
  "conflicting_duplicate",
] as const;

export const RejectReasonSchema = z.enum(REJECT_REASONS);
export type RejectReason = (typeof REJECT_REASONS)[number];

/** Event names: lowercase, start with a letter, ≤64 chars total. */
export const NAME_PATTERN = /^[a-z][a-z0-9_.]{0,63}$/;

// Request limits (blueprint §Abuse protection).
export const MAX_EVENTS_PER_REQUEST = 500;
export const MAX_PROPS_KEYS = 20;
export const MAX_PROPS_BYTES = 4096; // serialised size of props
export const MAX_PROPS_DEPTH = 2;

// ---------------------------------------------------------------------------
// props: JSON object, ≤20 keys, ≤4 KB serialised, depth ≤2, leaf values are
// string | number | boolean | null. Depth and leaf types are enforced
// structurally; size and key count by refine. Every violation maps to the
// single reason `props_too_large`.
// ---------------------------------------------------------------------------

const PropLeaf = z.union([z.string(), z.number(), z.boolean(), z.null()]);

const PropValue = z.union([PropLeaf, z.array(PropLeaf), z.record(z.string(), PropLeaf)]);

export const PropsSchema = z.record(z.string(), PropValue).superRefine((props, ctx) => {
  const keys = Object.keys(props);
  if (keys.length > MAX_PROPS_KEYS) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `props has ${keys.length} keys (max ${MAX_PROPS_KEYS})`,
    });
  }
  let size: number;
  try {
    size = JSON.stringify(props).length;
  } catch {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "props is not serialisable",
    });
    return;
  }
  if (size > MAX_PROPS_BYTES) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: `props is ${size} bytes (max ${MAX_PROPS_BYTES})`,
    });
  }
});

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

export const EventSchema = z
  .object({
    event_id: z.string().uuid(),
    /** Optional: machine-originated events may have no human session (D29). */
    session_id: z.string().uuid().nullable().optional(),
    /** Client-supplied sequence number within the session, if any. */
    seq: z.number().int().min(0).nullable().optional(),
    /** Per-event visitor id; overrides the envelope (server batches mix visitors). */
    anon_id: z.string().min(1).nullable().optional(),
    user_id: z.string().nullable().optional(),
    /** Producer's original timestamp. Corrected per source (D32). */
    time: z.string().datetime({ offset: true }),
    name: z
      .string()
      .max(64)
      .regex(NAME_PATTERN, "name must match ^[a-z][a-z0-9_.]{0,63}$"),
    url: z.string().nullable().optional(),
    /** Provenance; defaults to `client` (D28). */
    source: SourceSchema.optional(),
    props: PropsSchema.optional(),
  })
  .strict();

export type Event = z.infer<typeof EventSchema>;

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

export const EnvelopeSchema = z
  .object({
    schema_version: z
      .number()
      .int()
      .refine(
        (v) => ACCEPTED_SCHEMA_VERSIONS.includes(v),
        `schema_version must be one of ${ACCEPTED_SCHEMA_VERSIONS.join(", ")}`,
      ),
    /** Required for session-bound events; optional for sessionless ones (D29). */
    anon_id: z.string().min(1).optional(),
    sent_at: z.string().datetime({ offset: true }),
    events: z.array(EventSchema).min(1).max(MAX_EVENTS_PER_REQUEST),
  })
  .strict();

export type Envelope = z.infer<typeof EnvelopeSchema>;
