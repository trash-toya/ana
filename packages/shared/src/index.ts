// @session-analytics/shared — contract types, zod schemas, canonical JSON +
// payloadHash, event category table, rejection reasons (docs/plan/04-services.md).

export {
  ACCEPTED_SCHEMA_VERSIONS,
  EnvelopeSchema,
  EventSchema,
  MAX_EVENTS_PER_REQUEST,
  MAX_PROPS_BYTES,
  MAX_PROPS_DEPTH,
  MAX_PROPS_KEYS,
  NAME_PATTERN,
  PropsSchema,
  REJECT_REASONS,
  RejectReasonSchema,
  SCHEMA_VERSION,
  SOURCES,
  SOURCES_BY_KEY_KIND,
  SourceSchema,
} from "./contract.js";
export type { Envelope, Event, RejectReason, Source } from "./contract.js";

export {
  SESSION_CONTEXT_HEADER,
  SESSION_IDLE_MS,
  SESSION_MAX_AGE_MS,
  SessionStartRequestSchema,
  SessionValidateRequestSchema,
} from "./sessions.js";
export type {
  SessionDescriptor,
  SessionInvalidReason,
  SessionStartRequest,
  SessionValidateRequest,
  SessionValidation,
} from "./sessions.js";

export { CATEGORY, categorize } from "./categories.js";

export { canonicalJson } from "./canonical-json.js";

export { payloadHash } from "./payload-hash.js";
export type { PayloadHashFields } from "./payload-hash.js";
