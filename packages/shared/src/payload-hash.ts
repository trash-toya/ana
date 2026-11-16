// payloadHash — the idempotency key for events (docs/plan/04-services.md).
//
// sha256(canonicalJson({session_id, name, url, props, time, seq})) computed
// with the **producer's original** time, before any correction. `user_id`,
// `anon_id`, `cat` and `source` are deliberately excluded: a retry after
// `identify()` keeps the stored event's first-wins attribution, and moving a
// producer from the client SDK to the server SDK must not turn a re-delivery
// into `conflicting_duplicate` (D28).
//
// Absent optional fields are normalised to `null` so that a retry where the
// SDK serialises `seq: null` versus omits the key hashes identically.

import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical-json.js";

export interface PayloadHashFields {
  /** null for sessionless machine-originated events (D29). */
  session_id: string | null;
  name: string;
  url?: string | null;
  props?: unknown;
  /** Producer's original (uncorrected) ISO timestamp. */
  time: string;
  seq?: number | null;
}

export function payloadHash(event: PayloadHashFields): string {
  return createHash("sha256")
    .update(
      canonicalJson({
        session_id: event.session_id,
        name: event.name,
        url: event.url ?? null,
        props: event.props ?? {},
        time: event.time,
        seq: event.seq ?? null,
      }),
    )
    .digest("hex");
}
