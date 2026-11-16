// Durable queue: one record per event, key `sa_ev:<event_id>`, written to
// localStorage BEFORE track() resolves (05-sdks.md §"Durable queue").
// The IndexedDB mirror (mirror.ts) is the async recovery copy; quota overflow
// drops heartbeats first, then oldest, and counts them for sdk_dropped.

import type { EventMirror } from "./mirror.js";

export interface QueueStorage {
  keys(): string[];
  get(key: string): string | null;
  set(key: string, value: string): void;
  remove(key: string): void;
}

export const EVENT_PREFIX = "sa_ev:";

export interface QueuedEvent {
  event_id: string;
  [key: string]: unknown;
}

/** All queued events in enqueue order (key sort = UUIDv7 time order). */
export function readQueue(storage: QueueStorage): QueuedEvent[] {
  return storage
    .keys()
    .filter((k) => k.startsWith(EVENT_PREFIX))
    .sort()
    .map((k) => {
      try {
        return JSON.parse(storage.get(k)!) as QueuedEvent;
      } catch {
        storage.remove(k); // corrupt record: drop, do not block the queue
        return null;
      }
    })
    .filter((e): e is QueuedEvent => e !== null);
}

/** Persist one event before the caller resolves. Throws on quota (caller decides). */
export function enqueue(storage: QueueStorage, event: QueuedEvent): void {
  storage.set(EVENT_PREFIX + event.event_id, JSON.stringify(event));
}

/** Remove acknowledged (or permanently rejected) events by ID. */
export function dequeueMany(storage: QueueStorage, eventIds: Iterable<string>): void {
  for (const id of eventIds) storage.remove(EVENT_PREFIX + id);
}

export function queueDepth(storage: QueueStorage): number {
  return storage.keys().filter((k) => k.startsWith(EVENT_PREFIX)).length;
}

/** Highest queued seq per session — the restore-time reconciliation floor. */
export function maxQueuedSeq(storage: QueueStorage, sessionId: string): number {
  let max = 0;
  for (const e of readQueue(storage)) {
    if (e.session_id === sessionId && typeof e.seq === "number" && e.seq > max) {
      max = e.seq;
    }
  }
  return max;
}

// ---------------------------------------------------------------------------
// Quota handling (05-sdks.md §"Durable queue" table): drop heartbeats first,
// then the oldest events; count every victim in the pending `sdk_dropped`
// counter (`sa:dropped`), reported on the next flush/track.
// ---------------------------------------------------------------------------

export const DROPPED_KEY = "sa:dropped";

export function addDropped(storage: QueueStorage, n: number): void {
  const prev = Number(storage.get(DROPPED_KEY) ?? "0");
  storage.set(DROPPED_KEY, String(prev + n));
}

/** Read-and-clear the pending drop count (call under the session lock). */
export function takeDropped(storage: QueueStorage): number {
  const n = Number(storage.get(DROPPED_KEY) ?? "0");
  if (n > 0) storage.set(DROPPED_KEY, "0");
  return n;
}

/**
 * Make room for an event of `neededBytes`: drop heartbeats first, then the
 * OLDEST events, until enough bytes are freed (one slot is not enough when
 * the incoming event is larger than the victim — under a full store that
 * would lose the incoming event AND leave the counter growing forever).
 * Never drops the incoming event.
 */
export function dropForQuota(
  storage: QueueStorage,
  incomingId: string,
  neededBytes = 0,
): number {
  let freed = 0;
  let victims = 0;
  for (;;) {
    const events = readQueue(storage); // re-read: keys change as we drop
    if (events.length === 0 || (neededBytes > 0 && freed >= neededBytes)) break;
    const candidates = events.filter((e) => e.event_id !== incomingId);
    const hearts = candidates.filter((e) => e.name === "heartbeat");
    const victim = hearts[0] ?? candidates[0];
    if (!victim) break;
    const size = JSON.stringify(victim).length + 2;
    dequeueMany(storage, [victim.event_id]);
    freed += size;
    victims += 1;
  }
  if (victims > 0) addDropped(storage, victims);
  return victims;
}

// ---------------------------------------------------------------------------
// Primary + mirror enqueue. The primary is synchronous; the mirror is async.
// ---------------------------------------------------------------------------

export type EnqueueOutcome = "primary" | "mirror" | "lost";

/**
 * Persist one event: localStorage primary; on QuotaExceededError make room
 * and retry; if the primary still refuses, AWAIT the IndexedDB mirror write
 * before returning — otherwise "persisted before track() resolves" would be
 * false on the fallback path (05-sdks.md §"Durable queue").
 */
export async function enqueueResilient(
  storage: QueueStorage,
  mirror: EventMirror | undefined,
  event: QueuedEvent,
): Promise<EnqueueOutcome> {
  try {
    enqueue(storage, event);
    // Mirror copy: awaited so a fake mirror is deterministic in tests; the
    // primary is already durable at this point, so a mirror error is inert.
    if (mirror) await mirror.put(event).catch(() => {});
    return "primary";
  } catch (err) {
    if (!isQuotaError(err)) throw err;
    // Free enough space (counts the victims in sa:dropped), then retry.
    dropForQuota(storage, event.event_id, JSON.stringify(event).length + 2);
    try {
      enqueue(storage, event);
      if (mirror) await mirror.put(event).catch(() => {});
      return "primary";
    } catch {
      // Primary is unusable: the mirror write is awaited — the caller's
      // "persisted before resolve" guarantee rides on it.
      if (mirror) {
        await mirror.put(event);
        return "mirror";
      }
      addDropped(storage, 1);
      return "lost";
    }
  }
}

/** True for the quota family (QuotaExceededError / DOMException code 22/1014). */
export function isQuotaError(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;
  const e = err as { name?: string; code?: number };
  return e.name === "QuotaExceededError" || e.code === 22 || e.code === 1014;
}

/**
 * Restore-time recovery: re-enqueue mirrored events the primary lost
 * (localStorage eviction/between sessions). Best-effort; quota failures are
 * counted, not thrown.
 */
export async function recoverFromMirror(
  storage: QueueStorage,
  mirror: EventMirror | undefined,
): Promise<number> {
  if (!mirror) return 0;
  let recovered = 0;
  let mirrored: QueuedEvent[];
  try {
    mirrored = await mirror.readAll();
  } catch {
    return 0;
  }
  for (const e of mirrored) {
    if (!e?.event_id || storage.get(EVENT_PREFIX + e.event_id) !== null) continue;
    await enqueueResilient(storage, mirror, e);
    recovered += 1;
  }
  return recovered;
}

/** Mirror deletion is awaited in flush so a fake mirror is deterministic. */
export async function dequeueManyWithMirror(
  storage: QueueStorage,
  mirror: EventMirror | undefined,
  eventIds: Iterable<string>,
): Promise<void> {
  const ids = [...eventIds];
  dequeueMany(storage, ids);
  if (ids.length > 0) {
    try {
      await mirror?.delete(ids);
    } catch {
      /* mirror deletion is best-effort; recovery re-checks the primary */
    }
  }
}
