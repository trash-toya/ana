// Session record + Web Lock (05-sdks.md §"Multi-tab correctness"). lastAt and
// the counters live in the STORED record, never in tab memory: two tabs
// cannot emit the same time or the same seq. Every mutation happens inside
// one function, inside the lock.

import { uuidv7, randomBytes } from "./ids.js";

export const SESSION_KEY = "sa:session";
export const ANON_KEY = "sa:anon";
export const CONSENT_KEY = "sa:consent";
export const USER_KEY = "sa:user";
export const LOCK_NAME = "sa_session";
/** Minimal Web Locks shape (generic callback like navigator.locks). */
export interface LockManagerLike {
  request: <T>(name: string, cb: () => Promise<T>) => Promise<T>;
}

export const IDLE_LIMIT_MS = 30 * 60 * 1000; // idle > 30 min → new session
export const AGE_LIMIT_MS = 24 * 60 * 60 * 1000; // age > 24 h → new session
export const MAX_EVENTS = 5_000; // 5,000 events → new session

/** Durable per-tab-collection session state (the "stored session record"). */
export interface SessionRecord {
  session_id: string;
  started_at: number; // ms epoch
  last_seen_at: number; // ms epoch; moves on ENGAGEMENT only (drives expiry)
  /** Last minted event time — drives strict per-instance time monotonicity
   *  (max(now, last+1)) without letting heartbeats extend the session. */
  last_event_at: number;
  seq: number; // last assigned sequence number
  count: number; // events assigned to this session
  anon_id: string;
}

export interface EmitOutcome {
  events: Record<string, unknown>[]; // all events minted by this call
  sessionId: string;
}

/** expiry decision for an existing record at time `nowMs`. */
export function expiryReason(
  rec: SessionRecord,
  nowMs: number,
): "idle" | "age" | "count" | null {
  if (rec.count >= MAX_EVENTS) return "count";
  if (nowMs - rec.last_seen_at > IDLE_LIMIT_MS) return "idle";
  if (nowMs - rec.started_at > AGE_LIMIT_MS) return "age";
  return null;
}

/** Strictly-increasing per-instance time: max(now, last+1). */
export function nextTime(rec: SessionRecord | null, nowMs: number): number {
  return rec ? Math.max(nowMs, rec.last_seen_at + 1) : nowMs;
}

export function newSessionId(nowMs: number): string {
  return uuidv7(nowMs, randomBytes());
}

export function loadRecord(storage: {
  get(k: string): string | null;
}): SessionRecord | null {
  const raw = storage.get(SESSION_KEY);
  if (!raw) return null;
  try {
    const rec = JSON.parse(raw) as SessionRecord;
    if (typeof rec.session_id !== "string" || typeof rec.last_seen_at !== "number") {
      return null;
    }
    return rec;
  } catch {
    return null;
  }
}

export function saveRecord(
  storage: { set(k: string, v: string): void },
  rec: SessionRecord,
): void {
  storage.set(SESSION_KEY, JSON.stringify(rec));
}

export function clearRecord(storage: { remove(k: string): void }): void {
  storage.remove(SESSION_KEY);
}

/**
 * Web Locks wrapper. All session-record mutations run inside this callback so
 * two tabs share one record (seq/time are allocated once, globally).
 * No navigator.locks (Safari < 15.4): direct call — the no-lock fallback with
 * leader election is P2-3.
 */
export async function withLock<T>(
  locks: LockManagerLike | undefined,
  fn: () => Promise<T>,
): Promise<T> {
  if (locks && typeof locks.request === "function") {
    return locks.request(LOCK_NAME, fn);
  }
  return fn();
}

/** Run `fn` while holding the session lock, with the record loaded/saved. */
export async function mutateRecord<T>(
  storage: {
    get(k: string): string | null;
    set(k: string, v: string): void;
    remove(k: string): void;
  },
  locks: Parameters<typeof withLock>[0],
  nowMs: number,
  fn: (rec: SessionRecord | null) => T | Promise<T>,
): Promise<{ result: T; rec: SessionRecord | null }> {
  return withLock(locks, async () => {
    const rec = loadRecord(storage);
    const result = await fn(rec);
    const after = loadRecord(storage); // fn may have replaced the record
    return { result, rec: after };
  });
}
