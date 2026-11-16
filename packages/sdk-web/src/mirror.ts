// IndexedDB mirror (05-sdks.md §"Durable queue"): every queued event is also
// written asynchronously to IndexedDB. The primary (localStorage) is
// synchronous; the mirror is the recovery copy used when the primary throws
// QuotaExceededError (the enqueue then AWAITS the mirror write, so
// "persisted before track() resolves" holds on the fallback path) and at
// restore time to re-fill events the primary lost.

import type { QueuedEvent } from "./queue.js";

export interface EventMirror {
  put(event: QueuedEvent): Promise<void>;
  delete(eventIds: string[]): Promise<void>;
  readAll(): Promise<QueuedEvent[]>;
}

const DB_NAME = "sa_mirror";
const STORE = "events";

/** Promise wrapper around an IDB request. */
function done<T>(req: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error("idb request failed"));
  });
}

/**
 * Open the mirror. Returns undefined when IndexedDB is unavailable (SSR,
 * old browsers, private-mode rejection) — the primary then stands alone.
 */
export async function openMirror(
  factory: IDBFactory | undefined = typeof indexedDB === "undefined"
    ? undefined
    : indexedDB,
): Promise<EventMirror | undefined> {
  if (!factory) return undefined;
  let db: IDBDatabase;
  try {
    db = await new Promise<IDBDatabase>((resolve, reject) => {
      const req = factory.open(DB_NAME);
      req.onupgradeneeded = () => {
        if (!req.result.objectStoreNames.contains(STORE)) {
          req.result.createObjectStore(STORE, { keyPath: "event_id" });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error ?? new Error("idb open failed"));
    });
  } catch {
    return undefined; // degraded: primary-only
  }

  return {
    async put(event) {
      const tx = db.transaction(STORE, "readwrite");
      await done(tx.objectStore(STORE).put(event));
    },
    async delete(eventIds) {
      const tx = db.transaction(STORE, "readwrite");
      const store = tx.objectStore(STORE);
      await Promise.all(eventIds.map((id) => done(store.delete(id))));
    },
    async readAll() {
      const tx = db.transaction(STORE, "readonly");
      return (await done(tx.objectStore(STORE).getAll())) as QueuedEvent[];
    },
  };
}
