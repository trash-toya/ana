// T4 — conflicting duplicates. Same event_id, different payloads.
//   * Within one request: the API keeps the first by request order; `ord` is the SQL
//     fallback (DISTINCT ON). Either way: one event row, one hash, the loser rejected
//     as conflicting_duplicate.
//   * Across two requests: the first commit wins; the retry is conflicting_duplicate.
import { test } from "vitest";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { withTestDb, ingest, event, sha } from "../harness.mjs";

const randomUuid = () => crypto.randomUUID();
const tenantId = randomUuid();
const sessionId = randomUuid();

function row(eventId, seq, url, ord) {
  const ev = event({
    tenantId,
    sessionId,
    eventId,
    seq,
    time: new Date(Date.UTC(2026, 10, 15, 12, 0, 0)),
    name: "page_view",
    cat: "e",
    url,
  });
  ev.ord = ord;
  return ev;
}

test("T4a: same event_id twice in one request — first by request order wins", async () => {
  await withTestDb("t4a", async ({ pool }) => {
    const c = await pool.connect();
    try {
      const eventId = randomUuid();
      // Different payload hashes for the two rows (different url).
      const a = row(eventId, 1, "/first", 1);
      const b = row(eventId, 1, "/second", 2);
      b.payload_hash = sha(["second", eventId]);

      const r = await ingest(c, [a, b]);
      // The SQL defense-in-depth keeps the first row by ord; the loser never reaches
      // event_dedupe, so it cannot be in conflicts (that list classifies vs stored).
      assert.equal(r.stored.length, 1);
      assert.equal(r.conflicts.length, 0);

      const ev = await c.query("SELECT url FROM events");
      assert.equal(ev.rows.length, 1);
      assert.equal(ev.rows[0].url, "/first", "the first row by ord is stored");

      const dedupe = await c.query("SELECT count(*) AS n FROM event_dedupe");
      assert.equal(Number(dedupe.rows[0].n), 1, "one dedupe row, one hash");

      const guard = await c.query("SELECT total_events FROM session_guard");
      assert.equal(Number(guard.rows[0].total_events), 1, "loser consumes no quota");
    } finally {
      c.release();
    }
  });
});

test("T4b: same event_id across two requests — first commit wins", async () => {
  await withTestDb("t4b", async ({ pool }) => {
    const c = await pool.connect();
    try {
      const eventId = randomUuid();
      const first = row(eventId, 1, "/first", 0);
      const second = row(eventId, 1, "/second", 0);
      second.payload_hash = sha(["second", eventId]);

      const r1 = await ingest(c, [first]);
      assert.equal(r1.stored.length, 1);

      const r2 = await ingest(c, [second]);
      assert.deepEqual(r2.conflicts, [eventId], "conflicting_duplicate");
      assert.equal(r2.stored.length, 0);

      const ev = await c.query("SELECT url, count(*) AS n FROM events GROUP BY url");
      assert.equal(ev.rows.length, 1);
      assert.equal(ev.rows[0].url, "/first");

      const guard = await c.query("SELECT total_events FROM session_guard");
      assert.equal(Number(guard.rows[0].total_events), 1);
    } finally {
      c.release();
    }
  });
});

test("T4c: identical retry (same hash) is a duplicate, not a conflict", async () => {
  await withTestDb("t4c", async ({ pool }) => {
    const c = await pool.connect();
    try {
      const eventId = randomUuid();
      const ev = row(eventId, 1, "/same", 0);
      const r1 = await ingest(c, [ev]);
      assert.equal(r1.stored.length, 1);

      const r2 = await ingest(c, [{ ...ev, ord: 1 }]);
      assert.deepEqual(r2.dups, [eventId], "exact retry → dups (accepted)");
      assert.equal(r2.conflicts.length, 0);

      const n = await c.query("SELECT count(*) AS n FROM events");
      assert.equal(Number(n.rows[0].n), 1);
    } finally {
      c.release();
    }
  });
});
