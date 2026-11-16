import { test } from "vitest";
import assert from "node:assert/strict";
import pg from "pg";
import request from "supertest";
import { withTestDb } from "../../../db/test/harness.mjs";
import { createApp } from "../dist/app.js";
import { deadPool } from "./helpers.mjs";

test("GET /healthz is liveness only — no DB required", async () => {
  const pool = deadPool(pg);
  try {
    const app = createApp({ pool });
    const res = await request(app).get("/healthz");
    assert.equal(res.status, 200);
    assert.equal(res.body.ok, true);
  } finally {
    await pool.end();
  }
});

test("GET /readyz is 503 when the DB is unreachable", async () => {
  const pool = deadPool(pg);
  try {
    const app = createApp({ pool });
    const res = await request(app).get("/readyz");
    assert.equal(res.status, 503);
    assert.equal(res.body.ok, false);
  } finally {
    await pool.end();
  }
});

test("GET /readyz round-trips the DB through the service pool", async () => {
  await withTestDb("ingest_ready", async ({ pool }) => {
    const app = createApp({ pool });
    const res = await request(app).get("/readyz");
    assert.equal(res.status, 200);
    assert.equal(res.body.ok, true);
  });
});

test("unknown routes are 404 JSON", async () => {
  const pool = deadPool(pg);
  try {
    const app = createApp({ pool });
    const res = await request(app).get("/nope");
    assert.equal(res.status, 404);
    assert.equal(res.body.error, "not_found");
  } finally {
    await pool.end();
  }
});
