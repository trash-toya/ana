// API-key authentication for the read-only dashboard (04-services.md step 1
// pattern, same as the ingest service's ApiKeyAuth but kind='dashboard'):
// X-API-Key → sha256 → api_keys lookup (cached, 60 s TTL) → tenant_id. A
// tenant ID is never accepted from the request.
//
// The ingest auth deliberately excludes dashboard keys and this one accepts
// ONLY them: a leaked ingest key cannot read tenant data and vice versa.
// Revocation is effective within the TTL across instances; same-instance
// revocations are immediate via invalidate(). The cache keys on the digest,
// so an attacker cannot probe another tenant's cache entry.

import { createHash } from "node:crypto";
import type { NextFunction, Request, RequestHandler, Response } from "express";
import type { Pool, QueryResult, QueryResultRow } from "pg";
import "./types.js";

export interface AuthOptions {
  /** Cache TTL; 60 s per the key policy. */
  ttlMs?: number;
  /** Injectable clock for tests. */
  now?: () => number;
  /** Injectable query function for tests (defaults to pool.query). */
  query?: <R extends QueryResultRow>(
    text: string,
    values?: unknown[],
  ) => Promise<QueryResult<R>>;
}

export type AuthFailureReason =
  | "missing_key"
  | "invalid_key"
  | "wrong_key_kind"
  | "key_revoked";

export type AuthResult =
  | { ok: true; tenantId: string }
  | { ok: false; status: 401 | 403; reason: AuthFailureReason };

interface KeyRow {
  tenant_id: string;
  kind: string;
  revoked: boolean;
}

interface CacheEntry {
  tenantId: string;
  expiresAt: number;
}

const STATUS: Record<AuthFailureReason, 401 | 403> = {
  missing_key: 401,
  invalid_key: 401,
  wrong_key_kind: 401,
  key_revoked: 403,
};

export class DashboardKeyAuth {
  private readonly cache = new Map<string, CacheEntry>();
  private readonly ttlMs: number;
  private readonly now: () => number;
  private readonly query: <R extends QueryResultRow>(
    text: string,
    values?: unknown[],
  ) => Promise<QueryResult<R>>;

  constructor(pool: Pool, options: AuthOptions = {}) {
    this.ttlMs = options.ttlMs ?? 60_000;
    this.now = options.now ?? Date.now;
    this.query =
      options.query ??
      ((text, values) => pool.query(text as string, values as unknown[]));
  }

  authenticate(rawKey: string | undefined): Promise<AuthResult> {
    if (!rawKey) {
      return Promise.resolve({ ok: false, status: 401, reason: "missing_key" });
    }
    const hex = this.digest(rawKey);
    const cached = this.cache.get(hex);
    if (cached && cached.expiresAt > this.now()) {
      return Promise.resolve({ ok: true, tenantId: cached.tenantId });
    }
    return this.lookup(hex, rawKey);
  }

  private async lookup(hex: string, _rawKey: string): Promise<AuthResult> {
    const { rows } = await this.query<KeyRow>(
      `SELECT tenant_id, kind, revoked_at IS NOT NULL AS revoked
          FROM api_keys
         WHERE key_hash = $1`,
      [Buffer.from(hex, "hex")],
    );
    const row = rows[0];
    if (!row || row.kind !== "dashboard") {
      // An ingest/server key is simply not a dashboard key; do not reveal which.
      return { ok: false, status: 401, reason: row ? "wrong_key_kind" : "invalid_key" };
    }
    if (row.revoked) return { ok: false, status: 403, reason: "key_revoked" };

    this.cache.set(hex, { tenantId: row.tenant_id, expiresAt: this.now() + this.ttlMs });
    return { ok: true, tenantId: row.tenant_id };
  }

  /** Express middleware: sets req.tenantId or answers 401/403 itself. */
  middleware(): RequestHandler {
    return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
      let result: AuthResult;
      try {
        result = await this.authenticate(req.get("X-API-Key"));
      } catch (err) {
        next(err);
        return;
      }
      if (!result.ok) {
        res.status(STATUS[result.reason]).json({ error: result.reason });
        return;
      }
      req.tenantId = result.tenantId;
      next();
    };
  }

  /** Same-instance revocations are immediate. */
  invalidate(rawKey: string): void {
    this.cache.delete(this.digest(rawKey));
  }

  private digest(rawKey: string): string {
    return createHash("sha256").update(rawKey, "utf8").digest("hex");
  }
}
