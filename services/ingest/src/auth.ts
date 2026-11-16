// API-key authentication (04-services.md step 1): X-API-Key → sha256 →
// api_keys lookup (cached, 60 s TTL, invalidated on revoke in the same
// instance) → tenant_id. A tenant ID in the request body is never trusted.
//
// Revocation is effective within 60 s across instances (the customer-facing
// key policy statement); same-instance revocations are immediate via
// invalidate(). The cache deliberately keys on the *raw* key's digest so an
// attacker cannot probe cache entries.

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
  | { ok: true; tenantId: string; keyKind: IngestKeyKind }
  | { ok: false; status: 401 | 403; reason: AuthFailureReason };

/** Ingest-capable key kinds (D28); `dashboard` is never valid here. */
export type IngestKeyKind = "ingest" | "server";

interface CacheEntry {
  tenantId: string;
  keyKind: IngestKeyKind;
  expiresAt: number;
}

interface KeyRow extends QueryResultRow {
  tenant_id: string;
  kind: string;
  revoked: boolean;
}

export class ApiKeyAuth {
  private readonly ttlMs: number;
  private readonly now: () => number;
  private readonly query: <R extends QueryResultRow>(
    text: string,
    values?: unknown[],
  ) => Promise<QueryResult<R>>;
  private readonly cache = new Map<string, CacheEntry>();

  constructor(pool: Pool, options: AuthOptions = {}) {
    this.ttlMs = options.ttlMs ?? 60_000;
    this.now = options.now ?? (() => Date.now());
    this.query =
      options.query ??
      (<R extends QueryResultRow>(text: string, values?: unknown[]) =>
        pool.query<R>(text, values));
  }

  /** Drop a key's cache entry (used on same-instance revocation). */
  invalidate(rawKey: string): void {
    this.cache.delete(digestHex(rawKey));
  }

  async authenticate(rawKey: string | undefined): Promise<AuthResult> {
    if (!rawKey) return { ok: false, status: 401, reason: "missing_key" };
    const hex = digestHex(rawKey);

    const cached = this.cache.get(hex);
    if (cached && cached.expiresAt > this.now()) {
      return { ok: true, tenantId: cached.tenantId, keyKind: cached.keyKind };
    }

    const { rows } = await this.query<KeyRow>(
      `SELECT tenant_id, kind, revoked_at IS NOT NULL AS revoked
          FROM api_keys
         WHERE key_hash = $1`,
      [Buffer.from(hex, "hex")],
    );
    const row = rows[0];
    if (!row || (row.kind !== "ingest" && row.kind !== "server")) {
      // A dashboard key is simply not a valid ingest key; do not reveal which.
      return {
        ok: false,
        status: 401,
        reason: row ? "wrong_key_kind" : "invalid_key",
      };
    }
    if (row.revoked) return { ok: false, status: 403, reason: "key_revoked" };

    const keyKind = row.kind as IngestKeyKind;
    this.cache.set(hex, {
      tenantId: row.tenant_id,
      keyKind,
      expiresAt: this.now() + this.ttlMs,
    });
    return { ok: true, tenantId: row.tenant_id, keyKind };
  }

  /** Express middleware: sets req.tenantId or answers 401/403 itself. */
  middleware(): RequestHandler {
    return async (
      req: Request,
      res: Response,
      next: NextFunction,
    ): Promise<void> => {
      let result: AuthResult;
      try {
        result = await this.authenticate(req.get("X-API-Key"));
      } catch (err) {
        next(err);
        return;
      }
      if (!result.ok) {
        res.status(result.status).json({ error: result.reason });
        return;
      }
      req.tenantId = result.tenantId;
      req.keyKind = result.keyKind;
      next();
    };
  }
}

function digestHex(rawKey: string): string {
  return createHash("sha256").update(rawKey, "utf8").digest("hex");
}
