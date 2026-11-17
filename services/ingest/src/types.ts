// Express request augmentation shared by the ingest service.
// No runtime import: "express-serve-static-core" is a types-only module
// resolved through @types/express; the augmentation merges at compile time.

declare module "express-serve-static-core" {
  interface Request {
    /** Set by ApiKeyAuth.middleware(); never taken from the request body. */
    tenantId?: string;
    /** Key kind decides which `source` values the producer may claim (D28). */
    keyKind?: "ingest" | "server";
    /** sha256 of the presented key, in hex: the rate-limit bucket id. The raw
     *  key is never stored or logged. */
    keyDigest?: string;
  }
}
