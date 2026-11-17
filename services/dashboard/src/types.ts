// Express request augmentation shared by the dashboard service. Mirrors the
// ingest service's types.ts; the dashboard only accepts `dashboard` keys
// (04-services.md: read-only service, keys are scoped per service kind).
// No runtime import: "express-serve-static-core" is a types-only module
// resolved through @types/express; the augmentation merges at compile time.

declare module "express-serve-static-core" {
  interface Request {
    /** Set by DashboardKeyAuth.middleware(); never taken from query/body. */
    tenantId?: string;
  }
}
