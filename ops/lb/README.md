# Sticky-routing drill (P4-5)

Consistent-hash frontend + roundrobin control, two real ingest processes.

```bash
pnpm test:lb                        # migrate a scratch DB, start 2 ingest + HAProxy, drill, clean up
node ops/lb/lb-drill.mjs --keep     # leave the stack up for inspection
```

Requires Docker (HAProxy) and the base database (`pnpm db:up`); not part of PR CI
([06-testing.md](../../docs/plan/06-testing.md)). Design decision and measured results:
[DEVIATIONS.md](../../docs/plan/DEVIATIONS.md) D40 and P4-5 evidence in
[08-execution.md](../../docs/plan/08-execution.md).

| Host port | Frontend                                                  |
| --------- | --------------------------------------------------------- |
| 18080     | `balance hdr(X-Analytics-Route)` + `hash-type consistent` |
| 18081     | `balance roundrobin` (control)                            |

8079/8081-style ports are avoided because local dashboards commonly hold 8080; the
compose project is `sa-lb` so it can run beside the base (`sa-p0`) and HA (`sa-ha`)
stacks.

The drill starts `services/ingest/dist/server.js` twice with different `INSTANCE_ID`
(echoed as `X-Ingest-Instance`), sends 32 visitors × 5 requests per frontend, and asserts:

- every visitor stays on one instance under consistent hashing (0 split),
- both instances receive traffic (otherwise the result is void),
- the roundrobin control splits sampled visitors across both (affinity is real, not luck),
- no request fails.

It also samples `pg_stat_activity` for lock waits during a 120-request hot-session burst
through each frontend. At drill scale the wait count is 0; the 2×-peak contention number is
P4-6's measurement, not this drill's. All rate-limit scopes are disabled for the run
(routing, not limits) and `PG_POOL_MAX=40` gives the burst enough concurrency to expose
contention.

The advisory `X-Analytics-Route` header is derived in both SDKs (envelope `anon_id`, else
first event's `anon_id`, else `session_id`); the drill sets it explicitly. Nothing in the
ingest API reads it — a missing header only costs locality.
