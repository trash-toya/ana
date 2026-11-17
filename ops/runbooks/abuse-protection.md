# Runbook — abuse protection

Covers the P4-3 controls: token-bucket rate limits, the I10 event-name cardinality
monitor, and API-key rotation/revocation. Limits and their defaults are documented in
[`../../docs/plan/04-services.md`](../../docs/plan/04-services.md) §Abuse protection.

## 1. 429 storm (`rate_limited_total` rising)

The 429 body names the scope: `{"error":"rate_limited","scope":"ip|key|tenant|session"}`.

| Scope     | Most likely cause                                                       | First action                                                                                                                                                                                 |
| --------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ip`      | NAT/proxy egress collapsing many visitors onto one address, or a flood  | Confirm the LB forwards the client IP (`X-Forwarded-For` trust is NOT used — the peer address is the bucket id); raise `RATE_LIMIT_IP_*` or move the ingress.                                |
| `key`     | One producer hammering, or a client bug retrying a 5xx without backoff  | Identify the tenant from the key fingerprint; check ingest 5xx first — a real 5xx is the bug, not the limit. Raise `RATE_LIMIT_KEY_*` only for a known-good producer.                        |
| `tenant`  | Several keys of one tenant all busy                                     | Same as `key`, at tenant level.                                                                                                                                                              |
| `session` | A runaway session (bug loop, replay storm) or a legitimate huge backlog | A healthy client flushes ≤ 500 events; the default burst is 2000/s. If the client recovered from a long offline period, a 429 here means a loop — inspect its queue, do not raise the limit. |

Remember D38: `key`/`tenant` limits are **per instance**, so N instances allow N× the
configured rate. Check the per-instance metric before concluding that a tenant is over
its budget globally.

`rate_limit_buckets` (gauge) shows live bucket count; a number that keeps climbing
without traffic means ids are churning (session ids or peer addresses) — expected under
abuse, and pruned automatically once full and idle for 10 minutes.

## 2. Event-name cardinality alert (I10)

```sql
-- db/sql/monitors/tenant_event_cardinality.sql
SELECT tenant_id, count(DISTINCT name)::int AS event_names
  FROM events
 WHERE received_at >= now() - interval '1 hour'
 GROUP BY tenant_id
HAVING count(DISTINCT name) > 100
 ORDER BY event_names DESC, tenant_id;
```

1. Identify the tenant and the top names for that hour:
   ```sql
   SELECT name, count(*) AS events
     FROM events
    WHERE tenant_id = $1 AND received_at >= now() - interval '1 hour'
    GROUP BY name ORDER BY events DESC LIMIT 50;
   ```
2. If the names look machine-generated (ids, hashes, timestamps in the name), it is a
   client bug: contact the tenant, they should send a fixed name plus `props`. Do **not**
   rename rows retroactively — rollups stay as recorded.
3. If the tenant is unrecognised or the growth continues, treat it as abuse: revoke its
   keys (below). `session_guard` still caps each session at 5,000 events, so storage is
   bounded per session but not per tenant; alert early.
4. Threshold and window are constants in the SQL file (100 names / 1 hour). Change them
   only with a matching test in `db/test/monitors/cardinality.test.mjs`.

## 3. Rotate a key (zero downtime)

Policy: at most **two** active ingest-family keys (`ingest` + `server`) and **one**
dashboard key per tenant. Two ingest keys exist precisely to allow rotation.

```bash
# 1. create the replacement (raw key printed once, fingerprint on stderr)
node db/scripts/api-keys.mjs create --tenant <uuid> --kind ingest --origin web

# 2. deploy the new key to the producer and confirm traffic with it

# 3. revoke the old one by fingerprint (12 hex chars, shown by `list`)
node db/scripts/api-keys.mjs list --tenant <uuid>
node db/scripts/api-keys.mjs revoke --tenant <uuid> --fingerprint <12-hex>
```

- Revocation is effective within the auth cache TTL (60 s) across instances; same-instance
  revocation is immediate.
- `list` never shows raw keys — only fingerprints, kind, origin hint, timestamps, state.
- Revoking is idempotent in effect but a second revoke reports "no active key"; that is
  not an outage.
- If a key leaks: revoke first, then create the replacement (the policy permits dropping
  to one active key).

`DATABASE_URL` (or `SA_DB`, like the migration runner) selects the database. The script
exits `2` on a policy violation (e.g. trying to mint a third ingest key), `1` on a
usage/DB error — a CI-friendly distinction.
