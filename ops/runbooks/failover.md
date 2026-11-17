# Runbook — failover (primary loss)

Scope: the synchronous standby topology in [07-operations.md](../../docs/plan/07-operations.md).
Production uses Patroni/etcd for election and fencing ([DEVIATIONS.md](../../docs/plan/DEVIATIONS.md)
D39); this runbook describes the same sequence so an on-call engineer can reason about it,
and `ops/ha/` implements it as an executable drill (`pnpm test:ha`).

RPO target: **0** for acknowledged batches (`synchronous_commit = on`, one sync standby).
RTO target: **2 minutes** to a writable endpoint.

## Guarantees that make this safe

- A batch is acknowledged only after `ingest_batch()` commits; the commit waits for the
  synchronous standby to flush WAL. A standby that has the commit therefore has every
  acknowledged write.
- **Fencing is mandatory and comes first.** The old primary must be unable to accept
  writes before promotion. Killing/stopping it is fencing; if it is only unreachable
  (network partition), use the orchestrator's STONITH/fencing primitive — never promote
  while the old primary might still serve writes.
- Promote the standby that was `sync` (the orchestrator tracks it; in the drill there is
  exactly one).

## Sequence

1. **Confirm loss** — primary health checks fail, PgBouncer reports "server closed the
   connection unexpectedly", ingest returns 5xx/503 with `Retry-After`. Do not restart
   the old primary.
2. **Fence** — stop/kill the old primary with power-off semantics
   (`docker kill <primary>` in the drill; STONITH/watchdog in production). Verify it is
   gone, not merely unresponsive.
3. **Promote** — `SELECT pg_promote(true, 30)` on the standby until
   `pg_is_in_recovery()` is false.
4. **Clear the sync list** — the promoted node inherits
   `synchronous_standby_names = '*'`; with no standby left, commits would wait and fail:
   ```sql
   ALTER SYSTEM RESET synchronous_standby_names;
   SELECT pg_reload_conf();
   ```
   The orchestrator does this on promotion; do it manually if promoting by hand.
5. **Move the endpoint** — repoint PgBouncer's `DB_HOST` (or the DNS name/VIP it follows)
   at the new primary and recycle the pooler if it caches the address
   (`dns_max_ttl`/re-resolution). Ingest reconnects; queued SDK batches retry.
6. **Verify writes** — through PgBouncer:
   ```sql
   SELECT NOT pg_is_in_recovery() AS writable, now();
   ```
   A canary `ingest_batch()` call must return `stored`/`dups`, not a timeout.
7. **Verify jobs** — the four scheduled jobs must be present, and a job must execute:
   ```sql
   SELECT job_id, proc_name, schedule_interval, scheduled
     FROM timescaledb_information.jobs
    WHERE proc_name IN ('archive_sessions','cleanup_idempotency',
                        'reconcile_session_stats','rollup_daily');
   CALL archive_sessions(NULL, NULL);
   ```
   Check `sessions_active` backlog: the oldest row must fall below 100 minutes on the
   next archive run; if not, the scheduler did not resume.
8. **Re-add a standby** — rebuild from the new primary with `pg_basebackup` (the old
   primary's data may be ahead or behind; do not reuse it without `pg_rewind`), then set
   `synchronous_standby_names` again and confirm `sync_state = sync` before declaring
   the incident closed.

## Drill (T11)

```bash
pnpm test:ha                 # full setup + drill + teardown (needs Docker)
node ops/ha/t11-drill.mjs --keep   # leave the stack up for inspection
```

The drill: migrations with `0014`, streaming standby, synchronous replication enabled,
PgBouncer in front, 8 writers, then SIGKILL the primary → `pg_promote()` → clear the sync
list → recreate PgBouncer against the standby. It fails if any acknowledged event is
missing or the endpoint takes longer than 120 s. Measured locally on the pinned image
(2026-11-17): **RPO 0** (2,184–2,275 acknowledged events, 0 missing), **RTO 21–43 s**,
4/4 jobs present and `archive_sessions` executed on the promoted node. Results are
recorded in [P4-4 evidence](../../docs/plan/08-execution.md).

## Rollback / old primary

- Never bring the old primary up as a primary. Either rebuild it with
  `pg_basebackup` from the new primary, or `pg_rewind` it against the new primary and
  start it as a standby.
- If the old primary has commits the new one lacks (async setup only — not this one),
  those batches were never acknowledged; do not merge them.

## Known drill limitations (D39)

- The drill uses container stop/recreate as the virtual endpoint, not Patroni's DCS.
  Election correctness (multiple candidates, network partitions, watchdog) is Patroni's
  job in production and is not proven by this drill.
- A single standby means any failover leaves the cluster without redundancy until step 8.
