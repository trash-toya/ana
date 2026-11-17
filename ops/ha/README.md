# HA drill stack (P4-4)

Executable T11 drill: primary + synchronous streaming standby + PgBouncer, then
fence → promote → repoint → verify acknowledged events and jobs.

```bash
pnpm test:ha                        # reset, drill, teardown
node ops/ha/t11-drill.mjs --keep    # leave the stack up after the drill
```

Requires Docker; not part of PR CI ([06-testing.md](../../docs/plan/06-testing.md)).
The runbook (fencing-first sequence, sync-list reset, re-adding a standby) is
[`ops/runbooks/failover.md`](../runbooks/failover.md); the design decision is
[DEVIATIONS.md](../../docs/plan/DEVIATIONS.md) D39.

| Host port | Service                                                       |
| --------- | ------------------------------------------------------------- |
| 55432     | primary (`wal_level=replica`, sync enabled by the drill)      |
| 55433     | standby (base backup with `-R`, then `pg_promote()` on drill) |
| 56432     | PgBouncer, the virtual endpoint (recreated with `DB_HOST`)    |

The compose project is `sa-ha`, so it can run beside the base `sa-p0` stack. Ports and
volumes are separate; `pnpm test:ha` always starts from empty volumes so the base backup
and migration state are reproducible.

Files:

- `docker-compose.yml` — the stack. The primary allows replication from the compose
  network via `primary-init.sh`; sync replication is enabled only once the standby is
  streaming, otherwise the first migration commit would wait forever.
- `standby-entrypoint.sh` — `pg_basebackup -R` on an empty PGDATA, then the image's normal
  entrypoint (restarts reuse the promoted data).
- `t11-drill.mjs` — orchestrator and load driver; prints the JSON result and exits
  non-zero on RPO > 0 or RTO > 120 s.
