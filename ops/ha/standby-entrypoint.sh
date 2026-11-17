#!/bin/bash
# Standby bootstrap for the P4-4 drill: if PGDATA is empty, take a fast base
# backup from the primary with -R (writes primary_conninfo + standby.signal),
# then hand over to the image's normal entrypoint. Re-running with a populated
# PGDATA skips the copy, so restarts keep the promoted data.
set -euo pipefail

: "${PGDATA:=/home/postgres/pgdata/data}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  chmod 700 "$PGDATA"
  echo "[ha] waiting for primary..."
  until pg_isready -h primary -p 5432 -U postgres -d postgres >/dev/null 2>&1; do
    sleep 1
  done
  echo "[ha] base backup from primary"
  pg_basebackup \
    -h primary -p 5432 -U postgres \
    -D "$PGDATA" -Fp -Xs -R -c fast \
    --no-password
fi

exec /docker-entrypoint.sh "$@"
