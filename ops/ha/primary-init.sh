#!/bin/bash
# Runs once from /docker-entrypoint-initdb.d on an empty PGDATA (P4-4 drill).
# The image's generated pg_hba.conf allows replication only from localhost;
# standby base backups arrive from the compose network.
set -e
echo "host replication all all trust" >>"$PGDATA/pg_hba.conf"
