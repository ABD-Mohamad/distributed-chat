#!/bin/bash
set -e

PRIMARY_HOST="${PRIMARY_HOST:?PRIMARY_HOST required}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator}"

echo "Waiting for primary $PRIMARY_HOST to be ready..."
until pg_isready -h "$PRIMARY_HOST" -U "$REPLICATION_USER" 2>/dev/null; do
    sleep 2
done

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "Running pg_basebackup from $PRIMARY_HOST..."
    chmod 0700 "$PGDATA"
    su-exec postgres bash -c "PGPASSWORD='$REPLICATION_PASSWORD' pg_basebackup -h '$PRIMARY_HOST' -D '$PGDATA' -U '$REPLICATION_USER' -v -P --wal-method=stream"
    chmod 0700 "$PGDATA"
    touch "$PGDATA/standby.signal"
    echo "Replica setup complete."
fi

echo "primary_conninfo = 'host=$PRIMARY_HOST user=$REPLICATION_USER password=$REPLICATION_PASSWORD'" >> /etc/postgresql/postgresql.conf

exec su-exec postgres postgres -c config_file=/etc/postgresql/postgresql.conf
