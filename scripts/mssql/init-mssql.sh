#!/bin/bash
# ============================================================
# init-mssql.sh
#
# Custom entrypoint for the SQL Server container.
# Starts SQL Server in the background, waits for it, creates
# the employees database and schema, then keeps the process
# alive by waiting on the SQL Server PID.
# ============================================================
set -e

log() {
    echo "[mssql-init $(date '+%H:%M:%S')] $*"
}

# ── Start SQL Server in the background ────────────────────────
log "Starting SQL Server..."
/opt/mssql/bin/sqlservr &
MSSQL_PID=$!

# ── Wait for SQL Server to be ready ──────────────────────────
log "Waiting for SQL Server to accept connections..."
retries=60
while [ $retries -gt 0 ]; do
    if /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
        -No -Q "SELECT 1" &>/dev/null; then
        break
    fi
    retries=$((retries - 1))
    sleep 1
done

if [ $retries -eq 0 ]; then
    log "ERROR: SQL Server failed to start."
    exit 1
fi
log "SQL Server is ready."

# ── Create the database ──────────────────────────────────────
log "Creating database 'employees' if it doesn't exist..."
/opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No \
    -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'employees') CREATE DATABASE [employees];"

# ── Create the schema and tables ─────────────────────────────
log "Creating schema and tables..."
/opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -No \
    -d employees \
    -i /scripts/create-employees-schema.sql

log "Database and schema initialisation complete."

# ── Keep the container alive by waiting on SQL Server ────────
wait $MSSQL_PID
