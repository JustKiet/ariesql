#!/bin/bash
# ============================================================
# migrate-pg-to-mssql.sh  (automated – runs inside a container)
#
# One-shot migration: exports data from PostgreSQL as CSV,
# then bulk-imports it into SQL Server.
#
# This script is designed to run inside the mssql-migrator
# Docker service where both PG and MSSQL are reachable via
# their Docker network hostnames.
# ============================================================
set -euo pipefail

# ── Configuration (set via environment / docker-compose) ──────
PG_HOST="${PG_HOST:-postgres}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${POSTGRES_USER:-postgres}"
PG_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
PG_DB="${POSTGRES_DB:-employees}"
PG_SCHEMA="employees"

MSSQL_HOST="${MSSQL_HOST:-mssql}"
MSSQL_PORT="${MSSQL_PORT:-1433}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-YourStr0ngP@ssw0rd!}"
MSSQL_DB="employees"

export PGPASSWORD="$PG_PASSWORD"

STAGING_DIR="/tmp/csv-staging"
mkdir -p "$STAGING_DIR"

TABLES=(
    "employee"
    "department"
    "department_employee"
    "department_manager"
    "salary"
    "title"
)

# ── Helpers ───────────────────────────────────────────────────
log() {
    echo "[migrator $(date '+%H:%M:%S')] $*"
}

run_sqlcmd() {
    /opt/mssql-tools18/bin/sqlcmd \
        -S "$MSSQL_HOST,$MSSQL_PORT" -U sa -P "$MSSQL_SA_PASSWORD" \
        -No -d "$MSSQL_DB" "$@"
}

# ── Wait for PostgreSQL ──────────────────────────────────────
log "Waiting for PostgreSQL ($PG_HOST:$PG_PORT)..."
retries=60
while [ $retries -gt 0 ]; do
    if psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -c "SELECT 1" &>/dev/null; then
        break
    fi
    retries=$((retries - 1))
    sleep 2
done
if [ $retries -eq 0 ]; then
    log "ERROR: PostgreSQL did not become ready."; exit 1
fi
log "PostgreSQL is ready."

# ── Wait for SQL Server (DB + schema must be ready) ──────────
log "Waiting for SQL Server ($MSSQL_HOST:$MSSQL_PORT) and employees schema..."
retries=90
while [ $retries -gt 0 ]; do
    if /opt/mssql-tools18/bin/sqlcmd \
        -S "$MSSQL_HOST,$MSSQL_PORT" -U sa -P "$MSSQL_SA_PASSWORD" \
        -No -d "$MSSQL_DB" \
        -Q "SELECT 1 FROM employees.employee WHERE 1=0" &>/dev/null; then
        break
    fi
    retries=$((retries - 1))
    sleep 2
done
if [ $retries -eq 0 ]; then
    log "ERROR: SQL Server employees schema did not become ready."; exit 1
fi
log "SQL Server is ready."

# ── Check if migration already ran ───────────────────────────
existing=$(run_sqlcmd -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT COUNT(*) FROM employees.employee" 2>/dev/null | tr -d '[:space:]' || echo "0")
if [ "${existing:-0}" -gt "0" ] 2>/dev/null; then
    log "Data already present ($existing employees). Skipping migration."
    exit 0
fi

# ── Step 1: Export from PostgreSQL ────────────────────────────
log "Exporting data from PostgreSQL..."

export_table() {
    local table=$1
    log "  Exporting $PG_SCHEMA.$table ..."

    if [ "$table" = "employee" ]; then
        psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c \
            "\COPY (SELECT id, birth_date, first_name, last_name, gender::text, hire_date FROM $PG_SCHEMA.$table ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER false, NULL '')" \
            > "$STAGING_DIR/${table}.csv"
    else
        psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -c \
            "\COPY (SELECT * FROM $PG_SCHEMA.$table) TO STDOUT WITH (FORMAT csv, HEADER false, NULL '')" \
            > "$STAGING_DIR/${table}.csv"
    fi

    local rows
    rows=$(wc -l < "$STAGING_DIR/${table}.csv")
    log "  Exported $rows rows for $table"
}

for table in "${TABLES[@]}"; do
    export_table "$table"
done

# ── Step 2: Copy CSVs into the MSSQL container via shared vol
# The csv-staging volume is mounted at /csv-staging in both the
# migrator and the mssql containers.
log "Copying CSV files to shared volume..."
cp "$STAGING_DIR"/*.csv /csv-staging/
chmod 644 /csv-staging/*.csv

# ── Step 3: Import into SQL Server ───────────────────────────
log "Importing data into SQL Server..."

import_employee() {
    log "  Importing employee (with IDENTITY_INSERT)..."
    run_sqlcmd -Q "
SET IDENTITY_INSERT employees.employee ON;

BULK INSERT employees.employee
FROM '/csv-staging/employee.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    KEEPIDENTITY
);

SET IDENTITY_INSERT employees.employee OFF;

DECLARE @maxId BIGINT;
SELECT @maxId = MAX(id) FROM employees.employee;
DBCC CHECKIDENT ('employees.employee', RESEED, @maxId);
"
    local cnt
    cnt=$(run_sqlcmd -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM employees.employee" | tr -d '[:space:]')
    log "  Imported $cnt rows into employee"
}

import_table() {
    local table=$1
    log "  Importing $table..."
    run_sqlcmd -Q "
BULK INSERT employees.${table}
FROM '/csv-staging/${table}.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
"
    local cnt
    cnt=$(run_sqlcmd -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM employees.${table}" | tr -d '[:space:]')
    log "  Imported $cnt rows into $table"
}

import_employee
import_table "department"
import_table "department_employee"
import_table "department_manager"
import_table "salary"
import_table "title"

# ── Step 4: Verify ───────────────────────────────────────────
log ""
log "═══════════════════════════════════════════════════"
log "  Migration complete! Verifying row counts..."
log "═══════════════════════════════════════════════════"
log ""

all_ok=true
for table in "${TABLES[@]}"; do
    pg_count=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -t -c \
        "SELECT COUNT(*) FROM $PG_SCHEMA.$table" | tr -d '[:space:]')
    mssql_count=$(run_sqlcmd -h -1 -W -Q \
        "SET NOCOUNT ON; SELECT COUNT(*) FROM employees.$table" | tr -d '[:space:]')
    if [ "$pg_count" = "$mssql_count" ]; then
        status="✓"
    else
        status="✗ MISMATCH"
        all_ok=false
    fi
    printf "  %s %-25s  PG: %10s  MSSQL: %10s\n" "$status" "$table" "$pg_count" "$mssql_count"
done

log ""
if [ "$all_ok" = true ]; then
    log "All row counts match. Migration finished successfully."
else
    log "WARNING: Some row counts did not match!"
    exit 1
fi
