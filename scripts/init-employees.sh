#!/bin/bash
set -e

echo "Restoring employees database..."
pg_restore \
  --no-owner \
  --no-privileges \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  /docker-entrypoint-initdb.d/employees.dump
