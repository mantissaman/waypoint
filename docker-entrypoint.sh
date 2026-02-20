#!/bin/sh
set -e

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-postgres}"
DB_USERNAME="${DB_USERNAME:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-}"
CONNECT_RETRIES="${CONNECT_RETRIES:-50}"
SSL_MODE="${SSL_MODE:-prefer}"
LOCATIONS="${LOCATIONS:-/waypoint/sql}"

# Build postgres:// connection URL
if [ -n "$DB_PASSWORD" ]; then
  DATABASE_URL="postgres://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
else
  DATABASE_URL="postgres://${DB_USERNAME}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
fi

# Allow full URL override via WAYPOINT_DATABASE_URL
DATABASE_URL="${WAYPOINT_DATABASE_URL:-$DATABASE_URL}"
export WAYPOINT_DATABASE_URL="$DATABASE_URL"

# If no arguments, default to migrate
COMMAND="${1:-migrate}"
shift 2>/dev/null || true

exec waypoint \
  --url "$DATABASE_URL" \
  --locations "$LOCATIONS" \
  --connect-retries "$CONNECT_RETRIES" \
  --ssl-mode "$SSL_MODE" \
  "$COMMAND" "$@"
