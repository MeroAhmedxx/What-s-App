#!/usr/bin/env bash
set -euo pipefail

echo "[startup] generating Prisma client"
npx prisma generate

echo "[startup] waiting for database connection"
ATTEMPTS="${DB_CONNECT_ATTEMPTS:-20}"
SLEEP_SECS="${DB_CONNECT_SLEEP_SECS:-3}"
count=1
until npx prisma db execute --stdin --schema prisma/schema.prisma >/dev/null 2>&1 <<'SQL'
SELECT 1;
SQL
do
  if [ "$count" -ge "$ATTEMPTS" ]; then
    echo "[startup] database is still unreachable after ${ATTEMPTS} attempts"
    exit 1
  fi
  echo "[startup] database not reachable yet (attempt ${count}/${ATTEMPTS}), retrying in ${SLEEP_SECS}s"
  count=$((count + 1))
  sleep "$SLEEP_SECS"
done

echo "[startup] applying Prisma migrations"
npx prisma migrate deploy

echo "[startup] starting Nest application"
exec node dist/main.js
