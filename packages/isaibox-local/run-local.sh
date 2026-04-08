#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

APP_PORT=6789
ISAIBOX_CACHE_LIMIT_GB=20
if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker Desktop or Docker Engine is required."
  exit 1
fi

mkdir -p app/data app/exports app/.cache/audio
mkdir -p ../../data

if [ ! -f ../../data/masstamilan.duckdb ]; then
  echo "Missing shared database: ../../data/masstamilan.duckdb"
  exit 1
fi

if [ ! -f app/dist/index.html ]; then
  echo "Missing packaged frontend build: app/dist/index.html"
  exit 1
fi

docker compose up -d --build

echo "Waiting for frontend on http://127.0.0.1:${APP_PORT} ..."
READY=0
for _ in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${APP_PORT}/" >/dev/null 2>&1 && curl -fsS "http://127.0.0.1:${APP_PORT}/api/health" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done

if [ "${READY}" -ne 1 ]; then
  echo "isaibox did not become ready in time."
  exit 1
fi

curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/warmup" \
  -H "Content-Type: application/json" \
  -d '{"limit":24}' >/dev/null

echo "isaibox local is running on http://127.0.0.1:${APP_PORT}/"
echo "Frontend container port: 5173"
echo "Backend container port: 6060"
echo "Cache limit: ${ISAIBOX_CACHE_LIMIT_GB} GB"
