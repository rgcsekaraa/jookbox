#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker Desktop is required. Install Docker Desktop for Mac, then run this launcher again."
  read -r -p "Press Enter to close..."
  exit 1
fi

APP_PORT=6789
ISAIBOX_CACHE_LIMIT_GB=20
if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

docker compose up -d --build
echo "Waiting for isaibox on http://127.0.0.1:${APP_PORT} ..."
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${APP_PORT}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/warmup" \
  -H "Content-Type: application/json" \
  -d '{"limit":24}' >/dev/null
open "http://127.0.0.1:${APP_PORT}/"
echo "isaibox local is running on http://127.0.0.1:${APP_PORT}/"
echo "Cache limit: ${ISAIBOX_CACHE_LIMIT_GB} GB"
read -r -p "Press Enter to close..."
