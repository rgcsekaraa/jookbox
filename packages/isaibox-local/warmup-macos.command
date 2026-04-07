#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

APP_PORT=6789
if [ -f .env ]; then
  set -a
  source ./.env
  set +a
fi

echo "Waiting for isaibox on http://127.0.0.1:${APP_PORT} ..."
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${APP_PORT}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/warmup" \
  -H "Content-Type: application/json" \
  -d '{"limit":24}'

echo ""
echo "Warmup complete."
read -r -p "Press Enter to close..."
