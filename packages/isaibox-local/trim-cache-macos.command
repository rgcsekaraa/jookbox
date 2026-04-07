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

curl -fsS -X POST "http://127.0.0.1:${APP_PORT}/api/cache/trim" \
  -H "Content-Type: application/json" \
  -d '{"force":true}'

echo ""
echo "Cache trim complete."
read -r -p "Press Enter to close..."
