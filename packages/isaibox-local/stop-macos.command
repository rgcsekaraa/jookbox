#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker Desktop is required to stop this package."
  read -r -p "Press Enter to close..."
  exit 1
fi

docker compose down
echo "isaibox local stopped."
read -r -p "Press Enter to close..."
