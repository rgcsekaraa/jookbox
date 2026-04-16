#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DB_PATH="${1:-${SCRIPT_DIR}/app/data/masstamilan.duckdb}"
MANIFEST_PATH="${SCRIPT_DIR}/app/data/library-manifest.json"
RELEASE_TAG="${ISAIBOX_LIBRARY_RELEASE_TAG:-local-library}"
REPO="${ISAIBOX_GITHUB_REPO:-rgcsekaraa/isaibox}"
ASSET_NAME="masstamilan.duckdb"
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${RELEASE_TAG}/${ASSET_NAME}"

if [ ! -f "${DB_PATH}" ]; then
  echo "Missing DuckDB file: ${DB_PATH}" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required: https://cli.github.com/" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

cd "${REPO_ROOT}"

python3 - "${DB_PATH}" "${MANIFEST_PATH}" "${DOWNLOAD_URL}" <<'PY'
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

db_path = Path(sys.argv[1])
manifest_path = Path(sys.argv[2])
download_url = sys.argv[3]

hasher = hashlib.sha256()
with db_path.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        hasher.update(chunk)

conn = duckdb.connect(str(db_path), read_only=True)
try:
    songs = conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]
    albums = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
    latest = conn.execute("SELECT MAX(updated_at) FROM songs WHERE updated_at IS NOT NULL").fetchone()[0]
finally:
    conn.close()

updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
manifest = {
    "albums": int(albums),
    "download_url": download_url,
    "latest_song_updated_at": latest.isoformat() if latest else "",
    "sha256": hasher.hexdigest(),
    "size": db_path.stat().st_size,
    "songs": int(songs),
    "updated_at": updated_at,
    "version": updated_at,
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2, sort_keys=True))
PY

if gh release view "${RELEASE_TAG}" --repo "${REPO}" >/dev/null 2>&1; then
  gh release upload "${RELEASE_TAG}" "${DB_PATH}#${ASSET_NAME}" --repo "${REPO}" --clobber
else
  gh release create "${RELEASE_TAG}" "${DB_PATH}#${ASSET_NAME}" --repo "${REPO}" \
    --title "isaibox local library" \
    --notes "Release-backed DuckDB for isaibox local app database sync."
fi

echo "Updated ${MANIFEST_PATH}"
echo "Uploaded ${DOWNLOAD_URL}"
