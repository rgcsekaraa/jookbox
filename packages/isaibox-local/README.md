# isaibox local package

This folder is a packaged local-only copy of `isaibox`.

## What it does

- runs the app with the bundled DuckDB in `app/data/masstamilan.duckdb`
- disables login, Google auth, Spotify import, and admin UI
- uses a built-in local profile so favorites and playlists still work without sign-in
- runs separate frontend and backend containers
- restarts automatically if the container crashes
- warms an initial set of songs after launch so first playback is faster
- lets you choose the host port with `.env`
- lets you cap the local audio cache size
- checks GitHub for a newer shared DuckDB in the background and swaps it in safely

## Prerequisite

Install Docker Desktop:

- macOS: Docker Desktop for Mac
- Windows: Docker Desktop for Windows

No Python, Node, DuckDB, or app dependencies need to be installed on the target machine.

## One-click launch

- macOS: double-click `start-macos.command`
- Windows: double-click `start-windows.bat`

The app opens at `http://127.0.0.1:6789/` by default.
The launcher waits for health, warms the cache, and then opens the browser.

Internal port layout:

- frontend container: `5173`
- backend container: `6060`
- host/browser access: `6789`

## Warm cache only

- macOS: double-click `warmup-macos.command`
- Windows: double-click `warmup-windows.bat`

## Trim cache now

- macOS: double-click `trim-cache-macos.command`
- Windows: double-click `trim-cache-windows.bat`

## Change the port

Edit `.env` and change:

```env
APP_PORT=6789
ISAIBOX_CACHE_LIMIT_GB=20
ISAIBOX_DB_SYNC_ENABLED=1
ISAIBOX_DB_SYNC_MANIFEST_URL=https://raw.githubusercontent.com/rgcsekaraa/isaibox/main/packages/isaibox-local/app/data/library-manifest.json
ISAIBOX_DB_SYNC_INTERVAL_SECONDS=1800
```

For example, set `APP_PORT=9090` and restart the package.
Set `ISAIBOX_CACHE_LIMIT_GB=0` if you want no automatic cache limit.
Set `ISAIBOX_DB_SYNC_ENABLED=0` if you want to disable background library updates.

## Stop the package

- macOS: double-click `stop-macos.command`
- Windows: double-click `stop-windows.bat`

## Manual commands

```bash
docker compose up -d --build
docker compose logs -f
docker compose down
```

## Notes

- local mode hides account/login actions and only exposes the local library experience
- persistent data stays in `app/data`, `app/exports`, and `app/.cache`
- cached audio is stored in `app/.cache/audio`
- the app trims oldest cached songs automatically when the cache grows past `ISAIBOX_CACHE_LIMIT_GB`
- the app keeps using the current DuckDB while it checks GitHub in the background, then atomically swaps to the new DB after checksum validation
- if background library sync fails, the UI shows an error and links users to the GitHub issues page
- restart policy is `unless-stopped`
