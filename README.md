# isaibox — DuckDB + Actions

Daily incremental scraper and streaming app backend.  
Stores everything in a single **DuckDB** file. Scheduled and published via **GitHub Actions**.

---

## Project structure

```
isaibox/
├── db.py                 ← DuckDB schema + read/write helpers
├── scraper_core.py       ← HTTP fetch + HTML parsing
├── query.py              ← CLI query tool
├── run_standalone.py     ← Scraper execution script
│
├── scripts/
│   └── publish_library.py ← Safe DuckDB publishing and merge logic
│
├── .github/workflows/
│   └── refresh-library.yml ← Automated library refresh workflow
│
├── data/
│   └── <library>.duckdb  ← single DuckDB file (downloaded or created)
│
├── exports/
│   ├── songs_YYYYMMDD.parquet   ← daily Parquet snapshot
│   └── songs_latest.csv         ← latest CSV
│
└── app.py                ← FastAPI backend
```

---

## Setup (one time)

```bash
# 1. Clone / copy this folder, then create a virtual environment:
python3 -m venv venv
source venv/bin/activate

# 2. Install core dependencies:
pip install duckdb requests beautifulsoup4 lxml tenacity fastapi uvicorn
```

## isaibox auth setup

`isaibox` now reads local environment variables from `.env`.

Required for Google login:

```bash
GOOGLE_CLIENT_ID=your-google-client-id.apps.googleusercontent.com
ISAIBOX_SESSION_SECRET=any-long-random-secret
ISAIBOX_ADMIN_EMAILS=admin@example.com
```

`.env.example` shows the expected keys.
`.env` is already created locally with a generated session secret, but you still need to fill in a real `GOOGLE_CLIENT_ID` for Gmail login to work.

`ISAIBOX_ADMIN_EMAILS` is a comma-separated allowlist for admin users.

Optional Spotify integration:

```bash
SPOTIFY_CLIENT_ID=your-spotify-client-id
SPOTIFY_CLIENT_SECRET=your-spotify-client-secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8000
```

`SPOTIFY_REDIRECT_URI` must match a redirect URI registered in your Spotify app settings.

## isaibox backend

The backend now runs as a packaged FastAPI app:

```bash
source venv/bin/activate
python app.py
```

Production-style direct run:

```bash
source venv/bin/activate
uvicorn backend.main:create_app --factory --host 127.0.0.1 --port 8000
```

Relevant env vars:

```bash
ISAIBOX_HOST=127.0.0.1
ISAIBOX_PORT=8000
ISAIBOX_LOG_LEVEL=info
GEMINI_API_KEYS=comma,separated,keys
GEMINI_MODEL=gemini-2.5-flash
```

Radio stations and AI playlists are generated only from songs already stored in DuckDB.
The radio UI keeps the station loop internal and does not render the station queue as a visible list.

## Run local app (Docker)

If you just want to run the local playback application without installing Python or any dependencies, you can use the pre-configured local Docker package:

```bash
cd packages/isaibox-local
docker compose up -d --build
```

The application will be available at **http://127.0.0.1:6789/**. This runs a trimmed-down version focused purely on playback and local packaged database syncing (disabling the admin UI, scraping logic, and Google auth).
For more info or one-click run scripts, check the [packages/isaibox-local/README.md](packages/isaibox-local/README.md).

## Cache strategy

`isaibox` now supports layered backend cache:

1. local disk cache in `.cache/audio`
2. optional shared R2 cache
3. upstream source fallback

R2 is optional. If configured, the app will:
- serve local cache first
- restore missing local files from R2
- upload newly cached local audio back to R2 in the background

R2 environment variables:

```bash
R2_ACCOUNT_ID=
R2_BUCKET_NAME=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_ENDPOINT_URL=
```

`R2_ENDPOINT_URL` can be left empty if you want the app to derive it from `R2_ACCOUNT_ID`.

Note:
- local disk remains the fastest hot cache
- R2 is the shared backing cache for production
- if `boto3` is not installed, R2 is skipped and the app continues with local cache only

## Automated GitHub DuckDB refresh

The repository now uses GitHub Actions directly to refresh the GitHub-hosted DuckDB snapshot used by local package sync.

This is the right execution model because a full scan can take 2-3 hours, which fits GitHub Actions much better than a Cloudflare Worker runtime.

Pieces added for this flow:
- GitHub Actions workflow: `.github/workflows/refresh-library.yml`
- Publish helper: `scripts/publish_library.py`

Workflow behavior:
- runs automatically every 12 hours
- can still be started manually with `workflow_dispatch` (optional full scan override)
- runs the standalone scraper (`run_standalone.py`)
- safely merges new data into the existing published DuckDB via `scripts/publish_library.py` (using `INSERT ... ON CONFLICT DO UPDATE` to preserve historical rows)
- backs up the previous database version as a workflow artifact
- rewrites `packages/isaibox-local/app/data/library-manifest.json`
- commits and pushes only when the database or manifest actually changed

---

## How it works (Scraper)

```
discover_new_albums          Walk listing pages 2..N
                             Stop early when a page is fully known (incremental)
        │
        ▼ (×N batches of 20)
scrape_batch_0               Each batch scrapes 20 album detail pages in sequence
scrape_batch_1               All batches run in parallel
...
        │
        ▼
write_to_duckdb              Single writer — upserts albums + songs
        │
        ▼
export_parquet_and_csv       Writes exports/songs_YYYYMMDD.parquet + songs_latest.csv
        │
        ▼
log_run_summary              Prints execution stats
```

**Incremental logic**: The site lists newest albums first.  Discovery stops
as soon as it encounters a full listing page where every album is already in
DuckDB — so daily runs typically only walk 1-3 pages.

---

## Query the database

```bash
# Activate venv first
source venv/bin/activate

python3 query.py --stats
python3 query.py --search "anirudh"
python3 query.py --year 2025
python3 query.py --director "G. V. Prakash Kumar"
python3 query.py --singer "Sid Sriram"
python3 query.py --failed                          # albums that failed
python3 query.py --runs                            # scrape run history

# Raw SQL
python3 query.py --sql "SELECT * FROM songs WHERE year='2024' LIMIT 20"

# Export filtered results
python3 query.py --year 2025 --export-csv 2025.csv
python3 query.py --export-parquet all_songs.parquet

# Or query DuckDB directly (lightning fast)
duckdb data/<library>.duckdb
  > SELECT music_director, COUNT(*) FROM songs GROUP BY 1 ORDER BY 2 DESC;
  > COPY songs TO 'songs.parquet';
```

## Scraper smoke test

```bash
source venv/bin/activate

python3 smoke_scraper.py
python3 smoke_scraper.py --page 2
python3 smoke_scraper.py --album-url "https://source-site.example/example-songs"
python3 smoke_scraper.py --json
```

---

## DuckDB schema

### `songs`
| Column | Type | Notes |
|--------|------|-------|
| song_id | VARCHAR PK | md5(album_url + track_number) |
| album_url | VARCHAR | |
| movie_name | VARCHAR | |
| music_director | VARCHAR | |
| director | VARCHAR | |
| year | VARCHAR | |
| track_number | INTEGER | |
| track_name | VARCHAR | |
| singers | VARCHAR | |
| url_128kbps | VARCHAR | direct download link |
| url_320kbps | VARCHAR | direct download link |
| first_seen_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | |

### `albums`
| Column | Type | Notes |
|--------|------|-------|
| album_url | VARCHAR PK | |
| movie_name | VARCHAR | |
| starring | VARCHAR | comma-separated actors |
| music_director | VARCHAR | |
| director | VARCHAR | |
| lyricists | VARCHAR | |
| year | VARCHAR | |
| language | VARCHAR | default: Tamil |
| track_count | INTEGER | |
| scrape_ok | BOOLEAN | false = failed, will retry |
| first_seen_at | TIMESTAMPTZ | |
| updated_at | TIMESTAMPTZ | |

### `scrape_runs`
One row per scraper run — albums added, songs total, status.

### User & App State
- `playlists` and `playlist_songs`
- `users` and `user_sessions`
- `favorite_songs`
- `user_preferences`
