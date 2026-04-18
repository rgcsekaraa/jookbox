"""
db.py — DuckDB storage layer for the isaibox scraper
----------------------------------------------------
Single DuckDB file under data/

Tables
  albums  — one row per album page (dedup by url)
  songs   — one row per track (dedup by album_url + track_number)

DuckDB advantages over SQLite here:
  • Columnar storage → GROUP BY music_director/year is near-instant
  • Native Parquet export  (COPY TO 'songs.parquet')
  • INSERT OR REPLACE / ON CONFLICT support
  • Parallel reads, compressed on disk
"""

import duckdb
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Resolve path relative to this file so it works from any cwd
_HERE = Path(__file__).resolve().parent
DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH",
    str(_HERE / "data" / "masstamilan.duckdb"),
)

# ── DDL ──────────────────────────────────────────────────────────────────────

_SCHEMA = """
-- Albums table: one row per album URL
CREATE TABLE IF NOT EXISTS albums (
    album_url       VARCHAR PRIMARY KEY,
    movie_name      VARCHAR,
    starring        VARCHAR,
    music_director  VARCHAR,
    director        VARCHAR,
    lyricists       VARCHAR,
    year            VARCHAR,
    language        VARCHAR DEFAULT 'Tamil',
    track_count     INTEGER DEFAULT 0,
    scrape_ok       BOOLEAN DEFAULT TRUE,
    first_seen_at   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

-- Songs table: one row per track
CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR PRIMARY KEY,   -- md5(album_url + track_number)
    album_url       VARCHAR NOT NULL,
    movie_name      VARCHAR,
    music_director  VARCHAR,
    director        VARCHAR,
    year            VARCHAR,
    track_number    INTEGER,
    track_name      VARCHAR,
    singers         VARCHAR,
    url_128kbps     VARCHAR,
    url_320kbps     VARCHAR,
    first_seen_at   TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

-- Scrape run log
CREATE TABLE IF NOT EXISTS scrape_runs (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    pages_scraped   INTEGER DEFAULT 0,
    albums_new      INTEGER DEFAULT 0,
    albums_updated  INTEGER DEFAULT 0,
    albums_failed   INTEGER DEFAULT 0,
    songs_total     INTEGER DEFAULT 0,
    status          VARCHAR DEFAULT 'running'  -- running | success | failed
);

-- Indexes for faster JIT resolution
CREATE INDEX IF NOT EXISTS idx_songs_album_track_name ON songs (album_url, track_name);
CREATE INDEX IF NOT EXISTS idx_songs_album_track_num  ON songs (album_url, track_number);

CREATE TABLE IF NOT EXISTS users (
    user_id         VARCHAR PRIMARY KEY,
    google_sub      VARCHAR UNIQUE,
    email           VARCHAR,
    name            VARCHAR,
    picture         VARCHAR,
    is_admin        BOOLEAN DEFAULT FALSE,
    is_banned       BOOLEAN DEFAULT FALSE,
    ban_reason      VARCHAR,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id      VARCHAR PRIMARY KEY,
    user_id         VARCHAR NOT NULL,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS favorite_songs (
    user_id         VARCHAR NOT NULL,
    song_id         VARCHAR NOT NULL,
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (user_id, song_id)
);

CREATE TABLE IF NOT EXISTS favorite_albums (
    user_id         VARCHAR NOT NULL,
    album_name      VARCHAR NOT NULL,
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (user_id, album_name)
);

CREATE TABLE IF NOT EXISTS favorite_album_entities (
    user_id         VARCHAR NOT NULL,
    album_url       VARCHAR NOT NULL,
    album_name      VARCHAR,
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (user_id, album_url)
);

CREATE TABLE IF NOT EXISTS favorite_music_directors (
    user_id         VARCHAR NOT NULL,
    music_director  VARCHAR NOT NULL,
    created_at      TIMESTAMPTZ,
    PRIMARY KEY (user_id, music_director)
);

CREATE TABLE IF NOT EXISTS playlists (
    playlist_id     VARCHAR PRIMARY KEY,
    user_id         VARCHAR NOT NULL,
    name            VARCHAR,
    is_global       BOOLEAN DEFAULT FALSE,
    source          VARCHAR DEFAULT 'manual',
    source_url      VARCHAR,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS playlist_songs (
    playlist_id     VARCHAR NOT NULL,
    song_id         VARCHAR NOT NULL,
    position        INTEGER NOT NULL,
    added_at        TIMESTAMPTZ,
    PRIMARY KEY (playlist_id, position)
);

CREATE TABLE IF NOT EXISTS user_preferences (
    user_id             VARCHAR PRIMARY KEY,
    theme_preference    VARCHAR DEFAULT 'system',
    main_tab            VARCHAR DEFAULT 'library',
    recent_song_ids     VARCHAR,
    player_volume       DOUBLE DEFAULT 0.9,
    player_muted        BOOLEAN DEFAULT FALSE,
    playback_speed      DOUBLE DEFAULT 1.0,
    repeat_mode         VARCHAR DEFAULT 'off',
    autoplay_next       BOOLEAN DEFAULT TRUE,
    updated_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON user_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_favorites_user_id ON favorite_songs (user_id);
CREATE INDEX IF NOT EXISTS idx_favorite_albums_user_id ON favorite_albums (user_id);
CREATE INDEX IF NOT EXISTS idx_favorite_album_entities_user_id ON favorite_album_entities (user_id);
CREATE INDEX IF NOT EXISTS idx_favorite_music_directors_user_id ON favorite_music_directors (user_id);
CREATE INDEX IF NOT EXISTS idx_playlists_user_id ON playlists (user_id);
CREATE INDEX IF NOT EXISTS idx_playlist_songs_playlist_id ON playlist_songs (playlist_id);
"""

_MIGRATIONS = {
    "users": {
        "is_admin": "BOOLEAN DEFAULT FALSE",
        "is_banned": "BOOLEAN DEFAULT FALSE",
        "ban_reason": "VARCHAR",
        "last_login_at": "TIMESTAMPTZ",
    },
    "playlists": {
        "is_global": "BOOLEAN DEFAULT FALSE",
    },
    "user_preferences": {
        "theme_preference": "VARCHAR DEFAULT 'system'",
        "main_tab": "VARCHAR DEFAULT 'library'",
        "recent_song_ids": "VARCHAR",
        "player_volume": "DOUBLE DEFAULT 0.9",
        "player_muted": "BOOLEAN DEFAULT FALSE",
        "playback_speed": "DOUBLE DEFAULT 1.0",
        "repeat_mode": "VARCHAR DEFAULT 'off'",
        "autoplay_next": "BOOLEAN DEFAULT TRUE",
        "updated_at": "TIMESTAMPTZ",
    },
}


def _ensure_migrations(conn: duckdb.DuckDBPyConnection) -> None:
    for table_name, columns in _MIGRATIONS.items():
        existing = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
        for column_name, column_sql in columns.items():
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

# ── Connection factory ────────────────────────────────────────────────────────

def get_conn(path: str = DUCKDB_PATH, read_only: bool = False, initialize: bool = True) -> duckdb.DuckDBPyConnection:
    """Open DuckDB connection. Creates file + schema on first call."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path, read_only=read_only)
    if not read_only and initialize:
        conn.execute(_SCHEMA)
        _ensure_migrations(conn)
    return conn


# ── Album helpers ─────────────────────────────────────────────────────────────

def get_known_album_urls(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return set of album URLs already successfully scraped."""
    rows = conn.execute(
        "SELECT album_url FROM albums WHERE scrape_ok = TRUE"
    ).fetchall()
    return {r[0] for r in rows}


def get_album_urls_for_refresh(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int = 200,
    min_age_hours: float = 24,
    recent_year_window: int = 2,
    exclude_urls: set[str] | None = None,
) -> list[str]:
    """
    Return existing album URLs that should be revisited.

    Newest releases are prioritized, but ordering by the oldest successful
    refresh timestamp ensures the whole catalog is eventually revisited.
    """
    if limit <= 0:
        return []

    exclude_urls = exclude_urls or set()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max(min_age_hours, 0))
    recent_cutoff_year = now.year - max(recent_year_window, 0)
    fetch_limit = limit + len(exclude_urls)

    rows = conn.execute(
        """
        SELECT album_url
        FROM albums
        WHERE scrape_ok = TRUE
          AND album_url IS NOT NULL
          AND album_url != ''
          AND COALESCE(updated_at, first_seen_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') <= ?
        ORDER BY
          CASE
            WHEN TRY_CAST(year AS INTEGER) >= ? THEN 0
            ELSE 1
          END,
          COALESCE(updated_at, first_seen_at, TIMESTAMPTZ '1970-01-01 00:00:00+00') ASC,
          TRY_CAST(year AS INTEGER) DESC NULLS LAST,
          album_url ASC
        LIMIT ?
        """,
        [cutoff, recent_cutoff_year, fetch_limit],
    ).fetchall()

    urls: list[str] = []
    for (album_url,) in rows:
        if album_url in exclude_urls:
            continue
        urls.append(album_url)
        if len(urls) >= limit:
            break
    return urls


def upsert_album(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO albums
            (album_url, movie_name, starring, music_director, director,
             lyricists, year, language, track_count, scrape_ok,
             first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (album_url) DO UPDATE SET
            movie_name     = excluded.movie_name,
            starring       = excluded.starring,
            music_director = excluded.music_director,
            director       = excluded.director,
            lyricists      = excluded.lyricists,
            year           = excluded.year,
            track_count    = excluded.track_count,
            scrape_ok      = excluded.scrape_ok,
            updated_at     = excluded.updated_at
    """, [
        data["album_url"],
        data.get("movie_name", ""),
        data.get("starring", ""),
        data.get("music_director", ""),
        data.get("director", ""),
        data.get("lyricists", ""),
        data.get("year", ""),
        data.get("language", "Tamil"),
        data.get("track_count", 0),
        data.get("scrape_ok", True),
        now,   # first_seen_at — ignored on conflict (DO UPDATE doesn't touch it)
        now,   # updated_at
    ])


def mark_album_failed(conn: duckdb.DuckDBPyConnection, album_url: str) -> None:
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO albums (album_url, scrape_ok, first_seen_at, updated_at)
        VALUES (?, FALSE, ?, ?)
        ON CONFLICT (album_url) DO UPDATE SET
            scrape_ok  = FALSE,
            updated_at = excluded.updated_at
    """, [album_url, now, now])


# ── Song helpers ──────────────────────────────────────────────────────────────

def _song_id(album_url: str, track_number: int) -> str:
    import hashlib
    key = f"{album_url}::{track_number}"
    return hashlib.md5(key.encode()).hexdigest()


def upsert_songs(conn: duckdb.DuckDBPyConnection, songs: list[dict]) -> None:
    if not songs:
        return
    now = datetime.now(timezone.utc)
    conn.executemany("""
        INSERT INTO songs
            (song_id, album_url, movie_name, music_director, director, year,
             track_number, track_name, singers, url_128kbps, url_320kbps,
             first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (song_id) DO UPDATE SET
            movie_name     = excluded.movie_name,
            music_director = excluded.music_director,
            director       = excluded.director,
            year           = excluded.year,
            track_name     = excluded.track_name,
            singers        = excluded.singers,
            url_128kbps    = excluded.url_128kbps,
            url_320kbps    = excluded.url_320kbps,
            updated_at     = excluded.updated_at
    """, [
        [
            _song_id(s["album_url"], s["track_number"]),
            s["album_url"],
            s.get("movie_name", ""),
            s.get("music_director", ""),
            s.get("director", ""),
            s.get("year", ""),
            s.get("track_number", 0),
            s.get("track_name", ""),
            s.get("singers", ""),
            s.get("url_128kbps", ""),
            s.get("url_320kbps", ""),
            now,
            now,
        ]
        for s in songs
    ])


def replace_album_songs(conn: duckdb.DuckDBPyConnection, album_url: str, songs: list[dict]) -> None:
    """
    Replace the stored tracklist for a successfully re-scraped album.
    """
    if not album_url or not songs:
        return
    conn.execute("DELETE FROM songs WHERE album_url = ?", [album_url])
    upsert_songs(conn, songs)


# ── Run log helpers ───────────────────────────────────────────────────────────

def start_run(conn: duckdb.DuckDBPyConnection, run_id: str) -> None:
    conn.execute("""
        INSERT INTO scrape_runs (run_id, started_at, status)
        VALUES (?, ?, 'running')
        ON CONFLICT (run_id) DO NOTHING
    """, [run_id, datetime.now(timezone.utc)])


def finish_run(conn: duckdb.DuckDBPyConnection, run_id: str, stats: dict) -> None:
    conn.execute("""
        UPDATE scrape_runs SET
            finished_at    = ?,
            pages_scraped  = ?,
            albums_new     = ?,
            albums_updated = ?,
            albums_failed  = ?,
            songs_total    = ?,
            status         = ?
        WHERE run_id = ?
    """, [
        datetime.now(timezone.utc),
        stats.get("pages_scraped", 0),
        stats.get("albums_new", 0),
        stats.get("albums_updated", 0),
        stats.get("albums_failed", 0),
        stats.get("songs_total", 0),
        stats.get("status", "success"),
        run_id,
    ])


# ── Export helpers ────────────────────────────────────────────────────────────

def export_parquet(conn: duckdb.DuckDBPyConnection, path: str) -> int:
    """Export songs table to Parquet. Returns row count."""
    conn.execute(f"COPY songs TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]


def export_csv(conn: duckdb.DuckDBPyConnection, path: str) -> int:
    conn.execute(f"""
        COPY (
            SELECT movie_name, music_director, director, year,
                   track_number, track_name, singers,
                   url_128kbps, url_320kbps, album_url, updated_at
            FROM songs
            ORDER BY year DESC, movie_name, track_number
        ) TO '{path}' (HEADER TRUE, DELIMITER ',')
    """)
    return conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]


# ── Quick stats ───────────────────────────────────────────────────────────────

def print_stats(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n── DuckDB Stats ─────────────────────────────────────")
    albums_ok  = conn.execute("SELECT COUNT(*) FROM albums WHERE scrape_ok").fetchone()[0]
    albums_bad = conn.execute("SELECT COUNT(*) FROM albums WHERE NOT scrape_ok").fetchone()[0]
    songs      = conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]
    print(f"  Albums OK     : {albums_ok:,}")
    print(f"  Albums failed : {albums_bad:,}")
    print(f"  Total songs   : {songs:,}")

    print("\n  Top 10 Music Directors:")
    for r in conn.execute("""
        SELECT music_director, COUNT(*) cnt
        FROM songs WHERE music_director != ''
        GROUP BY music_director ORDER BY cnt DESC LIMIT 10
    """).fetchall():
        print(f"    {r[0]:35s} {r[1]:>5,}")

    print("\n  Songs by Year (recent first):")
    for r in conn.execute("""
        SELECT year, COUNT(*) cnt
        FROM songs WHERE year != ''
        GROUP BY year ORDER BY year DESC LIMIT 10
    """).fetchall():
        print(f"    {r[0]:6s}  {r[1]:>5,}")
