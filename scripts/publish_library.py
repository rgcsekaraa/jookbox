#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure the repository root (parent of scripts/) is importable so ``import db`` works
# regardless of the working directory Python uses to populate sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import db


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def collect_stats(path: Path) -> dict[str, object]:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        songs = int(conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0])
        albums = int(conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0])
        latest_updated_at = conn.execute(
            """
            SELECT CASE
                WHEN MAX(updated_at) IS NULL THEN ''
                ELSE REPLACE(CAST(MAX(updated_at) AS VARCHAR), ' +00', 'Z')
            END
            FROM songs
            WHERE updated_at IS NOT NULL
            """
        ).fetchone()[0]
    finally:
        conn.close()
    return {
        "songs": songs,
        "albums": albums,
        "latest_song_updated_at": latest_updated_at or "",
    }


def _duckdb_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or hashlib.md5(value.encode()).hexdigest()[:12]


def display_music_director_name(value: str) -> str:
    overrides = {
        "A.R.Rahman": "A.R. Rahman",
        "D.Imman": "D. Imman",
    }
    return overrides.get(value, value)


BGM_ALBUM_SQL_RE = r"(?i)\bbgm\b"


def playlist_eligible_song_sql(alias: str = "songs") -> str:
    prefix = f"{alias}." if alias else ""
    return f"NOT regexp_matches(COALESCE({prefix}movie_name, ''), '{BGM_ALBUM_SQL_RE}')"


def remove_bgm_album_tracks_from_playlists(conn: duckdb.DuckDBPyConnection) -> int:
    bgm_song_ids = [
        row[0]
        for row in conn.execute(
            f"""
            SELECT song_id
            FROM songs
            WHERE NOT {playlist_eligible_song_sql('')}
            """
        ).fetchall()
        if row[0]
    ]
    if not bgm_song_ids:
        return 0
    placeholders = ", ".join("?" for _ in bgm_song_ids)
    removed = conn.execute(
        f"DELETE FROM playlist_songs WHERE song_id IN ({placeholders}) RETURNING song_id",
        bgm_song_ids,
    ).fetchall()
    if removed:
        print(f"  Removed {len(removed):,} BGM-album playlist entries")
    return len(removed)


LEGACY_MUSIC_DIRECTOR_SHORTLISTS = {
    "A.R.Rahman": ["A.R. Rahman Hits"],
    "Anirudh Ravichander": ["Anirudh Ravichander Chartbusters"],
    "D.Imman": ["D. Imman Melodies"],
    "Deva": ["Deva Gaana Hits"],
    "G. V. Prakash Kumar": ["G.V. Prakash Kumar Favorites"],
    "Ilaiyaraaja": ["Ilaiyaraaja Classics"],
    "Santhosh Narayanan": ["Santhosh Narayanan Vibes"],
    "Vidyasagar": ["Vidyasagar Melodies"],
    "Yuvan Shankar Raja": ["Yuvan Shankar Raja Essentials"],
}


def remove_legacy_music_director_shortlists(
    conn: duckdb.DuckDBPyConnection,
    director_names: list[str],
) -> None:
    legacy_names = sorted(
        {
            playlist_name
            for director in director_names
            for playlist_name in LEGACY_MUSIC_DIRECTOR_SHORTLISTS.get(director, [])
        }
    )
    if not legacy_names:
        return

    placeholders = ", ".join("?" for _ in legacy_names)
    legacy_rows = conn.execute(
        f"""
        SELECT playlist_id, name
        FROM playlists
        WHERE is_global = TRUE
          AND source = 'gemini'
          AND name IN ({placeholders})
        """,
        legacy_names,
    ).fetchall()
    if not legacy_rows:
        return

    legacy_ids = [row[0] for row in legacy_rows]
    id_placeholders = ", ".join("?" for _ in legacy_ids)
    conn.execute(f"DELETE FROM playlist_songs WHERE playlist_id IN ({id_placeholders})", legacy_ids)
    conn.execute(f"DELETE FROM playlists WHERE playlist_id IN ({id_placeholders})", legacy_ids)
    for _playlist_id, playlist_name in legacy_rows:
        print(f"  Removed legacy 50-track playlist: {playlist_name}")


def ensure_top_music_director_playlists(path: Path, limit: int = 12) -> None:
    conn = db.get_conn(str(path))
    try:
        now = iso_now()
        directors = conn.execute(
            f"""
            SELECT music_director, COUNT(DISTINCT album_url) AS album_count, COUNT(*) AS song_count
            FROM songs
            WHERE COALESCE(TRIM(music_director), '') <> ''
              AND {playlist_eligible_song_sql('songs')}
            GROUP BY music_director
            ORDER BY album_count DESC, song_count DESC, music_director
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        director_names = [row[0] for row in directors]
        remove_legacy_music_director_shortlists(conn, director_names)

        for director, _album_count, _song_count in directors:
            playlist_id = f"global-md-{slugify(director)}"
            playlist_name = f"{display_music_director_name(director)} Songs"
            source_url = f"music-director:{director}"
            conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
            conn.execute(
                """
                INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
                VALUES (?, 'global', ?, TRUE, 'music-director', ?, ?, ?)
                ON CONFLICT (playlist_id) DO UPDATE SET
                    name = excluded.name,
                    is_global = TRUE,
                    source = excluded.source,
                    source_url = excluded.source_url,
                    updated_at = excluded.updated_at
                """,
                [playlist_id, playlist_name, source_url, now, now],
            )
            song_rows = conn.execute(
                f"""
                SELECT song_id
                FROM songs
                WHERE music_director = ?
                  AND {playlist_eligible_song_sql('songs')}
                ORDER BY
                    TRY_CAST(year AS INTEGER) DESC NULLS LAST,
                    updated_at DESC NULLS LAST,
                    movie_name,
                    track_number,
                    track_name
                """,
                [director],
            ).fetchall()
            conn.executemany(
                "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
                [(playlist_id, row[0], index + 1, now) for index, row in enumerate(song_rows)],
            )
            print(f"  {playlist_name}: {len(song_rows):,} songs")
    finally:
        conn.close()


# ── Schema introspection helpers ──────────────────────────────────────────────


def _get_shared_columns(conn: duckdb.DuckDBPyConnection, table: str, source_schema: str = "source") -> list[str]:
    """Return the list of column names present in both the target and source table."""
    target_cols = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    }
    source_cols = [
        row[1]
        for row in conn.execute(f"PRAGMA table_info('{source_schema}.{table}')").fetchall()
    ]
    # Preserve source ordering, keep only columns that exist in both schemas.
    return [c for c in source_cols if c in target_cols]


def _get_pk_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """Return the primary-key column names for *table* in the default catalog."""
    return [
        row[1]
        for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        if row[5]  # column index 5 = pk flag (non-zero means part of PK)
    ]


def _build_upsert_sql(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    cols: list[str],
    select_expr: str,
    from_clause: str,
    where_clause: str = "",
) -> str:
    """Build an INSERT … ON CONFLICT DO UPDATE statement.

    Existing rows are updated; rows absent from the source are **preserved**.
    """
    pk_cols = _get_pk_columns(conn, table)
    non_pk_cols = [c for c in cols if c not in pk_cols]

    col_list = ", ".join(cols)
    sql = f"INSERT INTO {table} ({col_list}) SELECT {select_expr} FROM {from_clause}"

    if where_clause:
        sql += f" WHERE {where_clause}"

    if pk_cols and non_pk_cols:
        conflict = ", ".join(pk_cols)
        update_set = ", ".join(f"{c} = excluded.{c}" for c in non_pk_cols)
        sql += f" ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"
    elif pk_cols:
        # All columns are part of PK → nothing to update, just skip duplicates.
        sql += f" ON CONFLICT ({', '.join(pk_cols)}) DO NOTHING"

    return sql


# ── Core export / merge ──────────────────────────────────────────────────────


def export_library_snapshot(source_db: Path, output_db: Path) -> None:
    """Merge the freshly-scraped *source_db* into the published *output_db*.

    Key behaviour:
    • Songs/albums/playlists already in *output_db* that are **not** in
      this scrape run are **preserved** (never deleted).
    • New rows from *source_db* are inserted; existing rows are updated.
    • A backup of the previous *output_db* is created before modification.
    """
    # ── 1. Back up existing output DB ─────────────────────────────────────
    backup = output_db.with_suffix(".backup.duckdb")
    if output_db.exists():
        shutil.copy2(output_db, backup)
        print(f"✓ Backed up existing DB → {backup}")

    # ── 2. Prepare a temp working copy ────────────────────────────────────
    temp_output = output_db.with_suffix(".tmp")
    if temp_output.exists():
        temp_output.unlink()

    if output_db.exists():
        # Start from the existing published DB so we preserve all prior data.
        shutil.copy2(output_db, temp_output)
        # Apply any new schema migrations on top of the existing data.
        conn = db.get_conn(str(temp_output))
    else:
        # First-ever run: create a brand-new DB with the full schema.
        conn = db.get_conn(str(temp_output))

    # ── 3. Merge source data via upserts ──────────────────────────────────
    try:
        conn.execute(f"ATTACH '{_duckdb_literal(source_db)}' AS source (READ_ONLY)")

        # --- albums, songs, scrape_runs ---
        for table in ("albums", "songs", "scrape_runs"):
            cols = _get_shared_columns(conn, table)
            col_list = ", ".join(cols)
            sql = _build_upsert_sql(
                conn, table, cols,
                select_expr=col_list,
                from_clause=f"source.{table}",
            )
            conn.execute(sql)

        # --- playlists (global only) ---
        pl_cols = _get_shared_columns(conn, "playlists")
        pl_col_list = ", ".join(pl_cols)
        sql = _build_upsert_sql(
            conn, "playlists", pl_cols,
            select_expr=pl_col_list,
            from_clause="source.playlists",
            where_clause="COALESCE(is_global, FALSE) = TRUE",
        )
        conn.execute(sql)

        # --- playlist_songs (from global playlists) ---
        ps_cols = _get_shared_columns(conn, "playlist_songs")
        ps_select_expr = ", ".join(f"ps.{c}" for c in ps_cols)
        sql = _build_upsert_sql(
            conn, "playlist_songs", ps_cols,
            select_expr=ps_select_expr,
            from_clause="source.playlist_songs ps JOIN source.playlists p ON p.playlist_id = ps.playlist_id",
            where_clause="COALESCE(p.is_global, FALSE) = TRUE",
        )
        conn.execute(sql)

        conn.execute("DETACH source")
    finally:
        conn.close()

    # ── 4. Atomically replace the output ──────────────────────────────────
    temp_output.replace(output_db)

    # ── 5. Print merge summary ────────────────────────────────────────────
    if backup.exists():
        old_stats = collect_stats(backup)
        new_stats = collect_stats(output_db)
        song_delta = new_stats["songs"] - old_stats["songs"]
        album_delta = new_stats["albums"] - old_stats["albums"]
        sign = lambda n: f"+{n}" if n > 0 else str(n)
        print(f"  Songs : {old_stats['songs']:,} → {new_stats['songs']:,} ({sign(song_delta)})")
        print(f"  Albums: {old_stats['albums']:,} → {new_stats['albums']:,} ({sign(album_delta)})")
    else:
        stats = collect_stats(output_db)
        print(f"  Songs : {stats['songs']:,}  (fresh snapshot)")
        print(f"  Albums: {stats['albums']:,}")

    print("✓ Ensuring top music-director global playlists")
    ensure_top_music_director_playlists(output_db)
    conn = db.get_conn(str(output_db))
    try:
        print("✓ Removing BGM-album tracks from playlists")
        remove_bgm_album_tracks_from_playlists(conn)
    finally:
        conn.close()


def build_manifest(stats_db: Path, download_path: Path, repo: str, ref: str, version: str) -> dict[str, object]:
    stats = collect_stats(stats_db)
    return {
        "version": version,
        "updated_at": version,
        "size": stats_db.stat().st_size,
        "sha256": sha256_file(stats_db),
        "download_url": f"https://raw.githubusercontent.com/{repo}/{ref}/{download_path.as_posix()}",
        "songs": stats["songs"],
        "albums": stats["albums"],
        "latest_song_updated_at": stats["latest_song_updated_at"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy and publish the packaged DuckDB manifest.")
    parser.add_argument("--db-path", required=True, help="Source DuckDB path")
    parser.add_argument("--output-db", required=True, help="Destination DuckDB path committed to Git")
    parser.add_argument("--manifest", required=True, help="Destination manifest JSON path")
    parser.add_argument("--repo", required=True, help="GitHub owner/repo")
    parser.add_argument("--ref", default="main", help="Git ref used for raw download URLs")
    parser.add_argument("--version", default="", help="Manifest version timestamp; defaults to current UTC time")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and print the manifest without writing files")
    args = parser.parse_args()

    source_db = Path(args.db_path).resolve()
    output_db = Path(args.output_db)
    manifest_path = Path(args.manifest)
    version = args.version or iso_now()

    if not source_db.exists():
        raise FileNotFoundError(f"Source DuckDB not found: {source_db}")

    if args.dry_run:
        manifest_source = source_db
    else:
        output_db.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        export_library_snapshot(source_db, output_db)
        manifest_source = output_db

    manifest = build_manifest(manifest_source, output_db, args.repo, args.ref, version)

    if args.dry_run:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output_db": output_db.as_posix(), "manifest": manifest}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
