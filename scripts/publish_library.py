#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
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
