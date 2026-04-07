#!/usr/bin/env python3
"""
query.py — Query the isaibox DuckDB from the command line
─────────────────────────────────────────────────────────
Examples:
  python3 query.py --stats
  python3 query.py --search "anirudh"
  python3 query.py --year 2025
  python3 query.py --director "G. V. Prakash Kumar"
  python3 query.py --singer "Sid Sriram"
  python3 query.py --sql "SELECT * FROM songs WHERE year='2024' LIMIT 20"
  python3 query.py --export-csv out.csv --year 2025
  python3 query.py --export-parquet out.parquet
  python3 query.py --failed          # list albums that failed to scrape
"""

import argparse
import sys
import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))
import db


DUCKDB_PATH = os.environ.get(
    "DUCKDB_PATH",
    str(PROJECT_DIR / "data" / "masstamilan.duckdb"),
)

SONG_FIELDS = [
    "movie_name", "music_director", "director", "year",
    "track_number", "track_name", "singers",
    "url_128kbps", "url_320kbps",
]


def get_conn(read_only=True):
    if not Path(DUCKDB_PATH).exists():
        print(f"No database at {DUCKDB_PATH}. Run setup + scraper first.")
        sys.exit(1)
    return db.get_conn(DUCKDB_PATH, read_only=read_only)


def print_rows(rows, fields=None):
    if not rows:
        print("No results.")
        return
    if fields is None:
        fields = list(rows[0].keys()) if hasattr(rows[0], "keys") else []
    cap = 55
    widths = {}
    for f in fields:
        vals = [str(r[f] or "") for r in rows]
        widths[f] = min(cap, max(len(f), max((len(v) for v in vals), default=0)))
    header = "  ".join(f.ljust(widths[f]) for f in fields)
    sep    = "  ".join("─" * widths[f] for f in fields)
    print(header)
    print(sep)
    for row in rows:
        print("  ".join(str(row[f] or "")[:widths[f]].ljust(widths[f]) for f in fields))
    print(f"\n  {len(rows):,} row(s)")


def main():
    p = argparse.ArgumentParser(description="Query the isaibox DuckDB")
    p.add_argument("--stats",           action="store_true", help="Show database stats")
    p.add_argument("--search",          metavar="KEYWORD",   help="Search movie/track/singer")
    p.add_argument("--year",            metavar="YEAR",      help="Filter by year")
    p.add_argument("--director",        metavar="NAME",      help="Filter by music director")
    p.add_argument("--singer",          metavar="NAME",      help="Filter by singer")
    p.add_argument("--failed",          action="store_true", help="List failed album URLs")
    p.add_argument("--runs",            action="store_true", help="Show scrape run history")
    p.add_argument("--sql",             metavar="SQL",       help="Run raw SQL")
    p.add_argument("--export-csv",      metavar="FILE",      help="Export results to CSV")
    p.add_argument("--export-parquet",  metavar="FILE",      help="Export all songs to Parquet")
    args = p.parse_args()

    conn = get_conn()

    if args.stats:
        db.print_stats(conn)

    elif args.search:
        kw = f"%{args.search}%"
        rows = conn.execute(f"""
            SELECT {','.join(SONG_FIELDS)}
            FROM songs
            WHERE movie_name ILIKE ?
               OR track_name  ILIKE ?
               OR singers     ILIKE ?
               OR music_director ILIKE ?
            ORDER BY year DESC, movie_name, track_number
        """, [kw] * 4).fetchall()
        if args.export_csv:
            _csv(conn, rows, args.export_csv)
        else:
            print_rows(rows, SONG_FIELDS)

    elif args.year:
        rows = conn.execute(f"""
            SELECT {','.join(SONG_FIELDS)}
            FROM songs WHERE year = ?
            ORDER BY movie_name, track_number
        """, [args.year]).fetchall()
        if args.export_csv:
            _csv(conn, rows, args.export_csv)
        else:
            print_rows(rows, SONG_FIELDS)

    elif args.director:
        rows = conn.execute(f"""
            SELECT {','.join(SONG_FIELDS)}
            FROM songs WHERE music_director ILIKE ?
            ORDER BY year DESC, movie_name, track_number
        """, [f"%{args.director}%"]).fetchall()
        if args.export_csv:
            _csv(conn, rows, args.export_csv)
        else:
            print_rows(rows, SONG_FIELDS)

    elif args.singer:
        rows = conn.execute(f"""
            SELECT {','.join(SONG_FIELDS)}
            FROM songs WHERE singers ILIKE ?
            ORDER BY year DESC, movie_name, track_number
        """, [f"%{args.singer}%"]).fetchall()
        if args.export_csv:
            _csv(conn, rows, args.export_csv)
        else:
            print_rows(rows, SONG_FIELDS)

    elif args.failed:
        rows = conn.execute("""
            SELECT album_url, updated_at
            FROM albums WHERE NOT scrape_ok
            ORDER BY updated_at DESC
        """).fetchall()
        print_rows(rows, ["album_url", "updated_at"])

    elif args.runs:
        rows = conn.execute("""
            SELECT run_id, started_at, finished_at,
                   albums_new, albums_failed, songs_total, status
            FROM scrape_runs ORDER BY started_at DESC LIMIT 20
        """).fetchall()
        print_rows(rows, ["run_id","started_at","albums_new","albums_failed","songs_total","status"])

    elif args.sql:
        try:
            rows = conn.execute(args.sql).fetchall()
            if args.export_csv:
                _csv(conn, rows, args.export_csv)
            else:
                print_rows(rows)
        except Exception as e:
            print(f"SQL error: {e}")

    elif args.export_parquet:
        n = db.export_parquet(conn, args.export_parquet)
        print(f"Exported {n:,} songs → {args.export_parquet}")

    else:
        db.print_stats(conn)

    conn.close()


def _csv(conn, rows, path):
    import csv
    if not rows:
        print("No rows to export.")
        return
    fields = list(rows[0].keys()) if hasattr(rows[0], "keys") else list(range(len(rows[0])))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for row in rows:
            w.writerow([row[k] for k in fields])
    print(f"Exported {len(rows):,} rows → {path}")


if __name__ == "__main__":
    main()
