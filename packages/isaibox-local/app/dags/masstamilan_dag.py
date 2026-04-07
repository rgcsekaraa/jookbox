"""
dags/masstamilan_dag.py
═══════════════════════════════════════════════════════════════════════════════
isaibox daily scraper DAG
─────────────────────────────
Schedule : daily at 06:00 IST (00:30 UTC)

Task graph
──────────
  discover_new_albums
        │
        ▼
  scrape_album_batch   (dynamic task mapping — one task per batch of 20 albums)
        │
        ▼
  write_to_duckdb
        │
        ▼
  export_parquet_and_csv
        │
        ▼
  log_run_summary

Design notes
─────────────
• Incremental  : On each run, discovery stops as soon as it hits a page of
                 already-known albums (site lists newest first).
• Batching     : Albums are chunked into groups of 20 so each mapped task
                 handles a reasonable amount of work.
• DuckDB       : Single .duckdb file under PROJECT_DIR/data/.  All writes
                 are done in a single task (write_to_duckdb) to avoid
                 concurrent writer conflicts.
• Backfill     : Set FULL_SCRAPE=true in Variables for a full re-scrape
                 from page 2 (useful on first run or after schema changes).
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Resolve project root so imports work regardless of cwd ──────────────────
PROJECT_DIR = Path(
    os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[2])
)
sys.path.insert(0, str(PROJECT_DIR))

import db            # db.py
import scraper_core  # scraper_core.py

log = logging.getLogger(__name__)

DUCKDB_PATH  = str(PROJECT_DIR / "data" / "masstamilan.duckdb")
EXPORTS_DIR  = PROJECT_DIR / "exports"
BATCH_SIZE   = 20          # albums per dynamic task
START_PAGE   = 2           # listing always starts at page 2

# ── DAG defaults ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "isaibox",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

# ═════════════════════════════════════════════════════════════════════════════
# Task functions
# ═════════════════════════════════════════════════════════════════════════════

def task_discover_new_albums(**ctx) -> dict[str, Any]:
    """
    Walk listing pages, collect album URLs not yet in DuckDB.
    Pushes: { "batches": [[url, ...], ...], "run_id": str, "total_pages": int }
    """
    run_id = ctx["run_id"]

    # Should we do a full scrape (ignore known URLs)?
    full_scrape = Variable.get("MASSTAMILAN_FULL_SCRAPE", default_var="false").lower() == "true"

    conn = db.get_conn(DUCKDB_PATH)
    db.start_run(conn, run_id)

    known_urls: set[str] = set() if full_scrape else db.get_known_album_urls(conn)
    conn.close()

    log.info(f"Run {run_id} | Full scrape: {full_scrape} | Known albums: {len(known_urls)}")

    new_urls, total_pages = scraper_core.discover_new_album_urls(
        start_page=START_PAGE,
        known_urls=known_urls,
    )

    # Chunk into batches
    batches = [
        new_urls[i : i + BATCH_SIZE]
        for i in range(0, max(len(new_urls), 1), BATCH_SIZE)
        if new_urls[i : i + BATCH_SIZE]
    ]
    if not batches:
        batches = [[]]  # ensure at least one downstream task runs

    log.info(f"New albums: {len(new_urls)} → {len(batches)} batch(es)")

    return {
        "batches":     batches,
        "run_id":      run_id,
        "total_pages": total_pages,
        "new_count":   len(new_urls),
    }


def task_scrape_batch(batch_index: int, **ctx) -> dict[str, Any]:
    """
    Scrape one batch of album URLs.
    Returns serialisable dict of albums + songs for the write task.
    """
    ti         = ctx["ti"]
    discovery  = ti.xcom_pull(task_ids="discover_new_albums")
    batches    = discovery["batches"]

    if batch_index >= len(batches):
        return {"albums": [], "songs": [], "failed": []}

    album_urls = batches[batch_index]
    if not album_urls:
        return {"albums": [], "songs": [], "failed": []}

    log.info(f"Batch {batch_index}: {len(album_urls)} album(s)")
    albums, songs, failed = scraper_core.scrape_albums(album_urls)

    return {
        "albums": albums,
        "songs":  songs,
        "failed": failed,
    }


def task_write_to_duckdb(**ctx) -> dict[str, Any]:
    """
    Collect all batch results via XCom, bulk-write to DuckDB.
    Single writer = no concurrent write conflicts.
    """
    ti       = ctx["ti"]
    run_id   = ctx["run_id"]
    discovery = ti.xcom_pull(task_ids="discover_new_albums")
    batches  = discovery["batches"]

    all_albums: list[dict] = []
    all_songs:  list[dict] = []
    all_failed: list[str]  = []

    for i in range(len(batches)):
        result = ti.xcom_pull(task_ids=f"scrape_batch_{i}")
        if result:
            all_albums.extend(result.get("albums", []))
            all_songs.extend(result.get("songs",  []))
            all_failed.extend(result.get("failed", []))

    log.info(f"Writing {len(all_albums)} albums, {len(all_songs)} songs to DuckDB…")

    conn = db.get_conn(DUCKDB_PATH)
    try:
        for album in all_albums:
            db.upsert_album(conn, album)

        for url in all_failed:
            db.mark_album_failed(conn, url)

        if all_songs:
            db.upsert_songs(conn, all_songs)

        total_songs = conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]
        total_albums = conn.execute(
            "SELECT COUNT(*) FROM albums WHERE scrape_ok"
        ).fetchone()[0]

        stats = {
            "albums_new":     len(all_albums),
            "albums_failed":  len(all_failed),
            "albums_updated": 0,
            "songs_total":    total_songs,
            "pages_scraped":  discovery["total_pages"],
            "status":         "success",
        }
        db.finish_run(conn, run_id, stats)
        conn.close()

        log.info(f"DuckDB write complete | {total_albums:,} albums | {total_songs:,} songs total")
        return stats

    except Exception as e:
        conn.close()
        raise e


def task_export(**ctx) -> str:
    """Export songs to Parquet + CSV in exports/ directory."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    date_str     = datetime.now(timezone.utc).strftime("%Y%m%d")
    parquet_path = str(EXPORTS_DIR / f"songs_{date_str}.parquet")
    csv_path     = str(EXPORTS_DIR / "songs_latest.csv")

    conn = db.get_conn(DUCKDB_PATH)
    rows = db.export_parquet(conn, parquet_path)
    db.export_csv(conn, csv_path)
    conn.close()

    log.info(f"Exported {rows:,} songs → {parquet_path}")
    log.info(f"Exported CSV → {csv_path}")
    return parquet_path


def task_summary(**ctx) -> None:
    """Print a human-readable run summary to Airflow logs."""
    ti    = ctx["ti"]
    write = ti.xcom_pull(task_ids="write_to_duckdb")
    disc  = ti.xcom_pull(task_ids="discover_new_albums")

    conn  = db.get_conn(DUCKDB_PATH, read_only=True)
    db.print_stats(conn)
    conn.close()

    log.info("\n" + "═" * 54)
    log.info("  RUN SUMMARY")
    log.info(f"  New albums scraped : {write.get('albums_new', 0):>6,}")
    log.info(f"  Failed albums      : {write.get('albums_failed', 0):>6,}")
    log.info(f"  Total songs in DB  : {write.get('songs_total', 0):>6,}")
    log.info(f"  Pages walked       : {write.get('pages_scraped', 0):>6,}")
    log.info("═" * 54)

    # After a full scrape, auto-reset the variable so next run is incremental
    full = Variable.get("MASSTAMILAN_FULL_SCRAPE", default_var="false")
    if full.lower() == "true":
        Variable.set("MASSTAMILAN_FULL_SCRAPE", "false")
        log.info("MASSTAMILAN_FULL_SCRAPE reset to false for next run.")


# ═════════════════════════════════════════════════════════════════════════════
# DAG definition
# ═════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="masstamilan_daily_scraper",
    description="Scrape source catalog daily — new albums only (incremental)",
    schedule_interval="30 0 * * *",     # 06:00 IST = 00:30 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["isaibox", "scraper", "music"],
    doc_md=__doc__,
) as dag:

    # ── T1: Discovery ────────────────────────────────────────────────────────
    discover = PythonOperator(
        task_id="discover_new_albums",
        python_callable=task_discover_new_albums,
    )

    # ── T2: Dynamic batch scrape tasks ──────────────────────────────────────
    # We pre-define a generous upper bound (200 batches = 4000 albums max per
    # run). In practice, daily incremental runs will use very few batches.
    MAX_BATCHES = 200

    scrape_tasks = []
    for i in range(MAX_BATCHES):
        t = PythonOperator(
            task_id=f"scrape_batch_{i}",
            python_callable=task_scrape_batch,
            op_kwargs={"batch_index": i},
            # Skip gracefully if this batch index doesn't exist
            trigger_rule="all_done",
        )
        discover >> t
        scrape_tasks.append(t)

    # ── T3: Write everything to DuckDB (single writer) ───────────────────────
    write = PythonOperator(
        task_id="write_to_duckdb",
        python_callable=task_write_to_duckdb,
        trigger_rule="all_done",   # run even if some batches failed
    )
    for t in scrape_tasks:
        t >> write

    # ── T4: Export ───────────────────────────────────────────────────────────
    export = PythonOperator(
        task_id="export_parquet_and_csv",
        python_callable=task_export,
    )

    # ── T5: Summary log ──────────────────────────────────────────────────────
    summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=task_summary,
    )

    write >> export >> summary
