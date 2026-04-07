#!/usr/bin/env python3
"""
refresh_links.py — Update expiring download URLs for existing albums.
----------------------------------------------------------------------
Source download links rotate/expire periodically. This script
re-scrapes the album pages to refresh the signed tokens in DuckDB.
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone

import db
import scraper_core

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-5s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("refresher")

def refresh_batch(album_urls):
    """Fetch latest HTML for a batch of albums and update their songs in DB."""
    conn = db.get_conn()
    try:
        updated_count = 0
        for i, url in enumerate(album_urls):
            try:
                logger.info(f"  [{i+1}/{len(album_urls)}] Refreshing: {url}")
                
                # 1. Fetch live page
                soup = scraper_core.fetch(url)
                
                # 2. Parse latest URLs
                album_data, songs = scraper_core.parse_album_page(soup, url)
                
                if songs:
                    # 3. Upsert to DB (overwrites old URLs + updates updated_at)
                    db.upsert_songs(conn, songs)
                    # Also update album timestamp
                    db.upsert_album(conn, album_data)
                    updated_count += 1
                else:
                    logger.warning(f"     ⚠️  No tracks found for {url}")

                # Politeness delay
                if i < len(album_urls) - 1:
                    time.sleep(1.5)

            except Exception as e:
                logger.error(f"     ❌ Failed to refresh {url}: {e}")

        return updated_count
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Refresh expiring download URLs in DuckDB.")
    parser.add_argument("--all", action="store_true", help="Refresh ALL albums in the database (caution: takes hours)")
    parser.add_argument("--year", type=str, help="Refresh albums from a specific year (e.g. 2025)")
    parser.add_argument("--limit", type=int, default=100, help="Number of recent albums to refresh (default: 100)")
    parser.add_argument("--dry-run", action="store_true", help="Just list what would be refreshed")
    
    args = parser.parse_args()

    # 1. Get targets
    conn = db.get_conn(read_only=True)
    query = "SELECT album_url, movie_name FROM albums WHERE scrape_ok = TRUE"
    params = []

    if args.year:
        query += " AND year = ?"
        params.append(args.year)
    
    if args.all:
        query += " ORDER BY updated_at DESC"
    else:
        query += f" ORDER BY updated_at DESC LIMIT {args.limit}"
    
    targets = conn.execute(query, params).fetchall()
    conn.close()

    if not targets:
        logger.info("No albums found matching criteria.")
        return

    logger.info(f"Targeting {len(targets)} albums for link refresh...")
    if args.dry_run:
        for url, name in targets:
            logger.info(f"  DRY RUN: Would refresh {name} ({url})")
        return

    # 2. Process in chunks of 20 to release locks
    chunk_size = 20
    total_updated = 0
    
    for i in range(0, len(targets), chunk_size):
        chunk = [t[0] for t in targets[i : i + chunk_size]]
        logger.info(f"Processing chunk {i//chunk_size + 1} ({len(chunk)} albums)...")
        
        count = refresh_batch(chunk)
        total_updated += count
        
        # Release OS file lock briefly
        if i + chunk_size < len(targets):
            logger.info("  Pausing 2s to release DB lock...")
            time.sleep(2)

    logger.info(f"✅ Finished! Refreshed links for {total_updated} albums.")

if __name__ == "__main__":
    main()
