import sys
import os
import argparse
from datetime import datetime, timedelta, timezone

# Ensure project root is in sys.path
PROJECT_DIR = "/Users/rgchandrasekaraa/Downloads/isaibox"
sys.path.insert(0, PROJECT_DIR)

import db
import scraper_core

def solve():
    parser = argparse.ArgumentParser(description="JIT Link Resolver for Isaibox")
    parser.add_argument("album_url", help="URL of the album")
    parser.add_argument("track_id", help="Track ID or Name to resolve")
    parser.add_argument("--quality", choices=["128", "320"], default="320", help="Audio quality")
    args = parser.parse_args()

    conn = db.get_conn()
    try:
        # 1. Check if we have a fresh link (< 1 hour old)
        # We check the 'updated_at' of the song
        query = """
            SELECT url_128kbps, url_320kbps, updated_at, track_name
            FROM songs 
            WHERE album_url = ? AND (LOWER(track_name) = LOWER(?) OR song_id = ? OR CAST(track_number AS VARCHAR) = ?)
        """
        res = conn.execute(query, (args.album_url, args.track_id, args.track_id, args.track_id)).fetchone()

        if res:
            url_128, url_320, updated_at, song_name = res
            
            # Use timezone-aware comparison
            now = datetime.now(timezone.utc)
            # If the database doesn't have a timezone, assume UTC
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            if now - updated_at < timedelta(hours=1):
                # Fresh enough!
                print(url_320 if args.quality == "320" else url_128)
                return

        # 2. Need to refresh
        album, songs = scraper_core.refresh_single_album(args.album_url)
        
        # Update DB
        db.upsert_album(conn, album)
        db.upsert_songs(conn, songs)
        
        # 3. Get the link again after refresh
        res = conn.execute(query, (args.album_url, args.track_id, args.track_id)).fetchone()
        if res:
            url_128, url_320, updated_at, song_name = res
            print(url_320 if args.quality == "320" else url_128)
        else:
            print(f"Error: Track '{args.track_id}' not found in album after refresh.", file=sys.stderr)
            sys.exit(1)

    finally:
        conn.close()

if __name__ == "__main__":
    solve()
