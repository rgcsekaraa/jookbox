import os
import sys
import logging

# Ensure project root is in sys.path
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

import db
import scraper_core

import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="isaibox standalone scraper")
    parser.add_argument("--mode", choices=["latest", "alphabet", "year", "all"], default="latest", 
                        help="Discovery mode: latest updates, alphabet index, yearly index, or everything.")
    parser.add_argument("--letter", help="Specific letter for alphabet mode (e.g., A, 0-9)")
    parser.add_argument("--year", type=int, help="Specific year for year mode (e.g., 2024)")
    parser.add_argument("--full", action="store_true", help="Scrape all discovered items (ignore batch limits)")
    parser.add_argument("--delay", type=float, default=1.3, help="Delay between page fetches")
    args = parser.parse_args()

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"  isaibox standalone scraper — MODE: {args.mode.upper()}")
    if args.letter: logger.info(f"  Target Letter: {args.letter}")
    if args.year:   logger.info(f"  Target Year  : {args.year}")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    conn = None
    try:
        # 1. Setup DB / Get Known URLs
        logger.info("[1/4] Connecting to DuckDB...")
        conn = db.get_conn()
        known = db.get_known_album_urls(conn)
        conn.close() 

        # 2. Discover URLs based on mode
        logger.info(f"[2/4] Discovering albums using '{args.mode}' mode...")
        discovery_targets = []

        if args.mode == "latest":
            discovery_targets.append("/tamil-songs?page={page}")
        
        if args.mode in ["alphabet", "all"]:
            letters = [args.letter] if args.letter else ["0-9"] + list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            for lit in letters:
                discovery_targets.append(f"/tag/{lit}?page={{page}}")
        
        if args.mode in ["year", "all"]:
            years = [args.year] if args.year else range(2026, 1951, -1)
            for y in years:
                discovery_targets.append(f"/browse-by-year/{y}?page={{page}}")

        new_urls = []
        for pattern in discovery_targets:
            urls, _ = scraper_core.discover_urls_from_path(
                path_pattern=pattern,
                known_urls=known,
                full_scan=args.full,
                delay=args.delay
            )
            for u in urls:
                if u not in new_urls and u not in known:
                    new_urls.append(u)

        if not new_urls:
            logger.info("✅  No new albums discovered. Database is up to date.")
            return

        # 3. Scrape batch
        if args.full:
            to_scrape = new_urls
            logger.info(f"[3/4] Scraping ALL {len(to_scrape)} discovered albums...")
        else:
            batch_size = 100
            to_scrape = new_urls[:batch_size]
            logger.info(f"[3/4] Scraping first {len(to_scrape)} discovered albums...")
        
        # Scrape in chunks to save progressively
        chunk_size = 20
        for i in range(0, len(to_scrape), chunk_size):
            chunk = to_scrape[i:i + chunk_size]
            logger.info(f"   Batch {i//chunk_size + 1}: Processing {len(chunk)} albums...")
            
            albums, songs, failed = scraper_core.scrape_albums(chunk, delay=args.delay)

            # 4. Save results
            batch_conn = db.get_conn()
            try:
                for a in albums:
                    db.upsert_album(batch_conn, a)
                if songs:
                    db.upsert_songs(batch_conn, songs)
                for url in failed:
                    db.mark_album_failed(batch_conn, url)
            finally:
                batch_conn.close()
            
            if i + chunk_size < len(to_scrape):
                import time
                time.sleep(1.5)

        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info("  ✅  Scrape Complete!")
        # Re-open for final stats
        final_conn = db.get_conn(read_only=True)
        db.print_stats(final_conn)
        final_conn.close()
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
    except Exception as e:
        logger.error(f"❌  Standalone scraper failed: {e}")
    finally:
        if conn:
            try: conn.close()
            except: pass

if __name__ == "__main__":
    main()
