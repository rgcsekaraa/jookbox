import os
import sys
import asyncio
import logging
import argparse
import time
import concurrent.futures
from datetime import datetime, timezone

from curl_cffi.requests import AsyncSession

# Ensure project root is in sys.path
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_DIR)

import db
import async_scraper
import scraper_core

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ── DB Writer Task ───────────────────────────────────────────────────────────

async def db_writer_task(queue: asyncio.Queue):
    """
    Consumer task that receives results and writes them to DuckDB sequentially.
    This prevents 'Database is locked' errors from multiple writers.
    """
    logger.info("  [DB] Writer task started.")
    
    while True:
        # Wait for a batch of results
        batch = await queue.get()
        if batch is None: # Sentinel value to stop
            break
            
        albums, all_songs, failed_urls = batch
        
        # Connect to DuckDB just for this write
        conn = db.get_conn()
        try:
            for album in albums:
                db.upsert_album(conn, album)
            
            if all_songs:
                db.upsert_songs(conn, all_songs)
                
            for url in failed_urls:
                db.mark_album_failed(conn, url)
                
            logger.info(f"  [DB] Saved {len(albums)} albums / {len(all_songs)} songs.")
        except Exception as e:
            logger.error(f"  [DB] Write error: {e}")
        finally:
            conn.close()
            
        queue.task_done()
    
    logger.info("  [DB] Writer task finished.")

# ── Scraper Worker Tasks ─────────────────────────────────────────────────────

async def scraper_worker(
    worker_id: int, 
    session: AsyncSession, 
    executor: concurrent.futures.ProcessPoolExecutor,
    input_queue: asyncio.Queue, 
    output_queue: asyncio.Queue,
    delay: float = 0.5
):
    """
    Worker task that fetches and parses albums.
    """
    logger.info(f"  [Worker {worker_id}] Started.")
    
    while True:
        url = await input_queue.get()
        if url is None: # Sentinel
            break
            
        try:
            # 1. Fetch HTML (impersonating Chrome to bypass Cloudflare)
            html = await async_scraper.fetch_html(session, url)
            
            # 2. Parse HTML (offload CPU-heavy soup to Process Pool)
            loop = asyncio.get_event_loop()
            album, songs, err = await loop.run_in_executor(executor, async_scraper.parse_album_worker, html, url)
            
            if err:
                logger.error(f"  [Worker {worker_id}] Error parsing {url}: {err}")
                await output_queue.put(([], [], [url]))
            else:
                # 3. Queue results for the DB writer
                await output_queue.put(([album], songs, []))
                
        except Exception as e:
            logger.error(f"  [Worker {worker_id}] Failed {url}: {e}")
            await output_queue.put(([], [], [url]))
        finally:
            input_queue.task_done()
            await asyncio.sleep(delay)

    logger.info(f"  [Worker {worker_id}] Finished.")

# ── Orchestrator ─────────────────────────────────────────────────────────────

async def turbo_scrape():
    parser = argparse.ArgumentParser(description="isaibox High-Speed Turbo Scraper")
    parser.add_argument("--mode", choices=["latest", "alphabet", "year", "all"], default="latest")
    parser.add_argument("--letter", help="Letter for alphabet mode")
    parser.add_argument("--year", type=int, help="Year for year mode")
    parser.add_argument("--concurrency", type=int, default=8, help="Number of workers (max 20)")
    parser.add_argument("--delay", type=float, default=0.2, help="Worker delay between fetches")
    parser.add_argument("--full", action="store_true", help="Scrape all discovered items")
    args = parser.parse_args()

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info(f"  isaibox TURBO SCRAPER — MODE: {args.mode.upper()}")
    logger.info(f"  Concurrency: {args.concurrency} | Delay: {args.delay}s")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━█")

    # 1. Initialize Pools and Queues
    input_queue = asyncio.Queue()
    db_queue = asyncio.Queue()
    
    process_executor = concurrent.futures.ProcessPoolExecutor(max_workers=min(os.cpu_count(), 4))
    
    # 2. Setup DB / Get Known URLs
    conn = db.get_conn()
    known = db.get_known_album_urls(conn)
    conn.close()

    # 3. Discovery Phase
    logger.info("[1/4] Discovery started...")
    
    async with AsyncSession(impersonate="chrome120") as session:
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
            urls = await async_scraper.discover_urls_async(
                session, process_executor, pattern, known, full_scan=args.full, delay=0.8
            )
            for u in urls:
                if u not in new_urls and u not in known:
                    new_urls.append(u)

    if not new_urls:
        logger.info("✅  No new albums discovered. Catalog up to date.")
        process_executor.shutdown()
        return

    # 4. Scraping Phase
    logger.info(f"[2/4] Scrape Phase started for {len(new_urls)} albums...")
    
    # Start DB writer
    writer_task = asyncio.create_task(db_writer_task(db_queue))
    
    # Start workers
    async with AsyncSession(impersonate="chrome120") as session:
        workers = []
        for i in range(args.concurrency):
            w = asyncio.create_task(scraper_worker(i, session, process_executor, input_queue, db_queue, delay=args.delay))
            workers.append(w)
            
        # Feed all discovered URLs into workers
        for url in new_urls:
            await input_queue.put(url)
            
        # Wait for workers to finish
        await input_queue.join()
        
        # Stop workers (sentinel)
        for _ in range(args.concurrency):
            await input_queue.put(None)
        await asyncio.gather(*workers)

    # 5. Finalize
    await db_queue.join()
    await db_queue.put(None) # Stop writer
    await writer_task
    
    process_executor.shutdown()
    
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  🚀  Turbo Scrape Complete!")
    final_conn = db.get_conn(read_only=True)
    db.print_stats(final_conn)
    final_conn.close()
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

if __name__ == "__main__":
    try:
        asyncio.run(turbo_scrape())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"FATAL: {e}")
