import asyncio
import logging
import concurrent.futures
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# Import core parsing logic to reuse it
import scraper_core

log = logging.getLogger(__name__)

BASE_URL = "https://www.masstamilan.dev"

# ── Async Fetching ──────────────────────────────────────────────────────────

async def fetch_html(session: AsyncSession, url: str, retries: int = 3) -> str:
    """Fetch HTML using curl_cffi to bypass Cloudflare."""
    for attempt in range(retries):
        try:
            resp = await session.get(url, timeout=30)
            if resp.status_code == 404:
                raise requests.exceptions.HTTPError("404 Not Found", response=resp)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                raise
            if attempt == retries - 1:
                raise
            wait = 2 * (attempt + 1)
            log.warning(f"Fetch failed for {url} ({e}). Retrying in {wait}s...")
            await asyncio.sleep(wait)
    return ""

# ── Parallel Parsing ─────────────────────────────────────────────────────────

def parse_album_worker(html: str, url: str):
    """
    Worker function to be run in a ProcessPoolExecutor.
    Takes HTML string, returns parsed data dicts.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        album, songs = scraper_core.parse_album_page(soup, url)
        return album, songs, None
    except Exception as e:
        return None, None, str(e)

def parse_listing_worker(html: str):
    """Worker for parsing listing pages (A-Z, Years)."""
    try:
        soup = BeautifulSoup(html, "lxml")
        urls = scraper_core.parse_listing_page(soup)
        return urls, None
    except Exception as e:
        return None, str(e)

# ── Discovery ─────────────────────────────────────────────────────────────────

async def discover_urls_async(
    session: AsyncSession,
    executor: concurrent.futures.ProcessPoolExecutor,
    path_pattern: str,
    known_urls: set[str],
    full_scan: bool = False,
    delay: float = 0.5
) -> list[str]:
    """Asynchronously discover URLs from a path pattern (e.g. /tag/A?page={page})."""
    new_urls = []
    page_num = 1
    
    while True:
        url = urljoin(BASE_URL, path_pattern.format(page=page_num))
        log.info(f"  Discovering: {url}")
        
        try:
            html = await fetch_html(session, url)
            # Offload parsing to process pool
            loop = asyncio.get_event_loop()
            page_urls, err = await loop.run_in_executor(executor, parse_listing_worker, html)
            
            if err or not page_urls:
                break
                
            page_new = [u for u in page_urls if u not in known_urls]
            new_urls.extend(page_new)
            
            log.info(f"    Page {page_num}: {len(page_new)} new / {len(page_urls)} total")
            
            # Incremental stop
            if not full_scan and not page_new and known_urls:
                log.info(f"    → Page {page_num} fully known. Stopping discovery.")
                break
                
            page_num += 1
            await asyncio.sleep(delay)
            
        except requests.exceptions.HTTPError as e:
            if "404" in str(e):
                break
            raise
        except Exception:
            break
            
    return new_urls
