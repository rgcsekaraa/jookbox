"""
scraper_core.py — HTTP fetch + HTML parsing (no Airflow dependency)
--------------------------------------------------------------------
Imported by both the Airflow DAG and the standalone CLI.
"""

import re
import time
import logging
from urllib.parse import urljoin, urlparse

import requests
import cloudscraper
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

log = logging.getLogger(__name__)

BASE_URL   = "https://www.masstamilan.dev"
LIST_URL   = BASE_URL + "/tamil-songs?page={page}"

_SESSION = None
_FALLBACK_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


def _looks_blocked(resp: requests.Response) -> bool:
    text = (resp.text or "")[:8000].lower()
    title = ""
    m = re.search(r"<title>(.*?)</title>", text, re.DOTALL)
    if m:
        title = m.group(1).strip().lower()
    blocked_markers = [
        "just a moment",
        "checking your browser",
        "cloudflare",
        "attention required",
        "access denied",
        "captcha",
    ]
    return resp.status_code in {403, 429, 503} or any(marker in text or marker in title for marker in blocked_markers)


def reset_session() -> cloudscraper.CloudScraper:
    global _SESSION
    _SESSION = None
    return get_session()

def get_session() -> cloudscraper.CloudScraper:
    global _SESSION
    if _SESSION is None:
        s = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        s.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": BASE_URL,
            "Origin": BASE_URL,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": _FALLBACK_UAS[0],
        })
        _SESSION = s
    return _SESSION


# ── Fetch with retry ──────────────────────────────────────────────────────────

from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, retry_if_exception

def _is_not_404(e):
    if isinstance(e, requests.exceptions.HTTPError):
        return e.response.status_code != 404
    return True

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    before_sleep=before_sleep_log(log, logging.WARNING),
    retry=retry_if_exception(_is_not_404),
    reraise=True,
)
def fetch(url: str) -> BeautifulSoup:
    session = get_session()
    resp = session.get(url, timeout=25, allow_redirects=True)
    if _looks_blocked(resp):
        log.warning("Blocked/challenged while fetching %s. Resetting session and retrying.", url)
        session = reset_session()
        session.headers["User-Agent"] = _FALLBACK_UAS[int(time.time()) % len(_FALLBACK_UAS)]
        time.sleep(1.2)
        resp = session.get(url, timeout=25, allow_redirects=True)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ── Listing page ──────────────────────────────────────────────────────────────

def get_total_pages(soup: BeautifulSoup) -> int:
    """Read last page number from pagy pagination nav."""
    nav = soup.select_one("nav.pagy")
    if not nav:
        return 1
    max_page = 1
    for a in nav.select("a[href]"):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def parse_listing_page(soup: BeautifulSoup) -> list[str]:
    """Return absolute album URLs from a listing page."""
    urls = []
    selectors = [
        "div.a-i a[href]",
        "article a[href]",
        "main a[href*='-songs']",
        "a[href*='masstamilan.dev/'][href*='-songs']",
        "a[href^='/'][href*='-songs']",
    ]

    for selector in selectors:
        for a in soup.select(selector):
            href = a.get("href", "").strip()
            if not href:
                continue
            href = urljoin(BASE_URL, href)
            path = urlparse(href).path.lower()
            if "/tamil-songs" in path:
                continue
            if not path.endswith("-songs") and "-songs" not in path:
                continue
            if href not in urls:
                urls.append(href)
        if urls:
            break
    return urls


def discover_urls_from_path(
    path_pattern: str,
    known_urls: set[str],
    max_pages: int | None = None,
    delay: float = 1.3,
    full_scan: bool = False,
) -> tuple[list[str], int]:
    """
    Walk listing pages based on a path_pattern (e.g. '/tag/A?page={page}').
    Stops when:
      a) We hit an empty page.
      b) We hit a page fully known (incremental mode).
      c) We hit max_pages limit.
    """
    log.info(f"Starting discovery on pattern: {path_pattern}...")
    new_urls: list[str] = []
    page_num = 1
    pages_processed = 0

    while True:
        if max_pages and pages_processed >= max_pages:
            log.info(f"Reached max_pages limit ({max_pages}).")
            break

        current_url = urljoin(BASE_URL, path_pattern.format(page=page_num))
        log.info(f"  Fetching listing page {page_num}: {current_url}")
        
        try:
            soup = fetch(current_url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                log.info(f"  → Page {page_num} returned 404. Reached end of category.")
                break
            raise
            
        page_urls = parse_listing_page(soup)

        if not page_urls:
            log.info(f"  → Page {page_num} is empty. Reached end of category.")
            break

        page_new = [u for u in page_urls if u not in known_urls]
        new_urls.extend(page_new)

        log.info(f"  Page {page_num}: {len(page_new)} new / {len(page_urls)} total")

        # Incremental logic: stop if we find no new albums on a page,
        # UNLESS we are in full_scan mode.
        if not full_scan and not page_new and known_urls:
            log.info(f"  → All albums on page {page_num} already known. Stopping early.")
            break

        page_num += 1
        pages_processed += 1
        time.sleep(delay)

    log.info(f"Discovery done: {len(new_urls)} total new album(s) discovered.")
    return new_urls, page_num - 1


def discover_new_album_urls(
    start_page: int,
    known_urls: set[str],
    max_pages: int | None = None,
    delay: float = 1.5,
    full_scan: bool = False,
) -> tuple[list[str], int]:
    """Maintain backward compatibility for the 'Latest Updates' discovery."""
    pattern = f"/tamil-songs?page={{page}}"
    # If start_page > 1, we manually adjust the loop inside or just use the pattern
    # But current pattern starts at {page}.
    return discover_urls_from_path(pattern, known_urls, max_pages, delay, full_scan)


# ── Album detail page ─────────────────────────────────────────────────────────

def _text_after(label: str, text: str) -> str:
    """Extract value after 'Label:' up to the next label or end."""
    m = re.search(
        rf"{label}:\s*(.+?)(?=\s+(?:Lyricist|Year|Language|Music|Director|Starring)\s*:|$)",
        text, re.DOTALL | re.IGNORECASE
    )
    return m.group(1).strip() if m else ""


def _clean_movie_name(raw: str) -> str:
    raw = re.sub(
        r"\s+(Tamil|Malayalam|Telugu|Hindi|Kannada)\s+mp3.*$",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(r"\s+songs?\s*(download)?\s*$", "", raw, flags=re.IGNORECASE)
    return raw.strip(" -|")


def _extract_field_links(root: BeautifulSoup, href_part: str) -> list[str]:
    values = []
    for a in root.select("a[href]"):
        href = a.get("href", "")
        txt = a.get_text(" ", strip=True)
        if href_part in href and txt and txt not in values:
            values.append(txt)
    return values


def _parse_song_links(row) -> tuple[str, str]:
    url_128 = ""
    url_320 = ""

    for dl in row.select("a[href]"):
        href = dl.get("href", "").strip()
        if not href:
            continue
        abs_href = urljoin(BASE_URL, href)
        title = " ".join(
            part for part in [
                dl.get("title", ""),
                dl.get_text(" ", strip=True),
                " ".join(dl.get("class", [])),
            ] if part
        ).lower()

        if "320" in title or "320" in abs_href:
            url_320 = url_320 or abs_href
        elif "128" in title or "128" in abs_href:
            url_128 = url_128 or abs_href

    return url_128, url_320


def parse_album_page(soup: BeautifulSoup, album_url: str) -> tuple[dict, list[dict]]:
    """
    Returns:
        album  — dict matching albums table columns
        songs  — list of dicts matching songs table columns
    """
    album = {
        "album_url":     album_url,
        "movie_name":    "",
        "starring":      "",
        "music_director": "",
        "director":      "",
        "lyricists":     "",
        "year":          "",
        "language":      "Tamil",
        "track_count":   0,
        "scrape_ok":     True,
    }

    # ── Movie metadata from fieldset ──
    fieldset = soup.select_one("fieldset") or soup.select_one("main fieldset") or soup.select_one("article fieldset")
    if fieldset:
        raw_text = fieldset.get_text(" ", strip=True)

        starring_links = _extract_field_links(fieldset, "/artist/")
        music_links = _extract_field_links(fieldset, "/music/")
        year_links = _extract_field_links(fieldset, "/browse-by-year/")
        language_links = _extract_field_links(fieldset, "/tamil-songs")

        album["starring"]       = ", ".join(starring_links)
        album["music_director"] = ", ".join(music_links)
        album["year"]           = year_links[0] if year_links else ""
        album["director"]       = _text_after("Director",  raw_text)
        album["lyricists"]      = _text_after("Lyricist",  raw_text)
        if language_links:
            album["language"] = language_links[0]

    # ── Movie name from h1 ──
    h1 = soup.select_one("h1") or soup.select_one("main h1") or soup.select_one("article h1")
    if h1:
        album["movie_name"] = _clean_movie_name(h1.get_text(" ", strip=True))

    # ── Tracks ──
    songs = []
    table = soup.select_one("table#tl") or soup.select_one("table")
    rows = []
    if table:
        rows = table.select("tr[itemprop='itemListElement']") or table.select("tbody tr") or table.select("tr")
    if not rows:
        rows = soup.select("tr[itemprop='itemListElement']") or soup.select("a.dlink[href]")

    if rows:
        for fallback_index, row in enumerate(rows, 1):
            if getattr(row, "name", None) == "a":
                continue
            song: dict = {
                "album_url":     album_url,
                "movie_name":    album["movie_name"],
                "music_director": album["music_director"],
                "director":      album["director"],
                "year":          album["year"],
                "track_number":  0,
                "track_name":    "",
                "singers":       "",
                "url_128kbps":   "",
                "url_320kbps":   "",
            }

            pos = row.select_one("span[itemprop='position']")
            if pos:
                try:
                    song["track_number"] = int(pos.get_text(strip=True))
                except ValueError:
                    pass
            elif fallback_index:
                song["track_number"] = fallback_index

            name_el = (
                row.select_one("span[itemprop='name']")
                or row.select_one("td:nth-of-type(2)")
                or row.select_one("strong")
                or row.select_one("b")
            )
            if name_el:
                song["track_name"] = name_el.get_text(" ", strip=True)

            artist_el = row.select_one("span[itemprop='byArtist']") or row.select_one("td:nth-of-type(3)")
            if artist_el:
                song["singers"] = artist_el.get_text(" ", strip=True)

            song["url_128kbps"], song["url_320kbps"] = _parse_song_links(row)

            if not song["track_name"] and (song["url_128kbps"] or song["url_320kbps"]):
                text_cells = [td.get_text(" ", strip=True) for td in row.select("td")]
                if len(text_cells) >= 2:
                    song["track_name"] = text_cells[1]
                elif text_cells:
                    song["track_name"] = text_cells[0]

            if song["track_name"] or song["url_128kbps"] or song["url_320kbps"]:
                songs.append(song)

    album["track_count"] = len(songs)
    return album, songs


# ── Batch scrape a list of album URLs ─────────────────────────────────────────

def scrape_albums(
    album_urls: list[str],
    delay: float = 0.9,
) -> tuple[list[dict], list[dict], list[str]]:
    """
    Scrape each album URL.
    Returns (albums, all_songs, failed_urls).
    """
    albums, all_songs, failed = [], [], []

    for idx, url in enumerate(album_urls, 1):
        log.info(f"  [{idx}/{len(album_urls)}] {url}")
        try:
            soup = fetch(url)
            album, songs = parse_album_page(soup, url)
            albums.append(album)
            all_songs.extend(songs)
            log.info(f"    → {album['movie_name']} | {len(songs)} track(s)")
        except Exception as e:
            log.error(f"    FAILED: {e}")
            failed.append(url)

        if idx < len(album_urls):
            time.sleep(delay)

    return albums, all_songs, failed


def refresh_single_album(album_url: str) -> tuple[dict, list[dict]]:
    """
    Fetch and parse a single album page for fresh links.
    Returns (album_dict, songs_list).
    """
    soup = fetch(album_url)
    return parse_album_page(soup, album_url)
