from __future__ import annotations

import base64
from contextvars import ContextVar
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import hmac
import inspect
import json
import logging
import os
import re
import secrets
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request as FastAPIRequest
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
import uvicorn

import db
import scraper_core
from storage_backends import get_shared_cache


ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT / "dist"
CACHE_DIR = ROOT / ".cache" / "audio"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_local_env()

_request_var: ContextVar["RequestShim | None"] = ContextVar("request_var", default=None)


class RequestShim:
    def __init__(self, scope_request: FastAPIRequest, json_body: Any = None):
        self._scope_request = scope_request
        self.headers = scope_request.headers
        self.cookies = scope_request.cookies
        self.args = scope_request.query_params
        self._json_body = json_body

    def get_json(self, silent: bool = False):
        if self._json_body is not None:
            return self._json_body
        if silent:
            return None
        raise ValueError("No JSON body available")


class RequestProxy:
    def __getattr__(self, name: str):
        current = _request_var.get()
        if current is None:
            raise RuntimeError("Request context unavailable")
        return getattr(current, name)


request = RequestProxy()


def has_request_context() -> bool:
    return _request_var.get() is not None


def json_response(payload: Any) -> JSONResponse:
    return JSONResponse(payload)


def file_response(path: Path | str, mimetype: str | None = None, conditional: bool = True, etag: bool = True, max_age: int | None = None):
    response = FileResponse(path, media_type=mimetype)
    if max_age is not None:
        response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return response


def directory_file_response(directory: Path | str, path: str):
    return FileResponse(Path(directory) / path)


def stream_generator(generator):
    return generator


def _translate_path(path: str) -> str:
    return re.sub(r"<(?:(\w+):)?(\w+)>", lambda match: "{" + match.group(2) + (":path" if match.group(1) == "path" else "") + "}", path)


def _normalize_response(result: Any):
    status_code = None
    headers = None
    if isinstance(result, tuple):
        if len(result) == 2:
            result, status_code = result
        elif len(result) == 3:
            result, status_code, headers = result
    if isinstance(result, Response):
        response = result
    elif isinstance(result, (dict, list)):
        response = JSONResponse(result)
    elif isinstance(result, (str, bytes)):
        response = Response(content=result)
    else:
        response = result
    if isinstance(response, Response):
        if status_code is not None:
            response.status_code = status_code
        if headers:
            for key, value in headers.items():
                response.headers[key] = value
    return response


class IsaiboxAPI(FastAPI):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("isaibox")

    def _register_legacy_handler(self, method: str, path: str, fn):
        fastapi_path = _translate_path(path)

        async def endpoint(_request: FastAPIRequest):
            json_body = None
            body = await _request.body()
            if body:
                try:
                    json_body = json.loads(body)
                except Exception:
                    json_body = None
            token = _request_var.set(RequestShim(_request, json_body))
            try:
                if inspect.iscoroutinefunction(fn):
                    result = await fn(**_request.path_params)
                else:
                    result = fn(**_request.path_params)
                return _normalize_response(result)
            finally:
                _request_var.reset(token)

        endpoint.__name__ = fn.__name__
        self.add_api_route(fastapi_path, endpoint, methods=[method], name=fn.__name__)
        return fn

    def get(self, path: str):
        return lambda fn: self._register_legacy_handler("GET", path, fn)

    def post(self, path: str):
        return lambda fn: self._register_legacy_handler("POST", path, fn)

    def put(self, path: str):
        return lambda fn: self._register_legacy_handler("PUT", path, fn)

    def delete(self, path: str):
        return lambda fn: self._register_legacy_handler("DELETE", path, fn)

    def run(self, host: str = "127.0.0.1", port: int = 8000, debug: bool = False):
        uvicorn.run(self, host=host, port=port, log_level="debug" if debug else "info")


app = IsaiboxAPI()
download_lock = threading.Lock()
active_downloads: set[str] = set()
status_cache: dict[str, dict] = {}
song_row_cache: dict[str, dict] = {}
song_cache_lock = threading.Lock()
refresh_lock = threading.Lock()
refreshing_albums: set[str] = set()
UPSTREAM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "audio/mpeg,audio/*;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.masstamilan.dev",
    "Referer": "https://www.masstamilan.dev/",
    "Connection": "keep-alive",
}
SESSION_COOKIE_NAME = "isaibox_session"
SESSION_TTL_DAYS = 30
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
SPOTIFY_REDIRECT_URI = os.environ.get("SPOTIFY_REDIRECT_URI", "")
SPOTIFY_SCOPES = "user-library-read playlist-read-private"
_GEMINI_KEYS_RAW = os.environ.get("GEMINI_API_KEYS", "") or os.environ.get("GEMINI_API_KEY", "")
GEMINI_API_KEYS = [key.strip() for key in re.split(r"[\n,]+", _GEMINI_KEYS_RAW) if key.strip()]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
SESSION_SECRET = os.environ.get("ISAIBOX_SESSION_SECRET", "isaibox-dev-session-secret")
ADMIN_EMAILS = {
    email.strip().lower()
    for email in os.environ.get("ISAIBOX_ADMIN_EMAILS", "").split(",")
    if email.strip()
}
SPOTIFY_PLAYLIST_RE = re.compile(r"(?:open\.spotify\.com/playlist/|spotify:playlist:)([A-Za-z0-9]+)")
library_match_cache: list[dict] = []
library_match_lock = threading.Lock()
AIRFLOW_HOME = ROOT / "airflow_home"
VENV_BIN = ROOT / "venv" / "bin"
shared_cache = get_shared_cache()
HEALTHY_STATUS_TTL = timedelta(minutes=10)
UNAVAILABLE_STATUS_TTL = timedelta(minutes=2)
NETWORK_STATUS_TTL = timedelta(seconds=20)
RADIO_STATION_COUNT = 25
RADIO_STATION_SONG_COUNT = 100
RADIO_STATION_CACHE_TTL = timedelta(hours=12)
RADIO_SHARED_SONG_SECONDS = 240
AI_PLAYLIST_COUNT = 8
AI_PLAYLIST_SONG_COUNT = 50
SPOTIFY_PUBLIC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://open.spotify.com/",
    "Origin": "https://open.spotify.com",
}
radio_station_cache: dict[str, object] = {}
radio_station_lock = threading.Lock()


def get_read_conn():
    return db.get_conn(read_only=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_cached_status(song_id: str) -> dict | None:
    cached = status_cache.get(song_id)
    if not cached:
        return None
    expires_at = cached.get("_expires_at")
    if expires_at and expires_at <= now_utc():
        status_cache.pop(song_id, None)
        return None
    return {
        "status": cached["status"],
        "label": cached["label"],
    }


def cache_status(song_id: str, result: dict) -> dict:
    status = result.get("status", "network")
    ttl = NETWORK_STATUS_TTL
    if status == "healthy":
        ttl = HEALTHY_STATUS_TTL
    elif status == "unavailable":
        ttl = UNAVAILABLE_STATUS_TTL
    status_cache[song_id] = {
        "status": status,
        "label": result.get("label", "orange"),
        "_expires_at": now_utc() + ttl,
    }
    return {
        "status": status,
        "label": result.get("label", "orange"),
    }


def normalize_text(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", (value or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def split_people(value: str) -> list[str]:
    if not value:
        return []
    parts = re.split(r",|/|&|\band\b", value, flags=re.IGNORECASE)
    return [part.strip() for part in parts if part and part.strip()]


def parse_year_value(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(19|20)\d{2}", str(value))
    return int(match.group(0)) if match else None


def get_radio_library_rows() -> list[dict]:
    with get_read_conn() as conn:
        rows = conn.execute(
            """
            SELECT song_id, movie_name, track_name, singers, music_director, year, updated_at
            FROM songs
            WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
            ORDER BY TRY_CAST(year AS INTEGER) DESC NULLS LAST, movie_name, track_name
            """
        ).fetchall()

    songs = []
    for row in rows:
        songs.append(
            {
                "id": row[0],
                "movie": row[1] or "",
                "track": row[2] or "",
                "singers": row[3] or "",
                "music_director": row[4] or "",
                "year": row[5] or "",
                "year_int": parse_year_value(row[5]),
                "updated_at": row[6].isoformat() if row[6] else "",
                "track_norm": normalize_text(row[2] or ""),
                "movie_norm": normalize_text(row[1] or ""),
                "singers_norm": normalize_text(row[3] or ""),
                "music_director_norm": normalize_text(row[4] or ""),
            }
        )
    return songs


def get_radio_library_signature() -> str:
    with get_read_conn() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*), MAX(updated_at)
            FROM songs
            WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
            """
        ).fetchone()
    return f"{row[0] or 0}:{row[1].isoformat() if row and row[1] else ''}"


def _top_counts(values: list[str], limit: int = 20) -> list[dict]:
    counts: dict[str, int] = {}
    for value in values:
        normalized = value.strip()
        if not normalized:
            continue
        counts[normalized] = counts.get(normalized, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    return [{"name": name, "count": count} for name, count in ranked[:limit]]


def build_radio_catalog_snapshot(songs: list[dict]) -> dict:
    years = _top_counts([str(song["year_int"]) for song in songs if song.get("year_int")], limit=40)
    artists = _top_counts([name for song in songs for name in split_people(song["singers"])], limit=30)
    directors = _top_counts([name for song in songs for name in split_people(song["music_director"])], limit=25)
    movies = _top_counts([song["movie"] for song in songs if song["movie"]], limit=30)
    step = max(1, len(songs) // 250)
    samples = [
        {
            "id": song["id"],
            "track": song["track"],
            "movie": song["movie"],
            "singers": song["singers"],
            "musicDirector": song["music_director"],
            "year": song["year_int"] or song["year"],
        }
        for index, song in enumerate(songs)
        if index % step == 0
    ][:250]
    return {
        "songCount": len(songs),
        "years": years,
        "topArtists": artists,
        "topDirectors": directors,
        "topMovies": movies,
        "sampleSongs": samples,
    }


def build_playlist_catalog_snapshot(songs: list[dict]) -> dict:
    snapshot = build_radio_catalog_snapshot(songs)
    snapshot["sampleSongs"] = snapshot.get("sampleSongs", [])[:180]
    return snapshot


def _extract_json_object(text: str) -> dict | None:
    if not text:
        return None
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return None
    return None


def generate_gemini_json(system_instruction: str, user_prompt: str, task_name: str, candidate_count: int = 1) -> dict:
    if not GEMINI_API_KEYS:
        raise RuntimeError("Gemini API keys not configured")

    def attempt(api_key: str) -> dict:
        response = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "system_instruction": {"parts": [{"text": system_instruction}]},
                "contents": [{"parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": 0.7,
                    "topP": 0.9,
                    "candidateCount": candidate_count,
                    "responseMimeType": "application/json",
                },
            },
            timeout=90,
        )
        response.raise_for_status()
        payload = response.json()
        texts = []
        for candidate in payload.get("candidates", []):
            text = ""
            for part in candidate.get("content", {}).get("parts", []):
                text += part.get("text", "")
            if text.strip():
                texts.append(text)
        for text in texts:
            parsed = _extract_json_object(text)
            if isinstance(parsed, dict):
                return parsed
        raise RuntimeError(f"{task_name}: Gemini returned invalid JSON")

    errors = []
    workers = min(len(GEMINI_API_KEYS), 4)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(attempt, api_key) for api_key in GEMINI_API_KEYS[:workers]]
        for future in as_completed(futures):
            try:
                return future.result()
            except Exception as exc:
                errors.append(str(exc))
    raise RuntimeError(errors[-1] if errors else f"{task_name}: Gemini failed")


def fallback_radio_blueprints(snapshot: dict) -> list[dict]:
    top_artists = [item["name"] for item in snapshot.get("topArtists", [])[:12]]
    top_directors = [item["name"] for item in snapshot.get("topDirectors", [])[:10]]
    templates = [
        ("night-drive", "Night Drive", "Late-night Tamil melodies with roadlight glow.", 1997, 2026, ["night", "drive", "melody"], top_artists[:3], top_directors[:2]),
        ("mazhai-seat", "Mazhai Seat", "Rain-window songs with soft ache and easy replay.", 1997, 2026, ["rain", "mazhai", "soft"], top_artists[2:5], top_directors[1:3]),
        ("tea-kadai", "Tea Kadai", "Casual familiar tunes that feel like an evening stop.", 1998, 2024, ["warm", "casual", "friends"], top_artists[4:7], top_directors[2:4]),
        ("moon-road", "Moon Road", "Cooler melodies for long quiet loops.", 2000, 2026, ["moon", "night", "cool"], top_artists[1:4], top_directors[:2]),
        ("bus-window", "Bus Window", "Moving-city songs with old-new crossover nostalgia.", 1997, 2014, ["travel", "nostalgia", "city"], top_artists[5:8], top_directors[3:5]),
        ("first-bench", "First Bench", "Playful clean hits with easy hooks.", 2002, 2026, ["light", "fun", "hook"], top_artists[0:3], top_directors[4:6]),
        ("pattu-note", "Pattu Note", "Big melody lines and strong chorus memory.", 1997, 2012, ["melody", "chorus", "classic"], top_artists[3:6], top_directors[0:2]),
        ("sunday-slow", "Sunday Slow", "Unhurried songs for an open afternoon.", 1999, 2026, ["slow", "breeze", "mellow"], top_artists[2:5], top_directors[5:7]),
        ("auto-stand", "Auto Stand", "Street-corner replay energy with crowd-pleasing rhythm.", 2000, 2026, ["street", "popular", "rhythm"], top_artists[6:9], top_directors[1:3]),
        ("thendral-mix", "Thendral Mix", "Airy romantic cuts from across eras.", 1997, 2026, ["breeze", "romance", "airy"], top_artists[:4], top_directors[2:5]),
        ("signal-green", "Signal Green", "Forward-moving songs with immediate lift.", 2004, 2026, ["move", "lift", "bright"], top_artists[1:4], top_directors[3:6]),
        ("midnight-note", "Midnight Note", "Low-key loop built for headphones.", 1997, 2026, ["midnight", "headphones", "soft"], top_artists[4:7], top_directors[0:3]),
        ("blue-shirt", "Blue Shirt", "Simple charming songs with a slightly quirky edge.", 1999, 2023, ["charm", "quirky", "simple"], top_artists[5:8], top_directors[2:4]),
        ("paper-rocket", "Paper Rocket", "Bright youthful tracks with quick replay value.", 2006, 2026, ["young", "bright", "quick"], top_artists[0:4], top_directors[4:7]),
        ("long-call", "Long Call", "Melodic heartline songs with steady emotional pull.", 1997, 2026, ["heart", "call", "melody"], top_artists[2:6], top_directors[:3]),
        ("rose-milk", "Rose Milk", "Sweet gentle cuts with low-stress flow.", 2000, 2026, ["sweet", "gentle", "soft"], top_artists[3:7], top_directors[3:6]),
        ("fm-gold", "FM Gold", "Familiar radio staples from the melody-heavy years.", 1997, 2010, ["fm", "familiar", "gold"], top_artists[:5], top_directors[:4]),
        ("lift-lobby", "Lift Lobby", "Polished modern songs with calm surface and motion.", 2012, 2026, ["modern", "smooth", "polished"], top_artists[4:8], top_directors[5:8]),
        ("river-side", "River Side", "Flowing softer songs with wide-open mood.", 1997, 2026, ["flow", "open", "mellow"], top_artists[1:5], top_directors[1:4]),
        ("soft-cut", "Soft Cut", "Tender soundtrack picks that stay out of the way.", 2001, 2026, ["tender", "soundtrack", "soft"], top_artists[5:9], top_directors[2:5]),
        ("brick-radio", "Brick Radio", "Solid catchy songs with repeat-friendly shape.", 1998, 2026, ["catchy", "solid", "repeat"], top_artists[:4], top_directors[4:6]),
        ("sun-after-rain", "Sun After Rain", "Hopeful melodic turns after a moody stretch.", 1997, 2026, ["hope", "rain", "lift"], top_artists[2:5], top_directors[0:2]),
        ("mirror-lane", "Mirror Lane", "Reflective songs with just enough shine.", 2003, 2026, ["reflective", "shine", "night"], top_artists[3:6], top_directors[1:4]),
        ("quiet-fire", "Quiet Fire", "Emotion-first tracks with restrained intensity.", 1997, 2026, ["emotion", "intense", "restrained"], top_artists[1:5], top_directors[3:5]),
        ("city-breeze", "City Breeze", "Urban-feeling melodies that stay light on their feet.", 2005, 2026, ["urban", "breeze", "light"], top_artists[4:7], top_directors[2:5]),
    ]
    return [
        {
            "id": slug,
            "name": name,
            "blurb": blurb,
            "yearStart": year_start,
            "yearEnd": year_end,
            "includeArtists": artists,
            "includeDirectors": directors,
            "includeMovies": [],
            "keywords": keywords,
        }
        for slug, name, blurb, year_start, year_end, keywords, artists, directors in templates[:RADIO_STATION_COUNT]
    ]


def fallback_ai_playlist_blueprints(snapshot: dict) -> list[dict]:
    top_artists = [item["name"] for item in snapshot.get("topArtists", [])[:10]]
    top_directors = [item["name"] for item in snapshot.get("topDirectors", [])[:8]]
    templates = [
        ("Late Bus", "Warm melody picks for the ride back home.", 1997, 2026, ["night", "melody", "city"], top_artists[:3], top_directors[:2]),
        ("Rain Shelf", "Soft rain-window songs with gentle replay energy.", 1997, 2026, ["rain", "soft", "ache"], top_artists[2:5], top_directors[1:3]),
        ("Sunday Tea", "Easy familiar songs for a slow, bright afternoon.", 1999, 2026, ["easy", "warm", "weekend"], top_artists[4:7], top_directors[2:4]),
        ("Signal Cut", "Popular rhythm-led tracks with clean momentum.", 2004, 2026, ["popular", "rhythm", "move"], top_artists[:4], top_directors[3:5]),
        ("Old Letter", "Nostalgic melody-first picks from the deeper catalog.", 1997, 2012, ["nostalgia", "melody", "classic"], top_artists[3:6], top_directors[:3]),
        ("Blue Bench", "Low-key romance songs that stay light and memorable.", 2000, 2026, ["romance", "light", "calm"], top_artists[1:4], top_directors[2:5]),
        ("City Soft", "Modern polished soundtrack songs with smooth pacing.", 2011, 2026, ["modern", "smooth", "urban"], top_artists[5:8], top_directors[4:6]),
        ("FM Loop", "Replay-friendly staples that feel like a good radio hour.", 1997, 2026, ["fm", "familiar", "replay"], top_artists[:5], top_directors[:4]),
    ]
    return [
        {
            "name": name,
            "blurb": blurb,
            "yearStart": year_start,
            "yearEnd": year_end,
            "includeArtists": artists,
            "includeDirectors": directors,
            "includeMovies": [],
            "keywords": keywords,
        }
        for name, blurb, year_start, year_end, keywords, artists, directors in templates[:AI_PLAYLIST_COUNT]
    ]


def generate_ai_playlist_blueprints_with_gemini(snapshot: dict) -> list[dict]:
    system_instruction = (
        "You create Tamil music playlist blueprints from library metadata. "
        "Return valid JSON only. Create exactly 8 distinct playlists. "
        "Use simple, local-sounding names. Keep blurbs short. "
        "Choose year spans and artist/director anchors broad enough to fill 50 songs each."
    )
    user_prompt = (
        "Using the catalog snapshot below, return JSON in this shape:\n"
        '{ "playlists": [ { "name": "Late Bus", "blurb": "Warm melody picks", "yearStart": 1997, "yearEnd": 2026, '
        '"includeArtists": ["Artist"], "includeDirectors": ["Director"], "includeMovies": ["Movie"], "keywords": ["night", "melody"] } ] }\n'
        "Rules:\n"
        "- Exactly 8 playlists.\n"
        "- Names should be simple and memorable.\n"
        "- Use 2 to 6 includeArtists, 1 to 4 includeDirectors, 0 to 4 includeMovies, and 2 to 6 keywords.\n"
        "- Keep yearStart/yearEnd between 1997 and 2026.\n"
        "- Focus on melody, replayability, rain, nostalgia, soft romance, urban polish, and easy favorites.\n\n"
        f"Catalog snapshot:\n{json.dumps(snapshot, ensure_ascii=True)}"
    )
    data = generate_gemini_json(system_instruction, user_prompt, "ai-playlists")
    playlists = data.get("playlists") if isinstance(data, dict) else None
    if not isinstance(playlists, list) or len(playlists) < AI_PLAYLIST_COUNT:
        raise RuntimeError("Gemini returned invalid AI playlist JSON")
    return playlists[:AI_PLAYLIST_COUNT]


def generate_radio_blueprints_with_gemini(snapshot: dict) -> list[dict]:
    system_instruction = (
        "You create Tamil music radio station blueprints from library metadata. "
        "Return valid JSON only. Create exactly 25 distinct stations. "
        "Use simple, native, slightly quirky names. Keep blurbs short. "
        "Choose broad year spans and artist/director anchors so each station can be filled to 100 songs. "
        "Prefer melody-heavy, replayable groupings and avoid duplicate station concepts."
    )
    user_prompt = (
        "Using the catalog snapshot below, return JSON in this shape:\n"
        '{ "stations": [ { "id": "night-drive", "name": "Night Drive", "blurb": "Late-night melodies", '
        '"yearStart": 1997, "yearEnd": 2026, "includeArtists": ["Artist"], "includeDirectors": ["Director"], '
        '"includeMovies": ["Movie"], "keywords": ["night", "melody"] } ] }\n'
        "Rules:\n"
        "- Exactly 25 stations.\n"
        "- Use 2 to 6 includeArtists, 1 to 4 includeDirectors, 0 to 4 includeMovies, and 2 to 6 keywords per station.\n"
        "- Keep yearStart/yearEnd between 1997 and 2026.\n"
        "- Station ids must be lowercase kebab-case and unique.\n"
        "- Names should feel simple, local, and memorable.\n"
        "- Build a mix of night-drive, rain, soft romance, city, nostalgia, modern melody, and crowd-pleasing stations.\n"
        "- Only use artists, directors, movies, and era ideas that plausibly fit the snapshot.\n\n"
        f"Catalog snapshot:\n{json.dumps(snapshot, ensure_ascii=True)}"
    )
    data = generate_gemini_json(system_instruction, user_prompt, "radio-stations")
    stations = data.get("stations") if isinstance(data, dict) else None
    if not isinstance(stations, list) or len(stations) < RADIO_STATION_COUNT:
        raise RuntimeError("Gemini returned invalid radio station JSON")
    return stations[:RADIO_STATION_COUNT]


def score_song_for_station(song: dict, station: dict) -> float:
    score = 0.0
    year = song.get("year_int")
    year_start = parse_year_value(station.get("yearStart")) or 1997
    year_end = parse_year_value(station.get("yearEnd")) or 2026
    if year is not None:
        if year_start <= year <= year_end:
            score += 40
            mid = (year_start + year_end) / 2
            score += max(0, 12 - abs(year - mid) * 0.5)
        else:
            score -= min(12, abs(year - (year_start if year < year_start else year_end)) * 0.35)

    haystacks = [
        song.get("track_norm", ""),
        song.get("movie_norm", ""),
        song.get("singers_norm", ""),
        song.get("music_director_norm", ""),
    ]
    all_text = " ".join(haystacks)

    for artist in station.get("includeArtists", []) or []:
        norm = normalize_text(artist)
        if norm and norm in song.get("singers_norm", ""):
            score += 18

    for director in station.get("includeDirectors", []) or []:
        norm = normalize_text(director)
        if norm and norm in song.get("music_director_norm", ""):
            score += 20

    for movie in station.get("includeMovies", []) or []:
        norm = normalize_text(movie)
        if norm and norm in song.get("movie_norm", ""):
            score += 16

    for keyword in station.get("keywords", []) or []:
        norm = normalize_text(keyword)
        if norm and norm in all_text:
            score += 8

    score += (int(hashlib.md5(f"{station.get('id', '')}:{song['id']}".encode()).hexdigest(), 16) % 1000) / 1000.0
    return score


def build_station_song_ids(station: dict, songs: list[dict]) -> list[str]:
    ranked = sorted(songs, key=lambda song: score_song_for_station(song, station), reverse=True)
    selected: list[str] = []
    movie_counts: dict[str, int] = {}
    director_counts: dict[str, int] = {}

    for song in ranked:
        if len(selected) >= RADIO_STATION_SONG_COUNT:
            break
        movie_key = song.get("movie_norm", "")
        director_key = song.get("music_director_norm", "")
        if movie_key and movie_counts.get(movie_key, 0) >= 4:
            continue
        if director_key and director_counts.get(director_key, 0) >= 12:
            continue
        selected.append(song["id"])
        if movie_key:
            movie_counts[movie_key] = movie_counts.get(movie_key, 0) + 1
        if director_key:
            director_counts[director_key] = director_counts.get(director_key, 0) + 1

    if len(selected) < RADIO_STATION_SONG_COUNT:
        for song in ranked:
            if len(selected) >= RADIO_STATION_SONG_COUNT:
                break
            if song["id"] in selected:
                continue
            selected.append(song["id"])

    return selected[:RADIO_STATION_SONG_COUNT]


def build_playlist_song_ids(blueprint: dict, songs: list[dict]) -> list[str]:
    ranked = sorted(songs, key=lambda song: score_song_for_station(song, blueprint), reverse=True)
    selected: list[str] = []
    movie_counts: dict[str, int] = {}
    director_counts: dict[str, int] = {}
    for song in ranked:
        if len(selected) >= AI_PLAYLIST_SONG_COUNT:
            break
        if song["id"] in selected:
            continue
        movie_key = song.get("movie_norm", "")
        director_key = song.get("music_director_norm", "")
        if movie_key and movie_counts.get(movie_key, 0) >= 3:
            continue
        if director_key and director_counts.get(director_key, 0) >= 8:
            continue
        selected.append(song["id"])
        if movie_key:
            movie_counts[movie_key] = movie_counts.get(movie_key, 0) + 1
        if director_key:
            director_counts[director_key] = director_counts.get(director_key, 0) + 1
    return selected[:AI_PLAYLIST_SONG_COUNT]


def normalize_station_payload(station: dict, fallback_index: int) -> dict:
    name = (station.get("name") or f"Station {fallback_index + 1}").strip()
    slug = normalize_text(station.get("id") or name).replace(" ", "-") or f"station-{fallback_index + 1}"
    year_start = max(1997, min(2026, parse_year_value(station.get("yearStart")) or 1997))
    year_end = max(1997, min(2026, parse_year_value(station.get("yearEnd")) or 2026))
    if year_start > year_end:
        year_start, year_end = year_end, year_start
    return {
        "id": slug,
        "name": name,
        "blurb": (station.get("blurb") or "A looping Tamil radio lane.").strip(),
        "yearStart": year_start,
        "yearEnd": year_end,
        "includeArtists": [value for value in (station.get("includeArtists") or []) if isinstance(value, str) and value.strip()][:6],
        "includeDirectors": [value for value in (station.get("includeDirectors") or []) if isinstance(value, str) and value.strip()][:4],
        "includeMovies": [value for value in (station.get("includeMovies") or []) if isinstance(value, str) and value.strip()][:4],
        "keywords": [value for value in (station.get("keywords") or []) if isinstance(value, str) and value.strip()][:6],
    }


def get_radio_stations(force_refresh: bool = False) -> dict:
    signature = get_radio_library_signature()
    cached = radio_station_cache.get("payload")
    cached_signature = radio_station_cache.get("signature")
    cached_generated_at = radio_station_cache.get("generated_at")
    if (
        not force_refresh
        and cached
        and cached_signature == signature
        and isinstance(cached_generated_at, datetime)
        and cached_generated_at + RADIO_STATION_CACHE_TTL > now_utc()
    ):
        return cached

    with radio_station_lock:
        cached = radio_station_cache.get("payload")
        cached_signature = radio_station_cache.get("signature")
        cached_generated_at = radio_station_cache.get("generated_at")
        if (
            not force_refresh
            and cached
            and cached_signature == signature
            and isinstance(cached_generated_at, datetime)
            and cached_generated_at + RADIO_STATION_CACHE_TTL > now_utc()
        ):
            return cached

        songs = get_radio_library_rows()
        snapshot = build_radio_catalog_snapshot(songs)
        source = "fallback"
        try:
            blueprint_rows = generate_radio_blueprints_with_gemini(snapshot)
            source = "gemini"
        except Exception as exc:
            print(f"[radio] falling back to local station generator: {exc}")
            blueprint_rows = fallback_radio_blueprints(snapshot)

        normalized_blueprints = [normalize_station_payload(row, index) for index, row in enumerate(blueprint_rows[:RADIO_STATION_COUNT])]
        stations = []
        seen_ids: set[str] = set()
        for index, blueprint in enumerate(normalized_blueprints):
            if blueprint["id"] in seen_ids:
                blueprint["id"] = f"{blueprint['id']}-{index + 1}"
            seen_ids.add(blueprint["id"])
            song_ids = build_station_song_ids(blueprint, songs)
            stations.append(
                {
                    "id": blueprint["id"],
                    "name": blueprint["name"],
                    "blurb": blueprint["blurb"],
                    "yearStart": blueprint["yearStart"],
                    "yearEnd": blueprint["yearEnd"],
                    "songIds": song_ids,
                    "trackCount": len(song_ids),
                }
            )

        generated_at = now_utc()
        epoch_seconds = int(generated_at.timestamp())
        for index, station in enumerate(stations):
            song_ids = station.get("songIds") or []
            if not song_ids:
                station["currentIndex"] = 0
                station["currentSongId"] = ""
                station["currentOffsetSeconds"] = 0
                station["sharedSongSeconds"] = RADIO_SHARED_SONG_SECONDS
                station["slotStartedAt"] = generated_at.isoformat()
                continue
            slot_number = (epoch_seconds // RADIO_SHARED_SONG_SECONDS) + index
            current_index = slot_number % len(song_ids)
            current_offset = epoch_seconds % RADIO_SHARED_SONG_SECONDS
            station["currentIndex"] = current_index
            station["currentSongId"] = song_ids[current_index]
            station["currentOffsetSeconds"] = current_offset
            station["sharedSongSeconds"] = RADIO_SHARED_SONG_SECONDS
            station["slotStartedAt"] = datetime.fromtimestamp(epoch_seconds - current_offset, timezone.utc).isoformat()

        payload = {"ok": True, "source": source, "generatedAt": generated_at.isoformat(), "stations": stations}
        radio_station_cache["signature"] = signature
        radio_station_cache["generated_at"] = generated_at
        radio_station_cache["payload"] = payload
        return payload


def create_ai_playlists_for_user(user_id: str) -> dict:
    songs = get_radio_library_rows()
    snapshot = build_playlist_catalog_snapshot(songs)
    source = "fallback"
    try:
        blueprint_rows = generate_ai_playlist_blueprints_with_gemini(snapshot)
        source = "gemini"
    except Exception as exc:
        print(f"[playlists] falling back to local playlist generator: {exc}")
        blueprint_rows = fallback_ai_playlist_blueprints(snapshot)

    created = []
    with db.get_conn() as conn:
        for index, row in enumerate(blueprint_rows[:AI_PLAYLIST_COUNT]):
            normalized = normalize_station_payload(
                {
                    "id": f"global-ai-playlist-{index + 1}",
                    **row,
                },
                index,
            )
            song_ids = build_playlist_song_ids(normalized, songs)
            if not song_ids:
                continue
            playlist_name = normalized["name"]
            source_url = f"isaibox:global-ai:{normalized['id']}"
            existing = conn.execute(
                """
                SELECT playlist_id
                FROM playlists
                WHERE is_global = TRUE AND source = 'gemini' AND source_url = ?
                LIMIT 1
                """,
                [source_url],
            ).fetchone()
            if existing:
                playlist_id = existing[0]
                conn.execute(
                    """
                    UPDATE playlists
                    SET user_id = ?, name = ?, updated_at = ?
                    WHERE playlist_id = ?
                    """,
                    [user_id, playlist_name, now_utc(), playlist_id],
                )
                conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
            else:
                playlist_id = secrets.token_hex(16)
                conn.execute(
                    """
                    INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
                    VALUES (?, ?, ?, TRUE, 'gemini', ?, ?, ?)
                    """,
                    [playlist_id, user_id, playlist_name, source_url, now_utc(), now_utc()],
                )
            conn.executemany(
                "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
                [[playlist_id, song_id, position + 1, now_utc()] for position, song_id in enumerate(song_ids)],
            )
            created.append(
                {
                    "id": playlist_id,
                    "name": playlist_name,
                    "trackCount": len(song_ids),
                    "source": "gemini",
                    "sourceDetail": source,
                    "isGlobal": True,
                    "sourceUrl": source_url,
                }
            )
    return {"ok": True, "source": source, "playlists": created}


def create_or_update_global_playlist(
    *,
    owner_user_id: str,
    name: str,
    song_ids: list[str],
    source: str,
    source_url: str,
) -> dict:
    cleaned_song_ids = [song_id for song_id in song_ids if isinstance(song_id, str) and song_id]
    if not cleaned_song_ids:
        raise ValueError("No songs available for playlist")

    with db.get_conn() as conn:
        existing = conn.execute(
            """
            SELECT playlist_id
            FROM playlists
            WHERE is_global = TRUE AND source = ? AND source_url = ?
            LIMIT 1
            """,
            [source, source_url],
        ).fetchone()
        if existing:
            playlist_id = existing[0]
            conn.execute(
                """
                UPDATE playlists
                SET user_id = ?, name = ?, updated_at = ?
                WHERE playlist_id = ?
                """,
                [owner_user_id, name, now_utc(), playlist_id],
            )
            conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
            updated_existing = True
        else:
            playlist_id = secrets.token_hex(16)
            conn.execute(
                """
                INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
                VALUES (?, ?, ?, TRUE, ?, ?, ?, ?)
                """,
                [playlist_id, owner_user_id, name, source, source_url, now_utc(), now_utc()],
            )
            updated_existing = False

        conn.executemany(
            "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
            [[playlist_id, song_id, index + 1, now_utc()] for index, song_id in enumerate(cleaned_song_ids)],
        )

    return {
        "id": playlist_id,
        "name": name,
        "trackCount": len(cleaned_song_ids),
        "source": source,
        "sourceUrl": source_url,
        "isGlobal": True,
        "updatedExisting": updated_existing,
    }


def rename_playlist_for_user(user: dict, playlist_id: str, name: str) -> dict:
    cleaned_name = (name or "").strip()
    if not cleaned_name:
        raise ValueError("Playlist name is required")
    with db.get_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id, source, source_url FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            raise LookupError("Playlist not found")
        conn.execute(
            "UPDATE playlists SET name = ?, updated_at = ? WHERE playlist_id = ?",
            [cleaned_name, now_utc(), playlist_id],
        )
    return {"id": playlist_id, "name": cleaned_name, "isGlobal": bool(playlist[1]), "source": playlist[3] or "manual", "sourceUrl": playlist[4] or ""}


def reorder_playlist_songs_for_user(user: dict, playlist_id: str, song_ids: list[str]) -> dict:
    cleaned_song_ids = [song_id for song_id in song_ids if isinstance(song_id, str) and song_id]
    with db.get_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            raise LookupError("Playlist not found")
        existing_rows = conn.execute(
            "SELECT song_id FROM playlist_songs WHERE playlist_id = ? ORDER BY position, added_at",
            [playlist_id],
        ).fetchall()
        existing_song_ids = [row[0] for row in existing_rows]
        if set(cleaned_song_ids) != set(existing_song_ids) or len(cleaned_song_ids) != len(existing_song_ids):
            raise ValueError("Reorder list must contain the same playlist songs")
        conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
        conn.executemany(
            "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
            [[playlist_id, song_id, index + 1, now_utc()] for index, song_id in enumerate(cleaned_song_ids)],
        )
        conn.execute("UPDATE playlists SET updated_at = ? WHERE playlist_id = ?", [now_utc(), playlist_id])
    return {"ok": True}


def create_global_playlist_from_radio_station(owner_user_id: str, station_id: str, *, mode: str = "upsert", target_playlist_id: str = "", name: str = "") -> dict:
    payload = get_radio_stations(force_refresh=False)
    station = next((item for item in payload.get("stations", []) if item.get("id") == station_id), None)
    if not station:
        raise ValueError("Radio station not found")
    playlist_name = (name or station["name"]).strip() or station["name"]
    if mode == "overwrite":
        if not target_playlist_id:
            raise ValueError("Target playlist is required for overwrite")
        with db.get_conn() as conn:
            existing = conn.execute(
                "SELECT playlist_id, is_global FROM playlists WHERE playlist_id = ?",
                [target_playlist_id],
            ).fetchone()
            if not existing or not existing[1]:
                raise ValueError("Target global playlist not found")
            conn.execute("UPDATE playlists SET user_id = ?, name = ?, source = 'radio', source_url = ?, updated_at = ? WHERE playlist_id = ?",
                         [owner_user_id, playlist_name, f"isaibox:radio:{station['id']}", now_utc(), target_playlist_id])
            conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [target_playlist_id])
            conn.executemany(
                "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
                [[target_playlist_id, song_id, index + 1, now_utc()] for index, song_id in enumerate(station.get("songIds", []))],
            )
        playlist = {
            "id": target_playlist_id,
            "name": playlist_name,
            "trackCount": len(station.get("songIds", [])),
            "source": "radio",
            "sourceUrl": f"isaibox:radio:{station['id']}",
            "isGlobal": True,
            "updatedExisting": True,
        }
    elif mode == "new":
        playlist_id = secrets.token_hex(16)
        with db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
                VALUES (?, ?, ?, TRUE, 'radio', ?, ?, ?)
                """,
                [playlist_id, owner_user_id, playlist_name, f"isaibox:radio:{station['id']}:{playlist_id}", now_utc(), now_utc()],
            )
            conn.executemany(
                "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
                [[playlist_id, song_id, index + 1, now_utc()] for index, song_id in enumerate(station.get("songIds", []))],
            )
        playlist = {
            "id": playlist_id,
            "name": playlist_name,
            "trackCount": len(station.get("songIds", [])),
            "source": "radio",
            "sourceUrl": f"isaibox:radio:{station['id']}:{playlist_id}",
            "isGlobal": True,
            "updatedExisting": False,
        }
    else:
        playlist = create_or_update_global_playlist(
            owner_user_id=owner_user_id,
            name=playlist_name,
            song_ids=station.get("songIds", []),
            source="radio",
            source_url=f"isaibox:radio:{station['id']}",
        )
    return {"ok": True, "playlist": playlist, "station": station}


def get_library_match_cache() -> list[dict]:
    if library_match_cache:
        return library_match_cache
    with library_match_lock:
        if library_match_cache:
            return library_match_cache
        with get_read_conn() as conn:
            rows = conn.execute(
                """
                SELECT song_id, track_name, singers, movie_name, music_director
                FROM songs
                WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
                """
            ).fetchall()
        library_match_cache.extend(
            {
                "id": row[0],
                "track": row[1] or "",
                "singers": row[2] or "",
                "movie": row[3] or "",
                "music_director": row[4] or "",
                "track_norm": normalize_text(row[1] or ""),
                "singers_norm": normalize_text(row[2] or ""),
                "movie_norm": normalize_text(row[3] or ""),
                "music_director_norm": normalize_text(row[4] or ""),
            }
            for row in rows
        )
    return library_match_cache


def invalidate_library_match_cache() -> None:
    with library_match_lock:
        library_match_cache.clear()


def sign_value(value: str) -> str:
    digest = hmac.new(SESSION_SECRET.encode(), value.encode(), hashlib.sha256).hexdigest()
    return f"{value}.{digest}"


def unsign_value(value: str) -> str | None:
    try:
        payload, digest = value.rsplit(".", 1)
    except ValueError:
        return None
    expected = hmac.new(SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, expected):
        return None
    return payload


def get_session_user():
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if not cookie:
        return None
    session_id = unsign_value(cookie)
    if not session_id:
        return None
    with get_read_conn() as conn:
        row = conn.execute(
            """
            SELECT u.user_id, u.email, u.name, u.picture, s.expires_at, u.is_admin, u.is_banned, u.ban_reason
            FROM user_sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.session_id = ?
            """,
            [session_id],
        ).fetchone()
    if not row or (row[4] and row[4] <= now_utc()) or row[6]:
        return None
    return {
        "user_id": row[0],
        "email": row[1] or "",
        "name": row[2] or "",
        "picture": row[3] or "",
        "is_admin": bool(row[5]),
        "is_banned": bool(row[6]),
        "ban_reason": row[7] or "",
        "session_id": session_id,
    }


def require_session_user():
    user = get_session_user()
    if not user:
        return None, (json_response({"ok": False, "message": "Authentication required"}), 401)
    return user, None


def require_admin_user():
    user, error_response = require_session_user()
    if error_response:
        return None, error_response
    if not user["is_admin"]:
        return None, (json_response({"ok": False, "message": "Admin access required"}), 403)
    return user, None


def issue_session_response(user_id: str, user_payload: dict):
    session_id = secrets.token_urlsafe(32)
    expires_at = now_utc() + timedelta(days=SESSION_TTL_DAYS)
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_sessions (session_id, user_id, expires_at, created_at)
            VALUES (?, ?, ?, ?)
            """,
            [session_id, user_id, expires_at, now_utc()],
        )
    response = json_response({"ok": True, "user": user_payload})
    response.set_cookie(
        SESSION_COOKIE_NAME,
        sign_value(session_id),
        httponly=True,
        samesite="Lax",
        secure=False,
        expires=expires_at,
    )
    return response


def clear_session_response():
    response = json_response({"ok": True})
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


def run_local_command(args: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        args,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "AIRFLOW_HOME": str(AIRFLOW_HOME),
            "PATH": f"{VENV_BIN}:{os.environ.get('PATH', '')}",
            "OBJC_DISABLE_INITIALIZE_FORK_SAFETY": "YES",
            "AIRFLOW__CORE__MP_START_METHOD": "spawn",
            "NO_PROXY": "*",
        },
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def read_pid_file(name: str) -> int | None:
    path = AIRFLOW_HOME / name
    if not path.exists():
        return None
    try:
        value = path.read_text().strip()
        return int(value) if value else None
    except (OSError, ValueError):
        return None


def pid_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def airflow_cli(args: list[str]) -> tuple[int, str, str]:
    return run_local_command([str(VENV_BIN / "airflow"), *args])


def airflow_process_status() -> dict:
    webserver_pid = read_pid_file("airflow-webserver.pid")
    scheduler_pid = read_pid_file("airflow-scheduler.pid")
    monitor_pid = read_pid_file("airflow-webserver-monitor.pid")
    scheduler_log = AIRFLOW_HOME / "airflow-scheduler.out"
    lsof_code, lsof_stdout, _ = run_local_command(["lsof", "-iTCP:8080", "-sTCP:LISTEN", "-n", "-P"])
    dags_code, dags_stdout, dags_stderr = airflow_cli(["dags", "list"])
    webserver_running = lsof_code == 0 and bool(lsof_stdout)
    scheduler_running = pid_running(scheduler_pid) or (scheduler_pid is not None and scheduler_log.exists())
    process_lines = []
    for label, pid, running in (
        ("webserver", webserver_pid, webserver_running),
        ("scheduler", scheduler_pid, scheduler_running),
        ("monitor", monitor_pid, pid_running(monitor_pid)),
    ):
        if pid:
            process_lines.append(f"{label}:{pid}:{'running' if running else 'stopped'}")
    with get_read_conn() as conn:
        latest_run = conn.execute(
            """
            SELECT run_id, started_at, finished_at, pages_scraped, albums_new, albums_updated, albums_failed, songs_total, status
            FROM scrape_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        recent_runs = conn.execute(
            """
            SELECT run_id, started_at, finished_at, pages_scraped, albums_new, albums_updated, albums_failed, songs_total, status
            FROM scrape_runs
            ORDER BY started_at DESC
            LIMIT 8
            """
        ).fetchall()
    return {
        "webserverRunning": webserver_running,
        "schedulerRunning": scheduler_running,
        "processes": process_lines[:10],
        "dagsOk": dags_code == 0,
        "dagError": dags_stderr if dags_code != 0 else "",
        "latestRun": {
            "runId": latest_run[0],
            "startedAt": latest_run[1].isoformat() if latest_run and latest_run[1] else "",
            "finishedAt": latest_run[2].isoformat() if latest_run and latest_run[2] else "",
            "pagesScraped": latest_run[3] if latest_run else 0,
            "albumsNew": latest_run[4] if latest_run else 0,
            "albumsUpdated": latest_run[5] if latest_run else 0,
            "albumsFailed": latest_run[6] if latest_run else 0,
            "songsTotal": latest_run[7] if latest_run else 0,
            "status": latest_run[8] if latest_run else "unknown",
        } if latest_run else None,
        "recentRuns": [
            {
                "runId": row[0],
                "startedAt": row[1].isoformat() if row[1] else "",
                "finishedAt": row[2].isoformat() if row[2] else "",
                "pagesScraped": row[3] or 0,
                "albumsNew": row[4] or 0,
                "albumsUpdated": row[5] or 0,
                "albumsFailed": row[6] or 0,
                "songsTotal": row[7] or 0,
                "status": row[8] or "unknown",
            }
            for row in recent_runs
        ],
    }


def favorite_song_ids_for_user(user_id: str) -> set[str]:
    with get_read_conn() as conn:
        rows = conn.execute("SELECT song_id FROM favorite_songs WHERE user_id = ?", [user_id]).fetchall()
    return {row[0] for row in rows}


def default_user_preferences() -> dict:
    return {
        "themePreference": "system",
        "mainTab": "library",
        "recentSongIds": [],
        "playerVolume": 0.9,
        "playerMuted": False,
        "repeatMode": "off",
        "autoplayNext": True,
    }


def user_preferences_for_user(user_id: str) -> dict:
    with get_read_conn() as conn:
        row = conn.execute(
            """
            SELECT theme_preference, main_tab, recent_song_ids, player_volume, player_muted, repeat_mode, autoplay_next
            FROM user_preferences
            WHERE user_id = ?
            """,
            [user_id],
        ).fetchone()
    if not row:
        return default_user_preferences()
    try:
        recent_song_ids = json.loads(row[2]) if row[2] else []
    except Exception:
        recent_song_ids = []
    return {
        "themePreference": row[0] or "system",
        "mainTab": row[1] or "library",
        "recentSongIds": recent_song_ids if isinstance(recent_song_ids, list) else [],
        "playerVolume": float(row[3]) if row[3] is not None else 0.9,
        "playerMuted": bool(row[4]),
        "repeatMode": row[5] or "off",
        "autoplayNext": True if row[6] is None else bool(row[6]),
    }


def save_user_preferences(user_id: str, payload: dict) -> dict:
    prefs = default_user_preferences()
    prefs.update(
        {
            "themePreference": payload.get("themePreference") or prefs["themePreference"],
            "mainTab": payload.get("mainTab") or prefs["mainTab"],
            "recentSongIds": payload.get("recentSongIds") if isinstance(payload.get("recentSongIds"), list) else prefs["recentSongIds"],
            "playerVolume": max(0.0, min(1.0, float(payload.get("playerVolume", prefs["playerVolume"])))),
            "playerMuted": bool(payload.get("playerMuted", prefs["playerMuted"])),
            "repeatMode": payload.get("repeatMode") or prefs["repeatMode"],
            "autoplayNext": bool(payload.get("autoplayNext", prefs["autoplayNext"])),
        }
    )
    if prefs["themePreference"] not in {"system", "light", "dark"}:
        prefs["themePreference"] = "system"
    if prefs["mainTab"] not in {"library", "recents", "favorites", "radio", "admin"}:
        prefs["mainTab"] = "library"
    if prefs["repeatMode"] not in {"off", "one", "album", "random"}:
        prefs["repeatMode"] = "off"
    prefs["recentSongIds"] = [song_id for song_id in prefs["recentSongIds"] if isinstance(song_id, str) and song_id][:80]

    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_preferences (
                user_id, theme_preference, main_tab, recent_song_ids,
                player_volume, player_muted, repeat_mode, autoplay_next, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                theme_preference = excluded.theme_preference,
                main_tab = excluded.main_tab,
                recent_song_ids = excluded.recent_song_ids,
                player_volume = excluded.player_volume,
                player_muted = excluded.player_muted,
                repeat_mode = excluded.repeat_mode,
                autoplay_next = excluded.autoplay_next,
                updated_at = excluded.updated_at
            """,
            [
                user_id,
                prefs["themePreference"],
                prefs["mainTab"],
                json.dumps(prefs["recentSongIds"]),
                prefs["playerVolume"],
                prefs["playerMuted"],
                prefs["repeatMode"],
                prefs["autoplayNext"],
                now_utc(),
            ],
        )
    return prefs


def playlists_for_user(user_id: str) -> list[dict]:
    with get_read_conn() as conn:
        rows = conn.execute(
            """
            SELECT p.playlist_id, p.name, p.is_global, p.source, p.source_url, p.updated_at, p.created_at, COUNT(ps.song_id) AS track_count
            FROM playlists p
            LEFT JOIN playlist_songs ps ON ps.playlist_id = p.playlist_id
            WHERE p.user_id = ? AND p.is_global = FALSE
            GROUP BY 1,2,3,4,5,6,7
            ORDER BY p.updated_at DESC, p.created_at DESC
            """,
            [user_id],
        ).fetchall()
    return [
        {
            "id": row[0],
            "name": row[1] or "",
            "isGlobal": bool(row[2]),
            "source": row[3] or "manual",
            "sourceUrl": row[4] or "",
            "updatedAt": row[5].isoformat() if row[5] else "",
            "trackCount": row[6] or 0,
        }
        for row in rows
    ]


def global_playlists() -> list[dict]:
    with get_read_conn() as conn:
        rows = conn.execute(
            """
            SELECT p.playlist_id, p.name, p.source, p.source_url, p.updated_at, p.created_at, COUNT(ps.song_id) AS track_count
            FROM playlists p
            LEFT JOIN playlist_songs ps ON ps.playlist_id = p.playlist_id
            WHERE p.is_global = TRUE
            GROUP BY 1,2,3,4,5,6
            ORDER BY LOWER(p.name) ASC, p.updated_at DESC, p.created_at DESC
            """
        ).fetchall()
    return [
        {
            "id": row[0],
            "name": row[1] or "",
            "isGlobal": True,
            "source": row[2] or "manual",
            "sourceUrl": row[3] or "",
            "updatedAt": row[4].isoformat() if row[4] else "",
            "trackCount": row[5] or 0,
        }
        for row in rows
    ]


def playlist_tracks(playlist_id: str) -> list[dict]:
    with get_read_conn() as conn:
        rows = conn.execute(
            """
            SELECT s.song_id, s.track_name, s.singers, s.movie_name, s.music_director, s.year
            FROM playlist_songs ps
            JOIN songs s ON s.song_id = ps.song_id
            WHERE ps.playlist_id = ?
            ORDER BY ps.position
            """,
            [playlist_id],
        ).fetchall()
    return [
        {
            "id": row[0],
            "track": row[1] or "",
            "singers": row[2] or "",
            "movie": row[3] or "",
            "musicDirector": row[4] or "",
            "year": row[5] or "",
            "audioUrl": f"/api/stream/{row[0]}",
        }
        for row in rows
    ]


def simplify_track_name(value: str) -> str:
    cleaned = value or ""
    patterns = (
        r"\([^)]*(feat|featuring|from|version|video|lyric|lyrics|remaster|remastered|original|official)[^)]*\)",
        r"\[[^\]]*(feat|featuring|from|version|video|lyric|lyrics|remaster|remastered|original|official)[^\]]*\]",
        r"\s+-\s+(from|feat|featuring|video|lyric|lyrics|remaster|remastered|original|official).*$",
        r"\s+(feat|featuring)\.?\s+.*$",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("&", " ")
    return normalize_text(cleaned)


def token_set(value: str) -> set[str]:
    return {token for token in simplify_track_name(value).split() if len(token) > 1}


def token_overlap_score(left: set[str], right: set[str], weight: int) -> int:
    if not left or not right:
        return 0
    overlap = len(left & right)
    if not overlap:
        return 0
    ratio = overlap / max(len(left), len(right))
    return int(weight * ratio * 10)


def match_spotify_track_detail(track_name: str, artists: list[str], album_name: str = "") -> dict | None:
    track_norm = simplify_track_name(track_name)
    track_tokens = token_set(track_name)
    artists_norm = normalize_text(" ".join(artists))
    artist_tokens = {
        token
        for artist in artists
        for token in normalize_text(artist).split()
        if len(token) > 1
    }
    album_norm = simplify_track_name(album_name)
    album_tokens = token_set(album_name)

    candidates = []
    for song in get_library_match_cache():
        score = 0
        reasons = []

        if song["track_norm"] == track_norm and track_norm:
            score += 120
            reasons.append("track_exact")
        elif track_norm and (track_norm in song["track_norm"] or song["track_norm"] in track_norm):
            score += 70
            reasons.append("track_partial")
        track_overlap = token_overlap_score(track_tokens, set(song["track_norm"].split()), 9)
        score += track_overlap
        if track_overlap:
            reasons.append("track_tokens")

        if artists_norm and artists_norm == song["singers_norm"]:
            score += 45
            reasons.append("artist_exact")
        singer_overlap = token_overlap_score(artist_tokens, set(song["singers_norm"].split()), 7)
        director_overlap = token_overlap_score(artist_tokens, set(song["music_director_norm"].split()), 4)
        score += singer_overlap
        score += director_overlap
        if singer_overlap:
            reasons.append("artist_tokens")
        if director_overlap:
            reasons.append("director_tokens")

        if album_norm and album_norm == song["movie_norm"]:
            score += 40
            reasons.append("album_exact")
        elif album_norm and (album_norm in song["movie_norm"] or song["movie_norm"] in album_norm):
            score += 24
            reasons.append("album_partial")
        album_overlap = token_overlap_score(album_tokens, set(song["movie_norm"].split()), 5)
        score += album_overlap
        if album_overlap:
            reasons.append("album_tokens")

        if score:
            candidates.append((score, song, reasons))

    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0], reverse=True)
    best_score, best_song, reasons = candidates[0]
    second_score = candidates[1][0] if len(candidates) > 1 else -1

    if best_score < 60:
        return None
    if second_score >= 0 and best_score - second_score < 8:
        return None
    return {
        "song_id": best_song["id"],
        "score": best_score,
        "reasons": reasons,
        "matchedTrack": best_song["track"],
        "matchedMovie": best_song["movie"],
        "matchedSingers": best_song["singers"],
    }


def match_spotify_track(track_name: str, artists: list[str], album_name: str = "") -> str | None:
    detail = match_spotify_track_detail(track_name, artists, album_name)
    return detail["song_id"] if detail else None


def match_spotify_tracks(tracks: list[dict]) -> tuple[list[str], list[dict], list[dict]]:
    matched_ids: list[str] = []
    matched_details: list[dict] = []
    unmatched: list[dict] = []

    for index, track in enumerate(tracks, start=1):
        detail = match_spotify_track_detail(track["name"], track["artists"], track.get("album", ""))
        if detail and detail["song_id"] not in matched_ids:
            matched_ids.append(detail["song_id"])
            matched_details.append(
                {
                    "position": index,
                    "sourceTrack": track["name"],
                    "sourceArtists": track["artists"],
                    "sourceAlbum": track.get("album", ""),
                    "songId": detail["song_id"],
                    "matchedTrack": detail["matchedTrack"],
                    "matchedMovie": detail["matchedMovie"],
                    "matchedSingers": detail["matchedSingers"],
                    "score": detail["score"],
                    "reasons": detail["reasons"],
                }
            )
        else:
            unmatched.append(
                {
                    "position": index,
                    "name": track["name"],
                    "artists": track["artists"],
                    "album": track.get("album", ""),
                }
            )

    return matched_ids, matched_details, unmatched


def extract_spotify_playlist_id(playlist_url: str) -> str:
    match = SPOTIFY_PLAYLIST_RE.search(playlist_url)
    if not match:
        raise ValueError("Invalid Spotify playlist link")
    return match.group(1)


def spotify_track_from_payload(track: dict | None) -> dict | None:
    if not track:
        return None
    track_name = (track.get("name") or "").strip()
    album_name = ((track.get("album") or {}).get("name") or "").strip()
    artists = [
        (artist.get("name") or "").strip()
        for artist in (track.get("artists") or [])
        if (artist.get("name") or "").strip()
    ]
    if not track_name:
        return None
    return {
        "name": track_name,
        "artists": artists,
        "album": album_name,
    }


def dedupe_spotify_tracks(tracks: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, tuple[str, ...], str]] = set()
    for track in tracks:
        key = (
            normalize_text(track.get("name", "")),
            tuple(normalize_text(artist) for artist in track.get("artists", [])),
            normalize_text(track.get("album", "")),
        )
        if not key[0] or key in seen:
            continue
        seen.add(key)
        deduped.append(track)
    return deduped


def get_spotify_client_credentials_token() -> str:
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        return ""
    response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
        timeout=15,
    )
    response.raise_for_status()
    return (response.json() or {}).get("access_token", "")


def get_spotify_web_access_token() -> str:
    token_response = requests.get(
        "https://open.spotify.com/get_access_token?reason=transport&productType=web_player",
        headers=SPOTIFY_PUBLIC_HEADERS,
        timeout=15,
    )
    token_response.raise_for_status()
    access_token = token_response.json().get("accessToken")
    if not access_token:
        raise ValueError("Spotify access token unavailable")
    return access_token


def resolve_spotify_playlist_tracks_public_api(playlist_id: str, access_token: str) -> tuple[str, list[dict]]:
    playlist_response = requests.get(
        f"https://api.spotify.com/v1/playlists/{playlist_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"fields": "name"},
        timeout=20,
    )
    playlist_response.raise_for_status()
    playlist_name = (playlist_response.json() or {}).get("name") or "Spotify Playlist"
    items = spotify_api_paginate(
        access_token,
        f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        {
            "limit": 100,
            "fields": "items(track(name,artists(name),album(name))),next",
        },
    )
    tracks: list[dict] = []
    for item in items:
        normalized = spotify_track_from_payload(item.get("track") or {})
        if normalized:
            tracks.append(normalized)
    return playlist_name, dedupe_spotify_tracks(tracks)


def _extract_tracks_from_spotify_json_blob(value) -> list[dict]:
    tracks: list[dict] = []

    def walk(node):
        if isinstance(node, dict):
            normalized = spotify_track_from_payload(node.get("track") if isinstance(node.get("track"), dict) else node)
            if normalized:
                tracks.append(normalized)
            for child in node.values():
                walk(child)
            return
        if isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return dedupe_spotify_tracks(tracks)


def _decode_json_parse_string(value: str) -> str:
    return bytes(value, "utf-8").decode("unicode_escape")


def _extract_json_payloads_from_script(script_text: str) -> list[object]:
    payloads: list[object] = []
    stripped = (script_text or "").strip()
    if not stripped:
        return payloads

    for candidate in (stripped,):
        if candidate.startswith("{") or candidate.startswith("["):
            try:
                payloads.append(json.loads(candidate))
            except Exception:
                pass

    for match in re.finditer(r"JSON\.parse\(\s*(['\"])(?P<body>(?:\\.|(?!\1).)*)\1\s*\)", stripped, flags=re.DOTALL):
        encoded = match.group("body")
        try:
            decoded = _decode_json_parse_string(encoded)
            payloads.append(json.loads(decoded))
        except Exception:
            continue

    for match in re.finditer(r"=\s*(\{.*?\}|\[.*?\])\s*;", stripped, flags=re.DOTALL):
        candidate = match.group(1)
        try:
            payloads.append(json.loads(candidate))
        except Exception:
            continue

    return payloads


def resolve_spotify_playlist_tracks_html(playlist_id: str) -> tuple[str, list[dict]]:
    response = requests.get(
        f"https://open.spotify.com/playlist/{playlist_id}",
        headers=SPOTIFY_PUBLIC_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    playlist_name = ""
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        playlist_name = og_title["content"].strip()
    if not playlist_name:
        title_tag = soup.find("title")
        if title_tag:
            playlist_name = title_tag.get_text(" ", strip=True).replace(" | Spotify", "").strip()

    payload_candidates: list[object] = []
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        payload_candidates.extend(_extract_json_payloads_from_script(next_data.string or next_data.get_text(" ", strip=False)))
    for script in soup.find_all("script"):
        script_text = script.string or script.get_text(" ", strip=False)
        script_type = (script.get("type") or "").lower()
        if script_type in {"application/json", "application/ld+json"}:
            payload_candidates.extend(_extract_json_payloads_from_script(script_text))
            continue
        if script_text and ("spotify:" in script_text.lower() or "track" in script_text.lower() or "playlist" in script_text.lower()):
            payload_candidates.extend(_extract_json_payloads_from_script(script_text))

    best_tracks: list[dict] = []
    for payload in payload_candidates:
        tracks = _extract_tracks_from_spotify_json_blob(payload)
        if len(tracks) > len(best_tracks):
            best_tracks = tracks
        if best_tracks:
            break

    if not best_tracks:
        raise ValueError("Unable to parse Spotify playlist page")

    return playlist_name or "Spotify Playlist", best_tracks


def resolve_spotify_playlist_tracks(playlist_url: str) -> tuple[str, list[dict]]:
    playlist_id = extract_spotify_playlist_id(playlist_url)
    errors: list[str] = []
    client_token = ""
    if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
      try:
          client_token = get_spotify_client_credentials_token()
      except Exception as exc:
          app.logger.warning("Spotify client-credentials token fetch failed", exc_info=True)
          errors.append(str(exc))

    resolver_attempts = []
    if client_token:
        resolver_attempts.append(("resolve_spotify_playlist_tracks_public_api_client_credentials", lambda pid: resolve_spotify_playlist_tracks_public_api(pid, client_token)))
    resolver_attempts.append(("resolve_spotify_playlist_tracks_public_api_web", lambda pid: resolve_spotify_playlist_tracks_public_api(pid, get_spotify_web_access_token())))
    resolver_attempts.append(("resolve_spotify_playlist_tracks_html", resolve_spotify_playlist_tracks_html))

    for resolver_name, resolver in resolver_attempts:
        try:
            playlist_name, tracks = resolver(playlist_id)
            if tracks:
                return playlist_name, tracks
        except Exception as exc:
            app.logger.warning("Spotify playlist resolver failed via %s", resolver_name, exc_info=True)
            errors.append(str(exc))
    message = errors[-1] if errors else "Unable to load Spotify playlist"
    raise ValueError(message)


def spotify_api_paginate(access_token: str, url: str, params: dict | None = None) -> list[dict]:
    items: list[dict] = []
    next_url = url
    next_params = params

    while next_url:
        response = requests.get(
            next_url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=next_params,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        items.extend(payload.get("items", []))
        next_url = payload.get("next")
        next_params = None

    return items


def resolve_spotify_account_playlists(access_token: str) -> list[dict]:
    items = spotify_api_paginate(
        access_token,
        "https://api.spotify.com/v1/me/playlists",
        {"limit": 50},
    )
    return [
        {
            "id": playlist.get("id", ""),
            "name": playlist.get("name", ""),
            "trackCount": ((playlist.get("tracks") or {}).get("total")) or 0,
            "owner": ((playlist.get("owner") or {}).get("display_name")) or "",
        }
        for playlist in items
        if playlist.get("id") and playlist.get("name")
    ]


def resolve_spotify_saved_tracks(access_token: str) -> list[dict]:
    items = spotify_api_paginate(
        access_token,
        "https://api.spotify.com/v1/me/tracks",
        {"limit": 50},
    )
    tracks: list[dict] = []
    for item in items:
        track = item.get("track") or {}
        artists = [artist.get("name", "") for artist in track.get("artists", []) if artist.get("name")]
        album = (track.get("album") or {}).get("name", "")
        tracks.append({"name": track.get("name", ""), "artists": artists, "album": album})
    return tracks


def resolve_spotify_playlist_tracks_api(access_token: str, playlist_id: str) -> tuple[str, list[dict]]:
    playlist_response = requests.get(
        f"https://api.spotify.com/v1/playlists/{playlist_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"fields": "name"},
        timeout=20,
    )
    playlist_response.raise_for_status()
    playlist_name = (playlist_response.json() or {}).get("name") or "Spotify Playlist"
    items = spotify_api_paginate(
        access_token,
        f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        {
            "limit": 100,
            "fields": "items(track(name,artists(name),album(name))),next",
        },
    )
    tracks: list[dict] = []
    for item in items:
        track = item.get("track") or {}
        artists = [artist.get("name", "") for artist in track.get("artists", []) if artist.get("name")]
        album = (track.get("album") or {}).get("name", "")
        tracks.append({"name": track.get("name", ""), "artists": artists, "album": album})
    return playlist_name, tracks


def ensure_song_row_cache() -> None:
    if song_row_cache:
        return
    with song_cache_lock:
        if song_row_cache:
            return
        with get_read_conn() as conn:
            rows = conn.execute(
                """
                SELECT song_id, album_url, track_number, track_name, url_320kbps
                FROM songs
                WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
                """
            ).fetchall()
        for row in rows:
            song_row_cache[row[0]] = {
                "song_id": row[0],
                "album_url": row[1],
                "track_number": row[2],
                "track_name": row[3],
                "url_320kbps": row[4],
            }


def get_song_row(song_id: str) -> dict | None:
    ensure_song_row_cache()
    return song_row_cache.get(song_id)


def invalidate_song_cache(song_id: str | None = None) -> None:
    with song_cache_lock:
        if song_id:
            song_row_cache.pop(song_id, None)
            status_cache.pop(song_id, None)
        else:
            song_row_cache.clear()
            status_cache.clear()


def get_cache_path(song_id: str) -> Path:
    return CACHE_DIR / f"{song_id}.mp3"


def get_shared_cache_key(song_id: str) -> str:
    return f"audio/{song_id}.mp3"


def is_audio_content_type(content_type: str | None) -> bool:
    if not content_type:
        return True
    normalized = content_type.lower()
    return normalized.startswith("audio/") or "octet-stream" in normalized


def classify_upstream_response(response: requests.Response | None) -> dict:
    if response is None:
        return {"status": "network", "label": "orange"}
    content_type = response.headers.get("Content-Type")
    if response.status_code in (200, 206) and is_audio_content_type(content_type):
        return {"status": "healthy", "label": "green"}
    if response.status_code in (401, 403, 404):
        return {"status": "unavailable", "label": "red"}
    return {"status": "network", "label": "orange"}


def is_playable_upstream_response(response: requests.Response | None) -> bool:
    if response is None:
        return False
    content_type = response.headers.get("Content-Type")
    return response.status_code in (200, 206) and is_audio_content_type(content_type)


def build_upstream_headers(song_id: str | None = None) -> dict[str, str]:
    headers = dict(UPSTREAM_HEADERS)
    if song_id:
        row = get_song_row(song_id)
        if row and row.get("album_url"):
            headers["Referer"] = row["album_url"]
    if has_request_context():
        for header in ("Range", "If-Range", "If-Modified-Since", "If-None-Match"):
            value = request.headers.get(header)
            if value:
                headers[header] = value
    return headers


def request_upstream(url: str, *, headers: dict[str, str], stream: bool, timeout: tuple[int, int]) -> tuple[requests.Response | None, str]:
    last_response: requests.Response | None = None
    last_source = "requests"
    clients = (
        ("requests", requests.get),
        ("cloudscraper", scraper_core.get_session().get),
    )

    for source, client in clients:
        try:
            response = client(
                url,
                headers=headers,
                stream=stream,
                timeout=timeout,
                allow_redirects=True,
            )
        except requests.RequestException:
            app.logger.warning("Upstream %s request failed for %s", source, url, exc_info=True)
            continue

        if is_playable_upstream_response(response):
            return response, source

        if last_response is not None:
            last_response.close()
        last_response = response
        last_source = source

    return last_response, last_source


def cache_file_looks_valid(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 64 * 1024:
        return False
    try:
        with path.open("rb") as handle:
            prefix = handle.read(512).lower()
    except OSError:
        return False
    return b"<!doctype html" not in prefix and b"<html" not in prefix


def is_cached(song_id: str) -> bool:
    path = get_cache_path(song_id)
    if cache_file_looks_valid(path):
        return True
    if path.exists():
        app.logger.warning("Removing invalid cached audio for %s: %s", song_id, path)
        try:
            path.unlink()
        except OSError:
            pass
    return False


def restore_from_shared_cache(song_id: str) -> bool:
    if not shared_cache.enabled:
        return False
    path = get_cache_path(song_id)
    if is_cached(song_id):
        return True
    restored = shared_cache.fetch_to_path(get_shared_cache_key(song_id), path)
    if not restored:
        return False
    if cache_file_looks_valid(path):
        app.logger.info("Restored %s from shared cache", song_id)
        return True
    try:
        path.unlink()
    except OSError:
        pass
    app.logger.warning("Discarded invalid shared-cache object for %s", song_id)
    return False


def upload_to_shared_cache_async(song_id: str, path: Path) -> None:
    if not shared_cache.enabled or not path.exists():
        return

    def upload():
        ok = shared_cache.upload_path(get_shared_cache_key(song_id), path)
        if not ok:
            app.logger.warning("Shared cache upload failed for %s", song_id)

    thread = threading.Thread(target=upload, daemon=True)
    thread.start()


def get_stream_health(song_id: str, url: str) -> dict:
    if is_cached(song_id):
        return {"status": "healthy", "label": "green"}
    if restore_from_shared_cache(song_id):
        return {"status": "healthy", "label": "green"}

    cached = get_cached_status(song_id)
    if cached:
        return cached

    headers = build_upstream_headers(song_id)
    headers["Range"] = "bytes=0-1"
    response, source = request_upstream(url, headers=headers, stream=True, timeout=(4, 10))
    result = classify_upstream_response(response)

    if response is not None and not is_playable_upstream_response(response):
        app.logger.warning(
            "Health check rejected upstream %s response for %s: status=%s content_type=%s",
            source,
            song_id,
            response.status_code,
            response.headers.get("Content-Type"),
        )
        response.close()
        refreshed = try_refresh_song_link(song_id)
        refreshed_url = refreshed["url_320kbps"] if refreshed else url
        response, source = request_upstream(refreshed_url or url, headers=headers, stream=True, timeout=(4, 10))
        result = classify_upstream_response(response)

    if response is not None:
        response.close()

    return cache_status(song_id, result)


def try_refresh_song_link(song_id: str) -> dict | None:
    row = get_song_row(song_id)
    if not row:
        return None

    album_url = row["album_url"]

    with refresh_lock:
        if album_url in refreshing_albums:
            return get_song_row(song_id)
        refreshing_albums.add(album_url)

    try:
        album, songs = scraper_core.refresh_single_album(album_url)
        with db.get_conn() as conn:
            db.upsert_album(conn, album)
            db.upsert_songs(conn, songs)
        invalidate_song_cache()
        invalidate_library_match_cache()
        return get_song_row(song_id)
    except Exception:
        app.logger.warning("Failed to refresh song link for %s", song_id, exc_info=True)
        return row
    finally:
        with refresh_lock:
            refreshing_albums.discard(album_url)


def download_song_to_cache(song_id: str, url: str) -> None:
    final_path = get_cache_path(song_id)
    temp_path = final_path.with_suffix(".part")

    if is_cached(song_id):
        return

    with download_lock:
        if song_id in active_downloads:
            return
        active_downloads.add(song_id)

    try:
        headers = build_upstream_headers(song_id)
        upstream, source = request_upstream(url, headers=headers, stream=True, timeout=(5, 60))
        if not is_playable_upstream_response(upstream):
            if upstream is not None:
                app.logger.warning(
                    "Cache download rejected upstream %s response for %s: status=%s content_type=%s",
                    source,
                    song_id,
                    upstream.status_code,
                    upstream.headers.get("Content-Type"),
                )
                upstream.close()
            refreshed = try_refresh_song_link(song_id)
            refreshed_url = refreshed["url_320kbps"] if refreshed else url
            upstream, source = request_upstream(refreshed_url or url, headers=headers, stream=True, timeout=(5, 60))
        if not is_playable_upstream_response(upstream):
            if upstream is not None:
                app.logger.warning(
                    "Refusing to cache non-audio upstream response for %s: status=%s content_type=%s",
                    song_id,
                    upstream.status_code,
                    upstream.headers.get("Content-Type"),
                )
                upstream.close()
            return
        with upstream:
            with temp_path.open("wb") as output:
                for chunk in upstream.iter_content(chunk_size=128 * 1024):
                    if chunk:
                        output.write(chunk)
        if cache_file_looks_valid(temp_path):
            temp_path.replace(final_path)
            cache_status(song_id, {"status": "healthy", "label": "green"})
        elif temp_path.exists():
            app.logger.warning("Discarding invalid temp audio cache for %s: %s", song_id, temp_path)
            temp_path.unlink()
    except Exception:
        app.logger.warning("Cache download failed for %s", song_id, exc_info=True)
        if temp_path.exists():
            temp_path.unlink()
    finally:
        with download_lock:
            active_downloads.discard(song_id)


def ensure_song_cached_async(song_id: str, url: str) -> None:
    if is_cached(song_id):
        return
    if restore_from_shared_cache(song_id):
        return
    with download_lock:
        if song_id in active_downloads:
            return
    thread = threading.Thread(target=download_song_to_cache, args=(song_id, url), daemon=True)
    thread.start()
