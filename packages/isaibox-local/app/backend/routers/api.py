from backend.services import *

@app.get("/api/health")
def health():
    return json_response({"ok": True})


@app.get("/api/db-sync/status")
def db_sync_status():
    return json_response({"ok": True, "sync": get_db_sync_state()})


@app.post("/api/db-sync/check")
def db_sync_check():
    if not LOCAL_MODE:
        return json_response({"ok": False, "message": "DB sync endpoint is for local mode"}), 403
    payload = request.get_json(silent=True) or {}
    state = sync_duckdb_from_remote(force=bool(payload.get("force")))
    return json_response({"ok": True, "sync": state})


@app.get("/api/stats")
def stats():
    with get_read_conn() as conn:
        songs = conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]
        albums = conn.execute("SELECT COUNT(*) FROM albums WHERE scrape_ok = TRUE").fetchone()[0]
        latest_year = conn.execute(
            "SELECT MAX(TRY_CAST(year AS INTEGER)) FROM songs WHERE year IS NOT NULL AND year != ''"
        ).fetchone()[0]
        latest_updated_at = conn.execute(
            "SELECT MAX(updated_at) FROM songs WHERE updated_at IS NOT NULL"
        ).fetchone()[0]
    if LOCAL_MODE:
        sync_state = get_db_sync_state()
        live_songs = sync_state.get("liveSongs")
        live_albums = sync_state.get("liveAlbums")
        if isinstance(live_songs, int) and live_songs >= 0:
            songs = live_songs
        if isinstance(live_albums, int) and live_albums >= 0:
            albums = live_albums
        sync_updated_at = (sync_state.get("updatedAt") or "").strip()
        if sync_updated_at:
            latest_updated_at_value = sync_updated_at
        else:
            latest_updated_at_value = latest_updated_at.isoformat() if latest_updated_at else ""
    else:
        latest_updated_at_value = latest_updated_at.isoformat() if latest_updated_at else ""
    return json_response(
        {
            "songs": songs,
            "albums": albums,
            "latestYear": latest_year,
            "latestUpdatedAt": latest_updated_at_value,
        }
    )


@app.get("/api/library")
def library():
    limit_raw = request.args.get("limit", "")
    try:
        limit = max(0, min(1000, int(limit_raw))) if limit_raw else 0
    except (TypeError, ValueError):
        limit = 0
    limit_clause = "LIMIT ?" if limit else ""
    params = [limit] if limit else []
    with get_read_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT
                song_id,
                movie_name,
                track_name,
                singers,
                music_director,
                year,
                track_number,
                url_320kbps,
                updated_at,
                album_url
            FROM songs
            WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
            ORDER BY TRY_CAST(year AS INTEGER) DESC NULLS LAST, movie_name, track_number
            {limit_clause}
            """,
            params,
        ).fetchall()

    return json_response({"songs": serialize_song_rows(rows)})


def serialize_song_rows(rows):
    return [
        serialize_song_row(row)
        for row in rows
    ]


def serialize_song_row(row):
    return {
        "id": row[0],
        "movie": row[1] or "",
        "track": row[2] or "",
        "singers": row[3] or "",
        "musicDirector": row[4] or "",
        "year": row[5] or "",
        "trackNumber": row[6] or 0,
        "audioUrl": f"/api/stream/{row[0]}",
        "updatedAt": row[8].isoformat() if row[8] else "",
        "albumUrl": row[9] or "",
    }


def build_library_groups(songs):
    album_groups = {}
    artist_groups = {}
    for song in songs[:600]:
        album_name = (song.get("movie") or "").strip()
        if album_name:
            album = album_groups.setdefault(album_name, {
                "album": album_name,
                "albumUrl": song.get("albumUrl") or "",
                "musicDirector": song.get("musicDirector") or "",
                "year": song.get("year") or "",
                "count": 0,
            })
            album["count"] += 1
        for singer in re.split(r",|&|/| feat\. | featuring ", song.get("singers") or "", flags=re.IGNORECASE):
            singer = singer.strip()
            if not singer:
                continue
            artist = artist_groups.setdefault(singer, {"artist": singer, "count": 0})
            artist["count"] += 1
    return {
        "albums": sorted(album_groups.values(), key=lambda item: (-item["count"], item["album"]))[:24],
        "artists": sorted(artist_groups.values(), key=lambda item: (-item["count"], item["artist"]))[:24],
    }


def search_tokens(value: str) -> list[str]:
    return [
        token
        for token in re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).split()
        if token
    ]


def score_search_song(song: dict, tokens: list[str], phrase: str) -> int:
    fields = {
        "track": search_tokens(song.get("track", "")),
        "movie": search_tokens(song.get("movie", "")),
        "musicDirector": search_tokens(song.get("musicDirector", "")),
        "singers": search_tokens(song.get("singers", "")),
        "year": search_tokens(song.get("year", "")),
    }
    joined = {
        key: " ".join(value)
        for key, value in fields.items()
    }
    score = 0
    matched = 0
    weights = {
        "track": (120, 80, 50),
        "movie": (90, 60, 40),
        "musicDirector": (70, 45, 30),
        "singers": (55, 35, 24),
        "year": (25, 20, 15),
    }
    for token in tokens:
        token_score = 0
        for key, text in joined.items():
            exact, prefix, contains = weights[key]
            if text == token:
                token_score = max(token_score, exact)
            elif text.startswith(token):
                token_score = max(token_score, prefix)
            elif token in text:
                token_score = max(token_score, contains)
            else:
                for field_token in fields[key]:
                    if field_token.startswith(token):
                        token_score = max(token_score, max(12, prefix // 2))
                        break
        if token_score:
            matched += 1
            score += token_score
    if not matched:
        return -1
    if matched == len(tokens):
        score += 50
    if phrase and phrase in joined["track"]:
        score += 70
    if phrase and phrase in joined["movie"]:
        score += 35
    return score


@app.get("/api/search")
def search_library():
    query = (request.args.get("q") or "").strip()
    limit_raw = request.args.get("limit", 240)
    try:
        limit = max(1, min(500, int(limit_raw)))
    except (TypeError, ValueError):
        limit = 240

    tokens = search_tokens(query)
    params = []
    where_query = ""
    if tokens:
        token_clauses = []
        for token in tokens[:5]:
            needle = f"%{token}%"
            token_clauses.append(
                """
                (
                  lower(coalesce(track_name, '')) LIKE ?
                  OR lower(coalesce(movie_name, '')) LIKE ?
                  OR lower(coalesce(singers, '')) LIKE ?
                  OR lower(coalesce(music_director, '')) LIKE ?
                  OR lower(coalesce(year, '')) LIKE ?
                )
                """
            )
            params.extend([needle, needle, needle, needle, needle])
        where_query = f"AND ({' OR '.join(token_clauses)})"
    candidate_limit = max(800, min(2500, limit * 8))
    params.append(candidate_limit)

    with get_read_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT
                song_id,
                movie_name,
                track_name,
                singers,
                music_director,
                year,
                track_number,
                url_320kbps,
                updated_at,
                album_url
            FROM songs
            WHERE url_320kbps IS NOT NULL
              AND url_320kbps != ''
              {where_query}
            ORDER BY TRY_CAST(year AS INTEGER) DESC NULLS LAST, movie_name, track_number
            LIMIT ?
            """,
            params,
        ).fetchall()

    songs = [
        serialize_song_row(row)
        for row in rows
    ]
    if tokens:
        phrase = " ".join(tokens)
        songs = [
            item
            for item in sorted(
                (
                    {**song, "_score": score_search_song(song, tokens, phrase)}
                    for song in songs
                ),
                key=lambda song: (-song["_score"], song.get("track", "")),
            )
            if item["_score"] >= 0
        ]
        for song in songs:
            song.pop("_score", None)
    groups = build_library_groups(songs)
    return json_response({
        "songs": songs[:200],
        "albums": groups["albums"],
        "artists": groups["artists"],
    })


@app.get("/api/album")
def album_tracks():
    album_url = (request.args.get("albumUrl") or "").strip()
    album_name = (request.args.get("album") or request.args.get("movie") or "").strip()
    year = (request.args.get("year") or "").strip()

    filters = ["url_320kbps IS NOT NULL", "url_320kbps != ''"]
    params = []
    if album_url:
        filters.append("album_url = ?")
        params.append(album_url)
    elif album_name:
        filters.append("lower(coalesce(movie_name, '')) = ?")
        params.append(album_name.lower())
        if year:
            filters.append("coalesce(year, '') = ?")
            params.append(year)
    else:
        return json_response({"songs": []})

    with get_read_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT
                song_id,
                movie_name,
                track_name,
                singers,
                music_director,
                year,
                track_number,
                url_320kbps,
                updated_at,
                album_url
            FROM songs
            WHERE {" AND ".join(filters)}
            ORDER BY track_number, track_name
            """,
            params,
        ).fetchall()

    return json_response({"songs": serialize_song_rows(rows)})


@app.post("/api/warmup")
def warmup():
    payload = request.get_json(silent=True) or {}
    limit_raw = payload.get("limit", request.args.get("limit", 24))
    try:
        limit = max(1, min(64, int(limit_raw)))
    except (TypeError, ValueError):
        limit = 24

    with get_read_conn() as conn:
        rows = conn.execute(
            """
            SELECT song_id, url_320kbps
            FROM songs
            WHERE url_320kbps IS NOT NULL AND url_320kbps != ''
            ORDER BY updated_at DESC NULLS LAST, movie_name, track_number
            LIMIT ?
            """,
            [limit],
        ).fetchall()

    queued = 0
    cached = 0
    for song_id, url in rows:
        if is_cached(song_id):
            cached += 1
            continue
        ensure_song_cached_async(song_id, url)
        queued += 1

    return json_response({"ok": True, "limit": limit, "queued": queued, "alreadyCached": cached})


@app.get("/api/cache/status")
def cache_status_overview():
    usage_bytes = get_cache_usage_bytes()
    limit_bytes = get_cache_limit_bytes()
    return json_response(
        {
            "ok": True,
            "usageBytes": usage_bytes,
            "usageMb": round(usage_bytes / (1024 * 1024), 2),
            "limitBytes": limit_bytes,
            "limitGb": round(limit_bytes / (1024 * 1024 * 1024), 2) if limit_bytes else 0,
        }
    )


@app.post("/api/cache/trim")
def cache_trim():
    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force"))
    result = trim_local_cache(force=force)
    return json_response(result)


@app.get("/api/radio/stations")
def radio_stations():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Radio disabled in local mode"}), 404
    force_refresh = request.args.get("refresh") == "1"
    return json_response(get_radio_stations(force_refresh=force_refresh))


@app.post("/api/prefetch")
def prefetch_songs():
    payload = request.get_json(silent=True) or {}
    ids = payload.get("ids") or []
    queued = 0

    for song_id in ids[:3]:
        row = get_song_row(song_id)
        if not row:
            continue
        if is_cached(song_id):
            continue
        ensure_song_cached_async(row["song_id"])
        queued += 1

    return json_response({"ok": True, "queued": queued})


@app.get("/api/song-status/<song_id>")
def song_status(song_id: str):
    row = get_song_row(song_id)
    if not row:
        return json_response({"status": "unavailable", "label": "red"}), 404
    url = row["url_320kbps"]
    return json_response(get_stream_health(song_id, url))


def open_upstream_stream(song_id: str, url: str, album_url: str | None = None):
    headers = build_upstream_headers(song_id=song_id, album_url=album_url)
    upstream, source = request_upstream(url, headers=headers, stream=True, timeout=(5, 30))

    if not is_playable_upstream_response(upstream):
        if upstream is not None:
            app.logger.warning(
                "Upstream stream rejected for %s via %s: status=%s content_type=%s",
                song_id,
                source,
                upstream.status_code,
                upstream.headers.get("Content-Type"),
            )
            upstream.close()
        refreshed = try_refresh_song_link_with_timeout(song_id, force_refresh=True)
        refreshed_url = refreshed["url_320kbps"] if refreshed else url
        upstream, source = request_upstream(refreshed_url or url, headers=headers, stream=True, timeout=(5, 30))
        url = refreshed_url or url
        if upstream is not None:
            app.logger.info(
                "Retried stream for %s via %s: status=%s content_type=%s",
                song_id,
                source,
                upstream.status_code,
                upstream.headers.get("Content-Type"),
            )

    cache_status(song_id, classify_upstream_response(upstream))

    return upstream, url


@app.get("/api/stream/<song_id>")
def stream_song(song_id: str):
    row = get_song_row(song_id)
    if not row:
        app.logger.warning("Stream request for unknown song_id=%s", song_id)
        return json_response({"ok": False, "message": "Song not found"}), 404

    cached_path = get_cache_path(song_id)
    if is_cached(song_id):
        return file_response(cached_path, mimetype="audio/mpeg", conditional=True, etag=True, max_age=3600)
    if restore_from_shared_cache(song_id):
        return file_response(cached_path, mimetype="audio/mpeg", conditional=True, etag=True, max_age=3600)

    row = resolve_song_row_for_playback(song_id) or row
    url = row["url_320kbps"]
    if DIRECT_STREAM_REDIRECT:
        redirect = safe_external_audio_redirect(
            url,
            row.get("album_url"),
            {
                "albumUrl": row.get("album_url"),
                "track": row.get("track_name"),
                "trackNumber": row.get("track_number"),
            },
        )
        if redirect is not None:
            return redirect

    upstream, url = open_upstream_stream(song_id, url, row.get("album_url"))
    if upstream is None or upstream.status_code not in (200, 206) or not is_audio_content_type(upstream.headers.get("Content-Type")):
        app.logger.warning(
            "Stream unavailable for %s: status=%s content_type=%s",
            song_id,
            upstream.status_code if upstream is not None else "none",
            upstream.headers.get("Content-Type") if upstream is not None else "none",
        )
        if upstream is not None:
            upstream.close()
        return json_response({"ok": False, "message": "Upstream stream unavailable"}), 502

    passthrough_headers = {}
    for header in ("Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified"):
        value = upstream.headers.get(header)
        if value:
            passthrough_headers[header] = value

    passthrough_headers.setdefault("Content-Type", "audio/mpeg")
    passthrough_headers.setdefault("Accept-Ranges", "bytes")
    passthrough_headers["Cache-Control"] = "public, max-age=3600"

    should_cache = not request.headers.get("Range") or request.headers.get("Range") == "bytes=0-"
    temp_path = cached_path.with_suffix(".part")

    def generate():
        output = None
        try:
            if should_cache and not is_cached(song_id):
                with download_lock:
                    if song_id not in active_downloads:
                        active_downloads.add(song_id)
                        output = temp_path.open("wb")
            for chunk in upstream.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                if output:
                    output.write(chunk)
                yield chunk
            if output:
                output.close()
                temp_path.replace(cached_path)
                upload_to_shared_cache_async(song_id, cached_path)
                output = None
        finally:
            upstream.close()
            if output:
                output.close()
            if temp_path.exists() and not is_cached(song_id):
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            if should_cache:
                with download_lock:
                    active_downloads.discard(song_id)

    return StreamingResponse(
        stream_generator(generate()),
        status_code=upstream.status_code,
        headers=passthrough_headers,
        media_type=passthrough_headers.get("Content-Type", "audio/mpeg"),
    )


@app.get("/api/config")
def config():
    origin = request.headers.get("Origin", "").rstrip("/")
    return json_response(
        {
            "localMode": LOCAL_MODE,
            "googleClientId": "" if LOCAL_MODE else GOOGLE_CLIENT_ID,
            "geminiRadioEnabled": False if LOCAL_MODE else bool(GEMINI_API_KEYS),
            "geminiKeyCount": len(GEMINI_API_KEYS),
            "spotifyClientId": "" if LOCAL_MODE else SPOTIFY_CLIENT_ID,
            "spotifyRedirectUri": "" if LOCAL_MODE else (SPOTIFY_REDIRECT_URI or origin),
            "spotifyScopes": "" if LOCAL_MODE else SPOTIFY_SCOPES,
        }
    )


@app.get("/api/auth/session")
def auth_session():
    user = get_session_user()
    return json_response({"ok": True, "user": user})


@app.get("/api/me/preferences")
def me_preferences():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    return json_response({"ok": True, "preferences": user_preferences_for_user(user["user_id"])})


@app.put("/api/me/preferences")
def update_me_preferences():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    prefs = save_user_preferences(user["user_id"], payload)
    return json_response({"ok": True, "preferences": prefs})


@app.post("/api/auth/google")
def auth_google():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Authentication disabled in local mode"}), 403
    payload = request.get_json(silent=True) or {}
    credential = payload.get("credential", "")
    if not credential:
        return json_response({"ok": False, "message": "Missing credential"}), 400
    try:
        response = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": credential},
            timeout=15,
        )
        response.raise_for_status()
        token = response.json()
    except requests.RequestException:
        return json_response({"ok": False, "message": "Google verification failed"}), 400

    if token.get("email_verified") not in ("true", True):
        return json_response({"ok": False, "message": "Google account is not verified"}), 400
    if GOOGLE_CLIENT_ID and token.get("aud") != GOOGLE_CLIENT_ID:
        return json_response({"ok": False, "message": "Invalid Google client"}), 400

    user_id = hashlib.md5((token.get("sub") or token.get("email") or "").encode()).hexdigest()
    email = token.get("email", "")
    is_admin = email.lower() in ADMIN_EMAILS if email else False
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, google_sub, email, name, picture, is_admin, last_login_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
                google_sub = excluded.google_sub,
                email = excluded.email,
                name = excluded.name,
                picture = excluded.picture,
                is_admin = CASE WHEN users.is_admin THEN TRUE ELSE excluded.is_admin END,
                last_login_at = excluded.last_login_at,
                updated_at = excluded.updated_at
            """,
            [
                user_id,
                token.get("sub", ""),
                email,
                token.get("name", ""),
                token.get("picture", ""),
                is_admin,
                now_utc(),
                now_utc(),
                now_utc(),
            ],
        )
        user_row = conn.execute(
            "SELECT is_admin, is_banned, ban_reason FROM users WHERE user_id = ?",
            [user_id],
        ).fetchone()
    if user_row and user_row[1]:
        return json_response({"ok": False, "message": user_row[2] or "Account has been banned"}), 403
    user_payload = {
        "user_id": user_id,
        "email": email,
        "name": token.get("name", ""),
        "picture": token.get("picture", ""),
        "is_admin": bool(user_row[0]) if user_row else False,
        "is_banned": bool(user_row[1]) if user_row else False,
        "ban_reason": user_row[2] or "" if user_row else "",
    }
    return issue_session_response(user_id, user_payload)


@app.post("/api/auth/logout")
def auth_logout():
    if LOCAL_MODE:
        return json_response({"ok": True})
    user = get_session_user()
    if user:
        with db.get_conn() as conn:
            conn.execute("DELETE FROM user_sessions WHERE session_id = ?", [user["session_id"]])
    return clear_session_response()


@app.get("/api/favorites")
def favorites():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    return json_response({"ok": True, **favorite_payload_for_user(user["user_id"])})


@app.post("/api/favorites/<song_id>")
def add_favorite(song_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO favorite_songs (user_id, song_id, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, song_id) DO NOTHING
            """,
            [user["user_id"], song_id, now_utc()],
        )
    return json_response({"ok": True})


@app.delete("/api/favorites/<song_id>")
def remove_favorite(song_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        conn.execute("DELETE FROM favorite_songs WHERE user_id = ? AND song_id = ?", [user["user_id"], song_id])
    return json_response({"ok": True})


@app.post("/api/favorites/albums")
def add_album_favorite():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    album_name = (payload.get("albumName") or "").strip()
    album_url = (payload.get("albumUrl") or "").strip()
    if not album_name and not album_url:
        return json_response({"ok": False, "message": "albumName or albumUrl is required"}), 400
    with db.get_conn() as conn:
        if album_url:
            conn.execute(
                """
                INSERT INTO favorite_album_entities (user_id, album_url, album_name, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (user_id, album_url) DO UPDATE SET
                    album_name = excluded.album_name,
                    created_at = excluded.created_at
                """,
                [user["user_id"], album_url, album_name, now_utc()],
            )
        elif album_name:
            conn.execute(
                """
                INSERT INTO favorite_albums (user_id, album_name, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id, album_name) DO NOTHING
                """,
                [user["user_id"], album_name, now_utc()],
            )
    return json_response({"ok": True})


@app.delete("/api/favorites/albums")
def remove_album_favorite():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    album_name = (payload.get("albumName") or "").strip()
    album_url = (payload.get("albumUrl") or "").strip()
    if not album_name and not album_url:
        return json_response({"ok": False, "message": "albumName or albumUrl is required"}), 400
    with db.get_conn() as conn:
        if album_url:
            conn.execute("DELETE FROM favorite_album_entities WHERE user_id = ? AND album_url = ?", [user["user_id"], album_url])
        if album_name:
            conn.execute("DELETE FROM favorite_albums WHERE user_id = ? AND album_name = ?", [user["user_id"], album_name])
    return json_response({"ok": True})


@app.post("/api/favorites/music-directors")
def add_music_director_favorite():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    music_director = (payload.get("musicDirector") or "").strip()
    if not music_director:
        return json_response({"ok": False, "message": "musicDirector is required"}), 400
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO favorite_music_directors (user_id, music_director, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, music_director) DO NOTHING
            """,
            [user["user_id"], music_director, now_utc()],
        )
    return json_response({"ok": True})


@app.delete("/api/favorites/music-directors")
def remove_music_director_favorite():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    music_director = (payload.get("musicDirector") or "").strip()
    if not music_director:
        return json_response({"ok": False, "message": "musicDirector is required"}), 400
    with db.get_conn() as conn:
        conn.execute(
            "DELETE FROM favorite_music_directors WHERE user_id = ? AND music_director = ?",
            [user["user_id"], music_director],
        )
    return json_response({"ok": True})


@app.get("/api/playlists")
def playlists():
    user = get_session_user()
    if not user:
        return json_response({"ok": True, "playlists": [], "globalPlaylists": global_playlists()})
    return json_response(
        {
            "ok": True,
            "playlists": playlists_for_user(user["user_id"]),
            "globalPlaylists": global_playlists(),
        }
    )


@app.post("/api/playlists")
def create_playlist():
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        name = normalize_playlist_name(payload.get("name", ""))
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    playlist_id = secrets.token_hex(16)
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
            VALUES (?, ?, ?, FALSE, 'manual', '', ?, ?)
            """,
            [playlist_id, user["user_id"], name, now_utc(), now_utc()],
        )
    return json_response({"ok": True, "playlist": {"id": playlist_id, "name": name, "isGlobal": False, "source": "manual", "sourceUrl": "", "trackCount": 0}})


@app.post("/api/admin/playlists/ai/generate")
def admin_generate_ai_playlists():
    user, error_response = require_admin_user()
    if error_response:
        return error_response
    payload = create_ai_playlists_for_user(user["user_id"])
    return json_response(payload)


@app.get("/api/playlists/<playlist_id>")
def get_playlist(playlist_id: str):
    user = get_session_user()
    if playlist_id == RANDOM_500_PLAYLIST_ID:
        return json_response({"ok": True, "playlist": random_500_playlist_detail()})
    with get_read_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, name, is_global, source, source_url, user_id, updated_at FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
    if not playlist:
        return json_response({"ok": False, "message": "Playlist not found"}), 404
    if not playlist[2]:
        if not user or playlist[5] != user["user_id"]:
            return json_response({"ok": False, "message": "Authentication required"}), 401
    elif user and not playlist[2] and playlist[5] != user["user_id"]:
        return json_response({"ok": False, "message": "Playlist not found"}), 404
    return json_response(
        {
            "ok": True,
            "playlist": {
                "id": playlist[0],
                "name": playlist[1] or "",
                "isGlobal": bool(playlist[2]),
                "source": playlist[3] or "manual",
                "sourceUrl": playlist[4] or "",
                "updatedAt": playlist[6].isoformat() if playlist[6] else "",
                "tracks": playlist_tracks(playlist_id),
            },
        }
    )


@app.put("/api/playlists/<playlist_id>")
def update_playlist(playlist_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        playlist = rename_playlist_for_user(user, playlist_id, payload.get("name", ""))
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    except LookupError as exc:
        return json_response({"ok": False, "message": str(exc)}), 404
    return json_response({"ok": True, "playlist": playlist})


@app.put("/api/playlists/<playlist_id>/songs/reorder")
def reorder_playlist(playlist_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        result = reorder_playlist_songs_for_user(user, playlist_id, payload.get("songIds") or [])
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    except LookupError as exc:
        return json_response({"ok": False, "message": str(exc)}), 404
    return json_response(result)


@app.post("/api/playlists/<playlist_id>/songs")
def add_song_to_playlist(playlist_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    song_id = payload.get("songId", "")
    if not song_id:
        return json_response({"ok": False, "message": "songId is required"}), 400
    with db.get_conn() as conn:
        song = conn.execute(
            f"""
            SELECT song_id
            FROM songs
            WHERE song_id = ?
              AND url_320kbps IS NOT NULL
              AND url_320kbps != ''
              AND {playlist_eligible_song_sql('')}
            LIMIT 1
            """,
            [song_id],
        ).fetchone()
        if not song:
            return json_response({"ok": False, "message": "Song not found"}), 404
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            return json_response({"ok": False, "message": "Playlist not found"}), 404
        existing = conn.execute(
            "SELECT 1 FROM playlist_songs WHERE playlist_id = ? AND song_id = ? LIMIT 1",
            [playlist_id, song_id],
        ).fetchone()
        if existing:
            track_count = conn.execute(
                "SELECT COUNT(*) FROM playlist_songs WHERE playlist_id = ?",
                [playlist_id],
            ).fetchone()[0]
            return json_response(
                {
                    "ok": True,
                    "alreadyExists": True,
                    "playlist": {"id": playlist_id, "trackCount": track_count},
                }
            )
        next_position = conn.execute(
            "SELECT COALESCE(MAX(position), 0) + 1 FROM playlist_songs WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO playlist_songs (playlist_id, song_id, position, added_at)
            VALUES (?, ?, ?, ?)
            """,
            [playlist_id, song_id, next_position, now_utc()],
        )
        conn.execute("UPDATE playlists SET updated_at = ? WHERE playlist_id = ?", [now_utc(), playlist_id])
        track_count = conn.execute(
            "SELECT COUNT(*) FROM playlist_songs WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()[0]
    return json_response(
        {
            "ok": True,
            "playlist": {"id": playlist_id, "trackCount": track_count},
            "track": next((item for item in playlist_tracks(playlist_id) if item["id"] == song_id), None),
        }
    )


@app.delete("/api/playlists/<playlist_id>/songs/<song_id>")
def remove_song_from_playlist(playlist_id: str, song_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            return json_response({"ok": False, "message": "Playlist not found"}), 404
        conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ? AND song_id = ?", [playlist_id, song_id])
        rows = conn.execute(
            "SELECT song_id FROM playlist_songs WHERE playlist_id = ? ORDER BY position, added_at",
            [playlist_id],
        ).fetchall()
        conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
        conn.executemany(
            "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
            [[playlist_id, row[0], index + 1, now_utc()] for index, row in enumerate(rows)],
        )
        conn.execute("UPDATE playlists SET updated_at = ? WHERE playlist_id = ?", [now_utc(), playlist_id])
    return json_response({"ok": True})


@app.delete("/api/playlists/<playlist_id>/songs")
def clear_playlist(playlist_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            return json_response({"ok": False, "message": "Playlist not found"}), 404
        conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
        conn.execute("UPDATE playlists SET updated_at = ? WHERE playlist_id = ?", [now_utc(), playlist_id])
    return json_response({"ok": True})


@app.delete("/api/playlists/<playlist_id>")
def delete_playlist(playlist_id: str):
    user, error_response = require_session_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        playlist = conn.execute(
            "SELECT playlist_id, is_global, user_id FROM playlists WHERE playlist_id = ?",
            [playlist_id],
        ).fetchone()
        if not playlist or (playlist[1] and not user["is_admin"]) or (not playlist[1] and playlist[2] != user["user_id"]):
            return json_response({"ok": False, "message": "Playlist not found"}), 404
        conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
        conn.execute("DELETE FROM playlists WHERE playlist_id = ?", [playlist_id])
    return json_response({"ok": True})


@app.post("/api/playlists/import/spotify")
def import_spotify_playlist():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Spotify import disabled in local mode"}), 403
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        playlist_url = normalize_spotify_playlist_url(payload.get("url", ""))
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    access_token = (payload.get("accessToken") or "").strip()
    try:
        playlist_name, tracks = resolve_spotify_playlist_tracks(playlist_url, access_token=access_token)
    except Exception as exc:
        app.logger.warning("Spotify import failed", exc_info=True)
        return json_response({"ok": False, "message": str(exc) or "Spotify import failed"}), 400

    matched_ids, matched_details, unmatched = match_spotify_tracks(tracks)
    with db.get_conn() as conn:
        existing_playlist = conn.execute(
            """
            SELECT playlist_id
            FROM playlists
            WHERE user_id = ? AND source = 'spotify' AND source_url = ? AND is_global = FALSE
            LIMIT 1
            """,
            [user["user_id"], playlist_url],
        ).fetchone()
        if existing_playlist:
            playlist_id = existing_playlist[0]
            conn.execute(
                "UPDATE playlists SET name = ?, updated_at = ? WHERE playlist_id = ?",
                [playlist_name, now_utc(), playlist_id],
            )
            conn.execute("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id])
        else:
            playlist_id = secrets.token_hex(16)
            conn.execute(
                """
                INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
                VALUES (?, ?, ?, FALSE, 'spotify', ?, ?, ?)
                """,
                [playlist_id, user["user_id"], playlist_name, playlist_url, now_utc(), now_utc()],
            )
        conn.executemany(
            "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
            [[playlist_id, song_id, index + 1, now_utc()] for index, song_id in enumerate(matched_ids)],
        )
    return json_response(
        {
            "ok": True,
            "playlist": {
                "id": playlist_id,
                "name": playlist_name,
                "isGlobal": False,
                "source": "spotify",
                "sourceUrl": playlist_url,
                "trackCount": len(matched_ids),
            },
            "matchedCount": len(matched_ids),
            "totalCount": len(tracks),
            "updatedExisting": bool(existing_playlist),
            "matched": matched_details[:20],
            "unmatched": unmatched[:20],
        }
    )


@app.post("/api/spotify/playlists")
def spotify_playlists():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Spotify disabled in local mode"}), 403
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    access_token = (payload.get("accessToken") or "").strip()
    if not access_token:
        return json_response({"ok": False, "message": "Spotify access token is required"}), 400
    try:
        playlists = resolve_spotify_account_playlists(access_token)
    except requests.RequestException:
        app.logger.warning("Spotify playlist listing failed", exc_info=True)
        return json_response({"ok": False, "message": "Unable to load Spotify playlists"}), 400
    return json_response({"ok": True, "playlists": playlists})


@app.post("/api/spotify/import/liked-songs")
def spotify_import_liked_songs():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Spotify disabled in local mode"}), 403
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    access_token = (payload.get("accessToken") or "").strip()
    if not access_token:
        return json_response({"ok": False, "message": "Spotify access token is required"}), 400
    try:
        tracks = resolve_spotify_saved_tracks(access_token)
    except requests.RequestException:
        app.logger.warning("Spotify liked songs import failed", exc_info=True)
        return json_response({"ok": False, "message": "Unable to load Spotify liked songs"}), 400

    matched_ids, _, unmatched = match_spotify_tracks(tracks)

    with db.get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO favorite_songs (user_id, song_id, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, song_id) DO NOTHING
            """,
            [[user["user_id"], song_id, now_utc()] for song_id in matched_ids],
        )

    return json_response(
        {
            "ok": True,
            "matchedCount": len(matched_ids),
            "totalCount": len(tracks),
            "unmatched": unmatched[:20],
        }
    )


@app.post("/api/spotify/import/playlist")
def spotify_import_account_playlist():
    if LOCAL_MODE:
        return json_response({"ok": False, "message": "Spotify disabled in local mode"}), 403
    user, error_response = require_session_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    access_token = (payload.get("accessToken") or "").strip()
    playlist_id = (payload.get("playlistId") or "").strip()
    if not access_token or not playlist_id:
        return json_response({"ok": False, "message": "Spotify access token and playlistId are required"}), 400

    try:
        playlist_name, tracks = resolve_spotify_playlist_tracks_api(access_token, playlist_id)
    except requests.RequestException:
        app.logger.warning("Spotify account playlist import failed", exc_info=True)
        return json_response({"ok": False, "message": "Unable to import Spotify playlist"}), 400

    matched_ids, _, unmatched = match_spotify_tracks(tracks)

    playlist_id_local = secrets.token_hex(16)
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
            VALUES (?, ?, ?, FALSE, 'spotify', ?, ?, ?)
            """,
            [playlist_id_local, user["user_id"], playlist_name, f"spotify:playlist:{playlist_id}", now_utc(), now_utc()],
        )
        conn.executemany(
            "INSERT INTO playlist_songs (playlist_id, song_id, position, added_at) VALUES (?, ?, ?, ?)",
            [[playlist_id_local, song_id, index + 1, now_utc()] for index, song_id in enumerate(matched_ids)],
        )

    return json_response(
        {
            "ok": True,
            "playlist": {
                "id": playlist_id_local,
                "name": playlist_name,
                "isGlobal": False,
                "source": "spotify",
                "sourceUrl": f"spotify:playlist:{playlist_id}",
                "trackCount": len(matched_ids),
            },
            "matchedCount": len(matched_ids),
            "totalCount": len(tracks),
            "unmatched": unmatched[:20],
        }
    )


@app.post("/api/admin/playlists")
def admin_create_global_playlist():
    admin_user, error_response = require_admin_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        name = normalize_playlist_name(payload.get("name", ""))
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    playlist_id = secrets.token_hex(16)
    with db.get_conn() as conn:
        conn.execute(
            """
            INSERT INTO playlists (playlist_id, user_id, name, is_global, source, source_url, created_at, updated_at)
            VALUES (?, ?, ?, TRUE, 'manual', '', ?, ?)
            """,
            [playlist_id, admin_user["user_id"], name, now_utc(), now_utc()],
        )
    return json_response({"ok": True, "playlist": {"id": playlist_id, "name": name, "isGlobal": True, "source": "manual", "sourceUrl": "", "trackCount": 0}})


@app.post("/api/admin/radio-stations/<station_id>/playlist")
def admin_save_radio_station_as_global_playlist(station_id: str):
    admin_user, error_response = require_admin_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    try:
        payload = create_global_playlist_from_radio_station(
            admin_user["user_id"],
            station_id,
            mode=(payload.get("mode") or "upsert").strip(),
            target_playlist_id=(payload.get("targetPlaylistId") or "").strip(),
            name=(payload.get("name") or "").strip(),
        )
    except ValueError as exc:
        return json_response({"ok": False, "message": str(exc)}), 400
    return json_response(payload)


@app.get("/api/admin/overview")
def admin_overview():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    with get_read_conn() as conn:
        users = conn.execute(
            """
            SELECT user_id, email, name, picture, is_admin, is_banned, ban_reason, last_login_at, created_at
            FROM users
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
    return json_response(
        {
            "ok": True,
            "users": [
                {
                    "userId": row[0],
                    "email": row[1] or "",
                    "name": row[2] or "",
                    "picture": row[3] or "",
                    "isAdmin": bool(row[4]),
                    "isBanned": bool(row[5]),
                    "banReason": row[6] or "",
                    "lastLoginAt": row[7].isoformat() if row[7] else "",
                    "createdAt": row[8].isoformat() if row[8] else "",
                }
                for row in users
            ],
            "airflow": airflow_process_status(),
        }
    )


@app.post("/api/admin/users/<user_id>/ban")
def admin_ban_user(user_id: str):
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    reason = (payload.get("reason") or "Banned by admin").strip()
    with db.get_conn() as conn:
        conn.execute(
            "UPDATE users SET is_banned = TRUE, ban_reason = ?, updated_at = ? WHERE user_id = ?",
            [reason, now_utc(), user_id],
        )
        conn.execute(
            "DELETE FROM user_sessions WHERE user_id = ?",
            [user_id],
        )
    return json_response({"ok": True})


@app.post("/api/admin/users/<user_id>/unban")
def admin_unban_user(user_id: str):
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    with db.get_conn() as conn:
        conn.execute(
            "UPDATE users SET is_banned = FALSE, ban_reason = '', updated_at = ? WHERE user_id = ?",
            [now_utc(), user_id],
        )
    return json_response({"ok": True})


@app.post("/api/admin/users/<user_id>/admin")
def admin_toggle_admin(user_id: str):
    admin_user, error_response = require_admin_user()
    if error_response:
        return error_response
    payload = request.get_json(silent=True) or {}
    is_admin = bool(payload.get("isAdmin"))
    if admin_user["user_id"] == user_id and not is_admin:
        return json_response({"ok": False, "message": "You cannot remove your own admin access"}), 400
    with db.get_conn() as conn:
        conn.execute(
            "UPDATE users SET is_admin = ?, updated_at = ? WHERE user_id = ?",
            [is_admin, now_utc(), user_id],
        )
    return json_response({"ok": True})


@app.get("/api/admin/airflow")
def admin_airflow():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    return json_response({"ok": True, "airflow": airflow_process_status()})


@app.post("/api/admin/airflow/start")
def admin_airflow_start():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    code, stdout, stderr = run_local_command(["bash", "start.sh"])
    return json_response({"ok": code == 0, "stdout": stdout, "stderr": stderr, "airflow": airflow_process_status()}), (200 if code == 0 else 500)


@app.post("/api/admin/airflow/stop")
def admin_airflow_stop():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    code, stdout, stderr = run_local_command(["bash", "stop.sh"])
    return json_response({"ok": code == 0, "stdout": stdout, "stderr": stderr, "airflow": airflow_process_status()}), (200 if code == 0 else 500)


@app.post("/api/admin/airflow/trigger-full")
def admin_airflow_trigger_full():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    code, stdout, stderr = run_local_command(["bash", "trigger_full.sh"])
    return json_response({"ok": code == 0, "stdout": stdout, "stderr": stderr, "airflow": airflow_process_status()}), (200 if code == 0 else 500)


@app.post("/api/admin/airflow/trigger")
def admin_airflow_trigger():
    _, error_response = require_admin_user()
    if error_response:
        return error_response
    code, stdout, stderr = airflow_cli(["dags", "trigger", "masstamilan_daily_scraper"])
    return json_response({"ok": code == 0, "stdout": stdout, "stderr": stderr, "airflow": airflow_process_status()}), (200 if code == 0 else 500)
