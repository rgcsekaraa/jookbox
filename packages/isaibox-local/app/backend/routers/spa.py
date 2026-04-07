from backend.services import DIST_DIR, app, directory_file_response, json_response


@app.get("/")
def serve_index():
    if DIST_DIR.exists():
        return directory_file_response(DIST_DIR, "index.html")
    return json_response(
        {
            "ok": False,
            "message": "Frontend not built yet. Run `npm install` then `npm run dev` or `npm run build`.",
        }
    ), 503


@app.get("/<path:path>")
def serve_spa(path: str):
    candidate = DIST_DIR / path
    if candidate.exists() and candidate.is_file():
        return directory_file_response(DIST_DIR, path)
    if DIST_DIR.exists():
        return directory_file_response(DIST_DIR, "index.html")
    return serve_index()
