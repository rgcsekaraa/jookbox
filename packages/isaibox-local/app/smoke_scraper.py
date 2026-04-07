#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

import scraper_core


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test the isaibox scraper.")
    parser.add_argument("--page", type=int, default=1, help="Listing page to fetch first")
    parser.add_argument("--album-url", help="Optional explicit album URL to test")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON summary")
    args = parser.parse_args()

    listing_url = scraper_core.LIST_URL.format(page=args.page)
    listing_soup = scraper_core.fetch(listing_url)
    album_urls = scraper_core.parse_listing_page(listing_soup)

    if not album_urls and not args.album_url:
      raise RuntimeError(f"No album URLs found on listing page {args.page}")

    album_url = args.album_url or album_urls[0]
    album_soup = scraper_core.fetch(album_url)
    album, songs = scraper_core.parse_album_page(album_soup, album_url)

    summary = {
        "listing_url": listing_url,
        "listing_album_count": len(album_urls),
        "album_url": album_url,
        "movie_name": album.get("movie_name", ""),
        "year": album.get("year", ""),
        "music_director": album.get("music_director", ""),
        "track_count": len(songs),
        "tracks_with_320": sum(1 for song in songs if song.get("url_320kbps")),
        "tracks_with_128": sum(1 for song in songs if song.get("url_128kbps")),
        "sample_tracks": [
            {
                "track_number": song.get("track_number"),
                "track_name": song.get("track_name"),
                "singers": song.get("singers"),
                "has_320": bool(song.get("url_320kbps")),
            }
            for song in songs[:3]
        ],
    }

    if args.json:
        print(json.dumps(summary, indent=2))
        return 0

    print("Scraper smoke test")
    print(f"Listing page       : {listing_url}")
    print(f"Albums discovered  : {summary['listing_album_count']}")
    print(f"Album tested       : {summary['album_url']}")
    print(f"Movie              : {summary['movie_name'] or '-'}")
    print(f"Year               : {summary['year'] or '-'}")
    print(f"Music director     : {summary['music_director'] or '-'}")
    print(f"Tracks parsed      : {summary['track_count']}")
    print(f"320kbps links      : {summary['tracks_with_320']}")
    print(f"128kbps links      : {summary['tracks_with_128']}")
    print("Sample tracks:")
    for track in summary["sample_tracks"]:
        print(
            f"  {track['track_number']}. {track['track_name'] or '-'}"
            f" | {track['singers'] or '-'} | 320={track['has_320']}"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Smoke test failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
