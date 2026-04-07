#!/usr/bin/env python3
"""
view_db.py — Comprehensive DuckDB viewer for isaibox data.
Allows searching by album, year, or director, and exporting to CSV.
"""

import os
import sys
import duckdb
from pathlib import Path
from datetime import datetime

# Build path to the database
_HERE = Path(__file__).resolve().parent
DB_PATH = _HERE / "data" / "masstamilan.duckdb"

def get_conn():
    if not DB_PATH.exists():
        print(f"❌ Error: Database not found at {DB_PATH}")
        print("   Run the scraper first to generate data.")
        sys.exit(1)
    return duckdb.connect(str(DB_PATH), read_only=True)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(title):
    print("\n" + "="*60)
    print(f" {title.upper()} ".center(60, " "))
    print("="*60)

def list_albums(conn, limit=20, offset=0):
    query = f"""
        SELECT movie_name, music_director, year, track_count, album_url 
        FROM albums 
        ORDER BY updated_at DESC 
        LIMIT {limit} OFFSET {offset}
    """
    rows = conn.execute(query).fetchall()
    
    print_header(f"Latest {len(rows)} Albums (Page {offset//limit + 1})")
    print(f"{'#':<3} {'Movie Name':<25} {'Director':<15} {'Year':<6} {'Tracks'}")
    print("-" * 60)
    for i, r in enumerate(rows, 1):
        print(f"{i:<3} {str(r[0])[:24]:<25} {str(r[1])[:14]:<15} {str(r[2]):<6} {r[3]}")
    
    return rows

def inspect_album(conn, album_url):
    album = conn.execute("SELECT * FROM albums WHERE album_url = ?", [album_url]).fetchone()
    if not album:
        print("❌ Album not found.")
        return

    # Map column names (crudely but effectively for a viewer)
    cols = ["URL", "Movie", "Starring", "MD", "Director", "Lyricists", "Year", "Lang", "Tracks", "OK", "Seen", "Updated"]
    
    print_header(f"Album Details: {album[1]}")
    for label, val in zip(cols, album):
        if val:
            print(f"{label:<15}: {val}")

    songs = conn.execute("SELECT track_number, track_name, singers, url_128kbps, url_320kbps FROM songs WHERE album_url = ? ORDER BY track_number", [album_url]).fetchall()
    print("\nTRACKLIST:")
    print(f"{'No':<3} {'Track Name':<25} {'128kbps':<10} {'320kbps':<10} {'Singers'}")
    print("-" * 80)
    for s in songs:
        # Show 'YES' or the actual URL if it's small, or handle display
        d128 = "✓" if s[3] else "-"
        d320 = "✓" if s[4] else "-"
        print(f"{s[0]:<3} {str(s[1])[:24]:<25} {d128:<10} {d320:<10} {str(s[2])[:25]}")
    
    print("\nDOWNLOAD LINKS:")
    for s in songs:
        if s[4] or s[3]:
            print(f"Track {s[0]} ({s[1]}):")
            if s[4]: print(f"  [320] {s[4]}")
            if s[3]: print(f"  [128] {s[3]}")

def search_db(conn, term):
    term = f"%{term}%"
    rows = conn.execute("""
        SELECT movie_name, music_director, year, album_url 
        FROM albums 
        WHERE movie_name ILIKE ? OR music_director ILIKE ? OR starring ILIKE ?
    """, [term, term, term]).fetchall()
    
    if not rows:
        print(f"\n🔍 No albums found matching '{term}'")
    else:
        print_header(f"Search Results for '{term[1:-1]}'")
        for i, r in enumerate(rows, 1):
            print(f"{i}. {r[0]} ({r[1]}, {r[2]})")
    return rows

def export_csv(conn):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = _HERE / f"isaibox_songs_{timestamp}.csv"
    print(f"\n🚀 Exporting all songs to {outfile.name}...")
    
    # DuckDB's native COPY command is extremely fast
    conn.execute(f"COPY songs TO '{outfile}' (HEADER, DELIMITER ',')")
    print(f"✅ Export complete! {conn.execute('SELECT COUNT(*) FROM songs').fetchone()[0]} rows written.")

def main_menu():
    conn = get_conn()
    current_page = 0
    page_size = 15

    while True:
        clear_screen()
        albums = list_albums(conn, limit=page_size, offset=current_page * page_size)
        
        print("\nCOMMANDS:")
        print("  [n] Next Page          [p] Prev Page")
        print("  [s] Search             [e] Export CSV")
        print("  [#] Open Album #       [q] Quit")
        
        choice = input("\nChoice > ").lower().strip()
        
        if choice == 'q':
            break
        elif choice == 'n':
            current_page += 1
        elif choice == 'p':
            current_page = max(0, current_page - 1)
        elif choice == 's':
            term = input("Search term (Movie/Actor/MD): ")
            results = search_db(conn, term)
            if results:
                try:
                    idx = int(input("\nEnter # to open, or enter to return: ")) - 1
                    inspect_album(conn, results[idx][3])
                    input("\nPress Enter to return...")
                except: pass
        elif choice == 'e':
            export_csv(conn)
            input("\nPress Enter to return...")
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(albums):
                inspect_album(conn, albums[idx][4])
                input("\nPress Enter to return...")
            else:
                print("Invalid index.")
                input()

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\nExiting...")
