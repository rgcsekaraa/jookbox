#!/bin/bash
# =============================================================================
#  check_progress.sh — See how much has been scraped
# =============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
DUCKDB_PATH="$PROJECT_DIR/data/masstamilan.duckdb"
VENV="$PROJECT_DIR/venv"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  isaibox — Progress Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ! -f "$DUCKDB_PATH" ]; then
    echo "❌  Database file not found at $DUCKDB_PATH"
    echo "    (The scraper hasn't started writing yet)"
    exit 0
fi

# Run counts using python/duckdb
"$VENV/bin/python3" -c "
import duckdb
try:
    conn = duckdb.connect('$DUCKDB_PATH', read_only=True)
    
    # Global counts
    albums_ok = conn.execute('SELECT COUNT(*) FROM albums WHERE scrape_ok').fetchone()[0]
    albums_total = conn.execute('SELECT COUNT(*) FROM albums').fetchone()[0]
    songs = conn.execute('SELECT COUNT(*) FROM songs').fetchone()[0]
    
    # Recent activity
    latest = conn.execute('SELECT movie_name, updated_at FROM albums ORDER BY updated_at DESC LIMIT 5').fetchall()
    
    print(f'  Albums Processed : {albums_ok:,} / {albums_total:,}')
    print(f'  Songs in DB      : {songs:,}')
    
    if latest:
        print('\n  Last 5 albums scraped:')
        for movie, dt in latest:
            print(f'    - {movie:30s} ({dt.strftime(\"%H:%M:%S\")})')
    
    conn.close()
except Exception as e:
    print(f'  ⚠️  Error reading DB: {e}')
    print('     (Tables might not be created until the first batch finishes)')
"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
