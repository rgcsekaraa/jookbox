#!/bin/bash

# Configuration
PROJECT_DIR="/Users/rgchandrasekaraa/Downloads/isaibox"
LOG_FILE="$PROJECT_DIR/data/daily_scrape.log"

cd "$PROJECT_DIR" || exit 1

echo "--------------------------------------------------------" >> "$LOG_FILE"
echo "Daily Scrape started at $(date)" >> "$LOG_FILE"

# Activate venv
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "ERROR: venv/bin/activate not found" >> "$LOG_FILE"
    exit 1
fi

# Run incremental scraper (without --full)
# It will naturally stop when it finds existing URLs on Page 1 or 2.
python3 run_standalone.py >> "$LOG_FILE" 2>&1

echo "Daily Scrape finished at $(date)" >> "$LOG_FILE"
echo "--------------------------------------------------------" >> "$LOG_FILE"
