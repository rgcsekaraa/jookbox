#!/bin/bash
# =============================================================================
#  trigger_full.sh — Force a full re-scrape
# =============================================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Triggering Full Scrape"
echo "  Project: $PROJECT_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

export AIRFLOW_HOME="$AIRFLOW_HOME"

# Set variable
echo "[1/2] Setting MASSTAMILAN_FULL_SCRAPE to true..."
"$VENV/bin/airflow" variables set MASSTAMILAN_FULL_SCRAPE true

# Trigger DAG
echo "[2/2] Triggering scraper DAG..."
"$VENV/bin/airflow" dags trigger masstamilan_daily_scraper

echo ""
echo "✅  DAG Triggered!"
echo "    Check logs at http://localhost:8080"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
