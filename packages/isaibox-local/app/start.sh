#!/bin/bash
# =============================================================================
#  start.sh — Start Airflow webserver and scheduler
# =============================================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting Airflow Services"
echo "  Home: $AIRFLOW_HOME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Ensure any old local Airflow processes are stopped first
bash "$PROJECT_DIR/stop.sh" >/dev/null 2>&1 || true

export AIRFLOW_HOME="$AIRFLOW_HOME"

# Ensure venv binaries are in PATH
export PATH="$VENV/bin:$PATH"

# macOS Stability — use 'spawn' for multiprocessing to avoid forking issues
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW__CORE__MP_START_METHOD=spawn
export NO_PROXY="*"

# Airflow Concurrency & SQLite Stability
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__DAG_CONCURRENCY=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=600

# Prevent Gunicorn SIGSEGVs
export AIRFLOW__WEBSERVER__WORKERS=1
export AIRFLOW__WEBSERVER__WORKER_CLASS=sync

# Start webserver in daemon mode
echo "[1/2] Starting Webserver on port 8080..."
airflow webserver --port 8080 -D

# Give webserver a moment to start before launching scheduler
sleep 5

# Start scheduler in daemon mode
echo "[2/2] Starting Scheduler..."
airflow scheduler -D

echo ""
echo "✅  Airflow is running!"
echo "    UI: http://localhost:8080"
echo "    Login: admin / admin"
echo ""
echo "Use 'bash stop.sh' to shut down."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
