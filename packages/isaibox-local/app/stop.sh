#!/bin/bash
# =============================================================================
#  stop.sh — Stop Airflow processes
# =============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Stopping Airflow Services"
echo "  Home: $AIRFLOW_HOME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Find and kill PIDs
find "$AIRFLOW_HOME" -name "*.pid" -type f -print | while read pid_file; do
    pid=$(cat "$pid_file" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        echo "Stopping process $pid (from $pid_file)..."
        kill "$pid" 2>/dev/null || echo "Process $pid already stopped."
    fi
    rm -f "$pid_file"
done

# Kill any listener currently bound to the Airflow webserver port.
lsof -ti tcp:8080 | while read pid; do
    if [ -n "$pid" ]; then
        echo "Stopping listener on 8080: $pid"
        kill "$pid" 2>/dev/null || true
    fi
done

echo "✅  All Airflow services stopped."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
