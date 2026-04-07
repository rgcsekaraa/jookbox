#!/bin/bash
# =============================================================================
#  setup.sh — One-time setup for isaibox Airflow + DuckDB scraper
#  Run once: bash setup.sh
#  Then run: bash start.sh
# =============================================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$PROJECT_DIR/venv"
AIRFLOW_HOME="$PROJECT_DIR/airflow_home"
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
AIRFLOW_VERSION="2.9.3"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  isaibox — Setup"
echo "  Python : $PYTHON_VERSION"
echo "  Airflow: $AIRFLOW_VERSION"
echo "  Dir    : $PROJECT_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Virtual env ──────────────────────────────────────────────────────────
echo ""
echo "[1/5] Creating virtualenv..."
python3 -m venv "$VENV"
source "$VENV/bin/activate"
pip install --quiet --upgrade pip setuptools wheel

# ── 2. Install dependencies ─────────────────────────────────────────────────
echo "[2/5] Installing packages (~2-3 min)..."

CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install --quiet \
  "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "$CONSTRAINT"

pip install --quiet \
  duckdb \
  requests \
  beautifulsoup4 \
  lxml \
  tenacity

echo "  ✓ All packages installed"

# ── 3. Airflow home structure ───────────────────────────────────────────────
echo "[3/5] Setting up Airflow home..."
export AIRFLOW_HOME="$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/dags"
mkdir -p "$AIRFLOW_HOME/logs"
mkdir -p "$AIRFLOW_HOME/plugins"
mkdir -p "$PROJECT_DIR/data"     # DuckDB lives here
mkdir -p "$PROJECT_DIR/exports"  # CSV / Parquet exports

# ── 4. Airflow config ───────────────────────────────────────────────────────
echo "[4/5] Writing airflow.cfg..."
cat > "$AIRFLOW_HOME/airflow.cfg" << AIRFLOWCFG
[core]
dags_folder = $AIRFLOW_HOME/dags
load_examples = False
executor = LocalExecutor

[database]
sql_alchemy_conn = sqlite:///$AIRFLOW_HOME/airflow_meta.db

[webserver]
web_server_port = 8080
secret_key = $(python3 -c "import secrets; print(secrets.token_hex(16))")

[scheduler]
min_file_process_interval = 60
dag_dir_list_interval = 120
AIRFLOWCFG

# Copy DAG into airflow dags folder
cp "$PROJECT_DIR/dags/masstamilan_dag.py" "$AIRFLOW_HOME/dags/"
echo "  ✓ DAG copied to $AIRFLOW_HOME/dags/"

# Write .env for convenience
cat > "$PROJECT_DIR/.env" << ENVFILE
AIRFLOW_HOME=$AIRFLOW_HOME
PROJECT_DIR=$PROJECT_DIR
DUCKDB_PATH=$PROJECT_DIR/data/masstamilan.duckdb
ENVFILE

# ── 5. Init Airflow DB ──────────────────────────────────────────────────────
echo "[5/5] Initialising Airflow metadata DB..."
AIRFLOW_HOME="$AIRFLOW_HOME" "$VENV/bin/airflow" db init 2>&1 | tail -3

# Create admin user
AIRFLOW_HOME="$AIRFLOW_HOME" "$VENV/bin/airflow" users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@isaibox.local 2>&1 | tail -2

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅  Setup complete!"
echo ""
echo "  Next steps:"
echo "    bash start.sh          → Start Airflow webserver + scheduler"
echo "    open http://localhost:8080"
echo "    Login: admin / admin"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
