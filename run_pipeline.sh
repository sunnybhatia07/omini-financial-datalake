#!/bin/bash
set -e

# 1. Set explicit paths so Cron can find core Linux tools
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

REPO_DIR="/home/ubuntu/omini-financial-datalake"
LOG_FILE="$REPO_DIR/logs/cron_main.log"
VENV_NAME="omini-financial-datalake"

# Explicitly target the virtual environment binaries
VENV_PYTHON="$REPO_DIR/$VENV_NAME/bin/python"
VENV_PIP="$REPO_DIR/$VENV_NAME/bin/pip"

cd "$REPO_DIR"

# Ensure logs folder exists
mkdir -p "$(dirname "$LOG_FILE")"

echo "--------------------------------------------------" >> "$LOG_FILE"
echo "[$(date)] Starting automated pipeline..." >> "$LOG_FILE"

# 2. Pull the latest code from GitHub
echo "[$(date)] Executing git pull..." >> "$LOG_FILE"
git pull origin main >> "$LOG_FILE" 2>&1

# 3. Check if the custom venv exists. If not, create it!
if [ ! -d "$VENV_NAME" ]; then
    echo "[$(date)] '$VENV_NAME' not found. Creating virtual environment..." >> "$LOG_FILE"
    python3 -m venv "$VENV_NAME" >> "$LOG_FILE" 2>&1
fi

# 4. Install/Update the exact dependencies using the isolated pip
echo "[$(date)] Verifying dependencies..." >> "$LOG_FILE"
"$VENV_PIP" install -r requirements.txt >> "$LOG_FILE" 2>&1

# 5. Set PYTHONPATH so Python knows where the "utils" and "ingestion" folders are
export PYTHONPATH="$REPO_DIR"

# 6. Run the actual Python ingestion script using the isolated Python
echo "[$(date)] Running ingestion_main.py..." >> "$LOG_FILE"
"$VENV_PYTHON" -m ingestion.ingestion_main >> "$LOG_FILE" 2>&1

# 7. Run silver layer via Glue (daily)
echo "[$(date)] Starting silver layer (Glue)..." >> "$LOG_FILE"
"$VENV_PYTHON" glue_launcher.py >> "$LOG_FILE" 2>&1
echo "[$(date)] Silver layer complete." >> "$LOG_FILE"

echo "[$(date)] Pipeline execution finished." >> "$LOG_FILE"