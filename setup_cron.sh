#!/bin/bash
# setup_cron.sh
# Run this script once to configure a cron job that runs main.py daily at 20:30.

set -e

# Path to your repository root and Python interpreter (adjust as needed)
REPO_DIR="/home/sunny/omini-financial-datalake"
PYTHON_BIN="/home/sunny/omini-financial-datalake/omini-financial-datalake/Scripts/python"  # or /usr/bin/python3 if using system env
SCRIPT_PATH="$REPO_DIR/main.py"
LOG_FILE="$REPO_DIR/logs/cron_main.log"

if [ ! -f "$SCRIPT_PATH" ]; then
  echo "Error: main.py not found at $SCRIPT_PATH" >&2
  exit 1
fi

mkdir -p "$(dirname "$LOG_FILE")"

CRON_CMD="$PYTHON_BIN $SCRIPT_PATH >> $LOG_FILE 2>&1"
CRON_EXPR="30 20 * * * $CRON_CMD"

# Avoid duplicate cron entries by removing old versions before adding.
( crontab -l 2>/dev/null | grep -v -F "$SCRIPT_PATH"; echo "$CRON_EXPR" ) | crontab -

echo "Cron job installed: $CRON_EXPR"

echo "Use 'crontab -l' to verify."