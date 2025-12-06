#!/usr/bin/env bash
set -euo pipefail

# Base directory (script location)
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Logs
mkdir -p /tmp/app_logs
WORKER_LOG="/tmp/app_logs/remind_worker.log"
WEB_LOG="/tmp/app_logs/web.log"

# Diagnostic info
echo "=== start-render.sh starting ==="
echo "PWD: $(pwd)"
echo "BASE_DIR: ${BASE_DIR}"
echo "PATH: ${PATH}"
python3 -V || true
if command -v gunicorn >/dev/null 2>&1; then
  echo "gunicorn found: $(command -v gunicorn)"
else
  echo "WARNING: gunicorn not found in PATH. Make sure 'gunicorn' is in requirements.txt"
fi

echo "Starting remind_worker in background (nohup) and logging to ${WORKER_LOG} ..."
nohup python3 "${BASE_DIR}/remind_worker.py" >> "${WORKER_LOG}" 2>&1 & 
WORKER_PID=$!
echo "remind_worker PID=${WORKER_PID}"

sleep 2

# Tail worker logs
echo "Tailing worker log (${WORKER_LOG})..."
( tail -n +1 -F "${WORKER_LOG}" 2>/dev/null ) & TAIL_PID=$!
echo "tail PID=${TAIL_PID}"

sleep 1

# 🔥 IN ENV Ở ĐÂY — TRƯỚC exec
echo "ENV: TIMEZONE=${TIMEZONE:-<not set>}"
echo "ENV: REMIND_HOUR=${REMIND_HOUR:-<not set>}"
echo "ENV: REMIND_MINUTE=${REMIND_MINUTE:-<not set>}"
echo "ENV: MIN_REPEAT_MINUTES=${MIN_REPEAT_MINUTES:-<not set>}"

echo "Starting web server (gunicorn) in foreground; web log will be ${WEB_LOG}"

# MUST BE LAST — exec replaces the shell
exec gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 >> "${WEB_LOG}" 2>&1

