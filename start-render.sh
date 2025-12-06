#!/usr/bin/env bash
set -euo pipefail

# Base directory (script location)
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Logs
mkdir -p /tmp/app_logs
WORKER_LOG="/tmp/app_logs/remind_worker.log"
WEB_LOG="/tmp/app_logs/web.log"

# Diagnostic info (printed to Render logs)
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

# small delay to let worker create logfile
sleep 2

# Tail the worker log in background so Render's console shows worker output too
# (tail runs in background and will not block starting gunicorn)
if [ -f "${WORKER_LOG}" ]; then
  echo "Tailing worker log (${WORKER_LOG}) to stdout (background)..."
  ( tail -n +1 -F "${WORKER_LOG}" 2>/dev/null ) & TAIL_PID=$!
  echo "tail PID=${TAIL_PID}"
else
  echo "Worker log ${WORKER_LOG} not found yet; starting tail will wait for file."
  ( tail -n +1 -F "${WORKER_LOG}" 2>/dev/null ) & TAIL_PID=$!
  echo "tail PID=${TAIL_PID}"
fi

sleep 1

echo "Starting web server (gunicorn) in foreground; web log will be ${WEB_LOG}"
# Run gunicorn in foreground; Render expects the foreground process to keep running.
exec gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 >> "${WEB_LOG}" 2>&1
