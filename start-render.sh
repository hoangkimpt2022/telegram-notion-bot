#!/usr/bin/env bash
set -e

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p /tmp/app_logs
WORKER_LOG="/tmp/app_logs/remind_worker.log"
WEB_LOG="/tmp/app_logs/web.log"

echo "Starting remind_worker in background..."
nohup python3 "${BASE_DIR}/remind_worker.py" >> "${WORKER_LOG}" 2>&1 &

sleep 2

echo "Starting web server (gunicorn) in foreground..."
exec gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120 >> "${WEB_LOG}" 2>&1
