#!/usr/bin/env bash
set -euo pipefail

# --- PATH & LOG SETUP ---
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p /tmp/app_logs

WORKER_LOG="/tmp/app_logs/remind_worker.log"
WEB_LOG="/tmp/app_logs/web.log"
PING_LOG="/tmp/app_logs/ping.log"

echo "=== start-render.sh starting ==="
echo "PWD: $(pwd)"
echo "BASE_DIR: ${BASE_DIR}"
echo "PATH: ${PATH}"
python3 -V || true


# --- START remind_worker.py IN BACKGROUND ---
echo "Starting remind_worker in background (nohup)..."
nohup python3 "${BASE_DIR}/remind_worker.py" >> "${WORKER_LOG}" 2>&1 &
WORKER_PID=$!
echo "remind_worker PID=${WORKER_PID}"

sleep 2

# --- TAIL WORKER LOG TO RENDER OUTPUT (NON-BLOCKING) ---
if [ -f "${WORKER_LOG}" ]; then
  echo "Tailing remind_worker log..."
  ( tail -n +1 -F "${WORKER_LOG}" ) &
else
  echo "remind_worker.log not found; tail will wait for file."
  ( tail -n +1 -F "${WORKER_LOG}" ) &
fi


# --- AUTO-PING BACKGROUND PROCESS ---
PING_URL="https://telegram-notion-bot-tpm2.onrender.com"

echo "Starting auto-ping in background..."

(
  while true; do
    # Lấy giờ UTC hiện tại rồi quy đổi sang giờ VN
    HOUR_UTC=$(date +"%H")
    VN_HOUR=$(( (HOUR_UTC + 7) % 24 ))

    if [ $VN_HOUR -ge 9 ] && [ $VN_HOUR -lt 24 ]; then
      STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PING_URL")
      TS=$(date "+%Y-%m-%d %H:%M:%S")
      echo "[AutoPing][${TS}] VN ${VN_HOUR}:00 → Ping: ${STATUS}" | tee -a "${PING_LOG}"
    else
      TS=$(date "+%Y-%m-%d %H:%M:%S")
      echo "[AutoPing][${TS}] VN ${VN_HOUR}:00 → Ngoài giờ làm việc — không ping." | tee -a "${PING_LOG}"
    fi

    sleep 300
  done
) &


# --- START GUNICORN (FOREGROUND) ---
echo "Starting web server (gunicorn)..."
exec gunicorn app:app \
  --bind 0.0.0.0:$PORT \
  --workers 2 \
  --threads 4 \
  --timeout 120 \
  >> "${WEB_LOG}" 2>&1
