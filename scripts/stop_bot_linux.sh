#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/runtime"
PID_FILE="$RUNTIME_DIR/bot.pid"
STOP_FILE="$RUNTIME_DIR/bot.stop"

mkdir -p "$RUNTIME_DIR"
touch "$STOP_FILE"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file found. Stop signal file created."
  exit 0
fi

BOT_PID="$(tr -d '[:space:]' < "$PID_FILE")"
if [[ -z "$BOT_PID" ]]; then
  rm -f "$PID_FILE"
  echo "PID file was empty. Stop signal file created."
  exit 0
fi

if ! kill -0 "$BOT_PID" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "Process $BOT_PID is not running."
  exit 0
fi

for _ in $(seq 1 45); do
  if ! kill -0 "$BOT_PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "Bot stopped gracefully."
    exit 0
  fi
  sleep 1
done

echo "Graceful stop timed out. Sending SIGTERM to PID $BOT_PID."
kill -TERM "$BOT_PID" 2>/dev/null || true

for _ in $(seq 1 15); do
  if ! kill -0 "$BOT_PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "Bot stopped after SIGTERM."
    exit 0
  fi
  sleep 1
done

echo "Process still running. Sending SIGKILL to PID $BOT_PID."
kill -KILL "$BOT_PID" 2>/dev/null || true
rm -f "$PID_FILE"
