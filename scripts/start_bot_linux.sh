#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/runtime"
PID_FILE="$RUNTIME_DIR/bot.pid"
STOP_FILE="$RUNTIME_DIR/bot.stop"
LOG_FILE="$RUNTIME_DIR/bot.log"

mkdir -p "$RUNTIME_DIR"
rm -f "$STOP_FILE"

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(tr -d '[:space:]' < "$PID_FILE")"
  if [[ -n "${EXISTING_PID}" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "Bot is already running with PID $EXISTING_PID."
    exit 0
  fi
  rm -f "$PID_FILE"
fi

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_CMD="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_CMD="python3"
fi

nohup "$PYTHON_CMD" -m app.main >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "Bot started. PID=$(cat "$PID_FILE")"
