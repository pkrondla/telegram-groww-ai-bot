#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/runtime"
PID_FILE="$RUNTIME_DIR/bot.pid"
STOP_FILE="$RUNTIME_DIR/bot.stop"

mkdir -p "$RUNTIME_DIR"
touch "$STOP_FILE"

collect_pids() {
  local pids=""
  if [[ -f "$PID_FILE" ]]; then
    local file_pid
    file_pid="$(tr -d '[:space:]' < "$PID_FILE")"
    if [[ -n "${file_pid}" ]]; then
      pids="${file_pid}"
    fi
  fi
  local extra
  extra="$(pgrep -f '[p]ython.*-m app\.main' || true)"
  if [[ -n "${extra}" ]]; then
    pids="$(printf '%s\n%s\n' "${pids}" "${extra}" | awk 'NF' | sort -u)"
  fi
  echo "${pids}"
}

PIDS="$(collect_pids)"
if [[ -z "${PIDS}" ]]; then
  rm -f "$PID_FILE"
  echo "No bot process found. Stop signal file created."
  exit 0
fi

echo "Signaled stop for PID(s) $(echo "${PIDS}" | tr '\n' ' ')."

for _ in $(seq 1 45); do
  still=""
  for pid in ${PIDS}; do
    if kill -0 "$pid" 2>/dev/null; then
      still="${still} ${pid}"
    fi
  done
  if [[ -z "${still// /}" ]]; then
    rm -f "$PID_FILE"
    echo "Bot stopped gracefully."
    exit 0
  fi
  sleep 1
done

echo "Graceful stop timed out. Sending SIGTERM."
for pid in ${PIDS}; do
  kill -TERM "$pid" 2>/dev/null || true
done

for _ in $(seq 1 15); do
  still=""
  for pid in ${PIDS}; do
    if kill -0 "$pid" 2>/dev/null; then
      still="${still} ${pid}"
    fi
  done
  if [[ -z "${still// /}" ]]; then
    rm -f "$PID_FILE"
    echo "Bot stopped after SIGTERM."
    exit 0
  fi
  sleep 1
done

echo "Process still running. Sending SIGKILL."
for pid in ${PIDS}; do
  kill -KILL "$pid" 2>/dev/null || true
done
leftover="$(pgrep -f '[p]ython.*-m app\.main' || true)"
if [[ -n "${leftover}" ]]; then
  echo "${leftover}" | xargs -r kill -KILL 2>/dev/null || true
fi
rm -f "$PID_FILE"
echo "Bot stop completed."
