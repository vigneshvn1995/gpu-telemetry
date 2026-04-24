#!/usr/bin/env bash
# =============================================================================
# stop-local.sh — stops all gpu-telemetry processes started by start-local.sh
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT/.local-pids"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No running processes found (.local-pids not present)."
  exit 0
fi

while IFS=' ' read -r name pid; do
  if kill "$pid" 2>/dev/null; then
    echo "Stopped $name (pid $pid)"
  else
    echo "Process $name (pid $pid) was not running"
  fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "Done."
