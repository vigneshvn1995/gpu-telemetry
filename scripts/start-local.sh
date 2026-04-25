#!/usr/bin/env bash
# =============================================================================
# start-local.sh — builds and starts the full gpu-telemetry pipeline locally.
#
# Usage:
#   ./scripts/start-local.sh
#   STREAMER_CSV=/path/to/metrics.csv ./scripts/start-local.sh
#
# Stop with:  ./scripts/stop-local.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

BIN="$ROOT/bin"
STREAMER_CSV="${STREAMER_CSV:-$ROOT/data/sample_metrics.csv}"
LOG_LEVEL="${LOG_LEVEL:-info}"
PID_FILE="$ROOT/.local-pids"

# ---- prerequisites ----------------------------------------------------------
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go is not installed. See https://go.dev/dl/"
  exit 1
fi

# ---- regenerate Swagger spec ------------------------------------------------
SWAG_BIN="${GOPATH:-$HOME/go}/bin/swag"
if [[ -x "$SWAG_BIN" ]]; then
  echo "[1/5] Regenerating Swagger spec..."
  "$SWAG_BIN" init -g cmd/api/main.go -o docs --quiet
else
  echo "[1/5] swag not found — skipping Swagger regeneration."
  echo "      To install: go install github.com/swaggo/swag/cmd/swag@latest"
fi

# ---- build ------------------------------------------------------------------
echo "[2/5] Building binaries..."
make build --no-print-directory

# ---- validate CSV -----------------------------------------------------------
if [[ ! -f "$STREAMER_CSV" ]]; then
  echo "ERROR: CSV file not found: $STREAMER_CSV"
  echo "       Set STREAMER_CSV to point to a valid DCGM metrics CSV."
  exit 1
fi

# ---- stop any stale processes -----------------------------------------------
echo "[3/5] Stopping any existing gpu-telemetry processes..."

# Always kill by binary name — catches processes started outside this script
for svc in broker collector streamer api; do
  if pgrep -x "$svc" >/dev/null 2>&1; then
    echo "  killing $svc process(es)..."
    pkill -x "$svc" 2>/dev/null || true
  fi
done

rm -f "$PID_FILE"
sleep 0.5

# ---- start services ---------------------------------------------------------
echo "[4/5] Starting services..."
rm -f "$PID_FILE"

start_svc() {
  local name="$1"; shift
  "$@" &
  local pid=$!
  echo "$name $pid" >> "$PID_FILE"
  echo "  started $name (pid $pid)"
}

MQ_ADDR=":7777" MQ_ADMIN_ADDR=":7778" LOG_LEVEL="$LOG_LEVEL" \
  start_svc "broker" "$BIN/broker"

echo "  Waiting for broker on :7777..."
for i in $(seq 1 30); do
  if nc -z localhost 7777 2>/dev/null; then break; fi
  if [[ $i -eq 30 ]]; then
    echo "WARNING: broker did not open :7777 within 15 s — continuing anyway"
  fi
  sleep 0.5
done

COLLECTOR_DB="$ROOT/telemetry.db" MQ_ADDR=":7777" LOG_LEVEL="$LOG_LEVEL" \
  start_svc "collector" "$BIN/collector"

STREAMER_CSV="$STREAMER_CSV" STREAMER_LOOP="true" STREAMER_DELAY="50ms" \
  MQ_ADDR=":7777" LOG_LEVEL="$LOG_LEVEL" \
  start_svc "streamer" "$BIN/streamer"

API_ADDR=":8080" API_DB="$ROOT/telemetry.db" \
  BROKER_ADMIN_ADDR="localhost:7778" LOG_LEVEL="$LOG_LEVEL" \
  start_svc "api" "$BIN/api"

# ---- wait for API -----------------------------------------------------------
echo "[5/5] Waiting for API on :8080..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080/health >/dev/null 2>&1; then break; fi
  if [[ $i -eq 30 ]]; then
    echo "WARNING: API did not respond on :8080 within 15 s — check logs above"
  fi
  sleep 0.5
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " gpu-telemetry is running"
echo ""
echo " API:          http://localhost:8080"
echo " Swagger UI:   http://localhost:8080/swagger/index.html"
echo " Broker admin: http://localhost:7778"
echo ""
echo " Quick tests:"
echo "   curl http://localhost:8080/health"
echo "   curl http://localhost:8080/api/v1/gpus | jq ."
echo "   curl http://localhost:8080/api/v1/broker/stats | jq ."
echo ""
echo " To stop:  ./scripts/stop-local.sh"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
