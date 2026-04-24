#!/bin/sh
# gpu-telemetry container entrypoint
#
# Selects which binary to run based on the first argument (or CMD).
#
# Usage:
#   docker run gpu-telemetry broker
#   docker run gpu-telemetry streamer
#   docker run gpu-telemetry collector
#   docker run gpu-telemetry api
#
# Additional arguments are forwarded to the selected binary:
#   docker run gpu-telemetry api --help
set -e

SERVICE="${1:-broker}"
shift || true   # shift is a no-op when $# == 0; tolerate it

case "$SERVICE" in
  broker|streamer|collector|api)
    exec "/app/${SERVICE}" "$@"
    ;;
  *)
    echo "Unknown service '${SERVICE}'." >&2
    echo "Valid services: broker  streamer  collector  api" >&2
    exit 1
    ;;
esac
