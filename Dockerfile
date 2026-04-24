# syntax=docker/dockerfile:1

# =============================================================================
# Builder stage
#
# CGO is intentionally disabled.  The project uses modernc.org/sqlite — a
# pure-Go, CGO-free SQLite implementation — so no C toolchain is required.
# This keeps the build hermetic, avoids musl/glibc ABI mismatches, and lets
# the final binary run on any linux/amd64 or linux/arm64 host without
# installing additional runtime libraries.
# =============================================================================
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Cache module downloads as a separate layer so that source-only changes
# do not re-download dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree.
COPY . .

# Build all four binaries.  A single RUN instruction shares the module cache
# across all four compilations.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
        -o /out/broker    ./cmd/broker    && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
        -o /out/streamer  ./cmd/streamer  && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
        -o /out/collector ./cmd/collector && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" \
        -o /out/api       ./cmd/api

# =============================================================================
# Runtime image
#
# alpine:3.20 provides a minimal base (~7 MB) with a shell for debugging,
# CA certificates for TLS, and a standard /etc/passwd for non-root users.
# Use scratch instead if you want the absolute smallest possible image.
# =============================================================================
FROM alpine:3.20 AS runtime

# Create a non-root user for least-privilege execution.
RUN addgroup -S gpu && adduser -S gpu -G gpu

WORKDIR /app

# Compiled binaries.
COPY --from=builder --chown=gpu:gpu /out/broker    ./broker
COPY --from=builder --chown=gpu:gpu /out/streamer  ./streamer
COPY --from=builder --chown=gpu:gpu /out/collector ./collector
COPY --from=builder --chown=gpu:gpu /out/api       ./api

# Container entrypoint – selects the binary to run from the CMD argument.
COPY --chown=gpu:gpu entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# Sample DCGM metrics CSV used by the Streamer in dev/demo mode.
# Place the CSV file at  data/<filename>  before running docker build.
# The data/.gitkeep placeholder ensures this directory is always present.
COPY --chown=gpu:gpu data/ ./data/

# Generated OpenAPI specification served by the API at /swagger/*.
COPY --chown=gpu:gpu docs/ ./docs/

USER gpu

# Expose the ports used by each service.  The actual service is chosen at
# runtime via CMD (or Kubernetes Deployment `command:`).
#   7777 – Broker MQ (TCP)
#   7778 – Broker admin HTTP
#   8080 – REST API HTTP
EXPOSE 7777 7778 8080

# Default service.  Override at runtime:
#   docker run gpu-telemetry streamer
#   docker run gpu-telemetry api
ENTRYPOINT ["./entrypoint.sh"]
CMD ["broker"]
