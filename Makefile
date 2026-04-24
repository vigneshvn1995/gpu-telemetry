# gpu-telemetry – top-level Makefile
#
# Targets:
#   build           Build all four binaries into ./bin/
#   test            Run all unit and integration tests
#   test-race       Run tests with -race detector enabled
#   test-coverage   Run tests and generate an HTML coverage report
#   lint            Run golangci-lint (requires golangci-lint in PATH)
#   tidy            Tidy and verify the module graph
#   docs            Regenerate OpenAPI spec with swag
#   docker-build    Build the multi-service container image
#   k8s-deploy      Apply all Kubernetes manifests in k8s/
#   k8s-down        Delete all Kubernetes resources defined in k8s/
#   run-broker      Start the broker server (foreground)
#   run-streamer    Stream the sample CSV file to the broker
#   run-collector   Start the collector (writes to ./telemetry.db)
#   run-api         Start the REST API (reads from ./telemetry.db)
#   clean           Remove ./bin/ and generated artefacts
#   help            Print this message

MODULE   := gpu-telemetry
BIN_DIR  := bin
CMDS     := broker streamer collector api

# Container image
IMAGE_NAME ?= gpu-telemetry
IMAGE_TAG  ?= latest

# Kubernetes
KUBECTL    ?= kubectl

# Go toolchain flags
GO       := go
GOFLAGS  := -trimpath
LDFLAGS  := -s -w

# Default target
.DEFAULT_GOAL := build

# -------------------------------------------------------------------------
# Build
# -------------------------------------------------------------------------

.PHONY: build
build: $(addprefix $(BIN_DIR)/,$(CMDS))

$(BIN_DIR)/%: FORCE
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $@ ./cmd/$*
	@echo "built $@"

FORCE:

# -------------------------------------------------------------------------
# Test
# -------------------------------------------------------------------------

.PHONY: test
test:
	$(GO) test ./... -v -count=1 -timeout 60s

.PHONY: test-race
test-race:
	$(GO) test ./... -race -v -count=1 -timeout 120s

.PHONY: test-short
test-short:
	$(GO) test ./... -short -count=1 -timeout 30s

.PHONY: test-coverage
test-coverage:
	$(GO) test ./... -coverprofile=coverage.out -timeout 60s
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# -------------------------------------------------------------------------
# Code quality
# -------------------------------------------------------------------------

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy
	$(GO) mod verify

# -------------------------------------------------------------------------
# Run individual services (development / demo)
# Each target reads from environment variables documented in the binary.
# Override with: make run-streamer STREAMER_CSV=/path/to/file.csv
# -------------------------------------------------------------------------

BROKER_ADDR     ?= :7777
BROKER_ADMIN    ?= :7778
STREAMER_CSV      ?= ./data/sample_metrics.csv
STREAMER_TOPIC_PFX ?= telemetry.gpu
STREAMER_LOOP     ?= true
STREAMER_DELAY    ?= 50ms
COLLECTOR_DB    ?= ./telemetry.db
API_DB          ?= ./telemetry.db
API_ADDR        ?= :8080
LOG_LEVEL       ?= info

.PHONY: run-broker
run-broker: $(BIN_DIR)/broker
	MQ_ADDR=$(BROKER_ADDR) MQ_ADMIN_ADDR=$(BROKER_ADMIN) LOG_LEVEL=$(LOG_LEVEL) \
	  ./$(BIN_DIR)/broker

.PHONY: run-streamer
run-streamer: $(BIN_DIR)/streamer
	STREAMER_CSV=$(STREAMER_CSV) STREAMER_TOPIC_PFX=$(STREAMER_TOPIC_PFX) \
	  STREAMER_LOOP=$(STREAMER_LOOP) STREAMER_DELAY=$(STREAMER_DELAY) \
	  MQ_ADDR=$(BROKER_ADDR) LOG_LEVEL=$(LOG_LEVEL) \
	  ./$(BIN_DIR)/streamer

.PHONY: run-collector
run-collector: $(BIN_DIR)/collector
	COLLECTOR_DB=$(COLLECTOR_DB) MQ_ADDR=$(BROKER_ADDR) LOG_LEVEL=$(LOG_LEVEL) \
	  ./$(BIN_DIR)/collector

.PHONY: run-api
run-api: $(BIN_DIR)/api
	API_ADDR=$(API_ADDR) API_DB=$(API_DB) LOG_LEVEL=$(LOG_LEVEL) \
	  ./$(BIN_DIR)/api

# -------------------------------------------------------------------------
# Demo: copy sample CSV to ./data/ so make run-streamer works out of the box
# -------------------------------------------------------------------------
.PHONY: demo-data
demo-data:
	@mkdir -p data
	@if [ -f "$(STREAMER_CSV)" ]; then \
	  cp $(STREAMER_CSV) data/; \
	else \
	  echo "Set STREAMER_CSV to your CSV file path"; \
	fi

# -------------------------------------------------------------------------
# Docker
# -------------------------------------------------------------------------

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "Image built: $(IMAGE_NAME):$(IMAGE_TAG)"

# -------------------------------------------------------------------------
# Kubernetes (raw manifests)
# -------------------------------------------------------------------------

.PHONY: k8s-deploy
k8s-deploy:
	$(KUBECTL) apply -f k8s/
	@echo "Manifests applied. Check status with: kubectl -n gpu-telemetry get all"

.PHONY: k8s-down
k8s-down:
	$(KUBECTL) delete -f k8s/ --ignore-not-found
	@echo "All gpu-telemetry Kubernetes resources removed."

# -------------------------------------------------------------------------
# Helm
# Requires: helm (https://helm.sh/docs/intro/install/)
# -------------------------------------------------------------------------

HELM           ?= helm
HELM_RELEASE   ?= gpu-telemetry
HELM_CHART     := ./helm/gpu-telemetry
HELM_NAMESPACE ?= gpu-telemetry

.PHONY: helm-lint
helm-lint:
	$(HELM) lint $(HELM_CHART)
	@echo "Helm chart linting passed."

.PHONY: helm-template
helm-template:
	$(HELM) template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)

.PHONY: helm-install
helm-install:
	$(HELM) upgrade --install $(HELM_RELEASE) $(HELM_CHART) \
	  --namespace $(HELM_NAMESPACE) --create-namespace
	@echo "Helm release '$(HELM_RELEASE)' installed/upgraded in namespace '$(HELM_NAMESPACE)'."

.PHONY: helm-uninstall
helm-uninstall:
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(HELM_NAMESPACE) --ignore-not-found
	@echo "Helm release '$(HELM_RELEASE)' uninstalled."

.PHONY: helm-status
helm-status:
	$(HELM) status $(HELM_RELEASE) --namespace $(HELM_NAMESPACE)

# -------------------------------------------------------------------------
# OpenAPI docs generation
# Requires: go install github.com/swaggo/swag/cmd/swag@latest
# -------------------------------------------------------------------------

.PHONY: docs
docs:
	swag init -g cmd/api/main.go -o docs
	@echo "swagger docs regenerated in ./docs/"

# -------------------------------------------------------------------------
# Clean
# -------------------------------------------------------------------------

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
	@echo "cleaned"

# -------------------------------------------------------------------------
# Help
# -------------------------------------------------------------------------

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}' || \
	  sed -n '/^# -------------------------/,/^# /!d; /^.PHONY/!d; s/^.PHONY: //p' Makefile
	@echo ""
	@echo "Use LOG_LEVEL=debug for verbose output."
