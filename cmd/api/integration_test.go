//go:build integration

// Integration tests for the full gpu-telemetry pipeline:
//
//	Broker (TCP) → Publisher client → Subscriber/collector goroutine
//	→ SQLite DB → API (httptest) → HTTP assertions
//
// Run with:
//
//	go test -tags integration ./cmd/api/ -v -run Integration
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/mq"
	"gpu-telemetry/pkg/storage"
)

// integrationLogger returns a discard logger so test output stays clean unless
// the test is run with -v.
func integrationLogger(t *testing.T) *slog.Logger {
	t.Helper()
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBroker starts a real BrokerServer on an OS-assigned port and returns
// its address. The server is stopped when the test ends.
func startBroker(t *testing.T, logger *slog.Logger) string {
	t.Helper()

	// Bind to :0 to let the OS pick a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("broker listen: %v", err)
	}

	broker := mq.NewMemoryBroker(mq.WithBrokerLogger(logger))
	srv := mq.NewBrokerServer(broker, mq.ServerConfig{AdminAddr: "-"}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		srv.Shutdown(3 * time.Second)
		_ = broker.Close()
	})

	go func() {
		if err := srv.ServeListener(ctx, ln); err != nil {
			logger.Error("broker serve error", "err", err)
		}
	}()

	return ln.Addr().String()
}

// collectIntoDB subscribes to the broker on the given topic and writes every
// received message to the store. It returns when ctx is cancelled.
func collectIntoDB(ctx context.Context, t *testing.T, brokerAddr, topic string,
	store storage.Store, logger *slog.Logger) {

	t.Helper()
	client := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)
	if err := client.Connect(ctx); err != nil {
		t.Errorf("collector connect: %v", err)
		return
	}

	msgCh, err := client.Subscribe(ctx, topic)
	if err != nil {
		t.Errorf("collector subscribe: %v", err)
		return
	}

	go func() {
		defer client.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-msgCh:
				if !ok {
					return
				}
				var tel models.Telemetry
				if err := json.Unmarshal(raw, &tel); err != nil {
					logger.Warn("collector unmarshal error", "err", err)
					continue
				}
				if err := store.Write(context.Background(), &tel); err != nil {
					logger.Warn("collector write error", "err", err)
				}
			}
		}
	}()
}

// publishTelemetry publishes n records for the given GPU UUID to the broker
// using the provided topic.
func publishTelemetry(t *testing.T, brokerAddr, topic string,
	uuid, gpuID, hostname string, n int, logger *slog.Logger) {

	t.Helper()
	ctx := context.Background()
	pub := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)
	if err := pub.Connect(ctx); err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	t.Cleanup(pub.Close)

	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		tel := models.Telemetry{
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GPUID:      gpuID,
			UUID:       uuid,
			ModelName:  "NVIDIA A100 80GB PCIe",
			Hostname:   hostname,
			Value:      float64(50 + i),
		}
		raw, err := json.Marshal(&tel)
		if err != nil {
			t.Fatalf("marshal telemetry: %v", err)
		}
		if err := pub.Publish(ctx, topic, raw); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
}

// waitForRecords polls the store until at least want records exist for uuid,
// or the deadline is exceeded.
func waitForRecords(t *testing.T, store storage.Store, uuid string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := store.Query(context.Background(), storage.QueryFilter{
			UUID:  uuid,
			Limit: want + 1,
		})
		if err == nil && len(rows) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records for uuid %s", want, uuid)
}

// -------------------------------------------------------------------------
// Integration tests
// -------------------------------------------------------------------------

// TestIntegration_BrokerToAPIViaMemoryStore verifies the full pipeline using
// an in-memory store (fastest, no disk I/O):
//
//	publisher → broker → collector goroutine → MemoryStore → API
func TestIntegration_BrokerToAPIViaMemoryStore(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)

	store := storage.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)

	// Give the subscriber a moment to register with the broker.
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-integration-0001"
		gpuID    = "0"
		hostname = "test-node-01"
		n        = 5
	)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.0", uuid, gpuID, hostname, n, logger)
	waitForRecords(t, store, uuid, n)

	// Start the API backed by the same MemoryStore.
	srv := newServer(":0", store, logger, 5*time.Second, "")
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	// --- Assert: GET /api/v1/gpus returns the GPU ---
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/gpus", ts.URL))
	if err != nil {
		t.Fatalf("GET /api/v1/gpus: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var gpuList gpuListResponse
	if err := json.NewDecoder(resp.Body).Decode(&gpuList); err != nil {
		t.Fatalf("decode gpuList: %v", err)
	}
	if gpuList.Total != 1 {
		t.Errorf("gpus total = %d, want 1", gpuList.Total)
	}
	if gpuList.Items[0].UUID != uuid {
		t.Errorf("uuid = %s, want %s", gpuList.Items[0].UUID, uuid)
	}

	// --- Assert: GET /api/v1/gpus/{uuid}/telemetry returns n records ---
	resp2, err := http.Get(fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=20", ts.URL, uuid))
	if err != nil {
		t.Fatalf("GET telemetry: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("telemetry status = %d, want 200", resp2.StatusCode)
	}
	var tel telemetryResponse
	if err := json.NewDecoder(resp2.Body).Decode(&tel); err != nil {
		t.Fatalf("decode telemetry: %v", err)
	}
	if tel.Count != n {
		t.Errorf("telemetry count = %d, want %d", tel.Count, n)
	}

	// --- Assert: pagination on /api/v1/gpus ---
	resp3, err := http.Get(fmt.Sprintf("%s/api/v1/gpus?limit=1&offset=0", ts.URL))
	if err != nil {
		t.Fatalf("GET /api/v1/gpus paginated: %v", err)
	}
	defer resp3.Body.Close()
	var page gpuListResponse
	if err := json.NewDecoder(resp3.Body).Decode(&page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if page.Total != 1 || page.Count != 1 {
		t.Errorf("page total=%d count=%d, want both 1", page.Total, page.Count)
	}
}

// TestIntegration_BrokerToAPIViaSQLite verifies the pipeline with a real
// SQLite database on disk, mirroring the production collector→API split:
//
//	publisher → broker → collector goroutine (OpenSQLite write) → API (OpenSQLiteReadOnly)
func TestIntegration_BrokerToAPIViaSQLite(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)

	// Create a temporary directory for the SQLite file.
	dir := t.TempDir()
	dbPath := dir + "/telemetry.db"

	// Collector uses read-write SQLite.
	writeStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("open write store: %v", err)
	}
	t.Cleanup(func() { _ = writeStore.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", writeStore, logger)
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-sqlite-integration-0001"
		gpuID    = "1"
		hostname = "test-node-02"
		n        = 8
	)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.1", uuid, gpuID, hostname, n, logger)
	waitForRecords(t, writeStore, uuid, n)

	// API uses read-only SQLite (same file, separate connection).
	readStore, err := storage.OpenSQLiteReadOnly(dbPath)
	if err != nil {
		t.Fatalf("open read store: %v", err)
	}
	t.Cleanup(func() { _ = readStore.Close() })

	srv := newServer(":0", readStore, logger, 5*time.Second, "")
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	// --- Assert: GPU appears in list ---
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/gpus", ts.URL))
	if err != nil {
		t.Fatalf("GET /api/v1/gpus: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var gpuList gpuListResponse
	if err := json.NewDecoder(resp.Body).Decode(&gpuList); err != nil {
		t.Fatalf("decode gpuList: %v", err)
	}
	if gpuList.Total < 1 {
		t.Fatalf("expected at least 1 GPU, got %d", gpuList.Total)
	}
	found := false
	for _, g := range gpuList.Items {
		if g.UUID == uuid {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("uuid %s not found in GPU list", uuid)
	}

	// --- Assert: telemetry records ---
	resp2, err := http.Get(fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=20", ts.URL, uuid))
	if err != nil {
		t.Fatalf("GET telemetry: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("telemetry status = %d, want 200", resp2.StatusCode)
	}
	var tel telemetryResponse
	if err := json.NewDecoder(resp2.Body).Decode(&tel); err != nil {
		t.Fatalf("decode telemetry: %v", err)
	}
	if tel.Count != n {
		t.Errorf("telemetry count = %d, want %d", tel.Count, n)
	}
}

// TestIntegration_MultiGPUPagination publishes telemetry for 15 GPUs and
// verifies that the /api/v1/gpus pagination returns the correct pages.
func TestIntegration_MultiGPUPagination(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)

	store := storage.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const totalGPUs = 15
	for i := 0; i < totalGPUs; i++ {
		uuid := fmt.Sprintf("GPU-page-test-%04d", i)
		topic := fmt.Sprintf("telemetry.gpu.%d", i)
		publishTelemetry(t, brokerAddr, topic, uuid, fmt.Sprintf("%d", i), "page-host", 1, logger)
	}

	// Wait until all 15 GPUs are visible.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		all, _ := store.QueryGPUSummaries(context.Background())
		if len(all) >= totalGPUs {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	srv := newServer(":0", store, logger, 5*time.Second, "")
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)

	// Fetch all pages with pageSize=6 and collect all UUIDs.
	seen := map[string]bool{}
	pageSize := 6
	offset := 0
	for {
		url := fmt.Sprintf("%s/api/v1/gpus?limit=%d&offset=%d", ts.URL, pageSize, offset)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET page offset=%d: %v", offset, err)
		}
		var page gpuListResponse
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			t.Fatalf("decode page: %v", err)
		}
		resp.Body.Close()

		if page.Total != totalGPUs {
			t.Errorf("total = %d, want %d (offset=%d)", page.Total, totalGPUs, offset)
		}
		for _, g := range page.Items {
			seen[g.UUID] = true
		}
		offset += pageSize
		if offset >= page.Total {
			break
		}
	}

	if len(seen) != totalGPUs {
		t.Errorf("saw %d unique GPUs across pages, want %d", len(seen), totalGPUs)
	}
}
