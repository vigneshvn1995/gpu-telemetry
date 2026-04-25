//go:build integration

// Integration tests for the full gpu-telemetry pipeline.
//
// Each test exercises real components (TCP broker, SQLite, API HTTP server)
// with no mocks. The layers under test are:
//
//	Publisher → TCP Broker → Subscriber/Collector → Store → API (httptest)
//
// Test coverage:
//   - GET /health
//   - GET /api/v1/gpus                  (list, pagination, out-of-range offset)
//   - GET /api/v1/gpus/:uuid/telemetry  (basic, metric_name filter, time range, limit/offset, 400 errors)
//   - GET /api/v1/gpus/:uuid/telemetry/summary  (happy path, missing metric → 400, no data → 404)
//   - GET /api/v1/gpus/:uuid/energy     (happy path, unknown uuid → 404)
//   - GET /api/v1/broker/stats          (happy path, broker down → 502)
//   - GET /api/v1/cluster/stranded-compute
//   - GET /api/v1/cluster/anomalies     (including seeded anomaly detection)
//   - SQLite write+read-only split      (mirrors production collector/API split)
//   - Multi-GPU pagination across pages
//
// Run with:
//
//	go test -tags integration ./cmd/api/ -v -run Integration -timeout 120s
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

// -------------------------------------------------------------------------
// Test infrastructure
// -------------------------------------------------------------------------

// integrationLogger returns a discard logger unless -v is passed.
func integrationLogger(t *testing.T) *slog.Logger {
	t.Helper()
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBroker starts a real BrokerServer on an OS-assigned port. Stops on
// test cleanup.
func startBroker(t *testing.T, logger *slog.Logger) string {
	t.Helper()
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
	go func() { _ = srv.ServeListener(ctx, ln) }()
	return ln.Addr().String()
}

// collectIntoDB subscribes to the broker and writes all received messages to
// the store. Runs until ctx is cancelled.
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
				if jsonErr := json.Unmarshal(raw, &tel); jsonErr != nil {
					logger.Warn("unmarshal error", "err", jsonErr)
					continue
				}
				_ = store.Write(context.Background(), &tel)
			}
		}
	}()
}

// publishTelemetry publishes n records for one GPU over a time series starting
// at base. Values cycle 50..99 so energy/anomaly logic has meaningful data.
func publishTelemetry(t *testing.T, brokerAddr, topic, uuid, gpuID, hostname string,
	n int, base time.Time, logger *slog.Logger) {
	t.Helper()
	ctx := context.Background()
	pub := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)
	if err := pub.Connect(ctx); err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	t.Cleanup(pub.Close)
	for i := 0; i < n; i++ {
		tel := models.Telemetry{
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GPUID:      gpuID,
			UUID:       uuid,
			ModelName:  "NVIDIA A100 80GB PCIe",
			Hostname:   hostname,
			Value:      float64(50 + (i % 50)),
		}
		raw, _ := json.Marshal(&tel)
		if err := pub.Publish(ctx, topic, raw); err != nil {
			t.Fatalf("publish i=%d: %v", i, err)
		}
	}
}

// publishMetric publishes n records with a specific metric name and fixed value.
func publishMetric(t *testing.T, brokerAddr, topic, uuid, gpuID, hostname,
	metricName string, value float64, n int, base time.Time, logger *slog.Logger) {
	t.Helper()
	ctx := context.Background()
	pub := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)
	if err := pub.Connect(ctx); err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	t.Cleanup(pub.Close)
	for i := 0; i < n; i++ {
		tel := models.Telemetry{
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			MetricName: metricName,
			GPUID:      gpuID,
			UUID:       uuid,
			ModelName:  "NVIDIA A100 80GB PCIe",
			Hostname:   hostname,
			Value:      value,
		}
		raw, _ := json.Marshal(&tel)
		if err := pub.Publish(ctx, topic, raw); err != nil {
			t.Fatalf("publish metric i=%d: %v", i, err)
		}
	}
}

// waitForRecords polls until at least want records exist for uuid.
func waitForRecords(t *testing.T, store storage.Store, uuid string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows, _ := store.Query(context.Background(), storage.QueryFilter{UUID: uuid, Limit: want + 1})
		if len(rows) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records for uuid %s", want, uuid)
}

// waitForGPUs polls until at least want distinct GPUs appear in the store.
func waitForGPUs(t *testing.T, store storage.Store, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		gpus, _ := store.QueryGPUSummaries(context.Background())
		if len(gpus) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d GPUs in store", want)
}

// apiServer starts the API backed by store, with an optional broker admin URL.
func apiServer(t *testing.T, store storage.Store, brokerAdminURL string, logger *slog.Logger) *httptest.Server {
	t.Helper()
	srv := newServer(":0", store, logger, 5*time.Second, brokerAdminURL)
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)
	return ts
}

// getJSON issues a GET and decodes the JSON body into v. Returns the status code.
func getJSON(t *testing.T, url string, v any) int {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return resp.StatusCode
}

// getStatus issues a GET and returns the HTTP status code only.
func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// fakeBrokerAdmin starts an httptest server that serves a hard-coded /stats
// response, simulating the broker admin endpoint.
func fakeBrokerAdmin(t *testing.T) string {
	t.Helper()
	stats := map[string]any{
		"total_published": 100,
		"total_delivered": 98,
		"total_dropped":   2,
		"active_topics":   4,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// -------------------------------------------------------------------------
// Test: health
// -------------------------------------------------------------------------

func TestIntegration_Health(t *testing.T) {
	logger := integrationLogger(t)
	store := storage.NewMemoryStore()
	ts := apiServer(t, store, "", logger)

	var body map[string]string
	code := getJSON(t, ts.URL+"/health", &body)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q, want \"ok\"", body["status"])
	}
}

// -------------------------------------------------------------------------
// Test: full pipeline via MemoryStore (broker → collector → API)
// -------------------------------------------------------------------------

// TestIntegration_BrokerToAPIViaMemoryStore verifies the happy-path pipeline:
//
//	publisher → broker → collector goroutine → MemoryStore → API
func TestIntegration_BrokerToAPIViaMemoryStore(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)
	store := storage.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-integration-0001"
		gpuID    = "0"
		hostname = "test-node-01"
		n        = 10
	)
	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.0", uuid, gpuID, hostname, n, base, logger)
	waitForRecords(t, store, uuid, n)

	ts := apiServer(t, store, "", logger)

	// GET /api/v1/gpus — basic
	var gpuList gpuListResponse
	if code := getJSON(t, ts.URL+"/api/v1/gpus", &gpuList); code != http.StatusOK {
		t.Fatalf("gpus status = %d", code)
	}
	if gpuList.Total != 1 {
		t.Errorf("total = %d, want 1", gpuList.Total)
	}
	if gpuList.Items[0].UUID != uuid {
		t.Errorf("uuid = %s, want %s", gpuList.Items[0].UUID, uuid)
	}

	// GET /api/v1/gpus/{uuid}/telemetry — all records
	var tel telemetryResponse
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=20", ts.URL, uuid), &tel); code != http.StatusOK {
		t.Fatalf("telemetry status = %d", code)
	}
	if tel.Count != n {
		t.Errorf("telemetry count = %d, want %d", tel.Count, n)
	}

	// GET /api/v1/gpus/{uuid}/telemetry — metric_name filter (should return all; wrong metric → 0)
	var filtered telemetryResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?metric_name=DCGM_FI_DEV_GPU_UTIL", ts.URL, uuid), &filtered)
	if filtered.Count != n {
		t.Errorf("filtered count = %d, want %d", filtered.Count, n)
	}
	var wrongMetric telemetryResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?metric_name=NO_SUCH_METRIC", ts.URL, uuid), &wrongMetric)
	if wrongMetric.Count != 0 {
		t.Errorf("wrong-metric count = %d, want 0", wrongMetric.Count)
	}

	// GET /api/v1/gpus/{uuid}/telemetry — time range filter
	midTime := base.Add(5 * time.Second).Format(time.RFC3339)
	endTime := base.Add(7 * time.Second).Format(time.RFC3339)
	var ranged telemetryResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?start_time=%s&end_time=%s", ts.URL, uuid, midTime, endTime), &ranged)
	if ranged.Count == 0 {
		t.Error("time-range query returned 0 records, want > 0")
	}

	// GET /api/v1/gpus/{uuid}/telemetry — limit/offset pagination
	var page1 telemetryResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=4&offset=0", ts.URL, uuid), &page1)
	if page1.Count != 4 {
		t.Errorf("page1 count = %d, want 4", page1.Count)
	}
	var page2 telemetryResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=4&offset=4", ts.URL, uuid), &page2)
	if page2.Count != 4 {
		t.Errorf("page2 count = %d, want 4", page2.Count)
	}

	// GET /api/v1/gpus/{uuid}/telemetry — invalid limit → 400
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=99999", ts.URL, uuid)); code != http.StatusBadRequest {
		t.Errorf("limit=99999 status = %d, want 400", code)
	}

	// GET /api/v1/gpus/{uuid}/telemetry — invalid start_time → 400
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?start_time=not-a-date", ts.URL, uuid)); code != http.StatusBadRequest {
		t.Errorf("bad start_time status = %d, want 400", code)
	}
}

// -------------------------------------------------------------------------
// Test: telemetry summary endpoint
// -------------------------------------------------------------------------

func TestIntegration_TelemetrySummary(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)
	store := storage.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-summary-0001"
		gpuID    = "2"
		hostname = "test-node-03"
		n        = 20
	)
	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.2", uuid, gpuID, hostname, n, base, logger)
	waitForRecords(t, store, uuid, n)

	ts := apiServer(t, store, "", logger)

	// Happy path.
	var summary telemetrySummaryResponse
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry/summary?metric_name=DCGM_FI_DEV_GPU_UTIL", ts.URL, uuid), &summary); code != http.StatusOK {
		t.Fatalf("summary status = %d, want 200", code)
	}
	if summary.SampleCount != n {
		t.Errorf("sample_count = %d, want %d", summary.SampleCount, n)
	}
	if summary.Min < 50 || summary.Max > 99 {
		t.Errorf("min=%v max=%v: out of expected 50–99 range", summary.Min, summary.Max)
	}
	if summary.P50 == 0 {
		t.Error("p50 = 0, expected non-zero")
	}

	// Missing metric_name → 400.
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry/summary", ts.URL, uuid)); code != http.StatusBadRequest {
		t.Errorf("no metric_name status = %d, want 400", code)
	}

	// Unknown UUID → 404.
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus/no-such-gpu/telemetry/summary?metric_name=DCGM_FI_DEV_GPU_UTIL", ts.URL)); code != http.StatusNotFound {
		t.Errorf("unknown uuid status = %d, want 404", code)
	}
}

// -------------------------------------------------------------------------
// Test: energy endpoint
// -------------------------------------------------------------------------

func TestIntegration_GPUEnergy(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)
	store := storage.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-energy-0001"
		gpuID    = "3"
		hostname = "test-node-04"
		n        = 30
	)
	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.3", uuid, gpuID, hostname, n, base, logger)
	waitForRecords(t, store, uuid, n)

	ts := apiServer(t, store, "", logger)

	// Happy path — should return energy > 0.
	var energy energyResponse
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/energy", ts.URL, uuid), &energy); code != http.StatusOK {
		t.Fatalf("energy status = %d, want 200", code)
	}
	if energy.TotalEnergyWh <= 0 {
		t.Errorf("total_energy_wh = %v, want > 0", energy.TotalEnergyWh)
	}
	if energy.CarbonGrams <= 0 {
		t.Errorf("carbon_grams = %v, want > 0", energy.CarbonGrams)
	}
	if energy.SampleCount != n {
		t.Errorf("sample_count = %d, want %d", energy.SampleCount, n)
	}

	// Unknown GPU → 404.
	if code := getStatus(t, ts.URL+"/api/v1/gpus/no-such-gpu/energy"); code != http.StatusNotFound {
		t.Errorf("unknown gpu energy status = %d, want 404", code)
	}

	// Invalid start_time → 400.
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus/%s/energy?start_time=bad", ts.URL, uuid)); code != http.StatusBadRequest {
		t.Errorf("bad start_time energy status = %d, want 400", code)
	}
}

// -------------------------------------------------------------------------
// Test: broker stats proxy
// -------------------------------------------------------------------------

func TestIntegration_BrokerStats(t *testing.T) {
	logger := integrationLogger(t)
	store := storage.NewMemoryStore()

	// Happy path: fake broker admin serves /stats.
	adminURL := fakeBrokerAdmin(t)
	ts := apiServer(t, store, adminURL, logger)

	var stats brokerStatsResponse
	if code := getJSON(t, ts.URL+"/api/v1/broker/stats", &stats); code != http.StatusOK {
		t.Fatalf("broker stats status = %d, want 200", code)
	}
	if stats.TotalPublished != 100 {
		t.Errorf("total_published = %d, want 100", stats.TotalPublished)
	}
	if stats.TotalDropped != 2 {
		t.Errorf("total_dropped = %d, want 2", stats.TotalDropped)
	}
	if stats.DropRatePct <= 0 {
		t.Errorf("drop_rate_pct = %v, expected > 0", stats.DropRatePct)
	}
	if stats.Note == "" {
		t.Error("note field must not be empty")
	}

	// Broker unreachable → 502.
	tsBad := apiServer(t, store, "http://127.0.0.1:1", logger) // port 1: always refused
	if code := getStatus(t, tsBad.URL+"/api/v1/broker/stats"); code != http.StatusBadGateway {
		t.Errorf("broker down status = %d, want 502", code)
	}
}

// -------------------------------------------------------------------------
// Test: stranded-compute
// -------------------------------------------------------------------------

func TestIntegration_StrandedCompute(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)
	store := storage.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const (
		uuid     = "GPU-stranded-0001"
		gpuID    = "4"
		hostname = "test-node-05"
		n        = 10
	)
	// Publish zero-utilisation records — this GPU should be classified as stranded.
	base := time.Now().UTC().Add(-5 * time.Minute)
	publishMetric(t, brokerAddr, "telemetry.gpu.4", uuid, gpuID, hostname,
		"DCGM_FI_DEV_GPU_UTIL", 0.0, n, base, logger)
	waitForRecords(t, store, uuid, n)

	ts := apiServer(t, store, "", logger)

	var result strandedComputeResponse
	if code := getJSON(t, ts.URL+"/api/v1/cluster/stranded-compute?window=15m&max_util=5", &result); code != http.StatusOK {
		t.Fatalf("stranded status = %d, want 200", code)
	}
	if result.StrandedCount == 0 {
		t.Error("expected at least 1 stranded GPU, got 0")
	}
	found := false
	for _, item := range result.Items {
		if item.UUID == uuid {
			found = true
			if item.WastedEnergyWh <= 0 {
				t.Errorf("wasted_energy_wh = %v, want > 0", item.WastedEnergyWh)
			}
		}
	}
	if !found {
		t.Errorf("uuid %s not in stranded list", uuid)
	}

	// Invalid window → 400.
	if code := getStatus(t, ts.URL+"/api/v1/cluster/stranded-compute?window=bad"); code != http.StatusBadRequest {
		t.Errorf("bad window status = %d, want 400", code)
	}
}

// -------------------------------------------------------------------------
// Test: anomaly detection
// -------------------------------------------------------------------------

func TestIntegration_ClusterAnomalies(t *testing.T) {
	logger := integrationLogger(t)
	store := storage.NewMemoryStore()
	ts := apiServer(t, store, "", logger)

	ctx := context.Background()
	base := time.Now().UTC().Add(-10 * time.Minute)

	// Seed a PerformanceDrop pattern: spike to 97% then collapse to 10%.
	// The detector looks for util > 95% followed by a ≥50pp drop within 5 samples.
	spikeAndDrop := []float64{97, 96, 10, 10, 10, 10}
	for i, v := range spikeAndDrop {
		tel := &models.Telemetry{
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GPUID:      "5",
			UUID:       "GPU-anomaly-0001",
			ModelName:  "NVIDIA A100 80GB PCIe",
			Hostname:   "test-node-06",
			Value:      v,
		}
		if err := store.Write(ctx, tel); err != nil {
			t.Fatalf("seed anomaly: %v", err)
		}
	}

	var result clusterAnomaliesResponse
	if code := getJSON(t, ts.URL+"/api/v1/cluster/anomalies?window=1h", &result); code != http.StatusOK {
		t.Fatalf("anomalies status = %d, want 200", code)
	}
	if result.AnomalyCount == 0 {
		t.Error("expected ≥1 anomaly, got 0")
	}

	// Invalid window → 400.
	if code := getStatus(t, ts.URL+"/api/v1/cluster/anomalies?window=not-a-duration"); code != http.StatusBadRequest {
		t.Errorf("bad window status = %d, want 400", code)
	}

	// Empty store (no anomalies) returns 200 with empty list.
	emptyStore := storage.NewMemoryStore()
	tsEmpty := apiServer(t, emptyStore, "", logger)
	var empty clusterAnomaliesResponse
	if code := getJSON(t, tsEmpty.URL+"/api/v1/cluster/anomalies", &empty); code != http.StatusOK {
		t.Errorf("empty anomalies status = %d, want 200", code)
	}
	if empty.AnomalyCount != 0 {
		t.Errorf("empty anomaly count = %d, want 0", empty.AnomalyCount)
	}
}

// -------------------------------------------------------------------------
// Test: SQLite write/read-only split (mirrors production collector/API pods)
// -------------------------------------------------------------------------

// TestIntegration_BrokerToAPIViaSQLite exercises the exact production pattern:
//
//	collector writes via OpenSQLite → API reads via OpenSQLiteReadOnly
func TestIntegration_BrokerToAPIViaSQLite(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)

	dbPath := t.TempDir() + "/telemetry.db"

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
		uuid     = "GPU-sqlite-0001"
		gpuID    = "6"
		hostname = "test-node-07"
		n        = 12
	)
	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	publishTelemetry(t, brokerAddr, "telemetry.gpu.6", uuid, gpuID, hostname, n, base, logger)
	waitForRecords(t, writeStore, uuid, n)

	// API opens the same file read-only.
	readStore, err := storage.OpenSQLiteReadOnly(dbPath)
	if err != nil {
		t.Fatalf("open read store: %v", err)
	}
	t.Cleanup(func() { _ = readStore.Close() })

	ts := apiServer(t, readStore, "", logger)

	var gpuList gpuListResponse
	if code := getJSON(t, ts.URL+"/api/v1/gpus", &gpuList); code != http.StatusOK {
		t.Fatalf("gpus status = %d", code)
	}
	if gpuList.Total < 1 {
		t.Fatalf("expected ≥1 GPU, got %d", gpuList.Total)
	}
	found := false
	for _, g := range gpuList.Items {
		if g.UUID == uuid {
			found = true
		}
	}
	if !found {
		t.Errorf("uuid %s not found in GPU list", uuid)
	}

	var tel telemetryResponse
	if code := getJSON(t, fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?limit=20", ts.URL, uuid), &tel); code != http.StatusOK {
		t.Fatalf("telemetry status = %d", code)
	}
	if tel.Count != n {
		t.Errorf("telemetry count = %d, want %d", tel.Count, n)
	}
}

// -------------------------------------------------------------------------
// Test: multi-GPU pagination
// -------------------------------------------------------------------------

// TestIntegration_MultiGPUPagination publishes 15 GPUs and walks all pages to
// confirm total, count, and complete coverage with no duplicates.
func TestIntegration_MultiGPUPagination(t *testing.T) {
	logger := integrationLogger(t)
	brokerAddr := startBroker(t, logger)
	store := storage.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	collectIntoDB(ctx, t, brokerAddr, "telemetry.gpu.*", store, logger)
	time.Sleep(100 * time.Millisecond)

	const totalGPUs = 15
	base := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	for i := 0; i < totalGPUs; i++ {
		publishTelemetry(t, brokerAddr, fmt.Sprintf("telemetry.gpu.%d", i),
			fmt.Sprintf("GPU-page-%04d", i), fmt.Sprintf("%d", i), "page-host", 1, base, logger)
	}
	waitForGPUs(t, store, totalGPUs)

	ts := apiServer(t, store, "", logger)

	// Walk pages with size=6.
	seen := map[string]bool{}
	pageSize, offset := 6, 0
	for {
		var page gpuListResponse
		url := fmt.Sprintf("%s/api/v1/gpus?limit=%d&offset=%d", ts.URL, pageSize, offset)
		if code := getJSON(t, url, &page); code != http.StatusOK {
			t.Fatalf("page offset=%d status = %d", offset, code)
		}
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
		t.Errorf("saw %d unique GPUs, want %d", len(seen), totalGPUs)
	}

	// Out-of-range offset returns empty items but correct total.
	var empty gpuListResponse
	getJSON(t, fmt.Sprintf("%s/api/v1/gpus?offset=999", ts.URL), &empty)
	if empty.Total != totalGPUs {
		t.Errorf("out-of-range total = %d, want %d", empty.Total, totalGPUs)
	}
	if empty.Count != 0 {
		t.Errorf("out-of-range count = %d, want 0", empty.Count)
	}

	// limit > 1000 → 400.
	if code := getStatus(t, fmt.Sprintf("%s/api/v1/gpus?limit=9999", ts.URL)); code != http.StatusBadRequest {
		t.Errorf("limit=9999 status = %d, want 400", code)
	}
}
