// Tests for the API server: all handlers, pure functions, and middleware.
// These live in package main so unexported helpers (computeEnergy, detectAnomalies,
// percentile, etc.) are directly testable.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/storage"
)

// -------------------------------------------------------------------------
// Test infrastructure
// -------------------------------------------------------------------------

// discardLogger returns a no-op structured logger so test output stays clean.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// buildAPI creates a full API test server backed by a fresh MemoryStore.
// brokerURL can be set to a fake httptest server URL, or left empty to let
// broker-stats tests exercise the 502 path.
func buildAPI(t *testing.T, brokerURL string) (*httptest.Server, *storage.MemoryStore) {
	t.Helper()
	store := storage.NewMemoryStore()
	srv := newServer(":0", store, discardLogger(), 5*time.Second, brokerURL)
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)
	return ts, store
}

// get is a convenience wrapper around http.Get with automatic cleanup.
func get(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url) //nolint:noctx // test helper; context not needed
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// decodeJSON reads the response body and unmarshals it into v.
func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

// seedTelemetry writes a slice of records into the store and fails the test if
// any write fails.
func seedTelemetry(t *testing.T, store *storage.MemoryStore, recs []*models.Telemetry) {
	t.Helper()
	ctx := context.Background()
	for _, r := range recs {
		if err := store.Write(ctx, r); err != nil {
			t.Fatalf("seed Write: %v", err)
		}
	}
}

// newTel builds a minimal Telemetry record for use in API tests.
func newTel(uuid, gpuID, host, metric string, val float64, ts time.Time) *models.Telemetry {
	return &models.Telemetry{
		Timestamp:  ts,
		MetricName: metric,
		GPUID:      gpuID,
		UUID:       uuid,
		ModelName:  "NVIDIA H100",
		Hostname:   host,
		Value:      val,
	}
}

var apiT0 = time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)

// -------------------------------------------------------------------------
// Liveness
// -------------------------------------------------------------------------

func TestHandleHealth(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/health")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body map[string]string
	decodeJSON(t, resp, &body)
	if body["status"] != "ok" {
		t.Errorf("status field = %q, want ok", body["status"])
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/gpus
// -------------------------------------------------------------------------

func TestHandleGetGPUs_Empty(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body gpuListResponse
	decodeJSON(t, resp, &body)
	if body.Count != 0 {
		t.Errorf("count = %d, want 0", body.Count)
	}
	if body.Items == nil {
		t.Error("items must be [] not null")
	}
}

func TestHandleGetGPUs_WithData(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	seedTelemetry(t, store, []*models.Telemetry{
		newTel("uuid-A", "0", "host-1", "util", 80, apiT0),
		newTel("uuid-B", "1", "host-2", "util", 60, apiT0),
	})

	resp := get(t, ts.URL+"/api/v1/gpus")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body gpuListResponse
	decodeJSON(t, resp, &body)
	if body.Count != 2 {
		t.Errorf("count = %d, want 2", body.Count)
	}
	if body.Total != 2 {
		t.Errorf("total = %d, want 2", body.Total)
	}
}

func TestHandleGetGPUs_Pagination(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	// Seed 3 GPUs.
	seedTelemetry(t, store, []*models.Telemetry{
		newTel("uuid-A", "0", "host-1", "util", 80, apiT0),
		newTel("uuid-B", "1", "host-2", "util", 60, apiT0),
		newTel("uuid-C", "2", "host-3", "util", 40, apiT0),
	})

	// First page: limit=2, offset=0.
	resp := get(t, ts.URL+"/api/v1/gpus?limit=2&offset=0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var page1 gpuListResponse
	decodeJSON(t, resp, &page1)
	if page1.Total != 3 {
		t.Errorf("total = %d, want 3", page1.Total)
	}
	if page1.Count != 2 {
		t.Errorf("count = %d, want 2", page1.Count)
	}

	// Second page: limit=2, offset=2.
	resp2 := get(t, ts.URL+"/api/v1/gpus?limit=2&offset=2")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp2.StatusCode)
	}
	var page2 gpuListResponse
	decodeJSON(t, resp2, &page2)
	if page2.Total != 3 {
		t.Errorf("total = %d, want 3", page2.Total)
	}
	if page2.Count != 1 {
		t.Errorf("count = %d, want 1", page2.Count)
	}

	// offset beyond total returns empty items.
	resp3 := get(t, ts.URL+"/api/v1/gpus?offset=99")
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp3.StatusCode)
	}
	var page3 gpuListResponse
	decodeJSON(t, resp3, &page3)
	if page3.Count != 0 {
		t.Errorf("count = %d, want 0 for out-of-range offset", page3.Count)
	}
	if page3.Total != 3 {
		t.Errorf("total = %d, want 3", page3.Total)
	}

	// limit > 1000 should return 400.
	resp4 := get(t, ts.URL+"/api/v1/gpus?limit=9999")
	if resp4.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for limit > 1000", resp4.StatusCode)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/gpus/:uuid/telemetry
// -------------------------------------------------------------------------

func TestHandleGetGPUTelemetry_Empty(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus/no-such-uuid/telemetry")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body telemetryResponse
	decodeJSON(t, resp, &body)
	if body.Count != 0 {
		t.Errorf("count = %d, want 0", body.Count)
	}
}

func TestHandleGetGPUTelemetry_WithData(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-tel"
	for i := 0; i < 5; i++ {
		seedTelemetry(t, store, []*models.Telemetry{
			newTel(uuid, "0", "h", "util", float64(i*10), apiT0.Add(time.Duration(i)*time.Second)),
		})
	}

	resp := get(t, ts.URL+"/api/v1/gpus/"+uuid+"/telemetry")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body telemetryResponse
	decodeJSON(t, resp, &body)
	if body.Count != 5 {
		t.Errorf("count = %d, want 5", body.Count)
	}
}

func TestHandleGetGPUTelemetry_DefaultLimit(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-lim"
	for i := 0; i < 150; i++ {
		seedTelemetry(t, store, []*models.Telemetry{
			newTel(uuid, "0", "h", "util", float64(i), apiT0.Add(time.Duration(i)*time.Second)),
		})
	}

	resp := get(t, ts.URL+"/api/v1/gpus/"+uuid+"/telemetry")
	var body telemetryResponse
	decodeJSON(t, resp, &body)
	// Default limit is 100.
	if body.Limit != 100 {
		t.Errorf("limit = %d, want 100", body.Limit)
	}
	if body.Count > 100 {
		t.Errorf("count = %d exceeds default limit 100", body.Count)
	}
}

func TestHandleGetGPUTelemetry_LimitExceeded(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus/any/telemetry?limit=10001")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetGPUTelemetry_BadStartTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus/any/telemetry?start_time=not-a-time")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetGPUTelemetry_BadEndTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus/any/telemetry?end_time=not-a-time")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetGPUTelemetry_TimeFilter(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-tf"
	for i := 0; i < 5; i++ {
		seedTelemetry(t, store, []*models.Telemetry{
			newTel(uuid, "0", "h", "util", float64(i), apiT0.Add(time.Duration(i)*time.Minute)),
		})
	}

	from := apiT0.Add(time.Minute)
	until := apiT0.Add(3 * time.Minute)
	url := fmt.Sprintf("%s/api/v1/gpus/%s/telemetry?start_time=%s&end_time=%s",
		ts.URL, uuid,
		from.Format(time.RFC3339),
		until.Format(time.RFC3339),
	)

	resp := get(t, url)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body telemetryResponse
	decodeJSON(t, resp, &body)
	if body.Count != 3 {
		t.Errorf("count = %d, want 3 (from+1m to until inclusive)", body.Count)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/gpus/:uuid/energy
// -------------------------------------------------------------------------

func TestHandleGetGPUEnergy_NoData(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/gpus/ghost-gpu/energy")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleGetGPUEnergy_SingleSample(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-egy-1"
	seedTelemetry(t, store, []*models.Telemetry{
		newTel(uuid, "0", "h", utilMetricName, 50.0, apiT0),
	})

	resp := get(t, ts.URL+"/api/v1/gpus/"+uuid+"/energy")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body energyResponse
	decodeJSON(t, resp, &body)
	// Single sample: no interval → 0 Wh, but power model fields must be present.
	if body.SampleCount != 1 {
		t.Errorf("sample_count = %d, want 1", body.SampleCount)
	}
	if body.PowerModelNote == "" {
		t.Error("power_model_note is empty")
	}
}

func TestHandleGetGPUEnergy_KnownValues(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-egy-2"
	// Two samples: 100% util for exactly 1 hour.
	// Power at 100% = 50 + 300 = 350 W.
	// Trapezoidal area = (350+350)/2 * 1h = 350 Wh.
	// Carbon = 350/1000 * 400 = 140 g.
	seedTelemetry(t, store, []*models.Telemetry{
		newTel(uuid, "0", "h", utilMetricName, 100.0, apiT0),
		newTel(uuid, "0", "h", utilMetricName, 100.0, apiT0.Add(time.Hour)),
	})

	resp := get(t, ts.URL+"/api/v1/gpus/"+uuid+"/energy")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body energyResponse
	decodeJSON(t, resp, &body)

	assertNear(t, "total_energy_wh", 350.0, body.TotalEnergyWh, 0.001)
	assertNear(t, "carbon_grams", 140.0, body.CarbonGrams, 0.001)
	assertNear(t, "avg_power_w", 350.0, body.AvgPowerW, 0.001)
}

func TestHandleGetGPUEnergy_BadStartTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/u/energy?start_time=bad")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetGPUEnergy_BadEndTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/u/energy?end_time=bad")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/broker/stats
// -------------------------------------------------------------------------

func TestHandleBrokerStats_OK(t *testing.T) {
	t.Parallel()

	// Fake broker admin server.
	fakeBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Produce 10 published, 8 delivered, 2 dropped.
		_, _ = fmt.Fprint(w, `{
			"total_published": 10,
			"total_delivered": 8,
			"total_dropped":   2,
			"active_topics":   3,
			"total_subscribers": 5
		}`)
	}))
	defer fakeBroker.Close()

	ts, _ := buildAPI(t, fakeBroker.URL)

	resp := get(t, ts.URL+"/api/v1/broker/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body brokerStatsResponse
	decodeJSON(t, resp, &body)

	if body.TotalPublished != 10 {
		t.Errorf("total_published = %d, want 10", body.TotalPublished)
	}
	if body.TotalDropped != 2 {
		t.Errorf("total_dropped = %d, want 2", body.TotalDropped)
	}
	// drop_rate_pct = 2/10 * 100 = 20.0
	assertNear(t, "drop_rate_pct", 20.0, body.DropRatePct, 0.001)
	if body.Note == "" {
		t.Error("note field should be populated")
	}
}

func TestHandleBrokerStats_ZeroPublished_NoDropRate(t *testing.T) {
	t.Parallel()

	fakeBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"total_published":0,"total_delivered":0,"total_dropped":0}`)
	}))
	defer fakeBroker.Close()

	ts, _ := buildAPI(t, fakeBroker.URL)
	resp := get(t, ts.URL+"/api/v1/broker/stats")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body brokerStatsResponse
	decodeJSON(t, resp, &body)
	// With zero published, drop rate must stay 0 (no divide-by-zero).
	if body.DropRatePct != 0 {
		t.Errorf("drop_rate_pct = %.2f, want 0.0", body.DropRatePct)
	}
}

func TestHandleBrokerStats_BrokerDown(t *testing.T) {
	t.Parallel()
	// Point at a port nothing is listening on.
	ts, _ := buildAPI(t, "http://127.0.0.1:1") // port 1 is reserved, no listener

	resp := get(t, ts.URL+"/api/v1/broker/stats")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestHandleBrokerStats_InvalidJSON(t *testing.T) {
	t.Parallel()

	fakeBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{invalid json`)
	}))
	defer fakeBroker.Close()

	ts, _ := buildAPI(t, fakeBroker.URL)
	resp := get(t, ts.URL+"/api/v1/broker/stats")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/cluster/stranded-compute
// -------------------------------------------------------------------------

func TestHandleStrandedCompute_DefaultParams(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body strandedComputeResponse
	decodeJSON(t, resp, &body)
	if body.Items == nil {
		t.Error("items must be [] not null")
	}
}

func TestHandleStrandedCompute_StrandedGPUsFound(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	now := time.Now().UTC()
	// GPU at 0 % util – should be classified as stranded.
	seedTelemetry(t, store, []*models.Telemetry{
		newTel("idle-uuid", "0", "host-idle", utilMetricName, 0, now.Add(-5*time.Minute)),
		newTel("idle-uuid", "0", "host-idle", utilMetricName, 0, now.Add(-2*time.Minute)),
		// Active GPU – must NOT appear.
		newTel("busy-uuid", "1", "host-busy", utilMetricName, 90, now.Add(-3*time.Minute)),
	})

	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?window=15m&max_util=0")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body strandedComputeResponse
	decodeJSON(t, resp, &body)

	for _, item := range body.Items {
		if item.UUID == "busy-uuid" {
			t.Error("active GPU included in stranded-compute response")
		}
	}

	found := false
	for _, item := range body.Items {
		if item.UUID == "idle-uuid" {
			found = true
			if item.IdlePowerW != idlePowerW {
				t.Errorf("idle_power_w = %.1f, want %.1f", item.IdlePowerW, idlePowerW)
			}
			if item.WastedEnergyWh <= 0 {
				t.Error("wasted_energy_wh must be positive")
			}
			if item.WastedCO2g <= 0 {
				t.Error("wasted_co2_g must be positive")
			}
		}
	}
	if !found {
		t.Error("idle GPU not in stranded-compute response")
	}
}

func TestHandleStrandedCompute_TotalsConsistency(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	now := time.Now().UTC()
	// Two stranded GPUs.
	for _, uuid := range []string{"s1", "s2"} {
		seedTelemetry(t, store, []*models.Telemetry{
			newTel(uuid, "0", "h", utilMetricName, 0, now.Add(-5*time.Minute)),
		})
	}

	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?window=15m&max_util=0")
	var body strandedComputeResponse
	decodeJSON(t, resp, &body)

	if body.StrandedCount != len(body.Items) {
		t.Errorf("stranded_count=%d != len(items)=%d", body.StrandedCount, len(body.Items))
	}
	if body.TotalWastedWh <= 0 && body.StrandedCount > 0 {
		t.Error("total_wasted_wh must be positive when GPUs are stranded")
	}
}

func TestHandleStrandedCompute_InvalidWindow(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?window=not-valid")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleStrandedCompute_NegativeWindow(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?window=-5m")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleStrandedCompute_InvalidMaxUtil(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?max_util=abc")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleStrandedCompute_MaxUtilOutOfRange(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/stranded-compute?max_util=150")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/gpus/:uuid/telemetry/summary
// -------------------------------------------------------------------------

func TestHandleTelemetrySummary_MissingMetric(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/any/telemetry/summary")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleTelemetrySummary_NoData(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/ghost/telemetry/summary?metric_name=util")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleTelemetrySummary_KnownPercentiles(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	uuid := "uuid-sum"
	// 10 samples: values 10, 20, …, 100.
	for i := 1; i <= 10; i++ {
		seedTelemetry(t, store, []*models.Telemetry{
			newTel(uuid, "0", "h", "DCGM_FI_DEV_GPU_UTIL", float64(i*10),
				apiT0.Add(time.Duration(i)*time.Minute)),
		})
	}

	resp := get(t, ts.URL+"/api/v1/gpus/"+uuid+"/telemetry/summary?metric_name=DCGM_FI_DEV_GPU_UTIL")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body telemetrySummaryResponse
	decodeJSON(t, resp, &body)

	if body.SampleCount != 10 {
		t.Errorf("sample_count = %d, want 10", body.SampleCount)
	}
	assertNear(t, "min", 10.0, body.Min, 0.001)
	assertNear(t, "max", 100.0, body.Max, 0.001)
	assertNear(t, "mean", 55.0, body.Mean, 0.001)
	// P50 of [10,20,…,100] (sorted) with linear interpolation at rank 4.5 → 55.
	assertNear(t, "p50", 55.0, body.P50, 0.001)
	// P90 at rank 8.1 → between 90 and 100 = 91.
	if body.P90 < 90 || body.P90 > 100 {
		t.Errorf("p90 = %.2f out of expected range [90, 100]", body.P90)
	}
	if body.P99 < 98 || body.P99 > 100 {
		t.Errorf("p99 = %.2f out of expected range [98, 100]", body.P99)
	}
}

func TestHandleTelemetrySummary_BadStartTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/u/telemetry/summary?metric_name=util&start_time=bad")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleTelemetrySummary_BadEndTime(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/gpus/u/telemetry/summary?metric_name=util&end_time=bad")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// GET /api/v1/cluster/anomalies
// -------------------------------------------------------------------------

func TestHandleClusterAnomalies_Empty(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")

	resp := get(t, ts.URL+"/api/v1/cluster/anomalies")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body clusterAnomaliesResponse
	decodeJSON(t, resp, &body)
	if body.AnomalyCount != 0 {
		t.Errorf("anomaly_count = %d, want 0", body.AnomalyCount)
	}
}

func TestHandleClusterAnomalies_AnomalyDetected(t *testing.T) {
	t.Parallel()
	ts, store := buildAPI(t, "")

	now := time.Now().UTC()
	uuid := "uuid-anom"
	// Spike at 98 % then crash to 10 % in the next sample.
	seedTelemetry(t, store, []*models.Telemetry{
		newTel(uuid, "0", "h", utilMetricName, 30, now.Add(-10*time.Minute)),
		newTel(uuid, "0", "h", utilMetricName, 98, now.Add(-5*time.Minute)), // trigger
		newTel(uuid, "0", "h", utilMetricName, 10, now.Add(-4*time.Minute)), // drop of 88 pp
	})

	resp := get(t, ts.URL+"/api/v1/cluster/anomalies?window=30m")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var body clusterAnomaliesResponse
	decodeJSON(t, resp, &body)

	if body.AnomalyCount == 0 {
		t.Fatal("expected at least one anomaly, got 0")
	}

	event := body.Items[0]
	if event.Kind != "PerformanceDrop" {
		t.Errorf("kind = %q, want PerformanceDrop", event.Kind)
	}
	if event.UUID != uuid {
		t.Errorf("uuid = %q, want %q", event.UUID, uuid)
	}
	if event.DropDeltaPp < anomalyDropPp {
		t.Errorf("drop_delta_pp = %.1f, want ≥%.1f", event.DropDeltaPp, anomalyDropPp)
	}
	if event.Description == "" {
		t.Error("description must not be empty")
	}
}

func TestHandleClusterAnomalies_InvalidWindow(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/anomalies?window=garbage")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleClusterAnomalies_NegativeWindow(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v1/cluster/anomalies?window=-1h")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// NoRoute
// -------------------------------------------------------------------------

func TestNoRoute(t *testing.T) {
	t.Parallel()
	ts, _ := buildAPI(t, "")
	resp := get(t, ts.URL+"/api/v999/nonexistent")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// -------------------------------------------------------------------------
// percentile() – pure function
// -------------------------------------------------------------------------

func TestPercentile_Empty(t *testing.T) {
	t.Parallel()
	if got := percentile([]float64{}, 50); got != 0 {
		t.Errorf("empty slice: got %.2f, want 0", got)
	}
}

func TestPercentile_SingleElement(t *testing.T) {
	t.Parallel()
	if got := percentile([]float64{42}, 50); got != 42 {
		t.Errorf("single element: got %.2f, want 42", got)
	}
}

func TestPercentile_P0_P100(t *testing.T) {
	t.Parallel()
	sorted := []float64{1, 2, 3, 4, 5}
	if got := percentile(sorted, 0); got != 1 {
		t.Errorf("P0 = %.2f, want 1", got)
	}
	if got := percentile(sorted, 100); got != 5 {
		t.Errorf("P100 = %.2f, want 5", got)
	}
}

func TestPercentile_P50_EvenLength(t *testing.T) {
	t.Parallel()
	// [1,2,3,4] → median by linear interpolation at rank 1.5 = 2.5.
	sorted := []float64{1, 2, 3, 4}
	got := percentile(sorted, 50)
	assertNear(t, "p50", 2.5, got, 0.0001)
}

func TestPercentile_Interpolation(t *testing.T) {
	t.Parallel()
	// 10 values 10..100.  P90 → rank = 0.90 * 9 = 8.1 → 90 + 0.1*10 = 91.
	sorted := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	got := percentile(sorted, 90)
	assertNear(t, "p90", 91.0, got, 0.0001)
}

func TestPercentile_AllSameValue(t *testing.T) {
	t.Parallel()
	sorted := []float64{7, 7, 7, 7, 7}
	for _, p := range []float64{0, 25, 50, 75, 100} {
		if got := percentile(sorted, p); got != 7 {
			t.Errorf("P%.0f of all-7 slice = %.2f, want 7", p, got)
		}
	}
}

// -------------------------------------------------------------------------
// computeEnergy() – pure function
// -------------------------------------------------------------------------

func TestComputeEnergy_SingleSample(t *testing.T) {
	t.Parallel()
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 100, apiT0),
	}
	result := computeEnergy(rows)
	// One point → no interval → 0 Wh.
	if result.TotalEnergyWh != 0 {
		t.Errorf("single sample energy = %.4f, want 0", result.TotalEnergyWh)
	}
	if result.SampleCount != 1 {
		t.Errorf("sample_count = %d, want 1", result.SampleCount)
	}
}

func TestComputeEnergy_FullLoadOneHour(t *testing.T) {
	t.Parallel()
	// 100 % util for 1 h → Power = 350 W → Energy = 350 Wh → Carbon = 140 g.
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 100, apiT0),
		newTel("u", "0", "h", utilMetricName, 100, apiT0.Add(time.Hour)),
	}
	result := computeEnergy(rows)
	assertNear(t, "TotalEnergyWh", 350.0, result.TotalEnergyWh, 0.001)
	assertNear(t, "CarbonGrams", 140.0, result.CarbonGrams, 0.001)
}

func TestComputeEnergy_IdleOneHour(t *testing.T) {
	t.Parallel()
	// 0 % util for 1 h → Power = 50 W → Energy = 50 Wh → Carbon = 20 g.
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 0, apiT0),
		newTel("u", "0", "h", utilMetricName, 0, apiT0.Add(time.Hour)),
	}
	result := computeEnergy(rows)
	assertNear(t, "TotalEnergyWh", 50.0, result.TotalEnergyWh, 0.001)
	assertNear(t, "CarbonGrams", 20.0, result.CarbonGrams, 0.001)
}

func TestComputeEnergy_MetadataPopulated(t *testing.T) {
	t.Parallel()
	rows := []*models.Telemetry{
		{Timestamp: apiT0, MetricName: utilMetricName, UUID: "u1",
			ModelName: "H100", Hostname: "box", Value: 50},
		{Timestamp: apiT0.Add(time.Hour), MetricName: utilMetricName, UUID: "u1",
			ModelName: "H100", Hostname: "box", Value: 50},
	}
	result := computeEnergy(rows)
	if result.UUID != "u1" {
		t.Errorf("UUID = %q, want u1", result.UUID)
	}
	if result.ModelName != "H100" {
		t.Errorf("ModelName = %q, want H100", result.ModelName)
	}
	if result.PowerModelNote == "" || result.CarbonIntensity == "" {
		t.Error("power model annotation strings must not be empty")
	}
}

func TestComputeEnergy_OutOfOrderInputSorted(t *testing.T) {
	t.Parallel()
	// Rows supplied in reverse order – computeEnergy must sort them first.
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 100, apiT0.Add(time.Hour)),
		newTel("u", "0", "h", utilMetricName, 100, apiT0),
	}
	result := computeEnergy(rows)
	assertNear(t, "TotalEnergyWh", 350.0, result.TotalEnergyWh, 0.001)
}

// -------------------------------------------------------------------------
// detectAnomalies() – pure function
// -------------------------------------------------------------------------

func TestDetectAnomalies_Empty(t *testing.T) {
	t.Parallel()
	events := detectAnomalies(nil)
	if len(events) != 0 {
		t.Errorf("want 0 anomalies, got %d", len(events))
	}
}

func TestDetectAnomalies_NoneFound_LowUtil(t *testing.T) {
	t.Parallel()
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 30, apiT0),
		newTel("u", "0", "h", utilMetricName, 40, apiT0.Add(time.Minute)),
		newTel("u", "0", "h", utilMetricName, 50, apiT0.Add(2*time.Minute)),
	}
	if events := detectAnomalies(rows); len(events) != 0 {
		t.Errorf("want 0 anomalies, got %d", len(events))
	}
}

func TestDetectAnomalies_NoneFound_GradualDrop(t *testing.T) {
	t.Parallel()
	// Spike to 96 % but the drop is only 30 pp – below anomalyDropPp.
	rows := []*models.Telemetry{
		newTel("u", "0", "h", utilMetricName, 96, apiT0),
		newTel("u", "0", "h", utilMetricName, 66, apiT0.Add(time.Minute)),
	}
	if events := detectAnomalies(rows); len(events) != 0 {
		t.Errorf("want 0 anomalies for small drop, got %d", len(events))
	}
}

func TestDetectAnomalies_OneFound(t *testing.T) {
	t.Parallel()
	rows := []*models.Telemetry{
		newTel("uuid-x", "0", "host-x", utilMetricName, 20, apiT0),
		newTel("uuid-x", "0", "host-x", utilMetricName, 98, apiT0.Add(time.Minute)),   // trigger
		newTel("uuid-x", "0", "host-x", utilMetricName, 10, apiT0.Add(2*time.Minute)), // drop 88 pp
	}
	events := detectAnomalies(rows)
	if len(events) != 1 {
		t.Fatalf("want 1 anomaly, got %d", len(events))
	}
	e := events[0]
	if e.Kind != "PerformanceDrop" {
		t.Errorf("kind = %q, want PerformanceDrop", e.Kind)
	}
	if e.UUID != "uuid-x" {
		t.Errorf("uuid = %q, want uuid-x", e.UUID)
	}
	assertNear(t, "PeakUtilPct", 98, e.PeakUtilPct, 0.001)
	assertNear(t, "DropUtilPct", 10, e.DropUtilPct, 0.001)
	assertNear(t, "DropDeltaPp", 88, e.DropDeltaPp, 0.001)
	if e.Description == "" {
		t.Error("description must be non-empty")
	}
}

func TestDetectAnomalies_Deduplication(t *testing.T) {
	t.Parallel()
	// The trigger sample is followed by multiple qualifying drops.
	// Only one anomaly must be emitted per trigger.
	rows := []*models.Telemetry{
		newTel("u-dup", "0", "h", utilMetricName, 96, apiT0),
		newTel("u-dup", "0", "h", utilMetricName, 10, apiT0.Add(time.Minute)),
		newTel("u-dup", "0", "h", utilMetricName, 5, apiT0.Add(2*time.Minute)),
	}
	events := detectAnomalies(rows)
	if len(events) != 1 {
		t.Errorf("want exactly 1 anomaly (dedup), got %d", len(events))
	}
}

func TestDetectAnomalies_MultipleGPUs(t *testing.T) {
	t.Parallel()
	// Two different GPUs, each with an anomaly.
	rows := []*models.Telemetry{
		newTel("gpu-1", "0", "h1", utilMetricName, 97, apiT0),
		newTel("gpu-1", "0", "h1", utilMetricName, 5, apiT0.Add(time.Minute)),
		newTel("gpu-2", "0", "h2", utilMetricName, 99, apiT0),
		newTel("gpu-2", "0", "h2", utilMetricName, 10, apiT0.Add(time.Minute)),
	}
	events := detectAnomalies(rows)
	if len(events) != 2 {
		t.Errorf("want 2 anomalies (one per GPU), got %d", len(events))
	}
}

func TestDetectAnomalies_SortedByDetectedAtDesc(t *testing.T) {
	t.Parallel()
	rows := []*models.Telemetry{
		// gpu-early: spike+drop earlier in time.
		newTel("gpu-early", "0", "h", utilMetricName, 96, apiT0),
		newTel("gpu-early", "0", "h", utilMetricName, 10, apiT0.Add(time.Minute)),
		// gpu-late: spike+drop later.
		newTel("gpu-late", "0", "h", utilMetricName, 96, apiT0.Add(10*time.Minute)),
		newTel("gpu-late", "0", "h", utilMetricName, 10, apiT0.Add(11*time.Minute)),
	}
	events := detectAnomalies(rows)
	if len(events) < 2 {
		t.Fatalf("want 2 anomalies, got %d", len(events))
	}
	// Most recent first.
	if !events[0].DetectedAt.After(events[1].DetectedAt) {
		t.Errorf("events not sorted desc by detected_at: %v, %v",
			events[0].DetectedAt, events[1].DetectedAt)
	}
}

func TestDetectAnomalies_BlankUUIDSkipped(t *testing.T) {
	t.Parallel()
	// Records without a UUID must be ignored.
	rows := []*models.Telemetry{
		newTel("", "0", "h", utilMetricName, 98, apiT0),
		newTel("", "0", "h", utilMetricName, 5, apiT0.Add(time.Minute)),
	}
	if events := detectAnomalies(rows); len(events) != 0 {
		t.Errorf("blank-UUID records produced %d anomalies, want 0", len(events))
	}
}

// -------------------------------------------------------------------------
// parseIntParam() helper
// -------------------------------------------------------------------------

func TestParseIntParam(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input    string
		fallback int
		want     int
	}{
		{"", 100, 100},
		{"0", 100, 0},
		{"42", 10, 42},
		{"-1", 10, 10},  // negative → fallback
		{"abc", 10, 10}, // non-numeric → fallback
	}
	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			t.Parallel()
			if got := parseIntParam(tc.input, tc.fallback); got != tc.want {
				t.Errorf("parseIntParam(%q, %d) = %d, want %d", tc.input, tc.fallback, got, tc.want)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Test helpers
// -------------------------------------------------------------------------

// assertNear fails the test if got and want differ by more than tol.
func assertNear(t *testing.T, label string, want, got, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.6f, want %.6f (tol %.6f)", label, got, want, tol)
	}
}
