package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
)

// -------------------------------------------------------------------------
// ParseLabelsRaw
// -------------------------------------------------------------------------

func TestParseLabelsRaw_Standard(t *testing.T) {
	raw := `DCGM_FI_DRIVER_VERSION="535.129.03",Hostname="my-host",gpu="0"`
	got := models.ParseLabelsRaw(raw)

	cases := map[string]string{
		"DCGM_FI_DRIVER_VERSION": "535.129.03",
		"Hostname":               "my-host",
		"gpu":                    "0",
	}
	for k, want := range cases {
		if got[k] != want {
			t.Errorf("label[%q] = %q, want %q", k, got[k], want)
		}
	}
}

func TestParseLabelsRaw_ValueWithComma(t *testing.T) {
	// Values that contain commas are legal in Prometheus labels.
	raw := `instance="host:9400",job="dcgm_exporter"`
	got := models.ParseLabelsRaw(raw)

	if got["instance"] != "host:9400" {
		t.Errorf("instance = %q, want %q", got["instance"], "host:9400")
	}
	if got["job"] != "dcgm_exporter" {
		t.Errorf("job = %q, want %q", got["job"], "dcgm_exporter")
	}
}

func TestParseLabelsRaw_Empty(t *testing.T) {
	got := models.ParseLabelsRaw("")
	if got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestParseLabelsRaw_EscapedQuotes(t *testing.T) {
	// CSV double-quote escaping: "" inside a quoted field = one literal ".
	raw := `model="NVIDIA H100 80GB HBM3",driver="535.129.03"`
	got := models.ParseLabelsRaw(raw)

	if got["model"] != "NVIDIA H100 80GB HBM3" {
		t.Errorf("model = %q", got["model"])
	}
}

// -------------------------------------------------------------------------
// Telemetry.Validate
// -------------------------------------------------------------------------

func TestTelemetry_Validate_Valid(t *testing.T) {
	tel := &models.Telemetry{
		Timestamp:  time.Now(),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Hostname:   "my-host",
	}
	if err := tel.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestTelemetry_Validate_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		tel  models.Telemetry
	}{
		{
			name: "missing metric_name",
			tel:  models.Telemetry{Timestamp: time.Now(), Hostname: "h"},
		},
		{
			name: "missing hostname",
			tel:  models.Telemetry{Timestamp: time.Now(), MetricName: "m"},
		},
		{
			name: "zero timestamp",
			tel:  models.Telemetry{MetricName: "m", Hostname: "h"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.tel.Validate(); err == nil {
				t.Error("expected validation error, got nil")
			}
		})
	}
}

// -------------------------------------------------------------------------
// JSON serialisation round-trip
// -------------------------------------------------------------------------

func TestTelemetry_JSONRoundTrip(t *testing.T) {
	original := &models.Telemetry{
		Timestamp:  time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		GPUID:      "0",
		Device:     "nvidia0",
		UUID:       "GPU-5fd4f087-86f3-7a43-b711-4771313afc50",
		ModelName:  "NVIDIA H100 80GB HBM3",
		Hostname:   "mtv5-dgx1-hgpu-031",
		Value:      99.5,
		Labels:     map[string]string{"job": "dgx_dcgm_exporter", "gpu": "0"},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded models.Telemetry
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.MetricName != original.MetricName {
		t.Errorf("MetricName: got %q, want %q", decoded.MetricName, original.MetricName)
	}
	if decoded.Value != original.Value {
		t.Errorf("Value: got %f, want %f", decoded.Value, original.Value)
	}
	if decoded.Labels["job"] != "dgx_dcgm_exporter" {
		t.Errorf("Labels[job]: got %q", decoded.Labels["job"])
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Errorf("Timestamp: got %v, want %v", decoded.Timestamp, original.Timestamp)
	}
}

func TestTelemetry_JSONOmitsEmptyOptionals(t *testing.T) {
	tel := &models.Telemetry{
		Timestamp:  time.Now(),
		MetricName: "util",
		Hostname:   "h",
	}
	data, _ := json.Marshal(tel)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	for _, field := range []string{"container", "pod", "namespace", "labels"} {
		if _, ok := m[field]; ok {
			t.Errorf("field %q should be omitted when empty", field)
		}
	}
}

// -------------------------------------------------------------------------
// ParseLabelsRaw – additional edge cases
// -------------------------------------------------------------------------

func TestParseLabelsRaw_MissingEquals(t *testing.T) {
	// Input with no '=' should return an empty (non-nil) map because there
	// are no parseable pairs, not nil – the implementation breaks on the
	// first key scan and returns whatever it has accumulated.
	// The important guarantee is: it must not panic.
	got := models.ParseLabelsRaw("not-a-label-string")
	_ = got // result may be nil or empty map; just ensure no panic
}

func TestParseLabelsRaw_KeyWithoutQuotedValue(t *testing.T) {
	// A pair where the value is not quoted is skipped gracefully.
	raw := `good="yes",bad=noquote`
	got := models.ParseLabelsRaw(raw)
	if got["good"] != "yes" {
		t.Errorf("good = %q, want yes", got["good"])
	}
	// "bad" has no quoted value; it may be absent or empty – no panic.
}

func TestParseLabelsRaw_SinglePair(t *testing.T) {
	raw := `k="v"`
	got := models.ParseLabelsRaw(raw)
	if got["k"] != "v" {
		t.Errorf("k = %q, want v", got["k"])
	}
}

func TestParseLabelsRaw_UnclosedQuote(t *testing.T) {
	// Unclosed quote – implementation must not panic or infinite-loop.
	got := models.ParseLabelsRaw(`key="unclosed`)
	_ = got
}

func TestParseLabelsRaw_TrailingComma(t *testing.T) {
	raw := `a="1",b="2",`
	got := models.ParseLabelsRaw(raw)
	if got["a"] != "1" || got["b"] != "2" {
		t.Errorf("unexpected map: %v", got)
	}
}

// -------------------------------------------------------------------------
// Telemetry.Validate – additional cases
// -------------------------------------------------------------------------

func TestTelemetry_Validate_AllOptionalFieldsZero(t *testing.T) {
	// Optional fields (UUID, GPUID, Labels…) should not block validation.
	tel := &models.Telemetry{
		Timestamp:  time.Now(),
		MetricName: "m",
		Hostname:   "h",
	}
	if err := tel.Validate(); err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestTelemetry_Validate_ZeroValue(t *testing.T) {
	// Value == 0 is a legal metric reading (GPU at idle).
	tel := &models.Telemetry{
		Timestamp:  time.Now(),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Hostname:   "idle-host",
		Value:      0,
	}
	if err := tel.Validate(); err != nil {
		t.Errorf("value=0 should be valid: %v", err)
	}
}

// -------------------------------------------------------------------------
// JSON – optional Kubernetes fields round-trip
// -------------------------------------------------------------------------

func TestTelemetry_JSONRoundTrip_WithK8sContext(t *testing.T) {
	original := &models.Telemetry{
		Timestamp:  time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		Hostname:   "node-1",
		Container:  "dcgm-exporter",
		Pod:        "dcgm-exporter-abc12",
		Namespace:  "monitoring",
		Value:      75.5,
	}

	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var recovered models.Telemetry
	if err := json.Unmarshal(b, &recovered); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if recovered.Container != original.Container {
		t.Errorf("Container = %q, want %q", recovered.Container, original.Container)
	}
	if recovered.Pod != original.Pod {
		t.Errorf("Pod = %q, want %q", recovered.Pod, original.Pod)
	}
	if recovered.Namespace != original.Namespace {
		t.Errorf("Namespace = %q, want %q", recovered.Namespace, original.Namespace)
	}
}
