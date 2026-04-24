// Package models defines the shared data structures used across all
// gpu-telemetry services. Types here are wire-compatible (JSON) so they
// can flow from Streamer → Broker → Collector → API without transformation.
package models

import (
	"fmt"
	"strings"
	"time"
)

// Telemetry represents a single GPU metric data point parsed from a DCGM CSV
// export or received through the message queue. All fields that are present in
// the upstream Prometheus / DCGM exporter label set are captured so downstream
// consumers (API, dashboards) have full context without needing a secondary
// look-up.
type Telemetry struct {
	// Core identity fields
	Timestamp  time.Time `json:"timestamp"`
	MetricName string    `json:"metric_name"`
	GPUID      string    `json:"gpu_id"`
	Device     string    `json:"device"`
	UUID       string    `json:"uuid"`
	ModelName  string    `json:"model_name"`
	Hostname   string    `json:"hostname"`

	// Kubernetes context (optional – empty when running bare-metal)
	Container string `json:"container,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Namespace string `json:"namespace,omitempty"`

	// The actual measured value.
	Value float64 `json:"value"`

	// Labels carries the full Prometheus label set parsed from the raw CSV
	// column.  Consumers that need driver version, job name, instance, etc.
	// can find them here without schema changes.
	Labels map[string]string `json:"labels,omitempty"`
}

// Validate performs lightweight field-level validation. It is intentionally
// not exhaustive – we validate at the system boundary (CSV ingestion) rather
// than deep inside the routing layer.
func (t *Telemetry) Validate() error {
	if t.MetricName == "" {
		return fmt.Errorf("models: metric_name is required")
	}
	if t.Hostname == "" {
		return fmt.Errorf("models: hostname is required")
	}
	if t.Timestamp.IsZero() {
		return fmt.Errorf("models: timestamp is required")
	}
	return nil
}

// ParseLabelsRaw parses a Prometheus-style labels string into a key/value map.
//
// Input format (as exported by dcgm-exporter):
//
//	key1="value1",key2="value2",...
//
// Values may contain commas or equals signs inside the quoted section.
// ParseLabelsRaw is intentionally lenient: malformed pairs are skipped so
// a single bad label does not drop an entire metric row.
func ParseLabelsRaw(raw string) map[string]string {
	if raw == "" {
		return nil
	}

	result := make(map[string]string)

	// Walk character by character to handle quoted values that may contain commas.
	remaining := raw
	for len(remaining) > 0 {
		// Find key (up to '=').
		eqIdx := strings.Index(remaining, "=")
		if eqIdx < 0 {
			break
		}
		key := strings.TrimSpace(remaining[:eqIdx])
		remaining = remaining[eqIdx+1:]

		// Value must start with '"'.
		if len(remaining) == 0 || remaining[0] != '"' {
			// No quoted value – skip to next comma.
			if idx := strings.Index(remaining, ","); idx >= 0 {
				remaining = remaining[idx+1:]
			} else {
				break
			}
			continue
		}

		// Scan for the closing '"', handling escaped '""' sequences.
		remaining = remaining[1:] // consume opening quote
		var sb strings.Builder
		for {
			if len(remaining) == 0 {
				break
			}
			if remaining[0] == '"' {
				if len(remaining) > 1 && remaining[1] == '"' {
					// Escaped quote inside value.
					sb.WriteByte('"')
					remaining = remaining[2:]
					continue
				}
				// Closing quote.
				remaining = remaining[1:]
				break
			}
			sb.WriteByte(remaining[0])
			remaining = remaining[1:]
		}

		if key != "" {
			result[key] = sb.String()
		}

		// Skip optional comma separator.
		if len(remaining) > 0 && remaining[0] == ',' {
			remaining = remaining[1:]
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}
