// Package storage defines the persistence interface used by the Collector
// and API services. The interface is deliberately narrow so that the backing
// store (SQLite, Postgres, TimescaleDB, etc.) can be swapped without touching
// application logic.
package storage

import (
	"context"
	"time"

	"gpu-telemetry/pkg/models"
)

// GPUSummary holds the identifying information for a unique GPU as observed
// in the telemetry store. It is used by the list-GPUs API endpoint.
type GPUSummary struct {
	UUID      string `json:"uuid"`
	GPUID     string `json:"gpu_id"`
	ModelName string `json:"model_name"`
	Hostname  string `json:"hostname"`
}

// StrandedGPU represents a GPU whose peak utilisation over the observation
// window was at or below the idle threshold – it is drawing power without
// performing useful work.  Used by the GreenOps stranded-compute endpoint.
type StrandedGPU struct {
	UUID        string  `json:"uuid"`
	GPUID       string  `json:"gpu_id"`
	ModelName   string  `json:"model_name"`
	Hostname    string  `json:"hostname"`
	PeakUtilPct float64 `json:"peak_util_pct"` // highest utilisation seen in window
	SampleCount int     `json:"sample_count"`  // number of samples examined
}

// QueryFilter holds optional predicates for telemetry queries.
// Zero values mean "no filter" (return all).
type QueryFilter struct {
	MetricName string
	GPUID      string
	Hostname   string
	UUID       string
	// Time range – both are inclusive. Zero value means unbounded.
	From  time.Time
	Until time.Time
	// Limit caps the number of results. 0 means no limit.
	Limit int
	// Offset enables pagination.
	Offset int
}

// Store is the persistence interface for GPU telemetry records.
// All methods must be safe for concurrent use.
type Store interface {
	// Write persists a single telemetry record. Implementations should batch
	// internally where possible for performance.
	Write(ctx context.Context, t *models.Telemetry) error

	// WriteBatch persists multiple records atomically (best-effort for engines
	// that don't support transactions). Partial failure returns the first error
	// encountered; successfully written records are NOT rolled back.
	WriteBatch(ctx context.Context, batch []*models.Telemetry) error

	// Query returns telemetry records matching the supplied filter.
	// Results are ordered by timestamp descending.
	Query(ctx context.Context, f QueryFilter) ([]*models.Telemetry, error)

	// QueryGPUSummaries returns one summary row per unique GPU UUID present
	// in the store, ordered by hostname then numeric GPU ID.
	QueryGPUSummaries(ctx context.Context) ([]GPUSummary, error)

	// QueryStrandedGPUs returns GPUs whose maximum value of metricName over the
	// window [since, now] is at or below maxUtilPct.  Used by the GreenOps
	// stranded-compute endpoint to surface idle hardware still drawing power.
	QueryStrandedGPUs(ctx context.Context, metricName string, since time.Time, maxUtilPct float64) ([]StrandedGPU, error)

	// DistinctMetrics returns the unique metric names present in the store.
	DistinctMetrics(ctx context.Context) ([]string, error)

	// DistinctGPUs returns the unique GPU UUIDs present in the store.
	DistinctGPUs(ctx context.Context) ([]string, error)

	// Close flushes pending writes and releases resources.
	Close() error
}
