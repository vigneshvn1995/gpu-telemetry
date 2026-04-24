package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"gpu-telemetry/pkg/models"
)

// MemoryStore is an in-memory Store implementation intended for unit tests and
// local development. It is NOT suitable for production use (no persistence,
// unbounded memory growth).
type MemoryStore struct {
	mu      sync.RWMutex
	records []*models.Telemetry
}

// NewMemoryStore returns a ready-to-use MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Write implements Store.
func (m *MemoryStore) Write(_ context.Context, t *models.Telemetry) error {
	cp := *t
	m.mu.Lock()
	m.records = append(m.records, &cp)
	m.mu.Unlock()
	return nil
}

// WriteBatch implements Store.
func (m *MemoryStore) WriteBatch(_ context.Context, batch []*models.Telemetry) error {
	m.mu.Lock()
	for _, t := range batch {
		cp := *t
		m.records = append(m.records, &cp)
	}
	m.mu.Unlock()
	return nil
}

// Query implements Store.
func (m *MemoryStore) Query(_ context.Context, f QueryFilter) ([]*models.Telemetry, error) {
	m.mu.RLock()
	snapshot := make([]*models.Telemetry, len(m.records))
	copy(snapshot, m.records)
	m.mu.RUnlock()

	var out []*models.Telemetry
	for _, t := range snapshot {
		if f.MetricName != "" && t.MetricName != f.MetricName {
			continue
		}
		if f.GPUID != "" && t.GPUID != f.GPUID {
			continue
		}
		if f.Hostname != "" && t.Hostname != f.Hostname {
			continue
		}
		if f.UUID != "" && t.UUID != f.UUID {
			continue
		}
		if !f.From.IsZero() && t.Timestamp.Before(f.From) {
			continue
		}
		if !f.Until.IsZero() && t.Timestamp.After(f.Until) {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}

	// Sort descending by timestamp.
	sort.Slice(out, func(i, j int) bool {
		return out[i].Timestamp.After(out[j].Timestamp)
	})

	if f.Offset > 0 {
		if f.Offset >= len(out) {
			return nil, nil
		}
		out = out[f.Offset:]
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// DistinctMetrics implements Store.
func (m *MemoryStore) DistinctMetrics(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := map[string]struct{}{}
	for _, t := range m.records {
		seen[t.MetricName] = struct{}{}
	}
	return mapKeys(seen), nil
}

// DistinctGPUs implements Store.
func (m *MemoryStore) DistinctGPUs(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := map[string]struct{}{}
	for _, t := range m.records {
		if t.UUID != "" {
			seen[t.UUID] = struct{}{}
		}
	}
	return mapKeys(seen), nil
}

// QueryGPUSummaries implements Store.
func (m *MemoryStore) QueryGPUSummaries(_ context.Context) ([]GPUSummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]GPUSummary)
	for _, t := range m.records {
		if t.UUID == "" {
			continue
		}
		if _, ok := seen[t.UUID]; !ok {
			seen[t.UUID] = GPUSummary{
				UUID:      t.UUID,
				GPUID:     t.GPUID,
				ModelName: t.ModelName,
				Hostname:  t.Hostname,
			}
		}
	}

	out := make([]GPUSummary, 0, len(seen))
	for _, g := range seen {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname != out[j].Hostname {
			return out[i].Hostname < out[j].Hostname
		}
		return out[i].GPUID < out[j].GPUID
	})
	return out, nil
}

// QueryStrandedGPUs implements Store.
func (m *MemoryStore) QueryStrandedGPUs(_ context.Context, metricName string, since time.Time, maxUtilPct float64) ([]StrandedGPU, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type agg struct {
		info        GPUSummary
		peakUtil    float64
		sampleCount int
	}
	seen := make(map[string]*agg)

	for _, t := range m.records {
		if t.MetricName != metricName || t.UUID == "" || t.Timestamp.Before(since) {
			continue
		}
		a, ok := seen[t.UUID]
		if !ok {
			a = &agg{info: GPUSummary{UUID: t.UUID, GPUID: t.GPUID, ModelName: t.ModelName, Hostname: t.Hostname}}
			seen[t.UUID] = a
		}
		if t.Value > a.peakUtil {
			a.peakUtil = t.Value
		}
		a.sampleCount++
	}

	var out []StrandedGPU
	for _, a := range seen {
		if a.peakUtil <= maxUtilPct {
			out = append(out, StrandedGPU{
				UUID:        a.info.UUID,
				GPUID:       a.info.GPUID,
				ModelName:   a.info.ModelName,
				Hostname:    a.info.Hostname,
				PeakUtilPct: a.peakUtil,
				SampleCount: a.sampleCount,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname != out[j].Hostname {
			return out[i].Hostname < out[j].Hostname
		}
		return out[i].GPUID < out[j].GPUID
	})
	return out, nil
}

// Close implements Store (no-op for in-memory).
func (m *MemoryStore) Close() error { return nil }

// Len returns the number of records (useful in tests).
func (m *MemoryStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.records)
}

// Since returns all records with Timestamp after t (useful in tests).
func (m *MemoryStore) Since(t time.Time) []*models.Telemetry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*models.Telemetry
	for _, r := range m.records {
		if r.Timestamp.After(t) {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
