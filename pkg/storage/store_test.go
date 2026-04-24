// Package storage_test provides integration tests for every Store implementation.
//
// The suite is driven by newStores which returns both MemoryStore and SQLiteStore
// (using an in-memory database) so a single set of test cases validates both
// backends identically.
package storage_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/storage"
)

// -------------------------------------------------------------------------
// Fixtures
// -------------------------------------------------------------------------

// t0 is a fixed UTC anchor used to build deterministic timestamps.
var t0 = time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)

// mkTel builds a minimal Telemetry record.  Only fields relevant to storage
// filtering are populated; the rest default to zero values.
func mkTel(uuid, gpuID, hostname, metric string, val float64, ts time.Time) *models.Telemetry {
	return &models.Telemetry{
		Timestamp:  ts,
		MetricName: metric,
		GPUID:      gpuID,
		UUID:       uuid,
		ModelName:  "NVIDIA H100",
		Hostname:   hostname,
		Value:      val,
	}
}

// newStores returns a fresh MemoryStore and a fresh SQLiteStore backed by an
// in-memory database.  Both are registered for cleanup on t.
func newStores(t *testing.T) []struct {
	name  string
	store storage.Store
} {
	t.Helper()

	mem := storage.NewMemoryStore()
	t.Cleanup(func() { _ = mem.Close() })

	lite, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite(:memory:): %v", err)
	}
	t.Cleanup(func() { _ = lite.Close() })

	return []struct {
		name  string
		store storage.Store
	}{
		{"MemoryStore", mem},
		{"SQLiteStore", lite},
	}
}

// -------------------------------------------------------------------------
// Write / Query
// -------------------------------------------------------------------------

func TestStore_Write_AndQuery(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			if err := tc.store.Write(ctx, mkTel("gpu-aa", "0", "host-1", "util", 80.0, t0)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			rows, err := tc.store.Query(ctx, storage.QueryFilter{UUID: "gpu-aa"})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("want 1 row, got %d", len(rows))
			}
			if rows[0].Value != 80.0 {
				t.Errorf("value = %.1f, want 80.0", rows[0].Value)
			}
			if rows[0].UUID != "gpu-aa" {
				t.Errorf("uuid = %q, want gpu-aa", rows[0].UUID)
			}
		})
	}
}

func TestStore_WriteBatch(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			batch := []*models.Telemetry{
				mkTel("gpu-bb", "0", "h", "util", 10, t0),
				mkTel("gpu-bb", "0", "h", "util", 20, t0.Add(time.Second)),
				mkTel("gpu-bb", "0", "h", "util", 30, t0.Add(2*time.Second)),
			}
			if err := tc.store.WriteBatch(ctx, batch); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}

			rows, err := tc.store.Query(ctx, storage.QueryFilter{UUID: "gpu-bb"})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rows) != 3 {
				t.Fatalf("want 3 rows, got %d", len(rows))
			}
		})
	}
}

func TestStore_WriteBatch_Empty(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if err := tc.store.WriteBatch(ctx, nil); err != nil {
				t.Fatalf("WriteBatch(nil): %v", err)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Query filters
// -------------------------------------------------------------------------

func TestStore_QueryFilter_MetricName(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			uuid := "gpu-mn"
			_ = tc.store.Write(ctx, mkTel(uuid, "0", "h", "DCGM_FI_DEV_GPU_UTIL", 50, t0))
			_ = tc.store.Write(ctx, mkTel(uuid, "0", "h", "DCGM_FI_DEV_MEM_COPY_UTIL", 10, t0))

			rows, err := tc.store.Query(ctx, storage.QueryFilter{
				UUID:       uuid,
				MetricName: "DCGM_FI_DEV_GPU_UTIL",
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("want 1, got %d", len(rows))
			}
			if rows[0].MetricName != "DCGM_FI_DEV_GPU_UTIL" {
				t.Errorf("unexpected metric: %s", rows[0].MetricName)
			}
		})
	}
}

func TestStore_QueryFilter_GPUID(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("gpu-gf", "0", "h", "util", 1, t0))
			_ = tc.store.Write(ctx, mkTel("gpu-gf", "1", "h", "util", 2, t0))

			rows, err := tc.store.Query(ctx, storage.QueryFilter{
				UUID:  "gpu-gf",
				GPUID: "0",
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("want 1, got %d", len(rows))
			}
		})
	}
}

func TestStore_QueryFilter_Hostname(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("gpu-hf1", "0", "host-alpha", "util", 1, t0))
			_ = tc.store.Write(ctx, mkTel("gpu-hf2", "0", "host-beta", "util", 2, t0))

			rows, err := tc.store.Query(ctx, storage.QueryFilter{Hostname: "host-alpha"})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			for _, r := range rows {
				if r.Hostname != "host-alpha" {
					t.Errorf("unexpected hostname %q", r.Hostname)
				}
			}
		})
	}
}

func TestStore_QueryFilter_TimeRange(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			uuid := "gpu-tr"
			// Write 5 records spaced 1 minute apart.
			for i := 0; i < 5; i++ {
				_ = tc.store.Write(ctx, mkTel(uuid, "0", "h", "util", float64(i), t0.Add(time.Duration(i)*time.Minute)))
			}

			// Request the middle 3 (t+1m, t+2m, t+3m inclusive).
			rows, err := tc.store.Query(ctx, storage.QueryFilter{
				UUID:  uuid,
				From:  t0.Add(time.Minute),
				Until: t0.Add(3 * time.Minute),
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if len(rows) != 3 {
				t.Fatalf("want 3 rows in range, got %d", len(rows))
			}
		})
	}
}

func TestStore_QueryFilter_LimitAndOffset(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			uuid := "gpu-pg"
			for i := 0; i < 10; i++ {
				_ = tc.store.Write(ctx, mkTel(uuid, "0", "h", "util", float64(i), t0.Add(time.Duration(i)*time.Second)))
			}

			page1, _ := tc.store.Query(ctx, storage.QueryFilter{UUID: uuid, Limit: 4, Offset: 0})
			page2, _ := tc.store.Query(ctx, storage.QueryFilter{UUID: uuid, Limit: 4, Offset: 4})
			all, _ := tc.store.Query(ctx, storage.QueryFilter{UUID: uuid})

			if len(page1) != 4 {
				t.Errorf("page1 len = %d, want 4", len(page1))
			}
			if len(page2) != 4 {
				t.Errorf("page2 len = %d, want 4", len(page2))
			}
			if len(all) != 10 {
				t.Errorf("all len = %d, want 10", len(all))
			}

			// Pages must be disjoint.
			p1vals := map[float64]bool{}
			for _, r := range page1 {
				p1vals[r.Value] = true
			}
			for _, r := range page2 {
				if p1vals[r.Value] {
					t.Errorf("page overlap: value %.0f appears in both pages", r.Value)
				}
			}
		})
	}
}

func TestStore_QueryFilter_OffsetBeyondEnd(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("gpu-ob", "0", "h", "util", 1, t0))

			rows, err := tc.store.Query(ctx, storage.QueryFilter{UUID: "gpu-ob", Offset: 100})
			if err != nil {
				t.Fatalf("Query with large offset: %v", err)
			}
			if len(rows) != 0 {
				t.Errorf("want 0 rows, got %d", len(rows))
			}
		})
	}
}

// -------------------------------------------------------------------------
// Descending order guarantee
// -------------------------------------------------------------------------

func TestStore_Query_DescendingOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			uuid := "gpu-ord"
			// Insert in reverse chronological order to catch storage bugs.
			for i := 4; i >= 0; i-- {
				_ = tc.store.Write(ctx, mkTel(uuid, "0", "h", "util", float64(i), t0.Add(time.Duration(i)*time.Second)))
			}

			rows, err := tc.store.Query(ctx, storage.QueryFilter{UUID: uuid})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			for i := 1; i < len(rows); i++ {
				if rows[i].Timestamp.After(rows[i-1].Timestamp) {
					t.Errorf("rows[%d].Timestamp > rows[%d].Timestamp: not descending", i, i-1)
				}
			}
		})
	}
}

// -------------------------------------------------------------------------
// QueryGPUSummaries
// -------------------------------------------------------------------------

func TestStore_QueryGPUSummaries(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("sum-A", "0", "host-a", "util", 1, t0))
			_ = tc.store.Write(ctx, mkTel("sum-B", "1", "host-a", "util", 2, t0))
			// Duplicate record for sum-A – should still produce one summary.
			_ = tc.store.Write(ctx, mkTel("sum-A", "0", "host-a", "util", 3, t0.Add(time.Minute)))

			summaries, err := tc.store.QueryGPUSummaries(ctx)
			if err != nil {
				t.Fatalf("QueryGPUSummaries: %v", err)
			}

			uuids := map[string]bool{}
			for _, s := range summaries {
				uuids[s.UUID] = true
			}
			if !uuids["sum-A"] || !uuids["sum-B"] {
				t.Errorf("missing UUIDs; got %v", summaries)
			}
		})
	}
}

func TestStore_QueryGPUSummaries_Empty(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			summaries, err := tc.store.QueryGPUSummaries(ctx)
			if err != nil {
				t.Fatalf("QueryGPUSummaries: %v", err)
			}
			if len(summaries) != 0 {
				t.Errorf("want 0, got %d", len(summaries))
			}
		})
	}
}

func TestStore_QueryGPUSummaries_SkipsBlankUUID(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			// Record without a UUID must not appear in summaries.
			rec := mkTel("", "0", "host-z", "util", 1, t0)
			_ = tc.store.Write(ctx, rec)

			summaries, err := tc.store.QueryGPUSummaries(ctx)
			if err != nil {
				t.Fatalf("QueryGPUSummaries: %v", err)
			}
			for _, s := range summaries {
				if s.UUID == "" {
					t.Error("blank UUID appeared in summaries")
				}
			}
		})
	}
}

// -------------------------------------------------------------------------
// QueryStrandedGPUs
// -------------------------------------------------------------------------

func TestStore_QueryStrandedGPUs_IdleFound(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			since := t0.Add(-time.Hour)

			// GPU that is always at 0 % → stranded.
			_ = tc.store.Write(ctx, mkTel("idle-gpu", "2", "host-s", "DCGM_FI_DEV_GPU_UTIL", 0.0, t0))
			_ = tc.store.Write(ctx, mkTel("idle-gpu", "2", "host-s", "DCGM_FI_DEV_GPU_UTIL", 0.0, t0.Add(time.Minute)))

			// GPU that peaks above threshold → not stranded.
			_ = tc.store.Write(ctx, mkTel("busy-gpu", "3", "host-a", "DCGM_FI_DEV_GPU_UTIL", 80.0, t0))

			stranded, err := tc.store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 0.0)
			if err != nil {
				t.Fatalf("QueryStrandedGPUs: %v", err)
			}

			found := false
			for _, g := range stranded {
				if g.UUID == "busy-gpu" {
					t.Error("active GPU incorrectly classified as stranded")
				}
				if g.UUID == "idle-gpu" {
					found = true
					if g.SampleCount < 2 {
						t.Errorf("sample_count = %d, want ≥2", g.SampleCount)
					}
				}
			}
			if !found {
				t.Error("idle GPU not returned as stranded")
			}
		})
	}
}

func TestStore_QueryStrandedGPUs_UtilThreshold(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			since := t0.Add(-time.Hour)

			// GPU with peak util at exactly 10 %.
			_ = tc.store.Write(ctx, mkTel("low-gpu", "0", "host-l", "DCGM_FI_DEV_GPU_UTIL", 5, t0))
			_ = tc.store.Write(ctx, mkTel("low-gpu", "0", "host-l", "DCGM_FI_DEV_GPU_UTIL", 10, t0.Add(time.Minute)))

			// max_util=10 should include it; max_util=9 should exclude it.
			include, err := tc.store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 10.0)
			if err != nil {
				t.Fatalf("QueryStrandedGPUs: %v", err)
			}
			exclude, err := tc.store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 9.0)
			if err != nil {
				t.Fatalf("QueryStrandedGPUs: %v", err)
			}

			foundIn, foundEx := false, false
			for _, g := range include {
				if g.UUID == "low-gpu" {
					foundIn = true
				}
			}
			for _, g := range exclude {
				if g.UUID == "low-gpu" {
					foundEx = true
				}
			}
			if !foundIn {
				t.Error("GPU with peak=10 not returned when max_util=10")
			}
			if foundEx {
				t.Error("GPU with peak=10 incorrectly returned when max_util=9")
			}
		})
	}
}

func TestStore_QueryStrandedGPUs_SinceFilter(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			// Record is older than the since window.
			old := t0.Add(-2 * time.Hour)
			_ = tc.store.Write(ctx, mkTel("old-gpu", "0", "h", "DCGM_FI_DEV_GPU_UTIL", 0, old))

			// Query with since = t0-1h should exclude the record.
			since := t0.Add(-time.Hour)
			stranded, err := tc.store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 0.0)
			if err != nil {
				t.Fatalf("QueryStrandedGPUs: %v", err)
			}
			for _, g := range stranded {
				if g.UUID == "old-gpu" {
					t.Error("stale record outside the since window returned as stranded")
				}
			}
		})
	}
}

// -------------------------------------------------------------------------
// DistinctMetrics / DistinctGPUs
// -------------------------------------------------------------------------

func TestStore_DistinctMetrics(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("gpu-dm", "0", "h", "metric_A", 1, t0))
			_ = tc.store.Write(ctx, mkTel("gpu-dm", "0", "h", "metric_A", 2, t0.Add(time.Second)))
			_ = tc.store.Write(ctx, mkTel("gpu-dm", "0", "h", "metric_B", 3, t0.Add(2*time.Second)))

			metrics, err := tc.store.DistinctMetrics(ctx)
			if err != nil {
				t.Fatalf("DistinctMetrics: %v", err)
			}
			sort.Strings(metrics)

			want := []string{"metric_A", "metric_B"}
			found := map[string]bool{}
			for _, m := range metrics {
				found[m] = true
			}
			for _, w := range want {
				if !found[w] {
					t.Errorf("metric %q missing; got %v", w, metrics)
				}
			}
		})
	}
}

func TestStore_DistinctGPUs(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_ = tc.store.Write(ctx, mkTel("uuid-X", "0", "h", "m", 1, t0))
			_ = tc.store.Write(ctx, mkTel("uuid-Y", "1", "h", "m", 2, t0))
			// Blank UUID must not appear.
			_ = tc.store.Write(ctx, mkTel("", "2", "h", "m", 3, t0))

			gpus, err := tc.store.DistinctGPUs(ctx)
			if err != nil {
				t.Fatalf("DistinctGPUs: %v", err)
			}
			found := map[string]bool{}
			for _, g := range gpus {
				found[g] = true
			}
			if !found["uuid-X"] || !found["uuid-Y"] {
				t.Errorf("expected uuid-X and uuid-Y; got %v", gpus)
			}
			if found[""] {
				t.Error("blank UUID should not appear in DistinctGPUs")
			}
		})
	}
}

// -------------------------------------------------------------------------
// Concurrent write safety
// -------------------------------------------------------------------------

func TestStore_ConcurrentWrites(t *testing.T) {
	t.Parallel()
	for _, tc := range newStores(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Not marked t.Parallel() because SQLite uses a single write connection.
			ctx := context.Background()
			const goroutines = 10
			const msgsPerGoroutine = 20

			var wg sync.WaitGroup
			for g := 0; g < goroutines; g++ {
				g := g
				wg.Add(1)
				go func() {
					defer wg.Done()
					for m := 0; m < msgsPerGoroutine; m++ {
						_ = tc.store.Write(ctx, mkTel(
							"gpu-concurrent",
							"0",
							"h",
							"util",
							float64(g*msgsPerGoroutine+m),
							t0.Add(time.Duration(g*msgsPerGoroutine+m)*time.Millisecond),
						))
					}
				}()
			}
			wg.Wait()

			rows, err := tc.store.Query(ctx, storage.QueryFilter{UUID: "gpu-concurrent"})
			if err != nil {
				t.Fatalf("Query after concurrent writes: %v", err)
			}
			if len(rows) != goroutines*msgsPerGoroutine {
				t.Errorf("want %d rows, got %d", goroutines*msgsPerGoroutine, len(rows))
			}
		})
	}
}

// -------------------------------------------------------------------------
// MemoryStore-specific helpers
// -------------------------------------------------------------------------

func TestMemoryStore_Len(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()

	ctx := context.Background()
	_ = store.Write(ctx, mkTel("u", "0", "h", "m", 1, t0))
	_ = store.Write(ctx, mkTel("u", "0", "h", "m", 2, t0.Add(time.Second)))

	if store.Len() != 2 {
		t.Errorf("Len = %d, want 2", store.Len())
	}
}
