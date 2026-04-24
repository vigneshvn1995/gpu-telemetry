// Package storage_test – MemoryStore-specific tests.
//
// store_test.go runs a shared suite against every Store implementation.
// This file adds tests that are specific to MemoryStore behaviours that are
// not part of the Store interface (Since, Len) and branches inside the
// in-memory filtering logic that the shared suite cannot easily exercise in
// isolation.
package storage_test

import (
	"context"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/storage"
)

// -------------------------------------------------------------------------
// Since
// -------------------------------------------------------------------------

func TestMemoryStore_Since_ReturnsAfterCutoff(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 1, base))
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 2, base.Add(time.Minute)))
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 3, base.Add(2*time.Minute)))

	// Ask for records strictly after base+30s: only the +1m and +2m records qualify.
	got := store.Since(base.Add(30 * time.Second))
	if len(got) != 2 {
		t.Fatalf("Since: got %d records, want 2", len(got))
	}
	for _, r := range got {
		if !r.Timestamp.After(base.Add(30 * time.Second)) {
			t.Errorf("record with ts %v is not after cutoff", r.Timestamp)
		}
	}
}

func TestMemoryStore_Since_EmptyStore(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()

	got := store.Since(time.Time{})
	if len(got) != 0 {
		t.Fatalf("Since on empty store: want 0, got %d", len(got))
	}
}

func TestMemoryStore_Since_NoneQualify(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 1, base))

	// Ask for records after base+1h; none qualify.
	got := store.Since(base.Add(time.Hour))
	if len(got) != 0 {
		t.Fatalf("want 0, got %d", len(got))
	}
}

// TestMemoryStore_Since_ReturnsCopy verifies that modifying the returned
// records does not affect what is stored.
func TestMemoryStore_Since_ReturnsCopy(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("copy-test", "0", "h", "m", 99, base.Add(time.Minute)))

	got := store.Since(base)
	if len(got) == 0 {
		t.Fatal("expected one record")
	}
	got[0].Value = 0 // mutate the returned slice

	// The stored record must be unchanged.
	all := store.Since(time.Time{})
	if all[0].Value != 99 {
		t.Errorf("stored value was mutated: got %.0f, want 99", all[0].Value)
	}
}

// -------------------------------------------------------------------------
// Query edge cases (MemoryStore-specific)
// -------------------------------------------------------------------------

// TestMemoryStore_Query_UUIDFilterMiss verifies that records whose UUID does
// not match the filter are excluded, covering the filter-miss branch.
func TestMemoryStore_Query_UUIDFilterMiss(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("aaa", "0", "h", "m", 1, base))
	_ = store.Write(ctx, mkTel("bbb", "0", "h", "m", 2, base))

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "aaa"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
	if rows[0].UUID != "aaa" {
		t.Errorf("UUID = %q, want aaa", rows[0].UUID)
	}
}

// TestMemoryStore_Query_HostnameFilterMiss covers the hostname-filter branch.
func TestMemoryStore_Query_HostnameFilterMiss(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "host-match", "m", 1, base))
	_ = store.Write(ctx, mkTel("u2", "0", "host-other", "m", 2, base))

	rows, err := store.Query(ctx, storage.QueryFilter{Hostname: "host-match"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0].Hostname != "host-match" {
		t.Errorf("wrong rows returned: %v", rows)
	}
}

// TestMemoryStore_Query_GPUIDFilterMiss covers the GPUID-filter branch.
func TestMemoryStore_Query_GPUIDFilterMiss(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 1, base))
	_ = store.Write(ctx, mkTel("u1", "1", "h", "m", 2, base))

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "u1", GPUID: "1"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0].GPUID != "1" {
		t.Errorf("wrong row; got %v", rows)
	}
}

// TestMemoryStore_Query_MetricFilterMiss covers the metric-name filter branch.
func TestMemoryStore_Query_MetricFilterMiss(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "wanted", 1, base))
	_ = store.Write(ctx, mkTel("u1", "0", "h", "unwanted", 2, base))

	rows, err := store.Query(ctx, storage.QueryFilter{MetricName: "wanted"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0].MetricName != "wanted" {
		t.Errorf("wrong rows: %v", rows)
	}
}

// TestMemoryStore_Query_FromFilter covers the From-filter branch.
func TestMemoryStore_Query_FromFilter(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 1, base))
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 2, base.Add(2*time.Minute)))

	// From = base+1m → only the +2m record qualifies.
	rows, err := store.Query(ctx, storage.QueryFilter{From: base.Add(time.Minute)})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
	if rows[0].Value != 2 {
		t.Errorf("wrong record returned")
	}
}

// TestMemoryStore_Query_UntilFilter covers the Until-filter branch where a
// record after Until is rejected.
func TestMemoryStore_Query_UntilFilter(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 1, base))
	_ = store.Write(ctx, mkTel("u1", "0", "h", "m", 2, base.Add(2*time.Minute)))

	// Until = base+1m → only the base record qualifies.
	rows, err := store.Query(ctx, storage.QueryFilter{Until: base.Add(time.Minute)})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1, got %d", len(rows))
	}
	if rows[0].Value != 1 {
		t.Errorf("wrong record returned")
	}
}

// TestMemoryStore_Query_OnlyOffset exercises the offset-without-limit path.
func TestMemoryStore_Query_OnlyOffset(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_ = store.Write(ctx, mkTel("u1", "0", "h", "m", float64(i), base.Add(time.Duration(i)*time.Second)))
	}

	// Offset 2 with no limit → returns records 2, 3, 4 (3 records).
	rows, err := store.Query(ctx, storage.QueryFilter{Offset: 2})
	if err != nil {
		t.Fatalf("Query with offset only: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("want 3, got %d", len(rows))
	}
}

// -------------------------------------------------------------------------
// Write / WriteBatch copy safety
// -------------------------------------------------------------------------

// TestMemoryStore_Write_StoresCopy verifies that mutating the original after
// Write does not affect the persisted record.
func TestMemoryStore_Write_StoresCopy(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	orig := mkTel("copy", "0", "h", "m", 42, base)
	_ = store.Write(ctx, orig)

	orig.Value = 999 // mutate original

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "copy"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if rows[0].Value != 42 {
		t.Errorf("stored value was mutated: got %.0f, want 42", rows[0].Value)
	}
}

// TestMemoryStore_WriteBatch_StoresCopies verifies mutation-safety for batch writes.
func TestMemoryStore_WriteBatch_StoresCopies(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rec := mkTel("batch-copy", "0", "h", "m", 7, base)
	_ = store.WriteBatch(ctx, []*models.Telemetry{rec})

	rec.Value = 999

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "batch-copy"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if rows[0].Value != 7 {
		t.Errorf("stored value was mutated: got %.0f, want 7", rows[0].Value)
	}
}

// -------------------------------------------------------------------------
// QueryGPUSummaries – MemoryStore-specific branches
// -------------------------------------------------------------------------

// TestMemoryStore_QueryGPUSummaries_SortOrder verifies hostname-then-GPUID
// lexicographic ordering.
func TestMemoryStore_QueryGPUSummaries_SortOrder(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Insert GPUs in deliberately wrong order.
	_ = store.Write(ctx, mkTel("zz", "2", "host-b", "m", 1, base))
	_ = store.Write(ctx, mkTel("aa", "0", "host-a", "m", 1, base))
	_ = store.Write(ctx, mkTel("bb", "1", "host-a", "m", 1, base))

	summaries, err := store.QueryGPUSummaries(ctx)
	if err != nil {
		t.Fatalf("QueryGPUSummaries: %v", err)
	}
	if len(summaries) != 3 {
		t.Fatalf("want 3, got %d", len(summaries))
	}
	// host-a/GPU-0, host-a/GPU-1, host-b/GPU-2
	if summaries[0].Hostname != "host-a" || summaries[0].GPUID != "0" {
		t.Errorf("wrong first entry: %+v", summaries[0])
	}
	if summaries[1].Hostname != "host-a" || summaries[1].GPUID != "1" {
		t.Errorf("wrong second entry: %+v", summaries[1])
	}
	if summaries[2].Hostname != "host-b" {
		t.Errorf("wrong third entry: %+v", summaries[2])
	}
}

// TestMemoryStore_QueryGPUSummaries_DeduplicatesUUID verifies that multiple
// records for the same UUID produce exactly one summary row.
func TestMemoryStore_QueryGPUSummaries_DeduplicatesUUID(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_ = store.Write(ctx, mkTel("dup-uuid", "0", "h", "m", float64(i), base.Add(time.Duration(i)*time.Second)))
	}

	summaries, err := store.QueryGPUSummaries(ctx)
	if err != nil {
		t.Fatalf("QueryGPUSummaries: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("want 1 summary for dup UUID, got %d", len(summaries))
	}
}

// -------------------------------------------------------------------------
// QueryStrandedGPUs – MemoryStore-specific branches
// -------------------------------------------------------------------------

// TestMemoryStore_QueryStrandedGPUs_BlankUUIDIgnored verifies that records
// with an empty UUID are skipped (the `t.UUID == ""` branch).
func TestMemoryStore_QueryStrandedGPUs_BlankUUIDIgnored(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(-time.Hour)

	// Write a record with blank UUID at 0% util – should NOT appear as stranded.
	_ = store.Write(ctx, &models.Telemetry{
		Timestamp:  base,
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		UUID:       "", // blank
		GPUID:      "0",
		Hostname:   "h",
		Value:      0,
	})

	stranded, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	for _, g := range stranded {
		if g.UUID == "" {
			t.Error("blank-UUID record appeared as stranded GPU")
		}
	}
}

// TestMemoryStore_QueryStrandedGPUs_WrongMetricIgnored verifies that records
// for a different metric name are skipped.
func TestMemoryStore_QueryStrandedGPUs_WrongMetricIgnored(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(-time.Hour)

	// Write with the WRONG metric name.
	_ = store.Write(ctx, mkTel("some-gpu", "0", "h", "WRONG_METRIC", 0, base))

	stranded, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	for _, g := range stranded {
		if g.UUID == "some-gpu" {
			t.Error("record with wrong metric appeared as stranded")
		}
	}
}

// TestMemoryStore_QueryStrandedGPUs_PeakUpdateAcrossSamples verifies that the
// second (and subsequent) samples for the same UUID update the peakUtil field
// (the `if t.Value > a.peakUtil` branch for an existing entry).
func TestMemoryStore_QueryStrandedGPUs_PeakUpdateAcrossSamples(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(-time.Hour)

	// Write two samples: first at 1%, second at 5%.
	_ = store.Write(ctx, mkTel("peak-gpu", "0", "h", "DCGM_FI_DEV_GPU_UTIL", 1, base))
	_ = store.Write(ctx, mkTel("peak-gpu", "0", "h", "DCGM_FI_DEV_GPU_UTIL", 5, base.Add(time.Minute)))

	// maxUtilPct=5 → should be included (peak=5 ≤ 5).
	include, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	found := false
	for _, g := range include {
		if g.UUID == "peak-gpu" {
			found = true
			if g.PeakUtilPct != 5 {
				t.Errorf("PeakUtilPct = %v, want 5", g.PeakUtilPct)
			}
			if g.SampleCount != 2 {
				t.Errorf("SampleCount = %d, want 2", g.SampleCount)
			}
		}
	}
	if !found {
		t.Error("peak-gpu not found in stranded result")
	}

	// maxUtilPct=4 → should be excluded (peak=5 > 4).
	exclude, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 4.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	for _, g := range exclude {
		if g.UUID == "peak-gpu" {
			t.Error("peak-gpu incorrectly included when peak>maxUtil")
		}
	}
}

// TestMemoryStore_QueryStrandedGPUs_SortOrder verifies hostname-then-GPUID
// ordering on the stranded results.
func TestMemoryStore_QueryStrandedGPUs_SortOrder(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(-time.Hour)

	_ = store.Write(ctx, mkTel("z-gpu", "9", "host-b", "DCGM_FI_DEV_GPU_UTIL", 0, base))
	_ = store.Write(ctx, mkTel("a-gpu", "0", "host-a", "DCGM_FI_DEV_GPU_UTIL", 0, base))

	stranded, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	if len(stranded) != 2 {
		t.Fatalf("want 2, got %d", len(stranded))
	}
	if stranded[0].Hostname != "host-a" {
		t.Errorf("first stranded entry should be host-a, got %q", stranded[0].Hostname)
	}
}

// TestMemoryStore_DistinctGPUs_SkipsBlankUUID verifies the blank-UUID guard
// in DistinctGPUs.
func TestMemoryStore_DistinctGPUs_SkipsBlankUUID(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStore()
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, &models.Telemetry{Timestamp: base, MetricName: "m", UUID: "", GPUID: "0", Hostname: "h", Value: 1})
	_ = store.Write(ctx, mkTel("real-uuid", "0", "h", "m", 2, base))

	gpus, err := store.DistinctGPUs(ctx)
	if err != nil {
		t.Fatalf("DistinctGPUs: %v", err)
	}
	for _, g := range gpus {
		if g == "" {
			t.Error("blank UUID must not appear in DistinctGPUs result")
		}
	}
	found := false
	for _, g := range gpus {
		if g == "real-uuid" {
			found = true
		}
	}
	if !found {
		t.Error("real-uuid missing from DistinctGPUs result")
	}
}
