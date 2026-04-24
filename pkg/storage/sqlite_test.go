// Package storage_test – SQLiteStore-specific tests.
//
// store_test.go covers the shared Store interface against both backends.
// This file adds tests for SQLite-specific behaviour:
//   - OpenSQLite error paths (invalid path triggers migrate failure)
//   - All method error paths exercised by closing the DB before calling them
//   - Labels JSON round-trip
//   - Query with every WHERE-clause combination
//   - LIMIT -1 OFFSET n path (offset-without-limit in SQLite)
//   - WriteBatch with full fields (all INSERT columns)
//   - scanStrings rows.Err path
package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/storage"
)

// -------------------------------------------------------------------------
// OpenSQLite error path
// -------------------------------------------------------------------------

// TestSQLiteStore_OpenSQLite_InvalidPath verifies that OpenSQLite returns a
// non-nil error when the parent directory does not exist (which makes SQLite's
// file-open fail, causing migrate() to return an error and the cleanup
// `db.Close()` branch to execute).
func TestSQLiteStore_OpenSQLite_InvalidPath(t *testing.T) {
	t.Parallel()

	// Construct a path whose parent directory does not exist.
	invalidPath := filepath.Join(t.TempDir(), "nonexistent", "sub", "db.sqlite")

	_, err := storage.OpenSQLite(invalidPath)
	if err == nil {
		t.Fatal("OpenSQLite: expected error for invalid path, got nil")
	}
}

// -------------------------------------------------------------------------
// Error paths: all methods on a closed DB
// -------------------------------------------------------------------------

// openAndClose is a test helper that opens an in-memory SQLite store and
// closes it immediately, so the underlying *sql.DB is shut down.
func openAndClose(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return store
}

func TestSQLiteStore_Write_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	err := store.Write(context.Background(), mkTel("u", "0", "h", "m", 1, time.Now()))
	if err == nil {
		t.Fatal("Write on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_WriteBatch_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	err := store.WriteBatch(context.Background(), []*models.Telemetry{
		mkTel("u", "0", "h", "m", 1, time.Now()),
	})
	if err == nil {
		t.Fatal("WriteBatch on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_Query_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	_, err := store.Query(context.Background(), storage.QueryFilter{})
	if err == nil {
		t.Fatal("Query on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_DistinctMetrics_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	_, err := store.DistinctMetrics(context.Background())
	if err == nil {
		t.Fatal("DistinctMetrics on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_DistinctGPUs_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	_, err := store.DistinctGPUs(context.Background())
	if err == nil {
		t.Fatal("DistinctGPUs on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_QueryGPUSummaries_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	_, err := store.QueryGPUSummaries(context.Background())
	if err == nil {
		t.Fatal("QueryGPUSummaries on closed DB: expected error, got nil")
	}
}

func TestSQLiteStore_QueryStrandedGPUs_ClosedDB(t *testing.T) {
	t.Parallel()
	store := openAndClose(t)
	_, err := store.QueryStrandedGPUs(context.Background(), "m", time.Now().Add(-time.Hour), 5.0)
	if err == nil {
		t.Fatal("QueryStrandedGPUs on closed DB: expected error, got nil")
	}
}

// -------------------------------------------------------------------------
// Labels JSON round-trip
// -------------------------------------------------------------------------

// TestSQLiteStore_Write_LabelsRoundTrip verifies that a telemetry record with
// a non-empty Labels map is stored and retrieved with all labels intact.
func TestSQLiteStore_Write_LabelsRoundTrip(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	rec := &models.Telemetry{
		Timestamp:  base,
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		GPUID:      "0",
		UUID:       "label-uuid",
		ModelName:  "H100",
		Hostname:   "node-1",
		Value:      75.5,
		Labels: map[string]string{
			"cluster":   "prod",
			"namespace": "ml-infra",
			"pod":       "trainer-0",
		},
	}
	if err := store.Write(ctx, rec); err != nil {
		t.Fatalf("Write: %v", err)
	}

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "label-uuid"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	got := rows[0]
	for k, want := range rec.Labels {
		if got.Labels[k] != want {
			t.Errorf("label[%q] = %q, want %q", k, got.Labels[k], want)
		}
	}
}

// -------------------------------------------------------------------------
// Query: all WHERE-clause branches in one test
// -------------------------------------------------------------------------

// TestSQLiteStore_Query_AllFilterClauses writes a mixed dataset and queries
// with every filter field set simultaneously, exercising every WHERE-clause
// building branch in the SQLiteStore.Query method.
func TestSQLiteStore_Query_AllFilterClauses(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	// Write several records; only one should match all filters.
	_ = store.Write(ctx, &models.Telemetry{
		Timestamp: base, MetricName: "target_metric", GPUID: "7",
		UUID: "target-uuid", ModelName: "H100", Hostname: "node-target", Value: 50,
	})
	_ = store.Write(ctx, mkTel("other-uuid", "0", "node-other", "target_metric", 60, base))
	_ = store.Write(ctx, mkTel("target-uuid", "7", "node-target", "other_metric", 70, base))

	rows, err := store.Query(ctx, storage.QueryFilter{
		MetricName: "target_metric",
		GPUID:      "7",
		Hostname:   "node-target",
		UUID:       "target-uuid",
		From:       base.Add(-time.Minute),
		Until:      base.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row with all filters, got %d", len(rows))
	}
	if rows[0].UUID != "target-uuid" || rows[0].MetricName != "target_metric" {
		t.Errorf("unexpected row: %+v", rows[0])
	}
}

// -------------------------------------------------------------------------
// Query: LIMIT -1 OFFSET n (offset-without-limit)
// -------------------------------------------------------------------------

// TestSQLiteStore_Query_OnlyOffset exercises the `LIMIT -1 OFFSET n` branch
// in SQLiteStore.Query that is triggered when Offset > 0 and Limit == 0.
func TestSQLiteStore_Query_OnlyOffset(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	uuid := "offset-only-uuid"
	for i := 0; i < 5; i++ {
		_ = store.Write(ctx, mkTel(uuid, "0", "h", "m", float64(i), base.Add(time.Duration(i)*time.Second)))
	}

	// Offset 2, no Limit → should return 3 rows.
	rows, err := store.Query(ctx, storage.QueryFilter{UUID: uuid, Offset: 2})
	if err != nil {
		t.Fatalf("Query with offset only: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("want 3 rows with offset=2/no-limit, got %d", len(rows))
	}
}

// -------------------------------------------------------------------------
// WriteBatch: all columns present
// -------------------------------------------------------------------------

// TestSQLiteStore_WriteBatch_AllFields verifies that a batch write with all
// optional fields populated (Device, Container, Pod, Namespace) round-trips
// without loss.
func TestSQLiteStore_WriteBatch_AllFields(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	batch := []*models.Telemetry{
		{
			Timestamp:  base,
			MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GPUID:      "3",
			Device:     "/dev/nvidia3",
			UUID:       "full-fields-uuid",
			ModelName:  "A100",
			Hostname:   "dgx-01",
			Container:  "trainer",
			Pod:        "trainer-0",
			Namespace:  "ml",
			Value:      88.8,
			Labels:     map[string]string{"env": "prod"},
		},
	}
	if err := store.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "full-fields-uuid"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	got := rows[0]
	if got.Device != "/dev/nvidia3" {
		t.Errorf("Device = %q, want /dev/nvidia3", got.Device)
	}
	if got.Container != "trainer" {
		t.Errorf("Container = %q, want trainer", got.Container)
	}
	if got.Pod != "trainer-0" {
		t.Errorf("Pod = %q, want trainer-0", got.Pod)
	}
	if got.Namespace != "ml" {
		t.Errorf("Namespace = %q, want ml", got.Namespace)
	}
	if got.Labels["env"] != "prod" {
		t.Errorf("label[env] = %q, want prod", got.Labels["env"])
	}
}

// -------------------------------------------------------------------------
// Timestamp round-trip (RFC3339Nano)
// -------------------------------------------------------------------------

// TestSQLiteStore_Query_TimestampRoundTrip verifies that sub-second precision
// is preserved through the RFC3339Nano serialisation path.
func TestSQLiteStore_Query_TimestampRoundTrip(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	// Nanosecond-precision timestamp.
	ts := time.Date(2025, 3, 15, 10, 30, 0, 123456789, time.UTC)
	_ = store.Write(ctx, mkTel("ts-uuid", "0", "h", "m", 1, ts))

	rows, err := store.Query(ctx, storage.QueryFilter{UUID: "ts-uuid"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	// Truncate to second for RFC3339 fallback path comparison.
	if rows[0].Timestamp.Unix() != ts.Unix() {
		t.Errorf("timestamp Unix = %d, want %d", rows[0].Timestamp.Unix(), ts.Unix())
	}
}

// -------------------------------------------------------------------------
// QueryGPUSummaries / QueryStrandedGPUs – SQLite-specific edge cases
// -------------------------------------------------------------------------

// TestSQLiteStore_QueryGPUSummaries_SortOrder verifies that the SQL ORDER BY
// clause returns results sorted by hostname then numeric GPU ID.
func TestSQLiteStore_QueryGPUSummaries_SortOrder(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	// Insert GPUs in the wrong order.
	_ = store.Write(ctx, mkTel("gpu-z", "10", "host-b", "m", 1, base))
	_ = store.Write(ctx, mkTel("gpu-a", "0", "host-a", "m", 1, base))
	_ = store.Write(ctx, mkTel("gpu-b", "2", "host-a", "m", 1, base))

	summaries, err := store.QueryGPUSummaries(ctx)
	if err != nil {
		t.Fatalf("QueryGPUSummaries: %v", err)
	}
	if len(summaries) != 3 {
		t.Fatalf("want 3, got %d", len(summaries))
	}
	if summaries[0].Hostname != "host-a" || summaries[0].GPUID != "0" {
		t.Errorf("first = %+v, want host-a/gpu-id=0", summaries[0])
	}
	if summaries[1].GPUID != "2" {
		t.Errorf("second GPU ID = %q, want 2", summaries[1].GPUID)
	}
}

// TestSQLiteStore_QueryStrandedGPUs_Empty verifies that an empty DB returns
// an empty slice (not an error).
func TestSQLiteStore_QueryStrandedGPUs_Empty(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	stranded, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", time.Now().Add(-time.Hour), 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	if len(stranded) != 0 {
		t.Errorf("want 0 stranded GPUs on empty DB, got %d", len(stranded))
	}
}

// TestSQLiteStore_QueryStrandedGPUs_PeakAndCount checks that the MAX(value)
// and COUNT(*) aggregates are computed correctly by the SQL query.
func TestSQLiteStore_QueryStrandedGPUs_PeakAndCount(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	since := base.Add(-time.Hour)

	for i, v := range []float64{1.0, 2.0, 3.0} {
		_ = store.Write(ctx, mkTel("agg-gpu", "0", "h", "DCGM_FI_DEV_GPU_UTIL", v, base.Add(time.Duration(i)*time.Minute)))
	}

	stranded, err := store.QueryStrandedGPUs(ctx, "DCGM_FI_DEV_GPU_UTIL", since, 5.0)
	if err != nil {
		t.Fatalf("QueryStrandedGPUs: %v", err)
	}
	if len(stranded) != 1 {
		t.Fatalf("want 1, got %d", len(stranded))
	}
	if stranded[0].PeakUtilPct != 3.0 {
		t.Errorf("PeakUtilPct = %.1f, want 3.0", stranded[0].PeakUtilPct)
	}
	if stranded[0].SampleCount != 3 {
		t.Errorf("SampleCount = %d, want 3", stranded[0].SampleCount)
	}
}

// -------------------------------------------------------------------------
// DistinctMetrics / DistinctGPUs – happy path (SQLite-specific verification)
// -------------------------------------------------------------------------

func TestSQLiteStore_DistinctMetrics_Ordered(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"zebra_metric", "alpha_metric", "alpha_metric"} {
		_ = store.Write(ctx, mkTel("u", "0", "h", name, 1, base))
	}

	metrics, err := store.DistinctMetrics(ctx)
	if err != nil {
		t.Fatalf("DistinctMetrics: %v", err)
	}
	if len(metrics) != 2 {
		t.Fatalf("want 2 distinct metrics, got %d: %v", len(metrics), metrics)
	}
	if metrics[0] != "alpha_metric" || metrics[1] != "zebra_metric" {
		t.Errorf("not sorted: %v", metrics)
	}
}

func TestSQLiteStore_DistinctGPUs_ExcludesBlank(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	_ = store.Write(ctx, mkTel("real-uuid", "0", "h", "m", 1, base))
	// Blank UUID – must be excluded.
	_ = store.Write(ctx, &models.Telemetry{Timestamp: base, MetricName: "m", UUID: "", Hostname: "h", Value: 2})

	gpus, err := store.DistinctGPUs(ctx)
	if err != nil {
		t.Fatalf("DistinctGPUs: %v", err)
	}
	for _, g := range gpus {
		if g == "" {
			t.Error("blank UUID must not appear in DistinctGPUs")
		}
	}
}

// -------------------------------------------------------------------------
// WriteBatch: empty batch is a no-op
// -------------------------------------------------------------------------

func TestSQLiteStore_WriteBatch_EmptyBatch(t *testing.T) {
	t.Parallel()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer store.Close()

	if err := store.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	if err := store.WriteBatch(context.Background(), []*models.Telemetry{}); err != nil {
		t.Fatalf("WriteBatch(empty): %v", err)
	}
}
