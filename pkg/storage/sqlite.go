package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gpu-telemetry/pkg/models"

	// Pure-Go SQLite driver; no CGO required.
	_ "modernc.org/sqlite"
)

// SQLiteStore is a Store implementation backed by SQLite via database/sql.
// It uses WAL mode and a write-ahead log for good concurrent read performance.
//
// Schema is created automatically on Open if the database file is new.
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLite opens (or creates) a SQLite database at the given path.
// Use ":memory:" for an ephemeral in-process database (useful in tests).
func OpenSQLite(path string) (*SQLiteStore, error) {
	// Enable WAL for better write throughput and concurrent reads.
	dsn := path + "?_journal=WAL&_timeout=5000&_foreign_keys=on"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite %q: %w", path, err)
	}

	// One write connection is sufficient; SQLite is not concurrent-write friendly.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	s := &SQLiteStore{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// migrate creates the schema if it does not already exist.
func (s *SQLiteStore) migrate() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS telemetry (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT    NOT NULL,
    metric_name TEXT    NOT NULL,
    gpu_id      TEXT    NOT NULL DEFAULT '',
    device      TEXT    NOT NULL DEFAULT '',
    uuid        TEXT    NOT NULL DEFAULT '',
    model_name  TEXT    NOT NULL DEFAULT '',
    hostname    TEXT    NOT NULL,
    container   TEXT    NOT NULL DEFAULT '',
    pod         TEXT    NOT NULL DEFAULT '',
    namespace   TEXT    NOT NULL DEFAULT '',
    value       REAL    NOT NULL,
    labels      TEXT    NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp   ON telemetry(timestamp);
CREATE INDEX IF NOT EXISTS idx_telemetry_metric_name ON telemetry(metric_name);
CREATE INDEX IF NOT EXISTS idx_telemetry_uuid        ON telemetry(uuid);
CREATE INDEX IF NOT EXISTS idx_telemetry_hostname    ON telemetry(hostname);
CREATE INDEX IF NOT EXISTS idx_telemetry_gpu_id      ON telemetry(gpu_id);
`
	_, err := s.db.Exec(ddl)
	return err
}

// Write implements Store.
func (s *SQLiteStore) Write(ctx context.Context, t *models.Telemetry) error {
	labels, err := json.Marshal(t.Labels)
	if err != nil {
		labels = []byte("{}")
	}

	const q = `
INSERT INTO telemetry
    (timestamp, metric_name, gpu_id, device, uuid, model_name, hostname,
     container, pod, namespace, value, labels)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, q,
		t.Timestamp.UTC().Format(time.RFC3339Nano),
		t.MetricName,
		t.GPUID,
		t.Device,
		t.UUID,
		t.ModelName,
		t.Hostname,
		t.Container,
		t.Pod,
		t.Namespace,
		t.Value,
		string(labels),
	)
	if err != nil {
		return fmt.Errorf("storage: write telemetry: %w", err)
	}
	return nil
}

// WriteBatch implements Store using a single transaction for atomicity and
// significantly better throughput than individual inserts.
func (s *SQLiteStore) WriteBatch(ctx context.Context, batch []*models.Telemetry) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const q = `
INSERT INTO telemetry
    (timestamp, metric_name, gpu_id, device, uuid, model_name, hostname,
     container, pod, namespace, value, labels)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("storage: prepare batch insert: %w", err)
	}
	defer stmt.Close()

	for _, t := range batch {
		labels, jerr := json.Marshal(t.Labels)
		if jerr != nil {
			labels = []byte("{}")
		}
		if _, err = stmt.ExecContext(ctx,
			t.Timestamp.UTC().Format(time.RFC3339Nano),
			t.MetricName,
			t.GPUID,
			t.Device,
			t.UUID,
			t.ModelName,
			t.Hostname,
			t.Container,
			t.Pod,
			t.Namespace,
			t.Value,
			string(labels),
		); err != nil {
			return fmt.Errorf("storage: batch insert row: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit batch: %w", err)
	}
	return nil
}

// Query implements Store.
func (s *SQLiteStore) Query(ctx context.Context, f QueryFilter) ([]*models.Telemetry, error) {
	var args []any
	var clauses []string

	if f.MetricName != "" {
		clauses = append(clauses, "metric_name = ?")
		args = append(args, f.MetricName)
	}
	if f.GPUID != "" {
		clauses = append(clauses, "gpu_id = ?")
		args = append(args, f.GPUID)
	}
	if f.Hostname != "" {
		clauses = append(clauses, "hostname = ?")
		args = append(args, f.Hostname)
	}
	if f.UUID != "" {
		clauses = append(clauses, "uuid = ?")
		args = append(args, f.UUID)
	}
	if !f.From.IsZero() {
		clauses = append(clauses, "timestamp >= ?")
		args = append(args, f.From.UTC().Format(time.RFC3339Nano))
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "timestamp <= ?")
		args = append(args, f.Until.UTC().Format(time.RFC3339Nano))
	}

	q := `SELECT timestamp, metric_name, gpu_id, device, uuid, model_name,
                 hostname, container, pod, namespace, value, labels
          FROM telemetry`

	if len(clauses) > 0 {
		q += " WHERE " + strings.Join(clauses, " AND ")
	}
	q += " ORDER BY timestamp DESC"

	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
		if f.Offset > 0 {
			q += fmt.Sprintf(" OFFSET %d", f.Offset)
		}
	} else if f.Offset > 0 {
		// SQLite requires LIMIT when OFFSET is present. Use -1 for "no cap".
		q += fmt.Sprintf(" LIMIT -1 OFFSET %d", f.Offset)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("storage: query: %w", err)
	}
	defer rows.Close()

	var results []*models.Telemetry
	for rows.Next() {
		var t models.Telemetry
		var tsStr, labelsStr string

		if err := rows.Scan(
			&tsStr, &t.MetricName, &t.GPUID, &t.Device, &t.UUID,
			&t.ModelName, &t.Hostname, &t.Container, &t.Pod, &t.Namespace,
			&t.Value, &labelsStr,
		); err != nil {
			return nil, fmt.Errorf("storage: scan row: %w", err)
		}

		t.Timestamp, err = time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			t.Timestamp, _ = time.Parse(time.RFC3339, tsStr)
		}
		_ = json.Unmarshal([]byte(labelsStr), &t.Labels)
		results = append(results, &t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage: rows error: %w", err)
	}
	return results, nil
}

// DistinctMetrics implements Store.
func (s *SQLiteStore) DistinctMetrics(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT DISTINCT metric_name FROM telemetry ORDER BY metric_name")
	if err != nil {
		return nil, fmt.Errorf("storage: distinct metrics: %w", err)
	}
	defer rows.Close()
	return scanStrings(rows)
}

// DistinctGPUs implements Store.
func (s *SQLiteStore) DistinctGPUs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT DISTINCT uuid FROM telemetry WHERE uuid != '' ORDER BY uuid")
	if err != nil {
		return nil, fmt.Errorf("storage: distinct gpus: %w", err)
	}
	defer rows.Close()
	return scanStrings(rows)
}

// QueryGPUSummaries implements Store.
// Returns one row per unique UUID, picking MIN(model_name) and MIN(hostname)
// for determinism when a GPU has appeared across multiple hostnames.
func (s *SQLiteStore) QueryGPUSummaries(ctx context.Context) ([]GPUSummary, error) {
	const q = `
		SELECT uuid, gpu_id, MIN(model_name) AS model_name, MIN(hostname) AS hostname
		FROM   telemetry
		WHERE  uuid != ''
		GROUP  BY uuid
		ORDER  BY MIN(hostname), CAST(gpu_id AS INTEGER)`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("storage: query gpu summaries: %w", err)
	}
	defer rows.Close()

	var out []GPUSummary
	for rows.Next() {
		var g GPUSummary
		if err := rows.Scan(&g.UUID, &g.GPUID, &g.ModelName, &g.Hostname); err != nil {
			return nil, fmt.Errorf("storage: scan gpu summary: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// QueryStrandedGPUs implements Store.
// A single aggregating query identifies GPUs where MAX(value) <= maxUtilPct
// across all samples in the window, making it O(n) in the SQLite scan with
// full index support on (metric_name, timestamp, uuid).
func (s *SQLiteStore) QueryStrandedGPUs(ctx context.Context, metricName string, since time.Time, maxUtilPct float64) ([]StrandedGPU, error) {
	const q = `
		SELECT   uuid,
		         gpu_id,
		         MIN(model_name) AS model_name,
		         MIN(hostname)   AS hostname,
		         MAX(value)      AS peak_util,
		         COUNT(*)        AS sample_count
		FROM     telemetry
		WHERE    metric_name = ?
		  AND    timestamp   >= ?
		  AND    uuid        != ''
		GROUP BY uuid
		HAVING   MAX(value) <= ?
		ORDER BY MIN(hostname), CAST(gpu_id AS INTEGER)`

	rows, err := s.db.QueryContext(ctx, q,
		metricName,
		since.UTC().Format(time.RFC3339Nano),
		maxUtilPct,
	)
	if err != nil {
		return nil, fmt.Errorf("storage: query stranded gpus: %w", err)
	}
	defer rows.Close()

	var out []StrandedGPU
	for rows.Next() {
		var g StrandedGPU
		if err := rows.Scan(&g.UUID, &g.GPUID, &g.ModelName, &g.Hostname, &g.PeakUtilPct, &g.SampleCount); err != nil {
			return nil, fmt.Errorf("storage: scan stranded gpu: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Close implements Store.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func scanStrings(rows *sql.Rows) ([]string, error) {
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
