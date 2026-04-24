// Command streamer reads a DCGM GPU metrics CSV file and continuously
// publishes each row as a JSON-encoded Telemetry message to the broker.
//
// Topic routing: each message is published to an individual, per-GPU topic:
//
//	telemetry.gpu.<gpu_id>   (e.g.  telemetry.gpu.0 , telemetry.gpu.3)
//
// This enables Collectors to subscribe to individual GPUs or use the wildcard
// pattern  telemetry.gpu.*  to receive all metrics.
//
// Streaming model (default – continuous):
//   - Rows are published one at a time with a configurable inter-message delay.
//   - When EOF is reached the file is seeked back to the first data row and
//     streaming continues indefinitely, simulating a live telemetry feed.
//   - SIGINT / SIGTERM trigger a clean shutdown.
//
// One-shot mode (STREAMER_LOOP=false):
//   - The file is read exactly once and the process exits.
//
// Environment variables:
//
//	STREAMER_CSV        Path to the DCGM metrics CSV file     (required)
//	STREAMER_LOOP       Continuously re-stream on EOF         (default true)
//	STREAMER_DELAY      Delay between consecutive publishes   (default 50ms)
//	STREAMER_TOPIC_PFX  Per-GPU topic prefix                  (default telemetry.gpu)
//	MQ_ADDR             Broker TCP address                    (default :7777)
//	LOG_LEVEL           debug|info|warn|error                 (default info)
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/mq"
)

// -------------------------------------------------------------------------
// CSV column indices – matches the dcgm-exporter output format.
// -------------------------------------------------------------------------

const (
	colTimestamp  = 0
	colMetricName = 1
	colGPUID      = 2
	colDevice     = 3
	colUUID       = 4
	colModelName  = 5
	colHostname   = 6
	colContainer  = 7
	colPod        = 8
	colNamespace  = 9
	colValue      = 10
	colLabelsRaw  = 11
	minColumns    = 11 // labels_raw is optional
)

// -------------------------------------------------------------------------
// Main
// -------------------------------------------------------------------------

func main() {
	logger := buildLogger(getEnv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	csvPath := getEnv("STREAMER_CSV", "")
	if csvPath == "" {
		logger.Error("STREAMER_CSV is required")
		os.Exit(1)
	}

	loop := getEnvBool("STREAMER_LOOP", true)
	delay := getEnvDuration("STREAMER_DELAY", 50*time.Millisecond)
	topicPrefix := getEnv("STREAMER_TOPIC_PFX", "telemetry.gpu")
	brokerAddr := getEnv("MQ_ADDR", ":7777")

	client := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := client.Connect(ctx); err != nil {
		logger.Error("failed to connect to broker", "addr", brokerAddr, "err", err)
		os.Exit(1)
	}
	defer client.Close()

	logger.Info("streamer started",
		"csv", csvPath,
		"loop", loop,
		"delay", delay,
		"topic_prefix", topicPrefix,
	)

	if err := runStream(ctx, client, csvPath, topicPrefix, delay, loop, logger); err != nil {
		logger.Error("stream error", "err", err)
		os.Exit(1)
	}
	logger.Info("streamer exited cleanly")
}

// -------------------------------------------------------------------------
// Core streaming loop
// -------------------------------------------------------------------------

// runStream opens csvPath and streams rows to the broker indefinitely (loop=true)
// or exactly once (loop=false). Each row is published to:
//
//	<topicPrefix>.<gpu_id>   e.g. telemetry.gpu.0
//
// The function waits delay between each row, yielding to ctx.Done() to
// ensure SIGINT/SIGTERM always produces a clean shutdown.
func runStream(
	ctx context.Context,
	client *mq.Client,
	csvPath, topicPrefix string,
	delay time.Duration,
	loop bool,
	logger *slog.Logger,
) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("open csv %q: %w", csvPath, err)
	}
	defer f.Close()

	// Record the byte offset right after the header so we can rewind on loop.
	headerEnd, err := skipHeader(f)
	if err != nil {
		return fmt.Errorf("read csv header: %w", err)
	}

	var (
		published   int64
		parseErrors int64
		loopCount   int
	)

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	reader := newCSVReader(f)

	for {
		record, rerr := reader.Read()
		if rerr == io.EOF {
			if !loop {
				break
			}
			// Rewind to the first data row and keep streaming.
			if _, err := f.Seek(headerEnd, io.SeekStart); err != nil {
				return fmt.Errorf("csv rewind: %w", err)
			}
			reader = newCSVReader(f)
			loopCount++
			logger.Info("CSV rewound", "loop", loopCount, "published", published)
			continue
		}
		if rerr != nil {
			parseErrors++
			logger.Warn("csv read error, skipping row", "err", rerr)
			continue
		}

		t, perr := parseRow(record)
		if perr != nil {
			parseErrors++
			logger.Debug("parse error, skipping row", "err", perr)
			continue
		}
		if err := t.Validate(); err != nil {
			parseErrors++
			continue
		}

		payload, merr := json.Marshal(t)
		if merr != nil {
			parseErrors++
			continue
		}

		topic := topicPrefix + "." + t.GPUID

		// Honour the per-message rate limit; bail immediately on shutdown.
		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Info("streamer shutdown signal",
				"published", published,
				"parse_errors", parseErrors,
				"loops", loopCount,
			)
			return nil
		}

		if pubErr := client.Publish(ctx, topic, payload); pubErr != nil {
			logger.Warn("publish error", "topic", topic, "err", pubErr)
			continue
		}
		published++

		if published%1000 == 0 {
			logger.Info("streamer progress", "published", published, "parse_errors", parseErrors)
		}
	}

	logger.Info("streamer one-shot complete", "published", published, "parse_errors", parseErrors)
	return nil
}

// skipHeader reads and discards the CSV header row, returning the byte offset
// of the first data row so callers can seek back to it on loop rewind.
//
// We use bufio.Reader.ReadString to count the exact bytes in the header line.
// Using csv.Reader here would be wrong: csv.Reader wraps its input in a
// bufio.Reader that reads ahead in 4 KiB chunks, so the OS file position
// would advance far beyond the header and data rows would be skipped.
func skipHeader(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	br := bufio.NewReader(f)
	line, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("csv: read header: %w", err)
	}
	if len(line) == 0 {
		return 0, fmt.Errorf("csv: empty file")
	}
	headerEnd := int64(len(line))
	if _, err := f.Seek(headerEnd, io.SeekStart); err != nil {
		return 0, err
	}
	return headerEnd, nil
}

func newCSVReader(r io.Reader) *csv.Reader {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	cr.LazyQuotes = true
	return cr
}

// -------------------------------------------------------------------------
// CSV row parser
// -------------------------------------------------------------------------

func parseRow(record []string) (*models.Telemetry, error) {
	if len(record) < minColumns {
		return nil, fmt.Errorf("expected at least %d columns, got %d", minColumns, len(record))
	}

	ts, err := time.Parse(time.RFC3339, strings.TrimSpace(record[colTimestamp]))
	if err != nil {
		ts, err = time.Parse("2006-01-02T15:04:05Z07:00", strings.TrimSpace(record[colTimestamp]))
		if err != nil {
			return nil, fmt.Errorf("parse timestamp %q: %w", record[colTimestamp], err)
		}
	}

	value, err := strconv.ParseFloat(strings.TrimSpace(record[colValue]), 64)
	if err != nil {
		return nil, fmt.Errorf("parse value %q: %w", record[colValue], err)
	}

	t := &models.Telemetry{
		Timestamp:  ts,
		MetricName: strings.TrimSpace(record[colMetricName]),
		GPUID:      strings.TrimSpace(record[colGPUID]),
		Device:     strings.TrimSpace(record[colDevice]),
		UUID:       strings.TrimSpace(record[colUUID]),
		ModelName:  strings.TrimSpace(record[colModelName]),
		Hostname:   strings.TrimSpace(record[colHostname]),
		Container:  strings.TrimSpace(record[colContainer]),
		Pod:        strings.TrimSpace(record[colPod]),
		Namespace:  strings.TrimSpace(record[colNamespace]),
		Value:      value,
	}
	if len(record) > colLabelsRaw {
		t.Labels = models.ParseLabelsRaw(strings.TrimSpace(record[colLabelsRaw]))
	}
	return t, nil
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	v := strings.ToLower(os.Getenv(key))
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
