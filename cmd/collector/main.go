// Command collector subscribes to the broker using a wildcard topic pattern,
// deserialises Telemetry messages, and persists them to a SQLite database.
//
// The default subscription pattern  telemetry.gpu.*  matches every per-GPU
// topic published by the streamer (e.g. telemetry.gpu.0, telemetry.gpu.3).
//
// Batching strategy: rows accumulate in memory and are flushed to SQLite when
// either the batch reaches COLLECTOR_BATCH_SIZE records OR the COLLECTOR_FLUSH_INT
// timer fires – whichever comes first.  This bounds both latency and write
// amplification while maintaining high throughput.
//
// Environment variables:
//
//	COLLECTOR_TOPIC      Wildcard topic pattern          (default telemetry.gpu.*)
//	COLLECTOR_DB         Path to SQLite database file   (default ./telemetry.db)
//	COLLECTOR_BATCH_SIZE Records per write transaction   (default 500)
//	COLLECTOR_FLUSH_INT  Max time between flushes        (default 2s)
//	MQ_ADDR              Broker TCP address              (default :7777)
//	LOG_LEVEL            debug|info|warn|error           (default info)
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gpu-telemetry/pkg/models"
	"gpu-telemetry/pkg/mq"
	"gpu-telemetry/pkg/storage"
)

func main() {
	logger := buildLogger(getEnv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	topic := getEnv("COLLECTOR_TOPIC", "telemetry.gpu.*")
	dbPath := getEnv("COLLECTOR_DB", "./telemetry.db")
	batchSize := getEnvInt("COLLECTOR_BATCH_SIZE", 500)
	flushInterval := getEnvDuration("COLLECTOR_FLUSH_INT", 2*time.Second)
	brokerAddr := getEnv("MQ_ADDR", ":7777")

	// Open the persistent store.
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		logger.Error("failed to open database", "path", dbPath, "err", err)
		os.Exit(1)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			logger.Error("db close error", "err", cerr)
		}
	}()

	// Connect to the broker.
	client := mq.NewClient(mq.ClientConfig{Addr: brokerAddr}, logger)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := client.Connect(ctx); err != nil {
		logger.Error("failed to connect to broker", "addr", brokerAddr, "err", err)
		os.Exit(1)
	}
	defer client.Close()

	msgCh, err := client.Subscribe(ctx, topic)
	if err != nil {
		logger.Error("failed to subscribe", "topic", topic, "err", err)
		os.Exit(1)
	}

	logger.Info("collector started",
		"topic", topic,
		"db", dbPath,
		"batch_size", batchSize,
		"flush_interval", flushInterval,
	)

	runCollector(ctx, msgCh, store, batchSize, flushInterval, logger)
	logger.Info("collector exited cleanly")
}

// runCollector drains msgCh and writes telemetry to store in batches.
// It flushes when the batch is full OR the flush timer fires.
func runCollector(
	ctx context.Context,
	msgCh <-chan []byte,
	store storage.Store,
	batchSize int,
	flushInterval time.Duration,
	logger *slog.Logger,
) {
	batch := make([]*models.Telemetry, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var totalWritten, totalDropped int64

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := store.WriteBatch(ctx, batch); err != nil {
			logger.Error("batch write failed", "size", len(batch), "err", err)
			totalDropped += int64(len(batch))
		} else {
			totalWritten += int64(len(batch))
			logger.Debug("batch written", "size", len(batch), "total", totalWritten)
		}
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				// Channel closed (client shutdown or broker disconnect).
				flush()
				return
			}

			var t models.Telemetry
			if err := json.Unmarshal(msg, &t); err != nil {
				logger.Warn("failed to unmarshal message", "err", err)
				totalDropped++
				continue
			}
			if err := t.Validate(); err != nil {
				logger.Warn("invalid telemetry, skipping", "err", err)
				totalDropped++
				continue
			}

			batch = append(batch, &t)
			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
			logger.Info("collector heartbeat",
				"total_written", totalWritten,
				"total_dropped", totalDropped,
			)

		case <-ctx.Done():
			flush()
			logger.Info("collector shutdown",
				"total_written", totalWritten,
				"total_dropped", totalDropped,
			)
			return
		}
	}
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

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
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

func getEnvBool(key string, fallback bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "true" || v == "1" {
		return true
	}
	if v == "false" || v == "0" {
		return false
	}
	return fallback
}
