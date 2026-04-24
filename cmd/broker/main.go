// Command broker runs the gpu-telemetry custom message queue server.
//
// The broker accepts TCP connections from Streamers (publishers) and
// Collectors (subscribers) and routes messages using the length-prefixed JSON
// frame protocol implemented in pkg/mq.
//
// Configuration is done entirely through environment variables so the binary
// is container-friendly with no required config files:
//
//	MQ_ADDR          TCP listen address          (default :7777)
//	MQ_ADMIN_ADDR    HTTP admin listen address   (default :7778)
//	MQ_MAX_CONNS     Maximum TCP connections      (default 0 = unlimited)
//	MQ_BUF_SIZE      Per-subscriber chan buffer   (default 256)
//	LOG_LEVEL        debug | info | warn | error  (default info)
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gpu-telemetry/pkg/mq"
)

func main() {
	logger := buildLogger(getEnv("LOG_LEVEL", "info"))
	slog.SetDefault(logger)

	cfg := mq.ServerConfig{
		Addr:           getEnv("MQ_ADDR", ":7777"),
		AdminAddr:      getEnv("MQ_ADMIN_ADDR", ":7778"),
		MaxConnections: getEnvInt("MQ_MAX_CONNS", 0),
		PingInterval:   30 * time.Second,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   10 * time.Second,
	}

	bufSize := getEnvInt("MQ_BUF_SIZE", 256)

	broker := mq.NewMemoryBroker(
		mq.WithBufferSize(bufSize),
		mq.WithBrokerLogger(logger),
	)

	server := mq.NewBrokerServer(broker, cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting gpu-telemetry broker",
		"addr", cfg.Addr,
		"admin_addr", cfg.AdminAddr,
		"buf_size", bufSize,
	)

	// Serve blocks until ctx is cancelled.
	go func() {
		if err := server.Serve(ctx); err != nil {
			logger.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	// Give in-flight connections up to 15 s to drain.
	server.Shutdown(15 * time.Second)
	_ = broker.Close()

	logger.Info("broker exited cleanly")
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
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}
