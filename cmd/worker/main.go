// Command worker claims due notifications and sends them to the provider.
//
// M1: wiring and lifecycle only. The per-channel claim loops and sender pools
// (D-004, D-005) arrive in M3.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("worker stopped with error", "error", err)
		stop()
		os.Exit(1)
	}
}

// run wires the worker and blocks until ctx is cancelled (SIGTERM/SIGINT).
// Shutdown order, once claim loops exist: stop claiming, wait for in-flight
// sends (bounded by SHUTDOWN_TIMEOUT), then close the database pool.
func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.RequireProviderURL(); err != nil {
		return err
	}
	logger, err := logging.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The provider URL is not logged: a webhook.site URL is a bearer secret.
	logger.InfoContext(ctx, "worker started",
		"worker_concurrency", cfg.Queue.WorkerConcurrency,
		"rate_limit_per_channel", cfg.RateLimit.PerChannel,
	)

	<-ctx.Done()

	logger.Info("worker shutting down")
	logger.Info("worker stopped")
	return nil
}
