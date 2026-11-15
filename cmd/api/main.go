// Command api serves the notification HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/httpapi"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/postgres"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("api stopped with error", "error", err)
		stop()
		os.Exit(1)
	}
}

// run wires the API and blocks until ctx is cancelled (SIGTERM/SIGINT), then
// shuts down in order: stop accepting requests and finish in-flight ones,
// then close the database pool.
func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
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

	store := postgres.NewStore(pool, cfg.Retry.MaxAttempts)
	srv := httpapi.NewServer(cfg.Server.Addr, httpapi.NewHandler(logger, pool, store),
		slog.NewLogLogger(logger.Handler(), slog.LevelWarn))

	serveErr := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "api listening", "addr", cfg.Server.Addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("api shutting down", "timeout", cfg.Server.ShutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logger.Info("api stopped")
	return nil
}
