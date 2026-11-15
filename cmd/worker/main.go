// Command worker claims due notifications and sends them to the provider:
// one dispatcher and a pool of senders per channel (D-004, D-007), with its
// own HTTP server for /health and a heartbeat row in the database (D-015).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/breaker"
	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/httpapi"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/metrics"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/provider"
	"github.com/asrasari/notification-service/internal/retry"
	"github.com/asrasari/notification-service/internal/worker"
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
// Shutdown order (D-007):
//  1. /health answers 503 and the dispatchers stop claiming;
//  2. in-flight sends finish; their context is cancelled only once
//     SHUTDOWN_TIMEOUT has passed;
//  3. the heartbeat stops and deletes its row;
//  4. the HTTP server stops, then the database pool closes.
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

	workerID, err := newWorkerID()
	if err != nil {
		return err
	}
	logger = logger.With("worker_id", workerID)

	reg := metrics.NewRegistry()
	wm := metrics.NewWorker(reg)
	w := newWorker(cfg, pool, logger, wm)
	metrics.RegisterWorkerState(reg, w)

	// The worker's own HTTP server: /health and /metrics.
	var shuttingDown atomic.Bool
	srv := httpapi.NewServer(cfg.Worker.HTTPAddr, worker.NewHTTPHandler(w.Status, &shuttingDown, metrics.Handler(reg)),
		slog.NewLogLogger(logger.Handler(), slog.LevelWarn))
	serveErr := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "worker http listening", "addr", cfg.Worker.HTTPAddr)
		serveErr <- srv.ListenAndServe()
	}()

	// The heartbeat outlives the claim loop: the worker is alive while it
	// drains in-flight sends.
	hbCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeat()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		worker.RunHeartbeat(hbCtx, postgres.NewHeartbeats(pool), workerID, cfg.Worker.HeartbeatInterval, w.Status, logger)
	}()

	claimCtx, stopClaiming := context.WithCancel(ctx)
	defer stopClaiming()
	sendCtx, cancelSends := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSends()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		w.Run(claimCtx, sendCtx)
	}()

	// The provider URL is not logged: a webhook.site URL is a bearer secret.
	logger.InfoContext(ctx, "worker started",
		"worker_concurrency", cfg.Queue.WorkerConcurrency,
		"rate_limit_per_channel", cfg.RateLimit.PerChannel,
		"priority_weights", cfg.Queue.PriorityWeights,
	)

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		runErr = fmt.Errorf("worker http server: %w", err)
	}

	// 1. Stop claiming.
	logger.Info("worker shutting down", "timeout", cfg.Server.ShutdownTimeout.String())
	shuttingDown.Store(true)
	stopClaiming()

	// 2. Let in-flight sends finish, up to SHUTDOWN_TIMEOUT.
	timeout := time.NewTimer(cfg.Server.ShutdownTimeout)
	select {
	case <-runDone:
		timeout.Stop()
	case <-timeout.C:
		logger.Warn("shutdown timeout reached; cancelling in-flight sends: each is written now as an unknown attempt and retried later (failed if it was the last attempt)")
		cancelSends()
		<-runDone
	}

	// 3. Stop the heartbeat; it deletes its row.
	stopHeartbeat()
	<-hbDone

	// 4. Stop the HTTP server. The pool closes in the deferred call.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("worker http shutdown: %w", err)
	}
	if runErr == nil {
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("worker http server: %w", err)
		}
	}
	if runErr != nil {
		return runErr
	}
	logger.Info("worker stopped")
	return nil
}

// newWorker builds one breaker and one rate limiter per channel and the
// worker around them, with its hooks connected to the metrics.
func newWorker(cfg config.Config, pool *pgxpool.Pool, logger *slog.Logger, wm *metrics.Worker) *worker.Worker {
	queue := postgres.NewQueue(pool, cfg.Queue.LeaseDuration)
	client := provider.New(provider.Options{
		URL:      cfg.Provider.URL,
		Timeout:  cfg.Provider.Timeout,
		MaxConns: cfg.Queue.WorkerConcurrency * len(notification.Channels),
	})

	deps := make([]worker.ChannelDeps, 0, len(notification.Channels))
	for _, ch := range notification.Channels {
		b := breaker.New(cfg.Breaker, breaker.Options{
			Name:       string(ch),
			IsExcluded: worker.IsExcluded,
			OnStateChange: func(name string, from, to breaker.State) {
				wm.BreakerState(name, to)
				logger.Warn("circuit breaker state changed", "channel", name, "from", from, "to", to)
			},
		})
		wm.BreakerState(string(ch), b.State())
		// Paced just under the provider's limit, burst 1 (D-007, R-M5).
		deps = append(deps, worker.ChannelDeps{Channel: ch, Breaker: b, Limiter: worker.NewLimiter(cfg.RateLimit.PerChannel)})
	}

	w := cfg.Queue.PriorityWeights
	return worker.New(worker.Config{
		PollInterval:       cfg.Queue.PollInterval,
		Concurrency:        cfg.Queue.WorkerConcurrency,
		Weights:            [3]int{w[0], w[1], w[2]},
		Backoff:            retry.Backoff{Base: cfg.Retry.Base, Cap: cfg.Retry.Cap},
		RetryAfterCap:      cfg.Retry.RetryAfterCap,
		BreakerOpenTimeout: cfg.Breaker.OpenTimeout,
		OnAttempt:          wm.OnAttempt,
		OnClaim:            wm.OnClaim,
	}, queue, client, logger, deps)
}

// newWorkerID is the hostname plus a short random suffix (D-015), so a
// restarted container with the same hostname gets a new row.
func newWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("worker id: %w", err)
	}
	return host + "-" + hex.EncodeToString(b), nil
}
