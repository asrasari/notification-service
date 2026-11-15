// Command fakeprovider is a stand-in notification provider for the load
// test only (D-020 §1); the normal stack and the README quick start use
// webhook.site. It answers like the case's provider (202 with messageId,
// status, timestamp) after FAKE_LATENCY, fails FAKE_FAILURE_RATE of the
// requests with a 500, and measures what it receives on its own: per
// channel, the requests received and the peak received in any 1-second
// window, served on GET /stats. The rate-limit proof comes from here, the
// receiving side, not from the service's own metrics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("fakeprovider stopped with error", "error", err)
		stop()
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	addr := envOr("FAKE_ADDR", ":9090")
	latency, err := time.ParseDuration(envOr("FAKE_LATENCY", "50ms"))
	if err != nil || latency < 0 {
		return fmt.Errorf("FAKE_LATENCY must be a non-negative duration, got %q", os.Getenv("FAKE_LATENCY"))
	}
	failureRate, err := strconv.ParseFloat(envOr("FAKE_FAILURE_RATE", "0"), 64)
	if err != nil || failureRate < 0 || failureRate > 1 {
		return fmt.Errorf("FAKE_FAILURE_RATE must be in [0, 1], got %q", os.Getenv("FAKE_FAILURE_RATE"))
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           newProvider(latency, failureRate, nil).handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		slog.Info("fakeprovider listening", "addr", addr, "latency", latency.String(), "failure_rate", failureRate)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
