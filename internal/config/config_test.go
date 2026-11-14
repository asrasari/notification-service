package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func baseVars() map[string]string {
	return map[string]string{"DATABASE_URL": "postgres://u:p@localhost:5432/db"}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := LoadFrom(baseVars())
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"HTTP_ADDR", cfg.Server.Addr, ":8080"},
		{"SHUTDOWN_TIMEOUT", cfg.Server.ShutdownTimeout, 20 * time.Second},
		{"DB_MAX_CONNS", cfg.Database.MaxConns, int32(10)},
		{"PROVIDER_URL", cfg.Provider.URL, ""},
		{"PROVIDER_TIMEOUT", cfg.Provider.Timeout, 10 * time.Second},
		{"LEASE_DURATION", cfg.Queue.LeaseDuration, 30 * time.Second},
		{"POLL_INTERVAL", cfg.Queue.PollInterval, 200 * time.Millisecond},
		{"WORKER_CONCURRENCY", cfg.Queue.WorkerConcurrency, 25},
		{"RATE_LIMIT_PER_CHANNEL", cfg.RateLimit.PerChannel, 100},
		{"RETRY_MAX_ATTEMPTS", cfg.Retry.MaxAttempts, 6},
		{"RETRY_BASE", cfg.Retry.Base, 10 * time.Second},
		{"RETRY_CAP", cfg.Retry.Cap, 10 * time.Minute},
		{"RETRY_AFTER_CAP", cfg.Retry.RetryAfterCap, 5 * time.Minute},
		{"BREAKER_WINDOW", cfg.Breaker.Window, 10 * time.Second},
		{"BREAKER_MIN_REQUESTS", cfg.Breaker.MinRequests, uint32(20)},
		{"BREAKER_FAILURE_RATIO", cfg.Breaker.FailureRatio, 0.5},
		{"BREAKER_OPEN_TIMEOUT", cfg.Breaker.OpenTimeout, 30 * time.Second},
		{"LOG_LEVEL", cfg.Log.Level, "info"},
		{"LOG_FORMAT", cfg.Log.Format, "json"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadValidation(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		wantErr string // substring; empty means the config is valid
	}{
		{name: "defaults are valid", vars: nil},
		{name: "missing DATABASE_URL", vars: map[string]string{"DATABASE_URL": ""}, wantErr: "DATABASE_URL"},
		{name: "lease equal to provider timeout", vars: map[string]string{"LEASE_DURATION": "10s", "PROVIDER_TIMEOUT": "10s"}, wantErr: "LEASE_DURATION (10s) must be greater than PROVIDER_TIMEOUT (10s)"},
		{name: "lease below provider timeout", vars: map[string]string{"LEASE_DURATION": "5s"}, wantErr: "LEASE_DURATION"},
		{name: "lease just above provider timeout", vars: map[string]string{"LEASE_DURATION": "11s"}},
		{name: "retry base equal to cap", vars: map[string]string{"RETRY_BASE": "1m", "RETRY_CAP": "1m"}, wantErr: "RETRY_BASE (1m0s) must be less than RETRY_CAP (1m0s)"},
		{name: "retry base above cap", vars: map[string]string{"RETRY_BASE": "20m"}, wantErr: "RETRY_BASE"},
		{name: "zero retry base", vars: map[string]string{"RETRY_BASE": "0s"}, wantErr: "RETRY_BASE must be positive"},
		{name: "zero max attempts", vars: map[string]string{"RETRY_MAX_ATTEMPTS": "0"}, wantErr: "RETRY_MAX_ATTEMPTS"},
		{name: "zero retry-after cap", vars: map[string]string{"RETRY_AFTER_CAP": "0s"}, wantErr: "RETRY_AFTER_CAP"},
		{name: "zero worker concurrency", vars: map[string]string{"WORKER_CONCURRENCY": "0"}, wantErr: "WORKER_CONCURRENCY"},
		{name: "negative worker concurrency", vars: map[string]string{"WORKER_CONCURRENCY": "-1"}, wantErr: "WORKER_CONCURRENCY"},
		{name: "zero rate limit", vars: map[string]string{"RATE_LIMIT_PER_CHANNEL": "0"}, wantErr: "RATE_LIMIT_PER_CHANNEL"},
		{name: "zero pool size", vars: map[string]string{"DB_MAX_CONNS": "0"}, wantErr: "DB_MAX_CONNS"},
		{name: "zero poll interval", vars: map[string]string{"POLL_INTERVAL": "0s"}, wantErr: "POLL_INTERVAL"},
		{name: "zero shutdown timeout", vars: map[string]string{"SHUTDOWN_TIMEOUT": "0s"}, wantErr: "SHUTDOWN_TIMEOUT"},
		{name: "breaker ratio zero", vars: map[string]string{"BREAKER_FAILURE_RATIO": "0"}, wantErr: "BREAKER_FAILURE_RATIO must be in (0, 1]"},
		{name: "breaker ratio above one", vars: map[string]string{"BREAKER_FAILURE_RATIO": "1.01"}, wantErr: "BREAKER_FAILURE_RATIO"},
		{name: "breaker ratio exactly one", vars: map[string]string{"BREAKER_FAILURE_RATIO": "1"}},
		{name: "breaker min requests zero", vars: map[string]string{"BREAKER_MIN_REQUESTS": "0"}, wantErr: "BREAKER_MIN_REQUESTS"},
		{name: "breaker window zero", vars: map[string]string{"BREAKER_WINDOW": "0s"}, wantErr: "BREAKER_WINDOW"},
		{name: "breaker open timeout zero", vars: map[string]string{"BREAKER_OPEN_TIMEOUT": "0s"}, wantErr: "BREAKER_OPEN_TIMEOUT"},
		{name: "unknown log level", vars: map[string]string{"LOG_LEVEL": "verbose"}, wantErr: "LOG_LEVEL"},
		{name: "unknown log format", vars: map[string]string{"LOG_FORMAT": "xml"}, wantErr: "LOG_FORMAT"},
		{name: "text log format", vars: map[string]string{"LOG_FORMAT": "text", "LOG_LEVEL": "debug"}},
		{name: "provider URL without scheme", vars: map[string]string{"PROVIDER_URL": "webhook.site/abc"}, wantErr: "PROVIDER_URL must be an absolute http(s) URL"},
		{name: "provider URL https", vars: map[string]string{"PROVIDER_URL": "https://webhook.site/abc"}},
		{name: "unparsable duration", vars: map[string]string{"RETRY_CAP": "ten minutes"}, wantErr: `invalid duration "ten minutes"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := baseVars()
			for k, v := range tt.vars {
				vars[k] = v
			}
			_, err := LoadFrom(vars)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	vars := baseVars()
	vars["WORKER_CONCURRENCY"] = "0"
	vars["LOG_FORMAT"] = "xml"
	_, err := LoadFrom(vars)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"WORKER_CONCURRENCY", "LOG_FORMAT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestRequireProviderURL(t *testing.T) {
	cfg, err := LoadFrom(baseVars())
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.RequireProviderURL(); !errors.Is(err, ErrProviderURLMissing) {
		t.Fatalf("got %v, want ErrProviderURLMissing", err)
	}
	const want = "PROVIDER_URL is not set. Create a URL at webhook.site and add it to .env (see README)"
	if ErrProviderURLMissing.Error() != want {
		t.Fatalf("message changed: %q", ErrProviderURLMissing.Error())
	}

	cfg.Provider.URL = "https://webhook.site/abc"
	if err := cfg.RequireProviderURL(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
