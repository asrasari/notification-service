// Package config loads the service configuration from environment variables
// into a typed struct and validates it once at startup (D-018).
//
// No other package reads the environment: run() calls Load and hands each
// component the part of Config it needs.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"
)

// ErrProviderURLMissing is returned by RequireProviderURL. The wording is
// fixed by D-018 because it is what a reviewer sees when the worker exits.
var ErrProviderURLMissing = errors.New("PROVIDER_URL is not set. Create a URL at webhook.site and add it to .env (see README)")

// Config is the full service configuration, grouped by area.
type Config struct {
	Server    Server
	Database  Database
	Provider  Provider
	Queue     Queue
	RateLimit RateLimit
	Retry     Retry
	Breaker   Breaker
	Log       Log
}

// Server configures the HTTP API process.
type Server struct {
	Addr            string        `env:"HTTP_ADDR" envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
}

// Database configures the Postgres connection pool.
type Database struct {
	URL      string `env:"DATABASE_URL,required,notEmpty"`
	MaxConns int32  `env:"DB_MAX_CONNS" envDefault:"10"`
}

// Provider configures the outbound notification provider (webhook.site).
// URL is optional at load time: only the worker needs it (RequireProviderURL).
type Provider struct {
	URL     string        `env:"PROVIDER_URL"`
	Timeout time.Duration `env:"PROVIDER_TIMEOUT" envDefault:"10s"`
}

// Queue configures claiming and leasing of notification rows.
type Queue struct {
	LeaseDuration     time.Duration `env:"LEASE_DURATION" envDefault:"30s"`
	PollInterval      time.Duration `env:"POLL_INTERVAL" envDefault:"200ms"`
	WorkerConcurrency int           `env:"WORKER_CONCURRENCY" envDefault:"50"`
}

// RateLimit configures the per-channel send rate.
type RateLimit struct {
	PerChannel int `env:"RATE_LIMIT_PER_CHANNEL" envDefault:"100"`
}

// Retry configures attempts and backoff (D-010).
type Retry struct {
	MaxAttempts   int           `env:"RETRY_MAX_ATTEMPTS" envDefault:"6"`
	Base          time.Duration `env:"RETRY_BASE" envDefault:"10s"`
	Cap           time.Duration `env:"RETRY_CAP" envDefault:"10m"`
	RetryAfterCap time.Duration `env:"RETRY_AFTER_CAP" envDefault:"5m"`
}

// Breaker configures the per-channel circuit breaker (D-010).
type Breaker struct {
	Window       time.Duration `env:"BREAKER_WINDOW" envDefault:"10s"`
	MinRequests  uint32        `env:"BREAKER_MIN_REQUESTS" envDefault:"20"`
	FailureRatio float64       `env:"BREAKER_FAILURE_RATIO" envDefault:"0.5"`
	OpenTimeout  time.Duration `env:"BREAKER_OPEN_TIMEOUT" envDefault:"30s"`
}

// Log configures the slog logger (D-015).
type Log struct {
	Level  string `env:"LOG_LEVEL" envDefault:"info"`
	Format string `env:"LOG_FORMAT" envDefault:"json"`
}

// Load reads the process environment and validates the result.
func Load() (Config, error) {
	return parse(env.Options{})
}

// LoadFrom is Load over an explicit set of variables instead of the process
// environment. It exists for tests.
func LoadFrom(vars map[string]string) (Config, error) {
	return parse(env.Options{Environment: vars})
}

func parse(opts env.Options) (Config, error) {
	cfg, err := env.ParseAsWithOptions[Config](opts)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the rules that span fields or need a range. It reports every
// problem at once so a misconfigured deploy is fixed in one pass.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	if c.Server.Addr == "" {
		add("HTTP_ADDR must not be empty")
	}
	if c.Server.ShutdownTimeout <= 0 {
		add("SHUTDOWN_TIMEOUT must be positive, got %s", c.Server.ShutdownTimeout)
	}
	if c.Database.MaxConns <= 0 {
		add("DB_MAX_CONNS must be positive, got %d", c.Database.MaxConns)
	}
	if c.Provider.URL != "" {
		if err := checkHTTPURL(c.Provider.URL); err != nil {
			add("PROVIDER_URL %w", err)
		}
	}
	if c.Provider.Timeout <= 0 {
		add("PROVIDER_TIMEOUT must be positive, got %s", c.Provider.Timeout)
	}
	if c.Queue.LeaseDuration <= c.Provider.Timeout {
		add("LEASE_DURATION (%s) must be greater than PROVIDER_TIMEOUT (%s), or a slow send could be reclaimed and sent twice",
			c.Queue.LeaseDuration, c.Provider.Timeout)
	}
	if c.Queue.PollInterval <= 0 {
		add("POLL_INTERVAL must be positive, got %s", c.Queue.PollInterval)
	}
	if c.Queue.WorkerConcurrency <= 0 {
		add("WORKER_CONCURRENCY must be positive, got %d", c.Queue.WorkerConcurrency)
	}
	if c.RateLimit.PerChannel <= 0 {
		add("RATE_LIMIT_PER_CHANNEL must be positive, got %d", c.RateLimit.PerChannel)
	}
	if c.Retry.MaxAttempts <= 0 {
		add("RETRY_MAX_ATTEMPTS must be positive, got %d", c.Retry.MaxAttempts)
	}
	if c.Retry.Base <= 0 {
		add("RETRY_BASE must be positive, got %s", c.Retry.Base)
	}
	if c.Retry.Base >= c.Retry.Cap {
		add("RETRY_BASE (%s) must be less than RETRY_CAP (%s)", c.Retry.Base, c.Retry.Cap)
	}
	if c.Retry.RetryAfterCap <= 0 {
		add("RETRY_AFTER_CAP must be positive, got %s", c.Retry.RetryAfterCap)
	}
	if c.Breaker.Window <= 0 {
		add("BREAKER_WINDOW must be positive, got %s", c.Breaker.Window)
	}
	if c.Breaker.MinRequests == 0 {
		add("BREAKER_MIN_REQUESTS must be positive")
	}
	if !(c.Breaker.FailureRatio > 0 && c.Breaker.FailureRatio <= 1) {
		add("BREAKER_FAILURE_RATIO must be in (0, 1], got %v", c.Breaker.FailureRatio)
	}
	if c.Breaker.OpenTimeout <= 0 {
		add("BREAKER_OPEN_TIMEOUT must be positive, got %s", c.Breaker.OpenTimeout)
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		add("LOG_LEVEL must be one of debug, info, warn, error, got %q", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		add("LOG_FORMAT must be json or text, got %q", c.Log.Format)
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
}

// RequireProviderURL fails with ErrProviderURLMissing when PROVIDER_URL is
// unset. Only the worker calls it: the API must start without a provider.
func (c Config) RequireProviderURL() error {
	if c.Provider.URL == "" {
		return ErrProviderURLMissing
	}
	return nil
}

func checkHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is not a valid URL: %v", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("must be an absolute http(s) URL, got %q", raw)
	}
	return nil
}
