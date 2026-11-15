// Command migrate applies the embedded database migrations (D-013).
//
//	migrate [up]   apply all pending migrations (default; what compose runs)
//	migrate down   roll back the most recent migration
//	migrate status print each migration and whether it is applied
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("migrate failed", "error", err)
		stop()
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
	if len(args) > 1 {
		return fmt.Errorf("unexpected arguments: %v", args[1:])
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger, err := logging.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}

	db, err := sql.Open("pgx", cfg.Database.URL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }() // nothing to do about a close error at exit

	p, err := migrations.NewProvider(db)
	if err != nil {
		return err
	}

	switch cmd {
	case "up":
		results, err := p.Up(ctx)
		for _, r := range results {
			logger.InfoContext(ctx, "migration applied", "version", r.Source.Version, "file", r.Source.Path, "duration", r.Duration.String())
		}
		if err != nil {
			return fmt.Errorf("migrate up: %w", err)
		}
	case "down":
		r, err := p.Down(ctx)
		if r != nil {
			logger.InfoContext(ctx, "migration rolled back", "version", r.Source.Version, "file", r.Source.Path, "duration", r.Duration.String())
		}
		if err != nil {
			return fmt.Errorf("migrate down: %w", err)
		}
	case "status":
		statuses, err := p.Status(ctx)
		if err != nil {
			return fmt.Errorf("migrate status: %w", err)
		}
		for _, s := range statuses {
			logger.InfoContext(ctx, "migration status", "version", s.Source.Version, "file", s.Source.Path, "state", string(s.State), "applied_at", s.AppliedAt)
		}
	default:
		return errors.New(`unknown command (want "up", "down" or "status")`)
	}

	version, err := p.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	logger.InfoContext(ctx, "migrate finished", "command", cmd, "schema_version", version)
	return nil
}
