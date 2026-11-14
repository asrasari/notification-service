// Package postgres holds all database access: the connection pool now, and
// the queue, transition and attempt-history SQL as it is added (D-013, D-019).
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/config"
)

// NewPool creates a pgx connection pool sized by DB_MAX_CONNS. It does not
// connect eagerly, so the API can start (and report 503 on /health) while
// Postgres is still unavailable.
func NewPool(ctx context.Context, db config.Database) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(db.URL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = db.MaxConns
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	return pool, nil
}
