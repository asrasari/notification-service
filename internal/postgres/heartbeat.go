package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// heartbeatRetention is how long a stale worker row is kept after it
// expired, so a crash stays visible for a while (D-015 continued).
const heartbeatRetention = time.Hour

// Heartbeats writes the worker_heartbeats table.
type Heartbeats struct {
	pool *pgxpool.Pool
}

// NewHeartbeats returns a Heartbeats on pool.
func NewHeartbeats(pool *pgxpool.Pool) *Heartbeats {
	return &Heartbeats{pool: pool}
}

// Beat upserts the worker's row: last_seen_at = now() and
// expires_at = now() + 3 × interval, on the DB clock (D-009), with state as
// JSON. The same statement deletes rows that expired more than an hour ago
// (crashed workers).
func (h *Heartbeats) Beat(ctx context.Context, workerID string, interval time.Duration, state []byte) error {
	_, err := h.pool.Exec(ctx, `
		WITH pruned AS (
			DELETE FROM worker_heartbeats
			WHERE expires_at < now() - $4::interval AND worker_id <> $1
		)
		INSERT INTO worker_heartbeats (worker_id, started_at, last_seen_at, expires_at, state)
		VALUES ($1, now(), now(), now() + 3 * $2::interval, $3)
		ON CONFLICT (worker_id) DO UPDATE
		SET last_seen_at = now(), expires_at = EXCLUDED.expires_at, state = EXCLUDED.state`,
		workerID, interval, string(state), heartbeatRetention)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// Remove deletes the worker's row on graceful shutdown.
func (h *Heartbeats) Remove(ctx context.Context, workerID string) error {
	if _, err := h.pool.Exec(ctx, `DELETE FROM worker_heartbeats WHERE worker_id = $1`, workerID); err != nil {
		return fmt.Errorf("remove heartbeat: %w", err)
	}
	return nil
}

// HeartbeatChannel is one channel's state as a worker reports it in its
// heartbeat row.
type HeartbeatChannel struct {
	Breaker string `json:"breaker"`
	Paused  bool   `json:"paused"`
}

// LiveWorkers returns the reported channel states of every worker whose row
// has not expired (expires_at > now(), DB clock).
func (h *Heartbeats) LiveWorkers(ctx context.Context) ([]map[string]HeartbeatChannel, error) {
	rows, err := h.pool.Query(ctx, `SELECT state FROM worker_heartbeats WHERE expires_at > now() ORDER BY worker_id`)
	if err != nil {
		return nil, fmt.Errorf("live workers: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]HeartbeatChannel, error) {
		var state map[string]HeartbeatChannel
		err := row.Scan(&state)
		return state, err
	})
	if err != nil {
		return nil, fmt.Errorf("live workers: %w", err)
	}
	return out, nil
}
