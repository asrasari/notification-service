package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/asrasari/notification-service/internal/notification"
)

// Queue states for notification_queue_depth (D-015 continued).
const (
	QueueStateDue        = "due"        // pending, available_at <= now()
	QueueStateDelayed    = "delayed"    // pending, available_at in the future (backoff or scheduled)
	QueueStateProcessing = "processing" // leased to a worker (an expired lease included)
)

// QueueGroup is one row of QueueStats: how many rows of a channel and
// priority are in a state. OldestDueSeconds is set for the due state: how
// long the oldest of them has been due (now() - available_at).
type QueueGroup struct {
	Channel          notification.Channel
	Priority         notification.Priority
	State            string
	Count            int
	OldestDueSeconds float64
}

// QueueStats counts the unfinished rows by channel, priority and state in
// one grouped query, on the DB clock. Only non-empty groups are returned.
// It reads only rows the claim index covers (status pending or processing).
func (s *Store) QueueStats(ctx context.Context) ([]QueueGroup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, priority, state, count(*),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(available_at) FILTER (WHERE state = 'due')), 0)::float8
		FROM (
			SELECT channel, priority, available_at,
			       CASE WHEN status = 'processing' THEN 'processing'
			            WHEN available_at <= now() THEN 'due'
			            ELSE 'delayed' END AS state
			FROM notifications
			WHERE status IN ('pending', 'processing')
		) q
		GROUP BY channel, priority, state`)
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}
	groups, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (QueueGroup, error) {
		var (
			g        QueueGroup
			channel  string
			priority int16
		)
		if err := row.Scan(&channel, &priority, &g.State, &g.Count, &g.OldestDueSeconds); err != nil {
			return g, err
		}
		g.Channel = notification.Channel(channel)
		p, err := notification.PriorityFromDB(priority)
		g.Priority = p
		return g, err
	})
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}
	return groups, nil
}
