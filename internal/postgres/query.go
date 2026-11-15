package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/asrasari/notification-service/internal/notification"
)

// Get returns one notification with its delivery attempts, oldest first.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (notification.Notification, []notification.Attempt, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{}, nil, ErrNotFound
	}
	if err != nil {
		return notification.Notification{}, nil, fmt.Errorf("get notification: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT attempt_no, started_at, duration_ms, http_status, outcome, error
		FROM delivery_attempts
		WHERE notification_id = $1
		ORDER BY started_at, attempt_no, id`, id)
	if err != nil {
		return notification.Notification{}, nil, fmt.Errorf("get attempts: %w", err)
	}
	attempts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (notification.Attempt, error) {
		var (
			a          notification.Attempt
			httpStatus *int16
			outcome    string
		)
		if err := row.Scan(&a.AttemptNo, &a.StartedAt, &a.DurationMS, &httpStatus, &outcome, &a.Error); err != nil {
			return a, err
		}
		a.StartedAt = a.StartedAt.UTC()
		a.Outcome = notification.Outcome(outcome)
		if httpStatus != nil {
			v := int(*httpStatus)
			a.HTTPStatus = &v
		}
		return a, nil
	})
	if err != nil {
		return notification.Notification{}, nil, fmt.Errorf("get attempts: %w", err)
	}
	return n, attempts, nil
}

// Cursor is the position after the last row of a page: its (created_at, id)
// (D-014). The API encodes it as an opaque string.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// ListFilter selects notifications for List. Nil fields don't filter.
type ListFilter struct {
	Status      *notification.Status
	Channel     *notification.Channel
	CreatedFrom *time.Time // inclusive
	CreatedTo   *time.Time // exclusive
	BatchID     *uuid.UUID
	After       *Cursor // start after this row
	Limit       int     // 1–200, checked by the caller
}

// List returns notifications newest first (created_at DESC, id DESC) and the
// cursor of the next page, or nil when there is none. It reads Limit+1 rows
// to know whether another page exists (D-014).
func (s *Store) List(ctx context.Context, f ListFilter) ([]notification.Notification, *Cursor, error) {
	var (
		where []string
		args  []any
	)
	// Values are always bound as parameters, never written into the SQL (D-013).
	add := func(cond string, v ...any) {
		for _, a := range v {
			args = append(args, a)
			cond = strings.Replace(cond, "?", fmt.Sprintf("$%d", len(args)), 1)
		}
		where = append(where, cond)
	}
	if f.Status != nil {
		add("status = ?", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = ?", string(*f.Channel))
	}
	if f.CreatedFrom != nil {
		add("created_at >= ?", *f.CreatedFrom)
	}
	if f.CreatedTo != nil {
		add("created_at < ?", *f.CreatedTo)
	}
	if f.BatchID != nil {
		add("batch_id = ?", *f.BatchID)
	}
	if f.After != nil {
		add("(created_at, id) < (?, ?)", f.After.CreatedAt, f.After.ID)
	}

	query := `SELECT ` + columns + ` FROM notifications`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (notification.Notification, error) {
		return scanNotification(row)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list notifications: %w", err)
	}

	if len(list) <= f.Limit {
		return list, nil, nil
	}
	list = list[:f.Limit]
	last := list[len(list)-1]
	return list, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// BatchSummary counts a batch's notifications by status. Every status is
// present in Counts, zeros included. ErrNotFound means no row has this batch.
func (s *Store) BatchSummary(ctx context.Context, batchID uuid.UUID) (notification.BatchSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return notification.BatchSummary{}, fmt.Errorf("batch summary: %w", err)
	}
	sum := notification.BatchSummary{BatchID: batchID, Counts: make(map[notification.Status]int, len(notification.Statuses))}
	for _, st := range notification.Statuses {
		sum.Counts[st] = 0
	}
	var (
		status string
		count  int
	)
	_, err = pgx.ForEachRow(rows, []any{&status, &count}, func() error {
		st, err := notification.ParseStatus(status)
		if err != nil {
			return err
		}
		sum.Counts[st] = count
		sum.Total += count
		return nil
	})
	if err != nil {
		return notification.BatchSummary{}, fmt.Errorf("batch summary: %w", err)
	}
	if sum.Total == 0 {
		return notification.BatchSummary{}, ErrNotFound
	}
	return sum, nil
}

// Cancel moves a pending notification to cancelled with one conditional
// UPDATE (D-016 §5). When nothing changed it re-reads the row: an already
// cancelled notification is returned as is (cancel is idempotent), any other
// status gives an *InvalidStateError, a missing row ErrNotFound.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (notification.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status = 'pending'
		RETURNING `+columns, id))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{}, fmt.Errorf("cancel notification: %w", err)
	}

	n, err = scanNotification(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return notification.Notification{}, ErrNotFound
	case err != nil:
		return notification.Notification{}, fmt.Errorf("cancel notification: %w", err)
	case n.Status == notification.StatusCancelled:
		return n, nil
	default:
		return notification.Notification{}, &InvalidStateError{Status: n.Status}
	}
}

// CancelBatch cancels every pending notification of a batch in one UPDATE
// and returns how many changed; 0 is a valid answer. ErrNotFound means no row
// has this batch.
func (s *Store) CancelBatch(ctx context.Context, batchID uuid.UUID) (int, error) {
	var (
		cancelled int
		exists    bool
	)
	err := s.pool.QueryRow(ctx, `
		WITH cancelled AS (
			UPDATE notifications SET status = 'cancelled', updated_at = now()
			WHERE batch_id = $1 AND status = 'pending'
			RETURNING 1
		)
		SELECT (SELECT count(*) FROM cancelled),
		       EXISTS (SELECT 1 FROM notifications WHERE batch_id = $1)`, batchID).Scan(&cancelled, &exists)
	if err != nil {
		return 0, fmt.Errorf("cancel batch: %w", err)
	}
	if !exists {
		return 0, ErrNotFound
	}
	return cancelled, nil
}
