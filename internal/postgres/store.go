package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
)

// ErrNotFound is returned when the notification or batch does not exist.
var ErrNotFound = errors.New("not found")

// ErrIdempotencyConflict is returned when an idempotency key is reused with
// different content (D-009).
var ErrIdempotencyConflict = errors.New("idempotency key already used with different content")

// InvalidStateError is returned when a notification can't be cancelled in
// its current status (D-016 §5).
type InvalidStateError struct {
	Status notification.Status
}

func (e *InvalidStateError) Error() string {
	return fmt.Sprintf("notification is %s and can no longer be cancelled", e.Status)
}

// Store is the notification storage used by the API.
type Store struct {
	pool        *pgxpool.Pool
	maxAttempts int
}

// NewStore returns a Store. maxAttempts (RETRY_MAX_ATTEMPTS) is stored on
// every new row, so a config change never alters rows already queued (D-010).
func NewStore(pool *pgxpool.Pool, maxAttempts int) *Store {
	return &Store{pool: pool, maxAttempts: maxAttempts}
}

// columns is the select list scanNotification reads, in order.
const columns = `id, batch_id, channel, recipient, content, priority, status,
	idempotency_key, request_hash, correlation_id, scheduled_at, available_at,
	attempts, max_attempts, lease_token, last_error, provider_message_id,
	created_at, updated_at`

func scanNotification(row pgx.Row) (notification.Notification, error) {
	var (
		n               notification.Notification
		channel, status string
		priority        int16
	)
	err := row.Scan(&n.ID, &n.BatchID, &channel, &n.Recipient, &n.Content, &priority, &status,
		&n.IdempotencyKey, &n.RequestHash, &n.CorrelationID, &n.ScheduledAt, &n.AvailableAt,
		&n.Attempts, &n.MaxAttempts, &n.LeaseToken, &n.LastError, &n.ProviderMessageID,
		&n.CreatedAt, &n.UpdatedAt)
	if err != nil {
		return notification.Notification{}, err
	}
	// The table CHECKs make these conversions safe; an error here means the
	// schema and the code disagree.
	if n.Channel, err = notification.ParseChannel(channel); err != nil {
		return notification.Notification{}, err
	}
	if n.Status, err = notification.ParseStatus(status); err != nil {
		return notification.Notification{}, err
	}
	if n.Priority, err = notification.PriorityFromDB(priority); err != nil {
		return notification.Notification{}, err
	}
	n.CreatedAt, n.UpdatedAt, n.AvailableAt = n.CreatedAt.UTC(), n.UpdatedAt.UTC(), n.AvailableAt.UTC()
	if n.ScheduledAt != nil {
		t := n.ScheduledAt.UTC()
		n.ScheduledAt = &t
	}
	return n, nil
}

// correlationID returns the request's correlation ID for the new row, or nil.
func correlationID(ctx context.Context) *string {
	if id, ok := logging.CorrelationID(ctx); ok {
		return &id
	}
	return nil
}

func newID() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("generate id: %w", err)
	}
	return id, nil
}
