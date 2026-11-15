package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/notification"
)

// ErrStaleLease is returned by Complete when the row no longer carries the
// caller's lease token: the lease expired and another claim took the row (or
// finished it). Nothing was written.
var ErrStaleLease = errors.New("lease is no longer held")

// lostLeaseError is recorded when an expired lease is found: the worker that
// held it crashed or stalled, so whether the provider got the message is
// unknown (D-009).
const lostLeaseError = "the lease expired before a result was written (worker crash or stall); outcome unknown"

// Queue is the worker's side of the notifications table: claiming due rows
// under a lease and writing the outcome of each send (D-005, D-007, D-008).
type Queue struct {
	pool  *pgxpool.Pool
	lease time.Duration
}

// NewQueue returns a Queue whose claims hold a row for lease
// (LEASE_DURATION, always longer than PROVIDER_TIMEOUT).
func NewQueue(pool *pgxpool.Pool, lease time.Duration) *Queue {
	return &Queue{pool: pool, lease: lease}
}

// ClaimResult is what one Claim did.
type ClaimResult struct {
	// Claimed is the row now leased to the caller, or nil when nothing was due.
	// It is in status processing, with attempts already incremented and a new
	// LeaseToken.
	Claimed *notification.Notification
	// Reclaimed is true when Claimed came from an expired lease.
	Reclaimed bool
	// Exhausted lists expired-lease rows that had no attempts left and were
	// failed on the way (R-M1).
	Exhausted []notification.Notification
}

// Claim leases one due row of the channel, trying the priorities in the given
// order and stopping at the first hit (D-006, D-007 amended). It runs in one
// short transaction that is committed before the caller contacts the provider
// (D-005).
//
// Each probe is its own SELECT … FOR UPDATE SKIP LOCKED with equality on
// priority, so it uses the claim index and stops at the first future row.
// Postgres does not allow FOR UPDATE with UNION, hence one probe per priority.
//
// An expired lease is reclaimed directly (processing → processing) and its
// lost attempt recorded as unknown. If that attempt was the last one, the row
// is failed instead and the probe continues (R-M1), so attempts never pass
// max_attempts.
func (q *Queue) Claim(ctx context.Context, channel notification.Channel, priorities []notification.Priority) (ClaimResult, error) {
	var res ClaimResult
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("claim: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit

	for _, p := range priorities {
		for {
			var (
				id                    uuid.UUID
				status                string
				attempts, maxAttempts int
			)
			err := tx.QueryRow(ctx, `
				SELECT id, status, attempts, max_attempts
				FROM notifications
				WHERE channel = $1 AND priority = $2
				  AND status IN ('pending', 'processing')
				  AND available_at <= now()
				ORDER BY available_at
				LIMIT 1
				FOR UPDATE SKIP LOCKED`, string(channel), int16(p)).Scan(&id, &status, &attempts, &maxAttempts)
			if errors.Is(err, pgx.ErrNoRows) {
				break // try the next priority
			}
			if err != nil {
				return res, fmt.Errorf("claim: probe: %w", err)
			}

			expired := status == string(notification.StatusProcessing)
			if expired {
				if err := recordLostAttempt(ctx, tx, id, attempts); err != nil {
					return res, err
				}
			}
			if attempts >= maxAttempts {
				n, err := failExhausted(ctx, tx, id)
				if err != nil {
					return res, err
				}
				res.Exhausted = append(res.Exhausted, n)
				continue // the same priority may have more due rows
			}

			token, err := uuid.NewRandom()
			if err != nil {
				return res, fmt.Errorf("claim: lease token: %w", err)
			}
			n, err := scanNotification(tx.QueryRow(ctx, `
				UPDATE notifications
				SET status = 'processing',
				    attempts = attempts + 1,
				    lease_token = $2,
				    available_at = now() + $3::interval,
				    last_error = CASE WHEN $4 THEN $5 ELSE last_error END,
				    updated_at = now()
				WHERE id = $1
				RETURNING `+columns, id, token, q.lease, expired, lostLeaseError))
			if err != nil {
				return res, fmt.Errorf("claim: lease row: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return ClaimResult{}, fmt.Errorf("claim: commit: %w", err)
			}
			res.Claimed, res.Reclaimed = &n, expired
			return res, nil
		}
	}

	// Nothing to lease; commit anyway so exhausted rows stay failed.
	if err := tx.Commit(ctx); err != nil {
		return ClaimResult{}, fmt.Errorf("claim: commit: %w", err)
	}
	return res, nil
}

// recordLostAttempt writes the unknown attempt of an expired lease. The claim
// set updated_at, and nothing else touches a processing row, so updated_at is
// when the lost attempt started.
func recordLostAttempt(ctx context.Context, tx pgx.Tx, id uuid.UUID, attemptNo int) error {
	aid, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_attempts (id, notification_id, attempt_no, started_at, duration_ms, http_status, outcome, error)
		SELECT $1, id, $3, updated_at, NULL, NULL, 'unknown', $4
		FROM notifications WHERE id = $2`, aid, id, max(attemptNo, 1), lostLeaseError)
	if err != nil {
		return fmt.Errorf("claim: record lost attempt: %w", err)
	}
	return nil
}

func failExhausted(ctx context.Context, tx pgx.Tx, id uuid.UUID) (notification.Notification, error) {
	n, err := scanNotification(tx.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'failed', lease_token = NULL, last_error = $2, updated_at = now()
		WHERE id = $1
		RETURNING `+columns, id, lostLeaseError))
	if err != nil {
		return n, fmt.Errorf("claim: fail exhausted row: %w", err)
	}
	return n, nil
}

// Completion is the result of one send, to be written by Complete.
type Completion struct {
	ID         uuid.UUID
	LeaseToken uuid.UUID
	// Status is the next status: sent, pending (retry or requeue) or failed.
	Status notification.Status
	// RefundAttempt gives the attempt back (attempts - 1). Only for a
	// channel-wide outcome returned to pending (D-010 §2).
	RefundAttempt bool
	// Delay sets available_at = now() + Delay when Status is pending: the
	// backoff, the 429 pause or BREAKER_OPEN_TIMEOUT (R-M1 Q-004).
	Delay time.Duration
	// ProviderMessageID is stored when Status is sent.
	ProviderMessageID *string
	// Attempt is the delivery_attempts row. Its Error also becomes
	// last_error; a sent row gets last_error = NULL instead, since the
	// attempt history keeps every error (R-M3).
	Attempt notification.Attempt
}

// Complete writes a send's outcome and its attempt row in one transaction,
// only if the caller still holds the lease (D-008 rule 2). Otherwise it
// writes nothing and returns ErrStaleLease. Times come from the DB clock.
func (q *Queue) Complete(ctx context.Context, c Completion) error {
	switch c.Status {
	case notification.StatusSent, notification.StatusFailed:
		if c.RefundAttempt {
			return fmt.Errorf("complete: a %s row can't refund its attempt", c.Status)
		}
	case notification.StatusPending:
	default:
		return fmt.Errorf("complete: invalid next status %q", c.Status)
	}

	refund := 0
	if c.RefundAttempt {
		refund = 1
	}
	var lastError *string
	if c.Status != notification.StatusSent {
		lastError = c.Attempt.Error
	}

	// http_status is NULL outside 100–599 (the column CHECK); the raw code
	// goes into the error text instead (R-M1 Q-005).
	httpStatus, attemptErr := c.Attempt.HTTPStatus, c.Attempt.Error
	if httpStatus != nil && (*httpStatus < 100 || *httpStatus > 599) {
		msg := fmt.Sprintf("provider answered with invalid HTTP status %d", *httpStatus)
		if attemptErr != nil {
			msg += ": " + *attemptErr
		}
		httpStatus, attemptErr = nil, &msg
		if lastError != nil {
			lastError = &msg
		}
	}

	aid, err := newID()
	if err != nil {
		return err
	}

	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("complete: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // a no-op after Commit

	tag, err := tx.Exec(ctx, `
		UPDATE notifications
		SET status = $3,
		    attempts = attempts - $4,
		    available_at = CASE WHEN $3 = 'pending' THEN now() + $5::interval ELSE available_at END,
		    lease_token = NULL,
		    last_error = CASE WHEN $3 = 'sent' THEN NULL ELSE COALESCE($6, last_error) END,
		    provider_message_id = COALESCE($7, provider_message_id),
		    updated_at = now()
		WHERE id = $1 AND lease_token = $2 AND status = 'processing'`,
		c.ID, c.LeaseToken, string(c.Status), refund, c.Delay, lastError, c.ProviderMessageID)
	if err != nil {
		return fmt.Errorf("complete: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleLease
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_attempts (id, notification_id, attempt_no, started_at, duration_ms, http_status, outcome, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		aid, c.ID, c.Attempt.AttemptNo, c.Attempt.StartedAt, c.Attempt.DurationMS, httpStatus, string(c.Attempt.Outcome), attemptErr)
	if err != nil {
		return fmt.Errorf("complete: insert attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("complete: commit: %w", err)
	}
	return nil
}
