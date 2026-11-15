package postgres

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/asrasari/notification-service/internal/notification"
)

// The window a new notification's scheduled_at must fall in, on the DB
// clock (D-021): a minute of grace for client clock skew, and at most 30 days
// ahead to catch typos like year 3026.
const (
	scheduleGrace   = time.Minute
	scheduleHorizon = 30 * 24 * time.Hour
)

// ScheduleError is returned when a new notification's scheduled_at is
// outside the allowed window. TooFar is true when it is more than 30 days
// ahead, false when it is in the past (beyond the minute of grace).
type ScheduleError struct {
	TooFar bool
}

func (e *ScheduleError) Error() string {
	if e.TooFar {
		return "scheduled_at is more than 30 days ahead"
	}
	return "scheduled_at is in the past"
}

// BatchScheduleError lists the batch items whose scheduled_at is outside the
// window. The whole batch was rolled back.
type BatchScheduleError struct {
	Items []BatchScheduleItem
}

// BatchScheduleItem is one rejected batch item.
type BatchScheduleItem struct {
	Index  int
	TooFar bool
}

func (e *BatchScheduleError) Error() string {
	return fmt.Sprintf("scheduled_at outside the allowed window at %d items", len(e.Items))
}

// Create stores one notification. With an idempotency key that already
// exists, it returns the existing row and created=false when the request
// hash matches, or ErrIdempotencyConflict when it doesn't (D-009).
//
// The insert uses ON CONFLICT DO NOTHING, so two concurrent requests with the
// same key are decided by the unique index: one inserts, the other waits for
// it to commit and then reads its row.
//
// A scheduled_at outside the window (see scheduleGrace) is checked inside the
// INSERT, so it only applies when the row would be new: a replay of an
// accepted request after its time still returns the stored row. When nothing
// was inserted and no row has the key, Create returns a *ScheduleError
// (D-021).
func (s *Store) Create(ctx context.Context, d notification.Draft) (n notification.Notification, created bool, err error) {
	id, err := newID()
	if err != nil {
		return notification.Notification{}, false, err
	}
	hash := d.Hash()

	row := s.pool.QueryRow(ctx, `
		INSERT INTO notifications
			(id, channel, recipient, content, priority, idempotency_key, request_hash,
			 scheduled_at, available_at, max_attempts, correlation_id)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8::timestamptz, coalesce($8::timestamptz, now()), $9, $10
		WHERE $8::timestamptz IS NULL
		   OR $8::timestamptz BETWEEN now() - $11::interval AND now() + $12::interval
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+columns,
		id, string(d.Channel), d.Recipient, d.Content, int16(d.Priority), d.IdempotencyKey, hash,
		d.ScheduledAt, s.maxAttempts, correlationID(ctx), scheduleGrace, scheduleHorizon)
	n, err = scanNotification(row)
	if err == nil {
		return n, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return notification.Notification{}, false, fmt.Errorf("insert notification: %w", err)
	}

	// Nothing inserted: either the key exists, or scheduled_at was outside
	// the window for a new row.
	if d.IdempotencyKey != nil {
		n, err = scanNotification(s.pool.QueryRow(ctx,
			`SELECT `+columns+` FROM notifications WHERE idempotency_key = $1`, d.IdempotencyKey))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return notification.Notification{}, false, fmt.Errorf("read existing notification: %w", err)
		}
	}
	if d.IdempotencyKey == nil || errors.Is(err, pgx.ErrNoRows) {
		if d.ScheduledAt == nil { // only the window or the key can stop the insert
			return notification.Notification{}, false, errors.New("insert notification: no row inserted and none found")
		}
		tooFar, err := s.isFuture(ctx, *d.ScheduledAt)
		if err != nil {
			return notification.Notification{}, false, err
		}
		return notification.Notification{}, false, &ScheduleError{TooFar: tooFar}
	}
	if !bytes.Equal(n.RequestHash, hash) {
		return notification.Notification{}, false, ErrIdempotencyConflict
	}
	return n, false, nil
}

// isFuture reports whether t is after now() on the DB clock. A rejected
// scheduled_at is either in the past or too far ahead, so this tells which.
func (s *Store) isFuture(ctx context.Context, t time.Time) (bool, error) {
	var future bool
	if err := s.pool.QueryRow(ctx, `SELECT $1::timestamptz > now()`, t).Scan(&future); err != nil {
		return false, fmt.Errorf("check scheduled_at: %w", err)
	}
	return future, nil
}

// BatchItem is the result for one input item, in input order.
type BatchItem struct {
	ID        uuid.UUID
	Status    notification.Status
	Duplicate bool
	BatchID   *uuid.UUID // the batch the stored row belongs to
}

// BatchResult is the outcome of CreateBatch.
type BatchResult struct {
	// BatchID is the new batch when at least one item was inserted; the
	// original batch when every item is a duplicate from that one batch;
	// nil otherwise (D-016 §6).
	BatchID *uuid.UUID
	// Created is true when at least one item was inserted.
	Created bool
	Items   []BatchItem
}

// BatchConflictError lists the input indexes whose idempotency key exists
// with different content. The whole batch was rolled back.
type BatchConflictError struct {
	Indexes []int
}

func (e *BatchConflictError) Error() string {
	return fmt.Sprintf("idempotency key already used with different content at items %v", e.Indexes)
}

// CreateBatch stores a validated batch all-or-nothing in one transaction
// (D-009, D-013): one INSERT … SELECT FROM unnest(…) ON CONFLICT DO NOTHING,
// then one SELECT for the rows that already existed, whose request hashes are
// compared. Any mismatch rolls everything back with a BatchConflictError.
//
// Rows are inserted sorted by idempotency key (keyless rows last), so two
// concurrent batches with overlapping keys wait on each other's index entries
// in the same order and can't deadlock (D-016 §6).
//
// The scheduled_at window is part of the INSERT, as in Create. A new item
// outside it rejects the whole batch with a *BatchScheduleError, which takes
// precedence over idempotency conflicts (D-021).
func (s *Store) CreateBatch(ctx context.Context, drafts []notification.Draft) (BatchResult, error) {
	batchID, err := newID()
	if err != nil {
		return BatchResult{}, err
	}

	type row struct {
		index int
		id    uuid.UUID
		draft notification.Draft
		hash  []byte
	}
	rows := make([]row, len(drafts))
	for i, d := range drafts {
		id, err := newID()
		if err != nil {
			return BatchResult{}, err
		}
		rows[i] = row{index: i, id: id, draft: d, hash: d.Hash()}
	}
	sorted := slices.Clone(rows)
	slices.SortStableFunc(sorted, func(a, b row) int {
		ka, kb := a.draft.IdempotencyKey, b.draft.IdempotencyKey
		switch {
		case ka == nil && kb == nil:
			return 0
		case ka == nil:
			return 1
		case kb == nil:
			return -1
		}
		return cmp.Compare(*ka, *kb)
	})

	n := len(sorted)
	var (
		ids        = make([]uuid.UUID, n)
		channels   = make([]string, n)
		recipients = make([]string, n)
		contents   = make([]string, n)
		priorities = make([]int16, n)
		keys       = make([]*string, n)
		hashes     = make([][]byte, n)
		scheduled  = make([]*time.Time, n)
	)
	for i, r := range sorted {
		ids[i] = r.id
		channels[i] = string(r.draft.Channel)
		recipients[i] = r.draft.Recipient
		contents[i] = r.draft.Content
		priorities[i] = int16(r.draft.Priority)
		keys[i] = r.draft.IdempotencyKey
		hashes[i] = r.hash
		scheduled[i] = r.draft.ScheduledAt
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BatchResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	// WITH ORDINALITY + ORDER BY keeps the sorted order through the INSERT.
	insertRows, err := tx.Query(ctx, `
		INSERT INTO notifications
			(id, batch_id, channel, recipient, content, priority, idempotency_key,
			 request_hash, scheduled_at, available_at, max_attempts, correlation_id)
		SELECT u.id, $8, u.channel, u.recipient, u.content, u.priority, u.idempotency_key,
		       u.request_hash, u.scheduled_at, coalesce(u.scheduled_at, now()), $9, $10
		FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::smallint[], $6::text[], $7::bytea[], $11::timestamptz[])
		     WITH ORDINALITY AS u(id, channel, recipient, content, priority, idempotency_key, request_hash, scheduled_at, ord)
		WHERE u.scheduled_at IS NULL
		   OR u.scheduled_at BETWEEN now() - $12::interval AND now() + $13::interval
		ORDER BY u.ord
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id`,
		ids, channels, recipients, contents, priorities, keys, hashes,
		batchID, s.maxAttempts, correlationID(ctx), scheduled, scheduleGrace, scheduleHorizon)
	if err != nil {
		return BatchResult{}, fmt.Errorf("insert batch: %w", err)
	}
	inserted, err := pgx.CollectRows(insertRows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return BatchResult{}, fmt.Errorf("insert batch: %w", err)
	}
	isNew := make(map[uuid.UUID]bool, len(inserted))
	for _, id := range inserted {
		isNew[id] = true
	}

	// A row not inserted has a key that already exists, or a scheduled_at
	// outside the window (or both).
	var dupKeys []string
	for _, r := range rows {
		if !isNew[r.id] && r.draft.IdempotencyKey != nil {
			dupKeys = append(dupKeys, *r.draft.IdempotencyKey)
		}
	}
	type existing struct {
		id      uuid.UUID
		batchID *uuid.UUID
		status  string
		hash    []byte
	}
	byKey := make(map[string]existing, len(dupKeys))
	if len(dupKeys) > 0 {
		exRows, err := tx.Query(ctx, `
			SELECT idempotency_key, id, batch_id, status, request_hash
			FROM notifications WHERE idempotency_key = ANY($1)`, dupKeys)
		if err != nil {
			return BatchResult{}, fmt.Errorf("read existing notifications: %w", err)
		}
		var (
			key string
			e   existing
		)
		_, err = pgx.ForEachRow(exRows, []any{&key, &e.id, &e.batchID, &e.status, &e.hash}, func() error {
			// The scan targets are reused for every row, so copy what they point to.
			c := e
			if e.batchID != nil {
				b := *e.batchID
				c.batchID = &b
			}
			c.hash = bytes.Clone(e.hash)
			byKey[key] = c
			return nil
		})
		if err != nil {
			return BatchResult{}, fmt.Errorf("read existing notifications: %w", err)
		}
	}

	// now() is the transaction's start time, the same instant the INSERT
	// compared against.
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		return BatchResult{}, fmt.Errorf("read database time: %w", err)
	}

	res := BatchResult{Items: make([]BatchItem, len(rows))}
	var (
		conflicts   []int
		outOfWindow []BatchScheduleItem
	)
	for _, r := range rows {
		if isNew[r.id] {
			res.Items[r.index] = BatchItem{ID: r.id, Status: notification.StatusPending, BatchID: &batchID}
			continue
		}
		var (
			e  existing
			ok bool
		)
		if r.draft.IdempotencyKey != nil {
			e, ok = byKey[*r.draft.IdempotencyKey]
		}
		if !ok {
			if r.draft.ScheduledAt == nil {
				return BatchResult{}, fmt.Errorf("item %d was neither inserted nor found", r.index)
			}
			outOfWindow = append(outOfWindow, BatchScheduleItem{Index: r.index, TooFar: r.draft.ScheduledAt.After(dbNow)})
			continue
		}
		if !bytes.Equal(e.hash, r.hash) {
			conflicts = append(conflicts, r.index)
			continue
		}
		status, err := notification.ParseStatus(e.status)
		if err != nil {
			return BatchResult{}, err
		}
		res.Items[r.index] = BatchItem{ID: e.id, Status: status, Duplicate: true, BatchID: e.batchID}
	}
	if len(outOfWindow) > 0 {
		slices.SortFunc(outOfWindow, func(a, b BatchScheduleItem) int { return cmp.Compare(a.Index, b.Index) })
		return BatchResult{}, &BatchScheduleError{Items: outOfWindow} // deferred Rollback undoes the inserts
	}
	if len(conflicts) > 0 {
		slices.Sort(conflicts)
		return BatchResult{}, &BatchConflictError{Indexes: conflicts}
	}
	if err := tx.Commit(ctx); err != nil {
		return BatchResult{}, fmt.Errorf("commit batch: %w", err)
	}

	if len(inserted) > 0 {
		res.BatchID, res.Created = &batchID, true
	} else {
		res.BatchID = commonBatchID(res.Items)
	}
	return res, nil
}

// commonBatchID returns the batch every item belongs to, or nil when they
// belong to different batches (or to none).
func commonBatchID(items []BatchItem) *uuid.UUID {
	first := items[0].BatchID
	if first == nil {
		return nil
	}
	for _, it := range items[1:] {
		if it.BatchID == nil || *it.BatchID != *first {
			return nil
		}
	}
	id := *first
	return &id
}
