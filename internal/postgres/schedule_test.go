package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

func scheduled(key string, at time.Time) notification.Draft {
	d := draft(key)
	t := at.UTC().Truncate(time.Microsecond)
	d.ScheduledAt = &t
	return d
}

func TestCreateScheduled(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	at := time.Now().Add(time.Hour)

	n, created, err := s.Create(ctx, scheduled("s-1", at))
	if err != nil || !created {
		t.Fatalf("Create = %v, %v", created, err)
	}
	want := at.UTC().Truncate(time.Microsecond)
	if n.ScheduledAt == nil || !n.ScheduledAt.Equal(want) || !n.AvailableAt.Equal(want) {
		t.Errorf("scheduled_at %v, available_at %v, want both %v", n.ScheduledAt, n.AvailableAt, want)
	}
	if next := n.NextAttemptAt(); next == nil || !next.Equal(want) {
		t.Errorf("next_attempt_at = %v", next)
	}

	// Not claimable before its time.
	q := postgres.NewQueue(pool, 30*time.Second)
	if res, err := q.Claim(ctx, notification.ChannelSMS, allPriorities); err != nil || res.Claimed != nil {
		t.Fatalf("claimed a scheduled row early: %+v, %v", res.Claimed, err)
	}

	// A replay hashes the same; a different time with the same key is a conflict.
	if _, created, err := s.Create(ctx, scheduled("s-1", at)); err != nil || created {
		t.Errorf("replay = %v, %v; want the stored row", created, err)
	}
	if _, _, err := s.Create(ctx, scheduled("s-1", at.Add(time.Minute))); !errors.Is(err, postgres.ErrIdempotencyConflict) {
		t.Errorf("other time, same key = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCreateScheduledWindow(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name    string
		at      time.Time
		wantErr *postgres.ScheduleError // nil = accepted
	}{
		{"within the minute of grace", now.Add(-30 * time.Second), nil},
		{"past", now.Add(-2 * time.Minute), &postgres.ScheduleError{TooFar: false}},
		{"29 days ahead", now.Add(29 * 24 * time.Hour), nil},
		{"31 days ahead", now.Add(31 * 24 * time.Hour), &postgres.ScheduleError{TooFar: true}},
	}
	for _, tt := range tests {
		for _, key := range []string{"", "w-" + tt.name} {
			_, _, err := s.Create(ctx, scheduled(key, tt.at))
			var se *postgres.ScheduleError
			switch {
			case tt.wantErr == nil && err != nil:
				t.Errorf("%s (key %q): %v", tt.name, key, err)
			case tt.wantErr != nil && (!errors.As(err, &se) || se.TooFar != tt.wantErr.TooFar):
				t.Errorf("%s (key %q): got %v, want %v", tt.name, key, err, tt.wantErr)
			}
		}
	}
}

// TestCreateScheduledReplayAfterItsTime: an accepted request repeated after
// its time has passed returns the stored row (200), not a 422.
func TestCreateScheduledReplayAfterItsTime(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	d := scheduled("late-1", time.Now().Add(-2*time.Hour))

	// The row as it would be had it been created two hours earlier.
	exec(t, pool, `INSERT INTO notifications (id, channel, recipient, content, priority, idempotency_key,
		request_hash, scheduled_at, available_at, max_attempts, status)
		VALUES (gen_random_uuid(), 'sms', $1, $2, 2, $3, $4, $5, $5, 6, 'sent')`,
		d.Recipient, d.Content, *d.IdempotencyKey, d.Hash(), *d.ScheduledAt)

	n, created, err := s.Create(ctx, d)
	if err != nil || created || n.Status != notification.StatusSent {
		t.Fatalf("replay after its time = %+v, %v, %v; want the stored sent row", n.Status, created, err)
	}
	// Without the stored row, the same request is rejected.
	other := d
	other.IdempotencyKey = ptr("late-2")
	var se *postgres.ScheduleError
	if _, _, err := s.Create(ctx, other); !errors.As(err, &se) || se.TooFar {
		t.Fatalf("new past request = %v, want ScheduleError{TooFar: false}", err)
	}
}

func TestCreateScheduledConcurrent(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	d := scheduled("conc-1", time.Now().Add(time.Hour))

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	for range 20 {
		wg.Go(func() {
			_, c, err := s.Create(context.Background(), d)
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			if c {
				mu.Lock()
				created++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if created != 1 || count(t, pool, "idempotency_key = 'conc-1'") != 1 {
		t.Fatalf("created %d times, %d rows; want exactly one", created, count(t, pool, "idempotency_key = 'conc-1'"))
	}
}

func TestCreateBatchScheduled(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	now := time.Now()

	// One past and one too-far item reject the whole batch, naming both.
	_, err := s.CreateBatch(ctx, []notification.Draft{
		scheduled("b-1", now.Add(time.Hour)),
		scheduled("", now.Add(-time.Hour)),
		draft("b-3"),
		scheduled("b-4", now.Add(40*24*time.Hour)),
	})
	var be *postgres.BatchScheduleError
	if !errors.As(err, &be) {
		t.Fatalf("err = %v, want BatchScheduleError", err)
	}
	if len(be.Items) != 2 || be.Items[0] != (postgres.BatchScheduleItem{Index: 1}) || be.Items[1] != (postgres.BatchScheduleItem{Index: 3, TooFar: true}) {
		t.Errorf("items = %+v", be.Items)
	}
	if n := count(t, pool, "true"); n != 0 {
		t.Fatalf("%d rows stored, want none (all or nothing)", n)
	}

	// A valid batch stores available_at = scheduled_at.
	at := now.Add(2 * time.Hour)
	res, err := s.CreateBatch(ctx, []notification.Draft{scheduled("b-1", at), draft("")})
	if err != nil || !res.Created {
		t.Fatalf("CreateBatch = %+v, %v", res, err)
	}
	n, _, _ := s.Get(ctx, res.Items[0].ID)
	if want := at.UTC().Truncate(time.Microsecond); !n.AvailableAt.Equal(want) || !n.ScheduledAt.Equal(want) {
		t.Errorf("available_at %v, scheduled_at %v, want %v", n.AvailableAt, n.ScheduledAt, want)
	}
	if m, _, _ := s.Get(ctx, res.Items[1].ID); m.ScheduledAt != nil {
		t.Errorf("unscheduled item has scheduled_at %v", m.ScheduledAt)
	}

	// A batch item whose time has passed but whose key is stored is a duplicate.
	old := scheduled("b-old", now.Add(-2*time.Hour))
	exec(t, pool, `INSERT INTO notifications (id, channel, recipient, content, priority, idempotency_key,
		request_hash, scheduled_at, available_at, max_attempts, status)
		VALUES (gen_random_uuid(), 'sms', $1, $2, 2, $3, $4, $5, $5, 6, 'sent')`,
		old.Recipient, old.Content, *old.IdempotencyKey, old.Hash(), *old.ScheduledAt)
	res, err = s.CreateBatch(ctx, []notification.Draft{old, draft("b-new")})
	if err != nil || !res.Items[0].Duplicate || res.Items[1].Duplicate {
		t.Fatalf("batch with a stored past item = %+v, %v", res, err)
	}
}
