package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

const lease = 30 * time.Second

var allPriorities = []notification.Priority{notification.PriorityHigh, notification.PriorityNormal, notification.PriorityLow}

func newQueue(t *testing.T) (*postgres.Queue, *postgres.Store, *pgxpool.Pool) {
	t.Helper()
	s, pool := newStore(t)
	return postgres.NewQueue(pool, lease), s, pool
}

// add creates a due notification of the channel and priority.
func add(t *testing.T, s *postgres.Store, ch notification.Channel, p notification.Priority) uuid.UUID {
	t.Helper()
	d := draft("")
	d.Channel, d.Priority = ch, p
	if ch == notification.ChannelEmail {
		d.Recipient = "a@example.com"
	}
	n, _, err := s.Create(context.Background(), d)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return n.ID
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func claim(t *testing.T, q *postgres.Queue, order ...notification.Priority) postgres.ClaimResult {
	t.Helper()
	if order == nil {
		order = allPriorities
	}
	res, err := q.Claim(context.Background(), notification.ChannelSMS, order)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return res
}

func get(t *testing.T, s *postgres.Store, id uuid.UUID) (notification.Notification, []notification.Attempt) {
	t.Helper()
	n, attempts, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return n, attempts
}

// expire makes a processing row's lease run out.
func expire(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET available_at = now() - interval '1 second' WHERE id = $1`, id)
}

func TestClaimLeasesOneRow(t *testing.T) {
	t.Parallel()
	q, s, _ := newQueue(t)
	id := add(t, s, notification.ChannelSMS, notification.PriorityNormal)

	before := time.Now()
	res := claim(t, q)
	n := res.Claimed
	if n == nil || n.ID != id || res.Reclaimed {
		t.Fatalf("claim = %+v, want row %s", res, id)
	}
	if n.Status != notification.StatusProcessing || n.Attempts != 1 || n.LeaseToken == nil {
		t.Errorf("claimed row: status %s, attempts %d, token %v", n.Status, n.Attempts, n.LeaseToken)
	}
	// available_at is the lease expiry, from the DB clock.
	if d := n.AvailableAt.Sub(before); d < lease-2*time.Second || d > lease+2*time.Second {
		t.Errorf("lease expiry is %v from now, want about %v", d, lease)
	}
	if !n.UpdatedAt.After(n.CreatedAt) {
		t.Errorf("updated_at %v not after created_at %v", n.UpdatedAt, n.CreatedAt)
	}
	if again := claim(t, q); again.Claimed != nil {
		t.Errorf("leased row claimed again: %+v", again.Claimed)
	}
}

func TestClaimChannelIsolation(t *testing.T) {
	t.Parallel()
	q, s, _ := newQueue(t)
	add(t, s, notification.ChannelEmail, notification.PriorityHigh)
	if res := claim(t, q); res.Claimed != nil {
		t.Fatalf("sms claim took an email row: %+v", res.Claimed)
	}
}

func TestClaimConcurrentNeverTwice(t *testing.T) {
	t.Parallel()
	q, s, _ := newQueue(t)
	const rows, claimers = 200, 12
	for i := range rows {
		add(t, s, notification.ChannelSMS, allPriorities[i%3])
	}

	var (
		mu   sync.Mutex
		seen = map[uuid.UUID]int{}
		wg   sync.WaitGroup
	)
	for range claimers {
		wg.Go(func() {
			for {
				res, err := q.Claim(context.Background(), notification.ChannelSMS, allPriorities)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if res.Claimed == nil {
					return
				}
				mu.Lock()
				seen[res.Claimed.ID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(seen) != rows {
		t.Errorf("claimed %d distinct rows, want %d", len(seen), rows)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("row %s claimed %d times", id, n)
		}
	}
}

func TestClaimPriorityOrderAndFallback(t *testing.T) {
	t.Parallel()
	q, s, _ := newQueue(t)
	high := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
	low := add(t, s, notification.ChannelSMS, notification.PriorityLow)

	// The picked priority comes first.
	if res := claim(t, q, notification.PriorityLow, notification.PriorityHigh, notification.PriorityNormal); res.Claimed == nil || res.Claimed.ID != low {
		t.Fatalf("picked low: got %+v, want %s", res.Claimed, low)
	}
	// Nothing normal is due: fall back in the given order.
	if res := claim(t, q, notification.PriorityNormal, notification.PriorityHigh, notification.PriorityLow); res.Claimed == nil || res.Claimed.ID != high {
		t.Fatalf("fallback: got %+v, want %s", res.Claimed, high)
	}
}

func TestClaimOldestDueFirst(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)
	a := add(t, s, notification.ChannelSMS, notification.PriorityNormal)
	b := add(t, s, notification.ChannelSMS, notification.PriorityNormal)
	exec(t, pool, `UPDATE notifications SET available_at = now() - interval '1 minute' WHERE id = $1`, b)

	for _, want := range []uuid.UUID{b, a} {
		if res := claim(t, q); res.Claimed == nil || res.Claimed.ID != want {
			t.Fatalf("got %+v, want %s", res.Claimed, want)
		}
	}
}

func TestClaimSkipsFutureAndFinishedRows(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)
	future := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
	exec(t, pool, `UPDATE notifications SET available_at = now() + interval '1 hour' WHERE id = $1`, future)
	cancelled := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
	if _, err := s.Cancel(context.Background(), cancelled); err != nil {
		t.Fatal(err)
	}
	for _, st := range []string{"sent", "failed"} {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		exec(t, pool, `UPDATE notifications SET status = $2, available_at = now() - interval '1 hour' WHERE id = $1`, id, st)
	}
	exec(t, pool, `UPDATE notifications SET available_at = now() - interval '1 hour' WHERE id = $1`, cancelled)

	if res := claim(t, q); res.Claimed != nil {
		t.Fatalf("claimed %s (%s)", res.Claimed.ID, res.Claimed.Status)
	}
}

func TestClaimReclaimsExpiredLease(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)
	id := add(t, s, notification.ChannelSMS, notification.PriorityNormal)
	first := claim(t, q).Claimed

	expire(t, pool, id)
	res := claim(t, q)
	second := res.Claimed
	if second == nil || second.ID != id || !res.Reclaimed {
		t.Fatalf("reclaim = %+v", res)
	}
	if *second.LeaseToken == *first.LeaseToken || second.Attempts != 2 || second.Status != notification.StatusProcessing {
		t.Errorf("reclaimed row: token %v (was %v), attempts %d, status %s", second.LeaseToken, first.LeaseToken, second.Attempts, second.Status)
	}

	// The lost first attempt is in the history as unknown, started at the
	// first claim.
	_, attempts := get(t, s, id)
	if len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].Outcome != notification.OutcomeUnknown ||
		attempts[0].DurationMS != nil || attempts[0].HTTPStatus != nil || attempts[0].Error == nil {
		t.Fatalf("attempts = %+v", attempts)
	}
	if !attempts[0].StartedAt.Equal(first.UpdatedAt) {
		t.Errorf("lost attempt started_at %v, want the first claim time %v", attempts[0].StartedAt, first.UpdatedAt)
	}

	// The first worker's late result is rejected and writes nothing.
	err := q.Complete(context.Background(), postgres.Completion{
		ID: id, LeaseToken: *first.LeaseToken, Status: notification.StatusSent,
		Attempt: notification.Attempt{AttemptNo: 1, StartedAt: time.Now(), Outcome: notification.OutcomeSuccess},
	})
	if !errors.Is(err, postgres.ErrStaleLease) {
		t.Fatalf("stale Complete = %v, want ErrStaleLease", err)
	}
	n, attempts := get(t, s, id)
	if n.Status != notification.StatusProcessing || len(attempts) != 1 {
		t.Errorf("after stale Complete: status %s, %d attempts", n.Status, len(attempts))
	}

	// The new holder's result is accepted.
	if err := q.Complete(context.Background(), postgres.Completion{
		ID: id, LeaseToken: *second.LeaseToken, Status: notification.StatusSent, ProviderMessageID: ptr("m-2"),
		Attempt: notification.Attempt{AttemptNo: 2, StartedAt: time.Now(), DurationMS: ptr(12), HTTPStatus: ptr(202), Outcome: notification.OutcomeSuccess},
	}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

func TestClaimNeverExceedsMaxAttempts(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)
	id := add(t, s, notification.ChannelSMS, notification.PriorityNormal)
	next := add(t, s, notification.ChannelSMS, notification.PriorityNormal)
	exec(t, pool, `UPDATE notifications SET available_at = now() + interval '1 hour' WHERE id = $1`, next)

	// A worker crashes on every attempt.
	for i := 1; i <= maxAttempts; i++ {
		res := claim(t, q)
		if res.Claimed == nil || res.Claimed.ID != id || res.Claimed.Attempts != i {
			t.Fatalf("claim %d = %+v", i, res.Claimed)
		}
		expire(t, pool, id)
	}

	// The last lease expired: the row is failed, not claimed, and the probe
	// moves on to the next due row.
	exec(t, pool, `UPDATE notifications SET available_at = now() - interval '1 second' WHERE id = $1`, next)
	res := claim(t, q)
	if res.Claimed == nil || res.Claimed.ID != next {
		t.Fatalf("claim after exhaustion = %+v, want %s", res.Claimed, next)
	}
	if len(res.Exhausted) != 1 || res.Exhausted[0].ID != id {
		t.Fatalf("Exhausted = %+v", res.Exhausted)
	}

	n, attempts := get(t, s, id)
	if n.Status != notification.StatusFailed || n.Attempts != maxAttempts || n.LeaseToken != nil || n.LastError == nil {
		t.Errorf("exhausted row: status %s, attempts %d, token %v, last_error %v", n.Status, n.Attempts, n.LeaseToken, n.LastError)
	}
	if len(attempts) != maxAttempts {
		t.Fatalf("%d attempts recorded, want %d", len(attempts), maxAttempts)
	}
	for i, a := range attempts {
		if a.AttemptNo != i+1 || a.Outcome != notification.OutcomeUnknown {
			t.Errorf("attempt %d = %+v", i, a)
		}
	}
}

func TestClaimExhaustedOnlyCommits(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)
	id := add(t, s, notification.ChannelSMS, notification.PriorityLow)
	exec(t, pool, `UPDATE notifications SET status = 'processing', attempts = max_attempts, lease_token = gen_random_uuid(),
		available_at = now() - interval '1 second' WHERE id = $1`, id)

	res := claim(t, q)
	if res.Claimed != nil || len(res.Exhausted) != 1 {
		t.Fatalf("claim = %+v", res)
	}
	if n, attempts := get(t, s, id); n.Status != notification.StatusFailed || len(attempts) != 1 || attempts[0].Outcome != notification.OutcomeUnknown {
		t.Fatalf("row %s with %+v", n.Status, attempts)
	}
}

func complete(t *testing.T, q *postgres.Queue, n *notification.Notification, c postgres.Completion) {
	t.Helper()
	c.ID, c.LeaseToken = n.ID, *n.LeaseToken
	if c.Attempt.AttemptNo == 0 {
		c.Attempt.AttemptNo = n.Attempts
	}
	if c.Attempt.StartedAt.IsZero() {
		c.Attempt.StartedAt = time.Now()
	}
	if err := q.Complete(context.Background(), c); err != nil {
		t.Fatalf("Complete: %v", err)
	}
}

func TestCompleteOutcomes(t *testing.T) {
	t.Parallel()
	q, s, pool := newQueue(t)

	t.Run("sent", func(t *testing.T) {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		c := claim(t, q).Claimed
		complete(t, q, c, postgres.Completion{
			Status: notification.StatusSent, ProviderMessageID: ptr("m-1"),
			Attempt: notification.Attempt{DurationMS: ptr(40), HTTPStatus: ptr(202), Outcome: notification.OutcomeSuccess},
		})
		n, attempts := get(t, s, id)
		if n.Status != notification.StatusSent || n.ProviderMessageID == nil || *n.ProviderMessageID != "m-1" || n.LeaseToken != nil || n.Attempts != 1 {
			t.Errorf("row = %+v", n)
		}
		if len(attempts) != 1 || attempts[0].Outcome != notification.OutcomeSuccess || *attempts[0].HTTPStatus != 202 || *attempts[0].DurationMS != 40 {
			t.Errorf("attempts = %+v", attempts)
		}
	})

	t.Run("retry with backoff", func(t *testing.T) {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		c := claim(t, q).Claimed
		before := time.Now()
		complete(t, q, c, postgres.Completion{
			Status: notification.StatusPending, Delay: 10 * time.Minute,
			Attempt: notification.Attempt{HTTPStatus: ptr(503), Outcome: notification.OutcomeTransient, Error: ptr("provider answered HTTP 503")},
		})
		n, attempts := get(t, s, id)
		if n.Status != notification.StatusPending || n.Attempts != 1 || n.LeaseToken != nil || n.LastError == nil || *n.LastError != "provider answered HTTP 503" {
			t.Errorf("row = %+v", n)
		}
		if d := n.AvailableAt.Sub(before); d < 10*time.Minute-2*time.Second || d > 10*time.Minute+2*time.Second {
			t.Errorf("available_at is %v from now, want about 10m", d)
		}
		if len(attempts) != 1 || attempts[0].Outcome != notification.OutcomeTransient {
			t.Errorf("attempts = %+v", attempts)
		}
		if res := claim(t, q); res.Claimed != nil {
			t.Errorf("row in backoff was claimed")
		}
	})

	t.Run("channel-wide requeue refunds the attempt", func(t *testing.T) {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		c := claim(t, q).Claimed
		complete(t, q, c, postgres.Completion{
			Status: notification.StatusPending, RefundAttempt: true, Delay: 30 * time.Second,
			Attempt: notification.Attempt{HTTPStatus: ptr(429), Outcome: notification.OutcomeChannelWide, Error: ptr("provider answered HTTP 429")},
		})
		n, attempts := get(t, s, id)
		if n.Status != notification.StatusPending || n.Attempts != 0 {
			t.Errorf("status %s attempts %d, want pending with 0", n.Status, n.Attempts)
		}
		if len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].Outcome != notification.OutcomeChannelWide {
			t.Errorf("attempts = %+v", attempts)
		}
		// The next claim uses attempt number 1 again.
		exec(t, pool, `UPDATE notifications SET available_at = now() WHERE id = $1`, id)
		if again := claim(t, q).Claimed; again == nil || again.Attempts != 1 {
			t.Errorf("reclaim after refund = %+v", again)
		}
	})

	t.Run("failed", func(t *testing.T) {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		c := claim(t, q).Claimed
		complete(t, q, c, postgres.Completion{
			Status:  notification.StatusFailed,
			Attempt: notification.Attempt{HTTPStatus: ptr(400), Outcome: notification.OutcomePermanent, Error: ptr("provider answered HTTP 400")},
		})
		n, _ := get(t, s, id)
		if n.Status != notification.StatusFailed || n.LastError == nil || n.LeaseToken != nil {
			t.Errorf("row = %+v", n)
		}
	})

	t.Run("status outside 100-599 is stored as NULL", func(t *testing.T) {
		id := add(t, s, notification.ChannelSMS, notification.PriorityHigh)
		c := claim(t, q).Claimed
		complete(t, q, c, postgres.Completion{
			Status: notification.StatusPending, Delay: time.Minute,
			Attempt: notification.Attempt{HTTPStatus: ptr(999), Outcome: notification.OutcomeUnknown, Error: ptr("provider answered HTTP 999")},
		})
		n, attempts := get(t, s, id)
		if len(attempts) != 1 || attempts[0].HTTPStatus != nil || attempts[0].Error == nil {
			t.Fatalf("attempts = %+v", attempts)
		}
		if want := "provider answered with invalid HTTP status 999: provider answered HTTP 999"; *attempts[0].Error != want || *n.LastError != want {
			t.Errorf("error %q, last_error %q, want %q", *attempts[0].Error, *n.LastError, want)
		}
	})
}

func TestCompleteRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	q, _, _ := newQueue(t)
	for _, c := range []postgres.Completion{
		{Status: notification.StatusProcessing},
		{Status: notification.StatusSent, RefundAttempt: true},
	} {
		if err := q.Complete(context.Background(), c); err == nil || errors.Is(err, postgres.ErrStaleLease) {
			t.Errorf("Complete(%+v) = %v, want an input error", c, err)
		}
	}
}

func TestCompleteOnCancelledOrUnknownRowIsStale(t *testing.T) {
	t.Parallel()
	q, _, _ := newQueue(t)
	err := q.Complete(context.Background(), postgres.Completion{
		ID: uuid.New(), LeaseToken: uuid.New(), Status: notification.StatusSent,
		Attempt: notification.Attempt{AttemptNo: 1, StartedAt: time.Now(), Outcome: notification.OutcomeSuccess},
	})
	if !errors.Is(err, postgres.ErrStaleLease) {
		t.Fatalf("got %v, want ErrStaleLease", err)
	}
}
