package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/testdb"
)

func TestMain(m *testing.M) { testdb.Main(m) }

const maxAttempts = 6

// newStore returns a Store on a fresh database, plus the pool for direct SQL.
func newStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.New(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewStore(pool, maxAttempts), pool
}

func ptr[T any](v T) *T { return &v }

func draft(key string) notification.Draft {
	d := notification.Draft{
		Channel:   notification.ChannelSMS,
		Recipient: "+905551234567",
		Content:   "Hello",
		Priority:  notification.PriorityNormal,
	}
	if key != "" {
		d.IdempotencyKey = ptr(key)
	}
	return d
}

func count(t *testing.T, pool *pgxpool.Pool, where string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications WHERE `+where, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestCreate(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := logging.WithCorrelationID(context.Background(), "corr-1")

	before := time.Now().Add(-time.Second)
	n, created, err := s.Create(ctx, draft("k1"))
	if err != nil || !created {
		t.Fatalf("Create = created %v, err %v", created, err)
	}
	if n.Status != notification.StatusPending || n.Attempts != 0 || n.MaxAttempts != maxAttempts {
		t.Errorf("new row: status %s attempts %d max %d", n.Status, n.Attempts, n.MaxAttempts)
	}
	if n.CorrelationID == nil || *n.CorrelationID != "corr-1" {
		t.Errorf("correlation_id = %v, want corr-1", n.CorrelationID)
	}
	if n.AvailableAt.Before(before) || n.AvailableAt.After(time.Now().Add(time.Second)) {
		t.Errorf("available_at = %v, want about now", n.AvailableAt)
	}
	if n.ID.Version() != 7 || n.BatchID != nil {
		t.Errorf("id %s (v%d), batch_id %v", n.ID, n.ID.Version(), n.BatchID)
	}

	t.Run("same key and content returns the existing row", func(t *testing.T) {
		again, created, err := s.Create(ctx, draft("k1"))
		if err != nil || created || again.ID != n.ID {
			t.Fatalf("got id %s created %v err %v; want existing %s", again.ID, created, err, n.ID)
		}
	})
	t.Run("same key with other content conflicts", func(t *testing.T) {
		d := draft("k1")
		d.Content = "Different"
		if _, _, err := s.Create(ctx, d); !errors.Is(err, postgres.ErrIdempotencyConflict) {
			t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
		}
	})
	t.Run("no key never deduplicates", func(t *testing.T) {
		a, _, err1 := s.Create(ctx, draft(""))
		b, _, err2 := s.Create(ctx, draft(""))
		if err1 != nil || err2 != nil || a.ID == b.ID {
			t.Fatalf("got %s and %s, errors %v %v", a.ID, b.ID, err1, err2)
		}
	})
	if got := count(t, pool, "idempotency_key = 'k1'"); got != 1 {
		t.Errorf("rows with k1 = %d, want 1", got)
	}
}

// D-017: concurrent creates with the same idempotency key → one row.
func TestCreateConcurrentSameKey(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	const workers = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = map[uuid.UUID]bool{}
		created int
	)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			n, c, err := s.Create(context.Background(), draft("same-key"))
			if err != nil {
				t.Errorf("Create: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[n.ID] = true
			if c {
				created++
			}
		})
	}
	close(start)
	wg.Wait()

	if len(ids) != 1 || created != 1 {
		t.Errorf("got %d distinct ids and %d creates, want 1 and 1", len(ids), created)
	}
	if got := count(t, pool, "idempotency_key = 'same-key'"); got != 1 {
		t.Errorf("rows = %d, want 1", got)
	}
}

func TestCreateBatch(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	first, err := s.CreateBatch(ctx, []notification.Draft{draft("b"), draft(""), draft("a")})
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if !first.Created || first.BatchID == nil || len(first.Items) != 3 {
		t.Fatalf("first batch: %+v", first)
	}
	for i, it := range first.Items {
		if it.Duplicate || it.Status != notification.StatusPending || it.BatchID == nil || *it.BatchID != *first.BatchID {
			t.Errorf("item %d: %+v", i, it)
		}
	}
	if got := count(t, pool, "batch_id = $1 AND max_attempts = $2", *first.BatchID, maxAttempts); got != 3 {
		t.Errorf("stored rows = %d, want 3", got)
	}

	t.Run("full replay returns the original batch", func(t *testing.T) {
		replay, err := s.CreateBatch(ctx, []notification.Draft{draft("a"), draft("b")})
		if err != nil {
			t.Fatal(err)
		}
		if replay.Created || replay.BatchID == nil || *replay.BatchID != *first.BatchID {
			t.Fatalf("replay batch_id %v created %v, want %s", replay.BatchID, replay.Created, *first.BatchID)
		}
		// Results follow input order, not the key-sorted insert order.
		if replay.Items[0].ID != first.Items[2].ID || replay.Items[1].ID != first.Items[0].ID {
			t.Errorf("replay items out of order: %+v", replay.Items)
		}
		for _, it := range replay.Items {
			if !it.Duplicate {
				t.Errorf("item not marked duplicate: %+v", it)
			}
		}
	})

	t.Run("mix of new and duplicate gets a new batch", func(t *testing.T) {
		mixed, err := s.CreateBatch(ctx, []notification.Draft{draft("a"), draft("c")})
		if err != nil {
			t.Fatal(err)
		}
		if !mixed.Created || *mixed.BatchID == *first.BatchID {
			t.Fatalf("mixed: %+v", mixed)
		}
		if !mixed.Items[0].Duplicate || *mixed.Items[0].BatchID != *first.BatchID {
			t.Errorf("duplicate item should keep its own batch: %+v", mixed.Items[0])
		}
		if mixed.Items[1].Duplicate || *mixed.Items[1].BatchID != *mixed.BatchID {
			t.Errorf("new item: %+v", mixed.Items[1])
		}
	})

	t.Run("duplicates from different batches have no batch id", func(t *testing.T) {
		res, err := s.CreateBatch(ctx, []notification.Draft{draft("b"), draft("c")})
		if err != nil {
			t.Fatal(err)
		}
		if res.Created || res.BatchID != nil {
			t.Fatalf("got batch_id %v created %v, want nil", res.BatchID, res.Created)
		}
	})

	t.Run("a content mismatch rolls the whole batch back", func(t *testing.T) {
		changed := draft("b")
		changed.Content = "Different"
		before := count(t, pool, "true")
		_, err := s.CreateBatch(ctx, []notification.Draft{draft("new-1"), changed, draft(""), draft("a")})
		var conflict *postgres.BatchConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("err = %v, want *BatchConflictError", err)
		}
		if diff := cmp.Diff([]int{1}, conflict.Indexes); diff != "" {
			t.Errorf("indexes (-want +got):\n%s", diff)
		}
		if after := count(t, pool, "true"); after != before {
			t.Errorf("rows %d → %d; nothing should be stored", before, after)
		}
	})
}

// D-017: two overlapping batches in opposite order, run concurrently, must
// neither deadlock nor store a key twice. Repeated to give the race a chance.
func TestCreateBatchOverlappingNoDeadlock(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)

	for round := range 20 {
		keys := make([]string, 50)
		for i := range keys {
			keys[i] = fmt.Sprintf("r%d-k%03d", round, i)
		}
		forward := make([]notification.Draft, len(keys))
		backward := make([]notification.Draft, len(keys))
		for i, k := range keys {
			forward[i] = draft(k)
			backward[len(keys)-1-i] = draft(k)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var (
			wg      sync.WaitGroup
			results [2]postgres.BatchResult
			errs    [2]error
		)
		start := make(chan struct{})
		for i, batch := range [][]notification.Draft{forward, backward} {
			wg.Go(func() {
				<-start
				results[i], errs[i] = s.CreateBatch(ctx, batch)
			})
		}
		close(start)
		wg.Wait()
		cancel()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d batch %d: %v", round, i, err) // a deadlock shows up as SQLSTATE 40P01
			}
		}
		// Both batches see the same row for each key.
		for i := range keys {
			a, b := results[0].Items[i], results[1].Items[len(keys)-1-i]
			if a.ID != b.ID {
				t.Fatalf("round %d key %s: ids %s and %s differ", round, keys[i], a.ID, b.ID)
			}
			if a.Duplicate == b.Duplicate {
				t.Fatalf("round %d key %s: exactly one batch should have inserted it", round, keys[i])
			}
		}
		if got := count(t, pool, "idempotency_key LIKE $1", fmt.Sprintf("r%d-%%", round)); got != len(keys) {
			t.Fatalf("round %d: %d rows, want %d", round, got, len(keys))
		}
	}
}

func TestGetWithAttempts(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	if _, _, err := s.Get(ctx, uuid.Must(uuid.NewV7())); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing id: err = %v, want ErrNotFound", err)
	}

	n, _, err := s.Create(ctx, draft(""))
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `
		INSERT INTO delivery_attempts (id, notification_id, attempt_no, started_at, duration_ms, http_status, outcome, error) VALUES
		($1, $3, 2, $5, 40, 202, 'success', NULL),
		($2, $3, 1, $4, NULL, NULL, 'unknown', 'timeout')`,
		uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), n.ID, t0, t0.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	got, attempts, err := s.Get(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != n.ID {
		t.Errorf("id = %s, want %s", got.ID, n.ID)
	}
	want := []notification.Attempt{
		{AttemptNo: 1, StartedAt: t0, Outcome: notification.OutcomeUnknown, Error: ptr("timeout")},
		{AttemptNo: 2, StartedAt: t0.Add(time.Minute), DurationMS: ptr(40), HTTPStatus: ptr(202), Outcome: notification.OutcomeSuccess},
	}
	if diff := cmp.Diff(want, attempts); diff != "" {
		t.Errorf("attempts (-want +got):\n%s", diff)
	}
}

func TestBatchSummary(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	if _, err := s.BatchSummary(ctx, uuid.Must(uuid.NewV7())); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing batch: err = %v, want ErrNotFound", err)
	}
	res, err := s.CreateBatch(ctx, []notification.Draft{draft(""), draft(""), draft("")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status = 'sent' WHERE id = $1`, res.Items[0].ID); err != nil {
		t.Fatal(err)
	}

	sum, err := s.BatchSummary(ctx, *res.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	want := notification.BatchSummary{BatchID: *res.BatchID, Total: 3, Counts: map[notification.Status]int{
		notification.StatusPending: 2, notification.StatusProcessing: 0, notification.StatusSent: 1,
		notification.StatusFailed: 0, notification.StatusCancelled: 0,
	}}
	if diff := cmp.Diff(want, sum); diff != "" {
		t.Errorf("summary (-want +got):\n%s", diff)
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	if _, err := s.Cancel(ctx, uuid.Must(uuid.NewV7())); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing id: err = %v, want ErrNotFound", err)
	}

	n, _, err := s.Create(ctx, draft(""))
	if err != nil {
		t.Fatal(err)
	}
	// Backdate updated_at so the change made by Cancel is visible.
	if _, err := pool.Exec(ctx, `UPDATE notifications SET updated_at = now() - interval '1 hour' WHERE id = $1`, n.ID); err != nil {
		t.Fatal(err)
	}
	c, err := s.Cancel(ctx, n.ID)
	if err != nil || c.Status != notification.StatusCancelled {
		t.Fatalf("cancel pending: status %s err %v", c.Status, err)
	}
	if time.Since(c.UpdatedAt) > time.Minute {
		t.Errorf("updated_at not refreshed: %v", c.UpdatedAt)
	}

	again, err := s.Cancel(ctx, n.ID)
	if err != nil || again.Status != notification.StatusCancelled || !again.UpdatedAt.Equal(c.UpdatedAt) {
		t.Fatalf("cancel again: status %s err %v; want unchanged cancelled row", again.Status, err)
	}

	for _, st := range []notification.Status{notification.StatusProcessing, notification.StatusSent, notification.StatusFailed} {
		t.Run(string(st), func(t *testing.T) {
			n, _, err := s.Create(ctx, draft(""))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE notifications SET status = $2 WHERE id = $1`, n.ID, string(st)); err != nil {
				t.Fatal(err)
			}
			_, err = s.Cancel(ctx, n.ID)
			var invalid *postgres.InvalidStateError
			if !errors.As(err, &invalid) || invalid.Status != st {
				t.Fatalf("err = %v, want InvalidStateError{%s}", err, st)
			}
		})
	}
}

func TestCancelBatch(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	if _, err := s.CancelBatch(ctx, uuid.Must(uuid.NewV7())); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("missing batch: err = %v, want ErrNotFound", err)
	}
	res, err := s.CreateBatch(ctx, []notification.Draft{draft(""), draft(""), draft("")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status = 'sent' WHERE id = $1`, res.Items[0].ID); err != nil {
		t.Fatal(err)
	}

	n, err := s.CancelBatch(ctx, *res.BatchID)
	if err != nil || n != 2 {
		t.Fatalf("CancelBatch = %d, %v; want 2", n, err)
	}
	if got := count(t, pool, "batch_id = $1 AND status = 'cancelled' AND updated_at > now() - interval '1 minute'", *res.BatchID); got != 2 {
		t.Errorf("cancelled rows = %d, want 2", got)
	}
	n, err = s.CancelBatch(ctx, *res.BatchID)
	if err != nil || n != 0 {
		t.Fatalf("second CancelBatch = %d, %v; want 0 and no error", n, err)
	}
}
