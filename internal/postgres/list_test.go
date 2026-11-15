package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

func ids(list []notification.Notification) []uuid.UUID {
	out := make([]uuid.UUID, len(list))
	for i, n := range list {
		out[i] = n.ID
	}
	return out
}

func TestListFilters(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	mk := func(ch notification.Channel, status notification.Status, created time.Time) uuid.UUID {
		t.Helper()
		d := draft("")
		d.Channel = ch
		if ch == notification.ChannelEmail {
			d.Recipient = "ann@example.com"
		}
		n, _, err := s.Create(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE notifications SET status = $2, created_at = $3 WHERE id = $1`, n.ID, string(status), created); err != nil {
			t.Fatal(err)
		}
		return n.ID
	}
	a := mk(notification.ChannelSMS, notification.StatusPending, t0)
	b := mk(notification.ChannelEmail, notification.StatusSent, t0.Add(1*time.Hour))
	c := mk(notification.ChannelSMS, notification.StatusSent, t0.Add(2*time.Hour))
	d := mk(notification.ChannelEmail, notification.StatusFailed, t0.Add(3*time.Hour))

	batch, err := s.CreateBatch(ctx, []notification.Draft{draft(""), draft("")})
	if err != nil {
		t.Fatal(err)
	}
	e, f := batch.Items[0].ID, batch.Items[1].ID
	// Same created_at for both: the order falls back to id DESC.
	if _, err := pool.Exec(ctx, `UPDATE notifications SET created_at = $2 WHERE batch_id = $1`, *batch.BatchID, t0.Add(4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	newerOf, olderOf := f, e
	if e.String() > f.String() {
		newerOf, olderOf = e, f
	}

	sent := notification.StatusSent
	email := notification.ChannelEmail
	from, to := t0.Add(1*time.Hour), t0.Add(3*time.Hour)

	tests := []struct {
		name   string
		filter postgres.ListFilter
		want   []uuid.UUID
	}{
		{"no filter, newest first, id breaks ties", postgres.ListFilter{}, []uuid.UUID{newerOf, olderOf, d, c, b, a}},
		{"status", postgres.ListFilter{Status: &sent}, []uuid.UUID{c, b}},
		{"channel", postgres.ListFilter{Channel: &email}, []uuid.UUID{d, b}},
		{"status and channel", postgres.ListFilter{Status: &sent, Channel: &email}, []uuid.UUID{b}},
		{"created_from inclusive, created_to exclusive", postgres.ListFilter{CreatedFrom: &from, CreatedTo: &to}, []uuid.UUID{c, b}},
		{"batch", postgres.ListFilter{BatchID: batch.BatchID}, []uuid.UUID{newerOf, olderOf}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.filter.Limit = 50
			list, next, err := s.List(ctx, tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, ids(list)); diff != "" {
				t.Errorf("ids (-want +got):\n%s", diff)
			}
			if next != nil {
				t.Errorf("next cursor = %v, want nil", next)
			}
		})
	}

	t.Run("pages follow the cursor", func(t *testing.T) {
		var got []uuid.UUID
		var cursor *postgres.Cursor
		for page := 0; ; page++ {
			list, next, err := s.List(ctx, postgres.ListFilter{Limit: 4, After: cursor})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, ids(list)...)
			if next == nil {
				break
			}
			if page > 3 {
				t.Fatal("too many pages")
			}
			cursor = next
		}
		if diff := cmp.Diff([]uuid.UUID{newerOf, olderOf, d, c, b, a}, got); diff != "" {
			t.Errorf("ids (-want +got):\n%s", diff)
		}
	})

	t.Run("an exact page has no next cursor", func(t *testing.T) {
		_, next, err := s.List(ctx, postgres.ListFilter{Limit: 6})
		if err != nil || next != nil {
			t.Fatalf("next = %v, err %v; want nil", next, err)
		}
	})
}

// D-017: rows inserted while a client pages through the list never cause a
// row to appear twice, and every row that existed at the start is seen once.
func TestListPagingWithConcurrentInserts(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	existing := map[uuid.UUID]bool{}
	for range 25 {
		n, _, err := s.Create(ctx, draft(""))
		if err != nil {
			t.Fatal(err)
		}
		existing[n.ID] = true
	}

	seen := map[uuid.UUID]int{}
	var cursor *postgres.Cursor
	for page := 0; page < 20; page++ {
		list, next, err := s.List(ctx, postgres.ListFilter{Limit: 4, After: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range list {
			seen[n.ID]++
		}
		// New rows arrive between page requests.
		for range 3 {
			if _, _, err := s.Create(ctx, draft("")); err != nil {
				t.Fatal(err)
			}
		}
		if next == nil {
			break
		}
		cursor = next
	}

	for id, n := range seen {
		if n > 1 {
			t.Errorf("%s returned %d times", id, n)
		}
	}
	for id := range existing {
		if seen[id] != 1 {
			t.Errorf("pre-existing row %s seen %d times, want 1", id, seen[id])
		}
	}
}
