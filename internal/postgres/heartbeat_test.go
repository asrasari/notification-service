package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/postgres"
)

type heartbeatRow struct {
	startedAt, lastSeenAt, expiresAt time.Time
	state                            map[string]any
}

func readHeartbeat(t *testing.T, pool *pgxpool.Pool, id string) (heartbeatRow, bool) {
	t.Helper()
	var (
		r     heartbeatRow
		state []byte
	)
	err := pool.QueryRow(context.Background(),
		`SELECT started_at, last_seen_at, expires_at, state FROM worker_heartbeats WHERE worker_id = $1`, id).
		Scan(&r.startedAt, &r.lastSeenAt, &r.expiresAt, &state)
	if err != nil {
		return r, false
	}
	if err := json.Unmarshal(state, &r.state); err != nil {
		t.Fatalf("state: %v", err)
	}
	return r, true
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()
	_, pool := newStore(t)
	h := postgres.NewHeartbeats(pool)
	ctx := context.Background()

	if err := h.Beat(ctx, "w-1", 5*time.Second, []byte(`{"sms":{"breaker":"closed","paused":false}}`)); err != nil {
		t.Fatal(err)
	}
	first, ok := readHeartbeat(t, pool, "w-1")
	if !ok {
		t.Fatal("no row after the first beat")
	}
	if got := first.expiresAt.Sub(first.lastSeenAt); got != 15*time.Second {
		t.Errorf("expires_at - last_seen_at = %v, want 3 × 5s", got)
	}

	time.Sleep(10 * time.Millisecond) // now() moves on between transactions
	if err := h.Beat(ctx, "w-1", 5*time.Second, []byte(`{"sms":{"breaker":"open","paused":true}}`)); err != nil {
		t.Fatal(err)
	}
	second, _ := readHeartbeat(t, pool, "w-1")
	if !second.startedAt.Equal(first.startedAt) || !second.lastSeenAt.After(first.lastSeenAt) {
		t.Errorf("second beat: started_at %v (was %v), last_seen_at %v (was %v)", second.startedAt, first.startedAt, second.lastSeenAt, first.lastSeenAt)
	}
	if sms := second.state["sms"].(map[string]any); sms["breaker"] != "open" || sms["paused"] != true {
		t.Errorf("state = %v", second.state)
	}

	if err := h.Remove(ctx, "w-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := readHeartbeat(t, pool, "w-1"); ok {
		t.Error("row still there after Remove")
	}
}

func TestHeartbeatPrunesLongExpiredRows(t *testing.T) {
	t.Parallel()
	_, pool := newStore(t)
	exec(t, pool, `INSERT INTO worker_heartbeats (worker_id, expires_at, state) VALUES
		('crashed-long-ago', now() - interval '2 hours', '{}'),
		('crashed-recently', now() - interval '30 minutes', '{}')`)

	if err := postgres.NewHeartbeats(pool).Beat(context.Background(), "w-1", 5*time.Second, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := readHeartbeat(t, pool, "crashed-long-ago"); ok {
		t.Error("a row expired more than 1h ago was not pruned")
	}
	if _, ok := readHeartbeat(t, pool, "crashed-recently"); !ok {
		t.Error("a row expired 30 minutes ago was pruned")
	}
}

func TestLiveWorkers(t *testing.T) {
	t.Parallel()
	_, pool := newStore(t)
	h := postgres.NewHeartbeats(pool)
	ctx := context.Background()

	if live, err := h.LiveWorkers(ctx); err != nil || len(live) != 0 {
		t.Fatalf("no rows: %v, %v", live, err)
	}
	if err := h.Beat(ctx, "w-1", 5*time.Second, []byte(`{"sms":{"breaker":"open","paused":true}}`)); err != nil {
		t.Fatal(err)
	}
	exec(t, pool, `INSERT INTO worker_heartbeats (worker_id, expires_at, state) VALUES ('stale', now() - interval '1 second', '{}')`)

	live, err := h.LiveWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0]["sms"] != (postgres.HeartbeatChannel{Breaker: "open", Paused: true}) {
		t.Fatalf("live = %+v, want only w-1", live)
	}
}
