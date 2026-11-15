package metrics_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/asrasari/notification-service/internal/metrics"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/testdb"
)

func TestMain(m *testing.M) { testdb.Main(m) }

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// TestQueueCollector runs the real query against rows in every state.
func TestQueueCollector(t *testing.T) {
	t.Parallel()
	pool, err := pgxpool.New(context.Background(), testdb.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.NewStore(pool, 6)

	add := func(ch notification.Channel, p notification.Priority, recipient string) string {
		n, _, err := store.Create(context.Background(), notification.Draft{Channel: ch, Recipient: recipient, Content: "x", Priority: p})
		if err != nil {
			t.Fatal(err)
		}
		return n.ID.String()
	}
	// sms/high: two due, the oldest due for 90 s.
	old := add(notification.ChannelSMS, notification.PriorityHigh, "+905551234567")
	exec(t, pool, `UPDATE notifications SET available_at = now() - interval '90 seconds' WHERE id = $1`, old)
	add(notification.ChannelSMS, notification.PriorityHigh, "+905551234567")
	// email/normal: one delayed (backoff), one processing with an expired lease.
	delayed := add(notification.ChannelEmail, notification.PriorityNormal, "a@example.com")
	exec(t, pool, `UPDATE notifications SET available_at = now() + interval '1 hour', attempts = 1 WHERE id = $1`, delayed)
	proc := add(notification.ChannelEmail, notification.PriorityNormal, "a@example.com")
	exec(t, pool, `UPDATE notifications SET status = 'processing', attempts = 1, lease_token = gen_random_uuid(),
		available_at = now() - interval '1 second' WHERE id = $1`, proc)
	// push/low: finished rows are not in the queue.
	for _, st := range []string{"sent", "failed", "cancelled"} {
		id := add(notification.ChannelPush, notification.PriorityLow, "device-1")
		exec(t, pool, `UPDATE notifications SET status = $2 WHERE id = $1`, id, st)
	}

	reg := metrics.NewRegistry()
	metrics.RegisterQueueCollector(reg, store, slog.New(slog.DiscardHandler))

	// Every combination is present; only the filled ones are non-zero.
	var want strings.Builder
	want.WriteString("# HELP notification_queue_depth Unfinished notifications by channel, priority and state (due, delayed, processing), read from the database at scrape time.\n# TYPE notification_queue_depth gauge\n")
	for _, ch := range []string{"email", "push", "sms"} {
		for _, p := range []string{"high", "low", "normal"} {
			for _, st := range []string{"delayed", "due", "processing"} {
				v := "0"
				switch {
				case ch == "sms" && p == "high" && st == "due":
					v = "2"
				case ch == "email" && p == "normal" && (st == "delayed" || st == "processing"):
					v = "1"
				}
				want.WriteString(`notification_queue_depth{channel="` + ch + `",priority="` + p + `",state="` + st + `"} ` + v + "\n")
			}
		}
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want.String()), "notification_queue_depth"); err != nil {
		t.Error(err)
	}

	oldest := gauges(t, reg, "notification_queue_oldest_due_seconds")
	if v := oldest["sms"]; v < 89 || v > 120 {
		t.Errorf("oldest due sms = %v, want about 90", v)
	}
	if oldest["email"] != 0 || oldest["push"] != 0 || len(oldest) != 3 {
		t.Errorf("oldest = %v, want 0 for email and push", oldest)
	}
}

// gauges returns a gauge's values keyed by its channel label.
func gauges(t *testing.T, g prometheus.Gatherer, name string) map[string]float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "channel" {
					out[lp.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	return out
}

type failingSource struct{}

func (failingSource) QueueStats(context.Context) ([]postgres.QueueGroup, error) {
	return nil, errors.New("database is down")
}

// TestQueueCollectorErrorSkipsOnlyQueueMetrics: a failed query logs a
// warning and drops the queue metrics for this scrape only.
func TestQueueCollectorErrorSkipsOnlyQueueMetrics(t *testing.T) {
	var logs bytes.Buffer
	reg := metrics.NewRegistry()
	metrics.NewAPI(reg).NotificationCreated("sms", "high")
	metrics.RegisterQueueCollector(reg, failingSource{}, slog.New(slog.NewJSONHandler(&logs, nil)))

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v (the rest of /metrics must still work)", err)
	}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	if names["notification_queue_depth"] || names["notification_queue_oldest_due_seconds"] {
		t.Error("queue metrics emitted although the query failed")
	}
	if !names["notifications_created_total"] || !names["go_goroutines"] {
		t.Errorf("other metrics missing: %v", names)
	}
	if !strings.Contains(logs.String(), `"level":"WARN"`) || !strings.Contains(logs.String(), "database is down") {
		t.Errorf("no warning logged: %s", logs.String())
	}
}

// slowSource checks that the query gets the 2 s deadline.
type slowSource struct{ deadline time.Duration }

func (s *slowSource) QueueStats(ctx context.Context) ([]postgres.QueueGroup, error) {
	d, _ := ctx.Deadline()
	s.deadline = time.Until(d)
	return nil, nil
}

func TestQueueCollectorTimeout(t *testing.T) {
	src := &slowSource{}
	reg := metrics.NewRegistry()
	metrics.RegisterQueueCollector(reg, src, slog.New(slog.DiscardHandler))
	if _, err := reg.Gather(); err != nil {
		t.Fatal(err)
	}
	if src.deadline <= time.Second || src.deadline > 2*time.Second {
		t.Errorf("query deadline %v away, want 2s", src.deadline)
	}
}
