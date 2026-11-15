package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegistryHasRuntimeCollectors(t *testing.T) {
	reg := NewRegistry()
	NewAPI(reg)
	NewWorker(reg)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{"go_goroutines", "process_start_time_seconds"} {
		if !names[want] {
			t.Errorf("registry lacks %s", want)
		}
	}
}

func TestAPIMetrics(t *testing.T) {
	reg := NewRegistry()
	m := NewAPI(reg)
	m.ObserveRequest("GET", "GET /notifications/{id}", 200, 3*time.Millisecond)
	m.ObserveRequest("GET", "GET /notifications/{id}", 200, 5*time.Millisecond)
	m.ObserveRequest("BREW", "unmatched", 404, time.Millisecond)
	m.NotificationCreated("sms", "high")

	if v := testutil.ToFloat64(m.requests.WithLabelValues("GET", "GET /notifications/{id}", "200")); v != 2 {
		t.Errorf("requests = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.requests.WithLabelValues("other", "unmatched", "404")); v != 1 {
		t.Errorf("unknown method not mapped to other: %v", v)
	}
	if n := testutil.CollectAndCount(m.duration); n != 2 {
		t.Errorf("duration series = %d, want 2", n)
	}
	if v := testutil.ToFloat64(m.created.WithLabelValues("sms", "high")); v != 1 {
		t.Errorf("created = %v", v)
	}

	var nilAPI *API // a handler without metrics
	nilAPI.ObserveRequest("GET", "x", 200, 0)
	nilAPI.NotificationCreated("sms", "high")
}

func TestWorkerMetrics(t *testing.T) {
	reg := NewRegistry()
	m := NewWorker(reg)
	m.Attempt("sms", "high", "success")
	m.Result("sms", "sent")
	m.SendDuration("sms", 40*time.Millisecond)
	m.DeliveryLatency("sms", "high", 2*time.Second)
	m.ClaimDuration("sms", 2*time.Millisecond)
	for state, want := range map[string]float64{"closed": 0, "half-open": 1, "open": 2} {
		m.BreakerState("email", state)
		if v := testutil.ToFloat64(m.breakerState.WithLabelValues("email")); v != want {
			t.Errorf("breaker %s = %v, want %v", state, v, want)
		}
	}
	RegisterChannelState(reg, func() []ChannelState {
		return []ChannelState{{Channel: "sms", Paused: true, Busy: 7}, {Channel: "push"}}
	})

	want := `
# HELP channel_paused 1 while the channel is paused by a provider 429.
# TYPE channel_paused gauge
channel_paused{channel="push"} 0
channel_paused{channel="sms"} 1
# HELP senders_busy In-flight provider sends, by channel.
# TYPE senders_busy gauge
senders_busy{channel="push"} 0
senders_busy{channel="sms"} 7
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "channel_paused", "senders_busy"); err != nil {
		t.Error(err)
	}
	if v := testutil.ToFloat64(m.attempts.WithLabelValues("sms", "high", "success")); v != 1 {
		t.Errorf("attempts = %v", v)
	}
	if n := testutil.CollectAndCount(m.deliveryLatency); n != 1 {
		t.Errorf("latency series = %d", n)
	}
}

func TestHandlerServesTextFormat(t *testing.T) {
	reg := NewRegistry()
	NewAPI(reg).NotificationCreated("push", "low")
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("got %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(string(body), `notifications_created_total{channel="push",priority="low"} 1`) {
		t.Errorf("body lacks the counter:\n%s", body)
	}
}
