package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/worker"
)

const (
	sms    = notification.ChannelSMS
	high   = notification.PriorityHigh
	normal = notification.PriorityNormal
	low    = notification.PriorityLow
)

func TestOnAttempt(t *testing.T) {
	reg := NewRegistry()
	m := NewWorker(reg)

	m.OnAttempt(worker.AttemptEvent{Channel: sms, Priority: high, Outcome: notification.OutcomeSuccess, Status: notification.StatusSent,
		SendDuration: 40 * time.Millisecond, DueAt: time.Now().Add(-3 * time.Second)})
	m.OnAttempt(worker.AttemptEvent{Channel: sms, Priority: high, Outcome: notification.OutcomeTransient, Status: notification.StatusPending,
		SendDuration: 20 * time.Millisecond})
	m.OnAttempt(worker.AttemptEvent{Channel: sms, Priority: normal, Outcome: notification.OutcomePermanent, Status: notification.StatusFailed,
		SendDuration: 10 * time.Millisecond})
	m.OnAttempt(worker.AttemptEvent{Channel: sms, Priority: high, Outcome: notification.OutcomeSuccess, Status: notification.StatusSent,
		SendDuration: 30 * time.Millisecond, Stale: true}) // nothing written

	want := `
# HELP notification_attempts_total Delivery attempts by channel, priority and outcome (success, transient, permanent, unknown, channel_wide).
# TYPE notification_attempts_total counter
notification_attempts_total{channel="sms",outcome="permanent",priority="normal"} 1
notification_attempts_total{channel="sms",outcome="success",priority="high"} 1
notification_attempts_total{channel="sms",outcome="transient",priority="high"} 1
# HELP notification_results_total Final states written by the worker (sent, failed), by channel.
# TYPE notification_results_total counter
notification_results_total{channel="sms",status="failed"} 1
notification_results_total{channel="sms",status="sent"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "notification_attempts_total", "notification_results_total"); err != nil {
		t.Error(err)
	}
	// All four provider calls are timed, the stale one included.
	if got := histogramCount(t, reg, "notification_send_duration_seconds"); got != 4 {
		t.Errorf("send duration samples = %d, want 4", got)
	}
	// Latency only for the one written as sent, about 3 s.
	if got := histogramCount(t, reg, "notification_delivery_latency_seconds"); got != 1 {
		t.Errorf("latency samples = %d, want 1", got)
	}
	if s := histogramSum(t, reg, "notification_delivery_latency_seconds"); s < 3 || s > 4 {
		t.Errorf("latency = %vs, want about 3s", s)
	}
}

func TestOnClaim(t *testing.T) {
	reg := NewRegistry()
	m := NewWorker(reg)
	m.OnClaim(worker.ClaimEvent{Channel: sms, Duration: 2 * time.Millisecond})
	m.OnClaim(worker.ClaimEvent{Channel: sms, Duration: 3 * time.Millisecond,
		Exhausted: []notification.Priority{low}, LostAttempts: []notification.Priority{low, high}})

	want := `
# HELP notification_attempts_total Delivery attempts by channel, priority and outcome (success, transient, permanent, unknown, channel_wide).
# TYPE notification_attempts_total counter
notification_attempts_total{channel="sms",outcome="unknown",priority="high"} 1
notification_attempts_total{channel="sms",outcome="unknown",priority="low"} 1
# HELP notification_results_total Final states written by the worker (sent, failed), by channel.
# TYPE notification_results_total counter
notification_results_total{channel="sms",status="failed"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "notification_attempts_total", "notification_results_total"); err != nil {
		t.Error(err)
	}
	if got := histogramCount(t, reg, "notification_claim_duration_seconds"); got != 2 {
		t.Errorf("claim duration samples = %d, want 2", got)
	}
}

type fakeWorkerState struct{}

func (fakeWorkerState) Status() map[string]worker.ChannelStatus {
	return map[string]worker.ChannelStatus{"sms": {Breaker: "open", Paused: true}}
}
func (fakeWorkerState) Busy() map[string]int { return map[string]int{"email": 12} }

func TestRegisterWorkerState(t *testing.T) {
	reg := NewRegistry()
	RegisterWorkerState(reg, fakeWorkerState{})
	want := `
# HELP channel_paused 1 while the channel is paused by a provider 429.
# TYPE channel_paused gauge
channel_paused{channel="email"} 0
channel_paused{channel="push"} 0
channel_paused{channel="sms"} 1
# HELP senders_busy In-flight provider sends, by channel.
# TYPE senders_busy gauge
senders_busy{channel="email"} 12
senders_busy{channel="push"} 0
senders_busy{channel="sms"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "channel_paused", "senders_busy"); err != nil {
		t.Error(err)
	}
}

func histogram(t *testing.T, g prometheus.Gatherer, name string) (count uint64, sum float64) {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				count += m.GetHistogram().GetSampleCount()
				sum += m.GetHistogram().GetSampleSum()
			}
		}
	}
	return count, sum
}

func histogramCount(t *testing.T, g prometheus.Gatherer, name string) uint64 {
	t.Helper()
	c, _ := histogram(t, g, name)
	return c
}

func histogramSum(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	_, s := histogram(t, g, name)
	return s
}
