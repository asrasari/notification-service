package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/worker"
)

// OnAttempt records one finished send (worker.Config.OnAttempt). A stale
// send wrote nothing, so only its provider call duration is recorded: the
// worker that reclaimed the row records the lost attempt instead.
func (m *Worker) OnAttempt(e worker.AttemptEvent) {
	ch := string(e.Channel)
	m.SendDuration(ch, e.SendDuration)
	if e.Stale {
		return
	}
	m.Attempt(ch, e.Priority.String(), string(e.Outcome))
	switch e.Status {
	case notification.StatusSent:
		m.Result(ch, string(notification.StatusSent))
		m.DeliveryLatency(ch, e.Priority.String(), time.Since(e.DueAt))
	case notification.StatusFailed:
		m.Result(ch, string(notification.StatusFailed))
	}
}

// OnClaim records one claim transaction (worker.Config.OnClaim): its
// duration, the unknown attempts recorded for expired leases, and the rows
// failed because their last lease expired (D-015).
func (m *Worker) OnClaim(e worker.ClaimEvent) {
	ch := string(e.Channel)
	m.ClaimDuration(ch, e.Duration)
	for _, p := range e.LostAttempts {
		m.Attempt(ch, p.String(), string(notification.OutcomeUnknown))
	}
	for range e.Exhausted {
		m.Result(ch, string(notification.StatusFailed))
	}
}

// WorkerState is the part of the worker that the state gauges read.
type WorkerState interface {
	Status() map[string]worker.ChannelStatus
	Busy() map[string]int
}

// RegisterWorkerState registers channel_paused and senders_busy, read from
// w at every scrape, for every channel.
func RegisterWorkerState(reg prometheus.Registerer, w WorkerState) {
	RegisterChannelState(reg, func() []ChannelState {
		status, busy := w.Status(), w.Busy()
		out := make([]ChannelState, 0, len(notification.Channels))
		for _, c := range notification.Channels {
			ch := string(c)
			out = append(out, ChannelState{Channel: ch, Paused: status[ch].Paused, Busy: busy[ch]})
		}
		return out
	})
}
