package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

// queueQueryTimeout bounds the scrape-time query (D-015 continued).
const queueQueryTimeout = 2 * time.Second

// QueueSource runs the grouped queue query; *postgres.Store implements it.
type QueueSource interface {
	QueueStats(ctx context.Context) ([]postgres.QueueGroup, error)
}

var (
	queueDepthDesc = prometheus.NewDesc("notification_queue_depth",
		"Unfinished notifications by channel, priority and state (due, delayed, processing), read from the database at scrape time.",
		[]string{"channel", "priority", "state"}, nil)
	oldestDueDesc = prometheus.NewDesc("notification_queue_oldest_due_seconds",
		"How long the oldest due pending notification of the channel has been waiting (0 when none).",
		[]string{"channel"}, nil)
)

var (
	queuePriorities = []notification.Priority{notification.PriorityHigh, notification.PriorityNormal, notification.PriorityLow}
	queueStates     = []string{postgres.QueueStateDue, postgres.QueueStateDelayed, postgres.QueueStateProcessing}
)

// RegisterQueueCollector registers the queue-depth collector: at each scrape
// it runs one grouped query with a 2 s timeout and emits every channel ×
// priority × state combination (0 when empty), so series never disappear.
// If the query fails it logs a warning and emits neither metric for that
// scrape; the rest of /metrics is unaffected.
func RegisterQueueCollector(reg prometheus.Registerer, src QueueSource, logger *slog.Logger) {
	reg.MustRegister(&queueCollector{src: src, logger: logger})
}

type queueCollector struct {
	src    QueueSource
	logger *slog.Logger
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
	ch <- oldestDueDesc
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), queueQueryTimeout)
	defer cancel()
	groups, err := c.src.QueueStats(ctx)
	if err != nil {
		c.logger.Warn("queue metrics skipped for this scrape", "error", err)
		return
	}

	type key struct {
		channel  notification.Channel
		priority notification.Priority
		state    string
	}
	counts := map[key]int{}
	oldest := map[notification.Channel]float64{}
	for _, g := range groups {
		counts[key{g.Channel, g.Priority, g.State}] = g.Count
		if g.State == postgres.QueueStateDue {
			oldest[g.Channel] = max(oldest[g.Channel], g.OldestDueSeconds)
		}
	}
	for _, chn := range notification.Channels {
		for _, p := range queuePriorities {
			for _, st := range queueStates {
				ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue,
					float64(counts[key{chn, p, st}]), string(chn), p.String(), st)
			}
		}
		ch <- prometheus.MustNewConstMetric(oldestDueDesc, prometheus.GaugeValue, oldest[chn], string(chn))
	}
}
