// Package metrics defines every Prometheus metric of the service (D-015
// continued). Each process creates one registry in main and passes it in;
// nothing here registers on the global default registry, so tests use a
// fresh registry and read values with prometheus/testutil.
//
// Label rule: labels come only from small fixed sets (channel, priority,
// status, outcome, state, method, route pattern). Never IDs, recipients,
// URLs or error text.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry returns a registry with the Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves the registry in the Prometheus text format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// Buckets (D-015): one provider call 5 ms … 10 s; delivery latency
// 100 ms … 1 h. Claim duration is one short DB transaction, 1 ms … 1 s.
var (
	SendDurationBuckets    = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	DeliveryLatencyBuckets = []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}
	ClaimDurationBuckets   = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1}
)

// knownMethods keeps the method label a fixed set: anything else (a client
// can send any token as the method) is reported as "other".
var knownMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodDelete: true, http.MethodOptions: true,
	http.MethodConnect: true, http.MethodTrace: true,
}

// MethodLabel maps a request method to the method label.
func MethodLabel(m string) string {
	if knownMethods[m] {
		return m
	}
	return "other"
}

// API holds the API process's metrics. A nil *API records nothing, so
// handlers can be built without metrics in tests.
type API struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	created  *prometheus.CounterVec
}

// NewAPI registers the API metrics on reg.
func NewAPI(reg prometheus.Registerer) *API {
	m := &API{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration by method and route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications created, by channel and priority. Idempotent replays are not counted.",
		}, []string{"channel", "priority"}),
	}
	reg.MustRegister(m.requests, m.duration, m.created)
	return m
}

// ObserveRequest records one finished request. route is the ServeMux
// pattern, or "unmatched".
func (m *API) ObserveRequest(method, route string, status int, d time.Duration) {
	if m == nil {
		return
	}
	method = MethodLabel(method)
	m.requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(method, route).Observe(d.Seconds())
}

// NotificationCreated counts one new row.
func (m *API) NotificationCreated(channel, priority string) {
	if m == nil {
		return
	}
	m.created.WithLabelValues(channel, priority).Inc()
}

// Breaker state values for circuit_breaker_state (D-015).
const (
	BreakerClosed   = 0
	BreakerHalfOpen = 1
	BreakerOpen     = 2
)

// Worker holds the worker process's metrics.
type Worker struct {
	attempts        *prometheus.CounterVec
	results         *prometheus.CounterVec
	sendDuration    *prometheus.HistogramVec
	deliveryLatency *prometheus.HistogramVec
	claimDuration   *prometheus.HistogramVec
	breakerState    *prometheus.GaugeVec
}

// NewWorker registers the worker metrics on reg. channel_paused and
// senders_busy are read from the worker at scrape time; see
// RegisterChannelState.
func NewWorker(reg prometheus.Registerer) *Worker {
	m := &Worker{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_attempts_total",
			Help: "Delivery attempts by channel, priority and outcome (success, transient, permanent, unknown, channel_wide).",
		}, []string{"channel", "priority", "outcome"}),
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_results_total",
			Help: "Final states written by the worker (sent, failed), by channel.",
		}, []string{"channel", "status"}),
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_send_duration_seconds",
			Help:    "Duration of one provider call, by channel.",
			Buckets: SendDurationBuckets,
		}, []string{"channel"}),
		deliveryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_latency_seconds",
			Help:    "Time from coalesce(scheduled_at, created_at) to sent, by channel and priority.",
			Buckets: DeliveryLatencyBuckets,
		}, []string{"channel", "priority"}),
		claimDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_claim_duration_seconds",
			Help:    "Duration of one claim transaction, by channel.",
			Buckets: ClaimDurationBuckets,
		}, []string{"channel"}),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state by channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
	}
	reg.MustRegister(m.attempts, m.results, m.sendDuration, m.deliveryLatency, m.claimDuration, m.breakerState)
	return m
}

// Attempt counts one delivery attempt.
func (m *Worker) Attempt(channel, priority, outcome string) {
	m.attempts.WithLabelValues(channel, priority, outcome).Inc()
}

// Result counts one final state (sent or failed).
func (m *Worker) Result(channel, status string) {
	m.results.WithLabelValues(channel, status).Inc()
}

// SendDuration records one provider call.
func (m *Worker) SendDuration(channel string, d time.Duration) {
	m.sendDuration.WithLabelValues(channel).Observe(d.Seconds())
}

// DeliveryLatency records the time from due to sent.
func (m *Worker) DeliveryLatency(channel, priority string, d time.Duration) {
	m.deliveryLatency.WithLabelValues(channel, priority).Observe(max(d, 0).Seconds())
}

// ClaimDuration records one claim transaction.
func (m *Worker) ClaimDuration(channel string, d time.Duration) {
	m.claimDuration.WithLabelValues(channel).Observe(d.Seconds())
}

// BreakerState sets circuit_breaker_state from a breaker state name
// ("closed", "half-open", "open").
func (m *Worker) BreakerState(channel, state string) {
	v := BreakerClosed
	switch state {
	case "half-open":
		v = BreakerHalfOpen
	case "open":
		v = BreakerOpen
	}
	m.breakerState.WithLabelValues(channel).Set(float64(v))
}

// ChannelState is one channel's live state, read at scrape time.
type ChannelState struct {
	Channel string
	Paused  bool
	Busy    int // in-flight sends
}

// RegisterChannelState registers channel_paused and senders_busy, read from
// state on every scrape.
func RegisterChannelState(reg prometheus.Registerer, state func() []ChannelState) {
	reg.MustRegister(&channelStateCollector{state: state})
}

var (
	pausedDesc = prometheus.NewDesc("channel_paused", "1 while the channel is paused by a provider 429.", []string{"channel"}, nil)
	busyDesc   = prometheus.NewDesc("senders_busy", "In-flight provider sends, by channel.", []string{"channel"}, nil)
)

type channelStateCollector struct {
	state func() []ChannelState
}

func (c *channelStateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pausedDesc
	ch <- busyDesc
}

func (c *channelStateCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.state() {
		paused := 0.0
		if s.Paused {
			paused = 1
		}
		ch <- prometheus.MustNewConstMetric(pausedDesc, prometheus.GaugeValue, paused, s.Channel)
		ch <- prometheus.MustNewConstMetric(busyDesc, prometheus.GaugeValue, float64(s.Busy), s.Channel)
	}
}
