// Package httpapi holds the HTTP handlers, routes and middleware.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/asrasari/notification-service/internal/metrics"
	"github.com/asrasari/notification-service/internal/postgres"
)

// Pinger reports whether the database is reachable. *pgxpool.Pool satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Workers reads the live workers' heartbeats; *postgres.Heartbeats
// implements it.
type Workers interface {
	LiveWorkers(ctx context.Context) ([]map[string]postgres.HeartbeatChannel, error)
}

// healthCheckTimeout bounds the DB checks so /health answers quickly even
// when Postgres hangs.
const healthCheckTimeout = 2 * time.Second

// Deps are what the API handler needs.
type Deps struct {
	Logger *slog.Logger
	DB     Pinger
	Store  Store
	// Workers feeds /health with the worker heartbeats; nil means no worker
	// is ever seen (degraded).
	Workers Workers
	// Metrics records the API metrics; nil records nothing.
	Metrics *metrics.API
	// Registry is served on GET /metrics; nil serves an empty registry.
	Registry *prometheus.Registry
}

// NewHandler returns the API's root handler with all routes and middleware.
// Routing is the stdlib ServeMux with method and {id} patterns (D-012).
func NewHandler(d Deps) http.Handler {
	logger, db := d.Logger, d.DB
	a := &handlers{logger: logger, store: d.Store, metrics: d.Metrics}
	reg := d.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	mux := http.NewServeMux()
	mux.Handle("GET /health", healthHandler(logger, db, d.Workers))
	mux.HandleFunc("POST /notifications", a.createNotification)
	mux.HandleFunc("POST /notifications/batch", a.createBatch)
	mux.HandleFunc("GET /notifications", a.listNotifications)
	mux.HandleFunc("GET /notifications/{id}", a.getNotification)
	mux.HandleFunc("POST /notifications/{id}/cancel", a.cancelNotification)
	mux.HandleFunc("GET /batches/{id}", a.getBatch)
	mux.HandleFunc("POST /batches/{id}/cancel", a.cancelBatch)
	mux.HandleFunc("GET /openapi.yaml", openAPIHandler)
	mux.HandleFunc("GET /docs", docsHandler)
	mux.Handle("GET /metrics", metrics.Handler(reg))

	return withCorrelationID(accessLog(logger, d.Metrics, jsonFallback(mux)))
}

// jsonFallback answers unmatched requests with the JSON problem format
// instead of the mux's plain text (D-012): 404 for an unknown path, 405 with
// the Allow header for a known path with the wrong method.
//
// A catch-all "/" pattern can't do this: it would also match known paths
// with a wrong method and hide the 405.
func jsonFallback(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		// Let the mux decide between 404 and 405 (and compute Allow), but
		// throw its plain-text body away.
		rec := &headerRecorder{header: http.Header{}, status: http.StatusOK}
		h.ServeHTTP(rec, r)
		switch rec.status {
		case http.StatusMethodNotAllowed:
			w.Header().Set("Allow", rec.header.Get("Allow"))
			writeProblem(w, r, problem{Status: http.StatusMethodNotAllowed, Detail: "This path does not support the " + r.Method + " method."})
		case http.StatusNotFound:
			notFound(w, r, "No route matches this path.")
		default:
			// Redirects for unclean paths (e.g. a trailing slash rule); pass them on.
			for k, v := range rec.header {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.status)
		}
	})
}

// headerRecorder captures the status and headers of the mux's fallback
// handler and discards its body.
type headerRecorder struct {
	header http.Header
	status int
}

func (h *headerRecorder) Header() http.Header         { return h.header }
func (h *headerRecorder) Write(b []byte) (int, error) { return len(b), nil }
func (h *headerRecorder) WriteHeader(code int)        { h.status = code }

// Health statuses (D-015 continued).
const (
	healthOK       = "ok"
	healthDegraded = "degraded"
	healthDown     = "down"
)

type healthResponse struct {
	Status string       `json:"status"`
	Checks healthChecks `json:"checks"`
}

type healthChecks struct {
	Database string        `json:"database"` // ok or down
	Workers  workersHealth `json:"workers"`
}

type workersHealth struct {
	Live     int                                  `json:"live"`
	Channels map[string]postgres.HeartbeatChannel `json:"channels"`
}

// breakerRank orders breaker states from healthy to worst, to merge the
// reports of several workers.
var breakerRank = map[string]int{"closed": 0, "half-open": 1, "open": 2}

// healthHandler reports (D-015 continued):
//   - 503 "down" when the database does not answer (ping, or the heartbeat
//     read);
//   - 200 "degraded" when no worker has a fresh heartbeat, or a fresh worker
//     reports a breaker that is not closed or a paused channel;
//   - 200 "ok" otherwise.
//
// Degraded stays 200: restarting the API would not fix a worker or provider
// problem. Channels merge every live worker's report: the worst breaker
// state, and paused if any worker is paused.
func healthHandler(logger *slog.Logger, db Pinger, workers Workers) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		defer cancel()

		down := healthResponse{Status: healthDown, Checks: healthChecks{
			Database: healthDown,
			Workers:  workersHealth{Channels: map[string]postgres.HeartbeatChannel{}},
		}}
		if err := db.Ping(ctx); err != nil {
			logger.WarnContext(r.Context(), "health check: database ping failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, down)
			return
		}
		var live []map[string]postgres.HeartbeatChannel
		if workers != nil {
			var err error
			if live, err = workers.LiveWorkers(ctx); err != nil {
				logger.WarnContext(r.Context(), "health check: reading worker heartbeats failed", "error", err)
				writeJSON(w, http.StatusServiceUnavailable, down)
				return
			}
		}

		res := healthResponse{Status: healthOK, Checks: healthChecks{
			Database: healthOK,
			Workers:  workersHealth{Live: len(live), Channels: map[string]postgres.HeartbeatChannel{}},
		}}
		if len(live) == 0 {
			res.Status = healthDegraded
		}
		for _, state := range live {
			for ch, s := range state {
				merged := res.Checks.Workers.Channels[ch]
				if merged.Breaker == "" || breakerRank[s.Breaker] > breakerRank[merged.Breaker] {
					merged.Breaker = s.Breaker
				}
				merged.Paused = merged.Paused || s.Paused
				res.Checks.Workers.Channels[ch] = merged
			}
		}
		for _, s := range res.Checks.Workers.Channels {
			if s.Breaker != "closed" || s.Paused {
				res.Status = healthDegraded
			}
		}
		writeJSON(w, http.StatusOK, res)
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the status line is already sent; nothing useful to do on error
}
