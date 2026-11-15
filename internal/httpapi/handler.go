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
)

// Pinger reports whether the database is reachable. *pgxpool.Pool satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// healthPingTimeout bounds the DB check so /health answers quickly even when
// Postgres hangs.
const healthPingTimeout = 2 * time.Second

// Deps are what the API handler needs.
type Deps struct {
	Logger *slog.Logger
	DB     Pinger
	Store  Store
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
	mux.Handle("GET /health", healthHandler(logger, db))
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

type healthResponse struct {
	Status string `json:"status"`
}

// healthHandler returns 200 {"status":"ok"} when the database answers a ping
// and 503 {"status":"down"} otherwise.
func healthHandler(logger *slog.Logger, db Pinger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthPingTimeout)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			logger.WarnContext(r.Context(), "health check: database ping failed", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, healthResponse{Status: "down"})
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the status line is already sent; nothing useful to do on error
}
