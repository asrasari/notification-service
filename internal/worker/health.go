package worker

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// healthJSON is the worker's /health body.
type healthJSON struct {
	Status   string                   `json:"status"` // "ok" or "shutting_down"
	Channels map[string]ChannelStatus `json:"channels"`
}

// NewHTTPHandler serves the worker's HTTP endpoints (D-015 continued):
//   - GET /health, a liveness check: 200 with each channel's breaker state
//     and pause flag, 503 only once shuttingDown is set. It does not touch
//     the database: a DB outage must not get the worker restarted in a loop.
//   - GET /metrics, served by metricsHandler.
func NewHTTPHandler(status func() map[string]ChannelStatus, shuttingDown *atomic.Bool, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metricsHandler)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		body := healthJSON{Status: "ok", Channels: status()}
		code := http.StatusOK
		if shuttingDown.Load() {
			body.Status, code = "shutting_down", http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	})
	return mux
}
