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

// NewHealthHandler serves the worker's HTTP endpoints: GET /health, a
// liveness check (D-015 continued). It answers 200 with each channel's
// breaker state and pause flag, and 503 only once shuttingDown is set. It
// does not touch the database: a DB outage must not get the worker
// restarted in a loop. /metrics is added in M4.
func NewHealthHandler(status func() map[string]ChannelStatus, shuttingDown *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
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
