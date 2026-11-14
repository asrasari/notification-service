// Package httpapi holds the HTTP handlers, routes and middleware.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Pinger reports whether the database is reachable. *pgxpool.Pool satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// healthPingTimeout bounds the DB check so /health answers quickly even when
// Postgres hangs.
const healthPingTimeout = 2 * time.Second

// NewHandler returns the API's root handler with all routes and middleware.
//
// Router: stdlib http.ServeMux until D-012 is decided.
func NewHandler(logger *slog.Logger, db Pinger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", healthHandler(logger, db))

	return withCorrelationID(accessLog(logger, mux))
}

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
