package httpapi

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/logging"
)

// CorrelationIDHeader is read from requests and echoed on every response.
const CorrelationIDHeader = "X-Correlation-ID"

const maxCorrelationIDLen = 128

// validCorrelationID accepts 1–128 characters of [A-Za-z0-9-] (D-015).
// Anything else is replaced rather than trusted, so a caller can't inject
// arbitrary text into our logs.
func validCorrelationID(s string) bool {
	if len(s) == 0 || len(s) > maxCorrelationIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
		default:
			return false
		}
	}
	return true
}

// withCorrelationID takes the caller's X-Correlation-ID when it is valid and
// generates a UUIDv7 otherwise. The ID is echoed on the response and stored in
// the request context, where the logging handler picks it up.
func withCorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationIDHeader)
		if !validCorrelationID(id) {
			id = newCorrelationID()
		}
		w.Header().Set(CorrelationIDHeader, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

func newCorrelationID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	// NewV7 only fails if the system random source fails; a v4 ID is still
	// unique enough to follow one request.
	return uuid.NewString()
}

// accessLog logs one line per request: method, route pattern, status and
// duration. correlation_id is added by the logging handler from the context.
// It must wrap the ServeMux directly, because the mux records the matched
// pattern on the *http.Request it receives.
func accessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		logger.LogAttrs(r.Context(), slog.LevelInfo, "http request",
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.Int("status", rec.status),
			slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000),
		)
	})
}

// statusRecorder captures the status code written by the handler.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
