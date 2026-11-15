package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/postgres"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func newTestHandler(t *testing.T, db Pinger) (http.Handler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger, err := logging.New(&buf, "debug", "json")
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(Deps{Logger: logger, DB: db}), &buf
}

type fakeWorkers struct {
	live []map[string]postgres.HeartbeatChannel
	err  error
}

func (f fakeWorkers) LiveWorkers(context.Context) ([]map[string]postgres.HeartbeatChannel, error) {
	return f.live, f.err
}

func healthy() map[string]postgres.HeartbeatChannel {
	return map[string]postgres.HeartbeatChannel{
		"sms": {Breaker: "closed"}, "email": {Breaker: "closed"}, "push": {Breaker: "closed"},
	}
}

func with(ch string, s postgres.HeartbeatChannel) map[string]postgres.HeartbeatChannel {
	m := healthy()
	m[ch] = s
	return m
}

func TestHealth(t *testing.T) {
	tests := []struct {
		name       string
		pingErr    error
		workers    fakeWorkers
		wantCode   int
		wantStatus string
		wantDB     string
		wantLive   int
		wantSMS    postgres.HeartbeatChannel
	}{
		{name: "database down", pingErr: errors.New("connection refused"), workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{healthy()}},
			wantCode: 503, wantStatus: "down", wantDB: "down"},
		{name: "heartbeat read fails", workers: fakeWorkers{err: errors.New("relation does not exist")},
			wantCode: 503, wantStatus: "down", wantDB: "down"},
		{name: "no live worker", wantCode: 200, wantStatus: "degraded", wantDB: "ok"},
		{name: "live and healthy", workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{healthy()}},
			wantCode: 200, wantStatus: "ok", wantDB: "ok", wantLive: 1, wantSMS: postgres.HeartbeatChannel{Breaker: "closed"}},
		{name: "breaker open", workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{with("sms", postgres.HeartbeatChannel{Breaker: "open"})}},
			wantCode: 200, wantStatus: "degraded", wantDB: "ok", wantLive: 1, wantSMS: postgres.HeartbeatChannel{Breaker: "open"}},
		{name: "breaker half-open", workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{with("sms", postgres.HeartbeatChannel{Breaker: "half-open"})}},
			wantCode: 200, wantStatus: "degraded", wantDB: "ok", wantLive: 1, wantSMS: postgres.HeartbeatChannel{Breaker: "half-open"}},
		{name: "channel paused", workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{with("sms", postgres.HeartbeatChannel{Breaker: "closed", Paused: true})}},
			wantCode: 200, wantStatus: "degraded", wantDB: "ok", wantLive: 1, wantSMS: postgres.HeartbeatChannel{Breaker: "closed", Paused: true}},
		{name: "two workers merge to the worst", workers: fakeWorkers{live: []map[string]postgres.HeartbeatChannel{
			with("sms", postgres.HeartbeatChannel{Breaker: "half-open"}), with("sms", postgres.HeartbeatChannel{Breaker: "closed", Paused: true})}},
			wantCode: 200, wantStatus: "degraded", wantDB: "ok", wantLive: 2, wantSMS: postgres.HeartbeatChannel{Breaker: "half-open", Paused: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger, _ := logging.New(&buf, "debug", "json")
			h := NewHandler(Deps{Logger: logger, DB: fakePinger{err: tt.pingErr}, Workers: tt.workers})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d: %s", rec.Code, tt.wantCode, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}
			var body healthResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			if body.Status != tt.wantStatus || body.Checks.Database != tt.wantDB || body.Checks.Workers.Live != tt.wantLive {
				t.Errorf("body = %+v", body)
			}
			if body.Checks.Workers.Channels == nil {
				t.Error("channels is null, want an object")
			}
			if tt.wantLive > 0 && body.Checks.Workers.Channels["sms"] != tt.wantSMS {
				t.Errorf("sms = %+v, want %+v", body.Checks.Workers.Channels["sms"], tt.wantSMS)
			}
		})
	}
}

func TestHealthRejectsOtherMethods(t *testing.T) {
	h, _ := newTestHandler(t, fakePinger{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/health", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestCorrelationID(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		wantKept bool
	}{
		{"valid ID is kept", "client-req-42", true},
		{"UUID is kept", "0191f2a6-7b6c-7c3e-9a1b-2f3e4d5c6b7a", true},
		{"128 characters is kept", strings.Repeat("a", 128), true},
		{"missing header is generated", "", false},
		{"129 characters is replaced", strings.Repeat("a", 129), false},
		{"spaces are replaced", "has space", false},
		{"newline injection is replaced", "abc\nlevel=ERROR", false},
		{"underscore is replaced", "a_b", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, logs := newTestHandler(t, fakePinger{})
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			if tt.header != "" {
				req.Header.Set(CorrelationIDHeader, tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(CorrelationIDHeader)
			if tt.wantKept {
				if got != tt.header {
					t.Fatalf("echoed %q, want %q", got, tt.header)
				}
			} else {
				id, err := uuid.Parse(got)
				if err != nil || id.Version() != 7 {
					t.Fatalf("expected a generated UUIDv7, got %q", got)
				}
			}

			var line map[string]any
			if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
				t.Fatalf("access log is not one JSON line: %v\n%s", err, logs)
			}
			if line[logging.CorrelationIDKey] != got {
				t.Errorf("access log correlation_id = %v, want %q", line[logging.CorrelationIDKey], got)
			}
		})
	}
}

func TestAccessLog(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantRoute  string
		wantStatus float64
	}{
		{"matched route", http.MethodGet, "/health", "GET /health", 200},
		{"unknown path", http.MethodGet, "/nope", "unmatched", 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, logs := newTestHandler(t, fakePinger{})
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tt.method, tt.path, nil))

			var line map[string]any
			if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
				t.Fatalf("access log is not one JSON line: %v\n%s", err, logs)
			}
			if line["msg"] != "http request" || line["level"] != slog.LevelInfo.String() {
				t.Errorf("unexpected record: %v", line)
			}
			if line["method"] != tt.method || line["route"] != tt.wantRoute || line["status"] != tt.wantStatus {
				t.Errorf("got method=%v route=%v status=%v", line["method"], line["route"], line["status"])
			}
			if _, ok := line["duration_ms"].(float64); !ok {
				t.Errorf("duration_ms missing: %v", line)
			}
		})
	}
}

// D-012: every server timeout is set, so a slow client can't hold a
// connection forever.
func TestNewServerTimeouts(t *testing.T) {
	srv := NewServer(":0", http.NotFoundHandler(), nil)
	for name, d := range map[string]time.Duration{
		"ReadHeaderTimeout": srv.ReadHeaderTimeout,
		"ReadTimeout":       srv.ReadTimeout,
		"WriteTimeout":      srv.WriteTimeout,
		"IdleTimeout":       srv.IdleTimeout,
	} {
		if d <= 0 {
			t.Errorf("%s is not set", name)
		}
	}
	if srv.ReadHeaderTimeout >= srv.ReadTimeout {
		t.Errorf("ReadHeaderTimeout %s should be shorter than ReadTimeout %s", srv.ReadHeaderTimeout, srv.ReadTimeout)
	}
}
