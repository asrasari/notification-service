package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestHealthHandler(t *testing.T) {
	status := func() map[string]ChannelStatus {
		return map[string]ChannelStatus{"sms": {Breaker: "open", Paused: false}, "email": {Breaker: "closed", Paused: true}}
	}
	var shutting atomic.Bool
	h := NewHealthHandler(status, &shutting)

	get := func() (*httptest.ResponseRecorder, healthJSON) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
		var body healthJSON
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("body %q: %v", rec.Body.String(), err)
		}
		return rec, body
	}

	rec, body := get()
	if rec.Code != 200 || body.Status != "ok" || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %+v", rec.Code, body)
	}
	if diff := cmp.Diff(status(), body.Channels); diff != "" {
		t.Errorf("channels (-want +got):\n%s", diff)
	}

	shutting.Store(true)
	if rec, body := get(); rec.Code != 503 || body.Status != "shutting_down" {
		t.Fatalf("while shutting down: %d %+v", rec.Code, body)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/health", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /health = %d", rec.Code)
	}
}

type fakeHeartbeats struct {
	mu      sync.Mutex
	beats   []time.Time
	states  []string
	removed []string
	err     error
}

func (f *fakeHeartbeats) Beat(_ context.Context, id string, interval time.Duration, state []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beats = append(f.beats, time.Now())
	f.states = append(f.states, string(state))
	return f.err
}

func (f *fakeHeartbeats) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, id)
	return nil
}

func TestRunHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeHeartbeats{err: errors.New("database is down")}
		logs := &syncBuffer{}
		logger := slog.New(slog.NewJSONHandler(logs, nil))
		state := func() map[string]ChannelStatus { return map[string]ChannelStatus{"sms": {Breaker: "closed"}} }

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		start := time.Now()
		go func() { RunHeartbeat(ctx, store, "w-1", 5*time.Second, state, logger); close(done) }()
		time.Sleep(12 * time.Second)
		cancel()
		<-done

		store.mu.Lock()
		defer store.mu.Unlock()
		var offsets []time.Duration
		for _, b := range store.beats {
			offsets = append(offsets, b.Sub(start))
		}
		if diff := cmp.Diff([]time.Duration{0, 5 * time.Second, 10 * time.Second}, offsets); diff != "" {
			t.Errorf("beat times (-want +got):\n%s", diff)
		}
		if store.states[0] != `{"sms":{"breaker":"closed","paused":false}}` {
			t.Errorf("state = %s", store.states[0])
		}
		if diff := cmp.Diff([]string{"w-1"}, store.removed); diff != "" {
			t.Errorf("removed (-want +got):\n%s", diff)
		}
		// Errors are logged and the beats go on.
		if got := logs.String(); !strings.Contains(got, "heartbeat write failed") {
			t.Errorf("logs = %s", got)
		}
	})
}
