package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func post(t *testing.T, h http.Handler, channel string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"to":"+905551234567","channel":"` + channel + `","content":"hi"}`
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/hook", strings.NewReader(body)))
	return rec
}

func getStats(t *testing.T, h http.Handler) map[string]channelStats {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/stats", nil))
	var out struct{ Channels map[string]channelStats }
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("stats: %v: %s", err, rec.Body.String())
	}
	return out.Channels
}

func TestAnswersLikeTheProvider(t *testing.T) {
	h := newProvider(0, 0, nil).handler()
	rec := post(t, h, "sms")
	if rec.Code != http.StatusAccepted || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(body["messageId"]) {
		t.Errorf("messageId = %q", body["messageId"])
	}
	if body["status"] != "accepted" {
		t.Errorf("status = %q", body["status"])
	}
	if _, err := time.Parse(time.RFC3339Nano, body["timestamp"]); err != nil {
		t.Errorf("timestamp %q: %v", body["timestamp"], err)
	}
}

func TestRejectsBadBody(t *testing.T) {
	h := newProvider(0, 0, nil).handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/hook", strings.NewReader("nope")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestFailureRate(t *testing.T) {
	p := newProvider(0, 0.5, nil)
	n := 0
	p.rnd = func() float64 { n++; return []float64{0.1, 0.9}[n%2] } // alternates fail / ok
	h := p.handler()
	codes := map[int]int{}
	for range 10 {
		codes[post(t, h, "email").Code]++
	}
	if codes[500] != 5 || codes[202] != 5 {
		t.Fatalf("codes = %v, want 5 × 500 and 5 × 202", codes)
	}
	if s := getStats(t, h)["email"]; s.Received != 10 || s.Failed != 5 {
		t.Errorf("stats = %+v", s)
	}
}

func TestLatency(t *testing.T) {
	h := newProvider(30*time.Millisecond, 0, nil).handler()
	start := time.Now()
	post(t, h, "push")
	if d := time.Since(start); d < 30*time.Millisecond {
		t.Errorf("answered after %v, want >= 30ms", d)
	}
}

// TestPeakPerSecond drives the clock by hand: the peak is the most arrivals
// in any 1-second window, per channel.
func TestPeakPerSecond(t *testing.T) {
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	p := newProvider(0, 0, func() time.Time { return now })
	h := p.handler()

	// sms: 100 arrivals 10 ms apart (0 … 990 ms), then one at exactly 1 s:
	// the window (0, 1s] holds 100, never 101.
	for i := range 100 {
		now = time.Date(2026, 11, 15, 10, 0, 0, i*int(10*time.Millisecond), time.UTC)
		post(t, h, "sms")
	}
	now = time.Date(2026, 11, 15, 10, 0, 1, 0, time.UTC)
	post(t, h, "sms")
	// email: a burst of 7 at one instant, then 3 more 2 s later.
	for range 7 {
		post(t, h, "email")
	}
	now = now.Add(2 * time.Second)
	for range 3 {
		post(t, h, "email")
	}

	st := getStats(t, h)
	if s := st["sms"]; s.Received != 101 || s.PeakPerSecond != 100 {
		t.Errorf("sms = %+v, want 101 received, peak 100", s)
	}
	if s := st["email"]; s.Received != 10 || s.PeakPerSecond != 7 {
		t.Errorf("email = %+v, want 10 received, peak 7", s)
	}
	if s := st["sms"]; s.FirstAt == nil || s.LastAt == nil || s.LastAt.Sub(*s.FirstAt) != time.Second {
		t.Errorf("sms first/last = %v/%v", s.FirstAt, s.LastAt)
	}
	if _, ok := st["push"]; ok {
		t.Error("push appears without requests")
	}
}
