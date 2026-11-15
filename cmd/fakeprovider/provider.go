package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	mathrand "math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// channelStats is what the provider measured for one channel.
type channelStats struct {
	Received int `json:"received"`
	Failed   int `json:"failed"` // answered 500 (FAKE_FAILURE_RATE)
	// PeakPerSecond is the most requests received in any 1-second window.
	PeakPerSecond int        `json:"peak_per_second"`
	FirstAt       *time.Time `json:"first_at"`
	LastAt        *time.Time `json:"last_at"`

	window []time.Time // arrivals in the last second
}

type provider struct {
	latency     time.Duration
	failureRate float64
	now         func() time.Time
	rnd         func() float64

	mu       sync.Mutex
	channels map[string]*channelStats
}

// newProvider returns a provider; now is the clock (nil = time.Now).
func newProvider(latency time.Duration, failureRate float64, now func() time.Time) *provider {
	if now == nil {
		now = time.Now
	}
	return &provider{latency: latency, failureRate: failureRate, now: now, rnd: mathrand.Float64, channels: map[string]*channelStats{}}
}

func (p *provider) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", p.stats)
	mux.HandleFunc("POST /", p.receive)
	return mux
}

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// receive counts the request on arrival, waits FAKE_LATENCY, then answers.
func (p *provider) receive(w http.ResponseWriter, r *http.Request) {
	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.Channel == "" {
		http.Error(w, "body must be JSON with to, channel, content", http.StatusBadRequest)
		return
	}
	fail := p.record(req.Channel)

	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-r.Context().Done():
			return
		}
	}
	if fail {
		http.Error(w, "simulated failure", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"messageId": newUUID(),
		"status":    "accepted",
		"timestamp": p.now().UTC().Format(time.RFC3339Nano),
	})
}

// record counts one arrival and reports whether to fail it.
func (p *provider) record(channel string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	s := p.channels[channel]
	if s == nil {
		s = &channelStats{}
		p.channels[channel] = s
	}
	s.Received++
	if s.FirstAt == nil {
		t := now
		s.FirstAt = &t
	}
	t := now
	s.LastAt = &t

	// Sliding window (now-1s, now]: the peak over all windows is reached by
	// one that ends at an arrival, so checking at each arrival is exact.
	cut := 0
	for cut < len(s.window) && !s.window[cut].After(now.Add(-time.Second)) {
		cut++
	}
	s.window = append(s.window[cut:], now)
	s.PeakPerSecond = max(s.PeakPerSecond, len(s.window))

	fail := p.failureRate > 0 && p.rnd() < p.failureRate
	if fail {
		s.Failed++
	}
	return fail
}

func (p *provider) stats(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	out := make(map[string]channelStats, len(p.channels))
	for ch, s := range p.channels {
		c := *s
		c.window = nil
		out[ch] = c
	}
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"channels": out})
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
