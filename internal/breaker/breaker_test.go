package breaker

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/retry"
	"github.com/asrasari/notification-service/internal/worker"
)

// cfg is the D-010 default configuration.
var cfg = config.Breaker{Window: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: 30 * time.Second}

var (
	transient = &worker.SendError{Class: retry.ClassTransient, Err: errors.New("HTTP 503")}
	configErr = &worker.SendError{Class: retry.ClassConfig, Err: errors.New("HTTP 404")}
	permanent = &worker.SendError{Class: retry.ClassPermanent, Err: errors.New("HTTP 400")}
	limited   = &worker.SendError{Class: retry.ClassRateLimited, Err: errors.New("HTTP 429")}
)

type changes struct {
	mu  sync.Mutex
	got []string
}

func (c *changes) record(name string, from, to State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, name+":"+string(from)+"->"+string(to))
}

func newBreaker(t *testing.T) (*Breaker, *changes) {
	t.Helper()
	ch := &changes{}
	return New(cfg, Options{Name: "sms", IsExcluded: worker.IsExcluded, OnStateChange: ch.record}), ch
}

// report runs n requests that all end with err.
func report(t *testing.T, b *Breaker, n int, err error) {
	t.Helper()
	for i := range n {
		done, aerr := b.Allow()
		if aerr != nil {
			t.Fatalf("request %d: Allow = %v", i, aerr)
		}
		done(err)
	}
}

// trip opens a closed breaker.
func trip(t *testing.T, b *Breaker) {
	t.Helper()
	report(t, b, 20, transient)
	if s := b.State(); s != StateOpen {
		t.Fatalf("state = %s after 20 failures, want open", s)
	}
}

// TestHalfOpenNothingClaimedKeepsProbeSlot is the D-007 required test: an
// empty claim in half-open must give the probe slot back.
func TestHalfOpenNothingClaimedKeepsProbeSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := newBreaker(t)
		trip(t, b)
		time.Sleep(cfg.OpenTimeout + time.Millisecond)
		if s := b.State(); s != StateHalfOpen {
			t.Fatalf("state = %s, want half-open", s)
		}

		for i := range 3 {
			done, err := b.Allow()
			if err != nil {
				t.Fatalf("poll %d: Allow = %v, want the probe slot back", i, err)
			}
			done(worker.ErrNothingClaimed)
			if s := b.State(); s != StateHalfOpen {
				t.Fatalf("poll %d: state = %s, want still half-open", i, s)
			}
		}

		// A real probe still decides.
		done, err := b.Allow()
		if err != nil {
			t.Fatalf("probe: Allow = %v", err)
		}
		done(nil)
		if s := b.State(); s != StateClosed {
			t.Fatalf("state = %s after a successful probe, want closed", s)
		}
	})
}

func TestTripsOnlyAtMinRequestsAndRatio(t *testing.T) {
	tests := []struct {
		name                string
		failures, successes int
		wantOpen            bool
	}{
		{"19 failures are below the minimum", 19, 0, false},
		{"20 failures", 20, 0, true},
		{"10 of 20 is 50 %", 10, 10, true},
		{"9 of 20 is below 50 %", 9, 11, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b, _ := newBreaker(t)
				// Successes first: gobreaker checks ReadyToTrip on each failure.
				report(t, b, tt.successes, nil)
				report(t, b, tt.failures, transient)
				if open := b.State() == StateOpen; open != tt.wantOpen {
					t.Fatalf("open = %v, want %v", open, tt.wantOpen)
				}
			})
		})
	}
	t.Run("config errors count as failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b, _ := newBreaker(t)
			report(t, b, 20, configErr)
			if s := b.State(); s != StateOpen {
				t.Fatalf("state = %s, want open", s)
			}
		})
	})
}

func TestExcludedResultsDontCount(t *testing.T) {
	for name, err := range map[string]error{
		"permanent":       permanent,
		"429":             limited,
		"nothing claimed": worker.ErrNothingClaimed,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b, _ := newBreaker(t)
				// Plenty of excluded results, then 19 failures: still below
				// the minimum, because excluded ones don't fill it.
				report(t, b, 100, err)
				report(t, b, 19, transient)
				if s := b.State(); s != StateClosed {
					t.Fatalf("state = %s, want closed", s)
				}
				// Excluded results don't dilute the ratio either.
				report(t, b, 1, transient)
				if s := b.State(); s != StateOpen {
					t.Fatalf("state = %s after the 20th failure, want open", s)
				}
			})
		})
	}
}

func TestOpenRefusesThenHalfOpenAllowsOneProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, ch := newBreaker(t)
		trip(t, b)
		if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
			t.Fatalf("Allow while open = %v, want ErrOpen", err)
		}
		time.Sleep(cfg.OpenTimeout - time.Second)
		if _, err := b.Allow(); !errors.Is(err, ErrOpen) {
			t.Fatalf("Allow before the timeout = %v, want ErrOpen", err)
		}
		time.Sleep(time.Second + time.Millisecond)

		done, err := b.Allow()
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if _, err := b.Allow(); !errors.Is(err, ErrTooManyRequests) {
			t.Fatalf("second Allow while probing = %v, want ErrTooManyRequests", err)
		}
		done(transient) // the probe fails: open again
		if s := b.State(); s != StateOpen {
			t.Fatalf("state = %s, want open", s)
		}

		time.Sleep(cfg.OpenTimeout + time.Millisecond)
		done, err = b.Allow()
		if err != nil {
			t.Fatalf("second probe: %v", err)
		}
		done(nil)

		want := []string{"sms:closed->open", "sms:open->half-open", "sms:half-open->open", "sms:open->half-open", "sms:half-open->closed"}
		ch.mu.Lock()
		defer ch.mu.Unlock()
		if len(ch.got) != len(want) {
			t.Fatalf("state changes = %v, want %v", ch.got, want)
		}
		for i := range want {
			if ch.got[i] != want[i] {
				t.Fatalf("state changes = %v, want %v", ch.got, want)
			}
		}
	})
}

func TestPermanentProbeKeepsHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := newBreaker(t)
		trip(t, b)
		time.Sleep(cfg.OpenTimeout + time.Millisecond)
		done, err := b.Allow()
		if err != nil {
			t.Fatal(err)
		}
		done(permanent) // says nothing about the provider
		if s := b.State(); s != StateHalfOpen {
			t.Fatalf("state = %s, want half-open", s)
		}
		if _, err := b.Allow(); err != nil {
			t.Fatalf("Allow after an excluded probe = %v", err)
		}
	})
}

// TestRollingWindow checks that failures leave the window gradually.
func TestRollingWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, _ := newBreaker(t)
		report(t, b, 15, transient) // t = 0
		time.Sleep(6 * time.Second) // still inside the 10 s window
		report(t, b, 4, transient)  // 19 in the window
		time.Sleep(5 * time.Second) // t = 11 s: the first 15 have left
		report(t, b, 15, transient) // 4 + 15 = 19: still closed
		if s := b.State(); s != StateClosed {
			t.Fatalf("state = %s, want closed (old failures should have left the window)", s)
		}
		report(t, b, 1, transient) // 20 within 10 s
		if s := b.State(); s != StateOpen {
			t.Fatalf("state = %s, want open", s)
		}
	})
}
