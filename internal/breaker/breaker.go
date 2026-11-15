// Package breaker adapts gobreaker v2's TwoStepCircuitBreaker to the small
// Breaker interface the worker declares (D-010, D-019). One breaker guards
// each channel.
package breaker

import (
	"github.com/sony/gobreaker/v2"

	"github.com/asrasari/notification-service/internal/config"
)

// State is a breaker state as reported on /health and, later, as a metric.
// It is an alias of string so the worker's Breaker interface needs no import
// of this package.
type State = string

const (
	StateClosed   State = "closed"
	StateHalfOpen State = "half-open"
	StateOpen     State = "open"
)

// Errors returned by Allow when the breaker refuses a request.
var (
	ErrOpen            = gobreaker.ErrOpenState
	ErrTooManyRequests = gobreaker.ErrTooManyRequests
)

// maxHalfOpenRequests is the number of probes while half-open (D-010).
const maxHalfOpenRequests = 1

// bucketsPerWindow splits BREAKER_WINDOW into buckets so results leave the
// window gradually (a rolling window) instead of all at once.
const bucketsPerWindow = 10

// Options are the per-breaker hooks.
type Options struct {
	// Name identifies the breaker, e.g. the channel.
	Name string
	// IsExcluded reports results that must count as neither success nor
	// failure (worker.IsExcluded).
	IsExcluded func(error) bool
	// OnStateChange is called on every state change, with the breaker's lock
	// held: it must be quick and must not call back into the breaker. The
	// worker logs here; M4 sets the circuit_breaker_state gauge here.
	OnStateChange func(name string, from, to State)
}

// Breaker is one channel's circuit breaker.
type Breaker struct {
	cb *gobreaker.TwoStepCircuitBreaker[struct{}]
}

// New returns a closed breaker. In tests with testing/synctest it must be
// created inside the bubble, because it reads the clock (D-010).
func New(cfg config.Breaker, o Options) *Breaker {
	bucket := cfg.Window / bucketsPerWindow
	if bucket <= 0 {
		bucket = cfg.Window
	}
	minRequests, ratio := cfg.MinRequests, cfg.FailureRatio
	st := gobreaker.Settings{
		Name:         o.Name,
		MaxRequests:  maxHalfOpenRequests,
		Interval:     cfg.Window,
		BucketPeriod: bucket,
		Timeout:      cfg.OpenTimeout,
		// Only finished, non-excluded results count. gobreaker's Requests
		// also includes excluded and in-flight calls.
		ReadyToTrip: func(c gobreaker.Counts) bool {
			counted := c.TotalSuccesses + c.TotalFailures
			return counted >= minRequests && float64(c.TotalFailures) >= ratio*float64(counted)
		},
		IsExcluded: o.IsExcluded,
	}
	if o.OnStateChange != nil {
		st.OnStateChange = func(name string, from, to gobreaker.State) {
			o.OnStateChange(name, state(from), state(to))
		}
	}
	return &Breaker{cb: gobreaker.NewTwoStepCircuitBreaker[struct{}](st)}
}

// Allow asks to make one request. If allowed, the caller must call done
// exactly once with the result: nil for success, or an error (which
// IsExcluded may exclude). Otherwise it returns ErrOpen or ErrTooManyRequests.
func (b *Breaker) Allow() (done func(error), err error) {
	return b.cb.Allow()
}

// State returns the current state. Reading it can move an open breaker whose
// timeout has passed to half-open.
func (b *Breaker) State() State {
	return state(b.cb.State())
}

func state(s gobreaker.State) State {
	switch s {
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	case gobreaker.StateOpen:
		return StateOpen
	}
	return StateClosed
}
