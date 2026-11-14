// Package retry decides what happens after one delivery attempt (D-010):
// how the provider's answer is classified, whether it uses up an attempt,
// what the row does next, and how long to back off.
//
// It works on plain values and imports none of the service's other packages;
// provider/ translates HTTP responses and transport errors into Result.
package retry

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// Transport says whether the request reached the provider.
type Transport int

const (
	// TransportResponded means an HTTP response was received; see StatusCode.
	TransportResponded Transport = iota
	// TransportNotSent means the request certainly never left (dial failed,
	// DNS failure, connection refused). Only use it when that is certain.
	TransportNotSent
	// TransportUnknown means the request may have been sent but no response
	// arrived (timeout, connection reset after writing). Anything uncertain
	// belongs here.
	TransportUnknown
)

// Result is what happened on one send, as plain values.
type Result struct {
	Transport  Transport
	StatusCode int // set when Transport == TransportResponded
	// RetryAfter is the parsed Retry-After header of a 429; HasRetryAfter is
	// false when the header was absent or unparsable.
	RetryAfter    time.Duration
	HasRetryAfter bool
}

// Class is the outcome class from the D-010 table.
type Class int

const (
	ClassSuccess     Class = iota + 1 // 2xx
	ClassTransient                    // 5xx, 408, or the request never sent
	ClassUnknown                      // may have been sent; retry with the same idempotency key
	ClassRateLimited                  // 429: pause the whole channel
	ClassConfig                       // 3xx, 401, 403, 404: our configuration, not this message
	ClassPermanent                    // any other 4xx: this message will never succeed
)

func (c Class) String() string {
	switch c {
	case ClassSuccess:
		return "success"
	case ClassTransient:
		return "transient"
	case ClassUnknown:
		return "unknown"
	case ClassRateLimited:
		return "rate_limited"
	case ClassConfig:
		return "config"
	case ClassPermanent:
		return "permanent"
	}
	return "invalid"
}

// Classify applies the D-010 table.
func Classify(r Result) Class {
	switch r.Transport {
	case TransportNotSent:
		return ClassTransient
	case TransportUnknown:
		return ClassUnknown
	}

	code := r.StatusCode
	switch {
	case code >= 200 && code <= 299:
		return ClassSuccess
	case code >= 500 && code <= 599, code == http.StatusRequestTimeout:
		return ClassTransient
	case code == http.StatusTooManyRequests:
		return ClassRateLimited
	case code >= 300 && code <= 399,
		code == http.StatusUnauthorized, code == http.StatusForbidden, code == http.StatusNotFound:
		return ClassConfig
	case code >= 400 && code <= 499:
		return ClassPermanent
	}
	// 1xx as a final answer, or a code outside 100–599: we can't tell whether
	// the provider took the message, so treat it as an unknown outcome.
	return ClassUnknown
}

// ChannelWide reports whether the problem is about the channel rather than
// this message (429 and configuration errors).
func (c Class) ChannelWide() bool {
	return c == ClassRateLimited || c == ClassConfig
}

// UsesAttempt reports whether the outcome counts against max_attempts. An
// attempt is used only when the problem could be about this message.
func (c Class) UsesAttempt() bool {
	switch c {
	case ClassTransient, ClassUnknown, ClassPermanent:
		return true
	}
	return false
}

// Outcome is the value stored in delivery_attempts.outcome.
func (c Class) Outcome() string {
	switch c {
	case ClassSuccess:
		return "success"
	case ClassTransient:
		return "transient"
	case ClassUnknown:
		return "unknown"
	case ClassPermanent:
		return "permanent"
	case ClassRateLimited, ClassConfig:
		return "channel_wide"
	}
	return "unknown"
}

// Action is what the worker does with the row after an attempt.
type Action int

const (
	// ActionMarkSent: processing → sent.
	ActionMarkSent Action = iota + 1
	// ActionRetry: processing → pending, available_at = now() + Backoff.Wait(attempts).
	ActionRetry
	// ActionFail: processing → failed.
	ActionFail
	// ActionRequeue: processing → pending and give the attempt back
	// (attempts - 1). Used for channel-wide outcomes.
	ActionRequeue
)

func (a Action) String() string {
	switch a {
	case ActionMarkSent:
		return "mark_sent"
	case ActionRetry:
		return "retry"
	case ActionFail:
		return "fail"
	case ActionRequeue:
		return "requeue"
	}
	return "invalid"
}

// Decide applies the attempts rule. attempts is the row's count after the
// claim incremented it, so it includes the attempt just made.
func Decide(c Class, attempts, maxAttempts int) Action {
	switch c {
	case ClassSuccess:
		return ActionMarkSent
	case ClassPermanent:
		return ActionFail
	case ClassRateLimited, ClassConfig:
		return ActionRequeue
	case ClassTransient, ClassUnknown:
		if attempts < maxAttempts {
			return ActionRetry
		}
		return ActionFail
	}
	// An invalid class is a programming error; failing the row is safer than
	// retrying it forever.
	return ActionFail
}

// ExhaustedOnReclaim reports whether a row found with an expired lease must be
// failed instead of reclaimed: the crashed attempt already used the last one.
// The caller records that attempt with outcome "unknown".
func ExhaustedOnReclaim(attempts, maxAttempts int) bool {
	return attempts >= maxAttempts
}

// DefaultRetryAfter is the channel pause used when a 429 carries no usable
// Retry-After header.
const DefaultRetryAfter = 30 * time.Second

// RateLimitPause returns how long to pause the channel after a 429: the
// provider's Retry-After, capped at maxPause (RETRY_AFTER_CAP), or
// DefaultRetryAfter when the header is absent or not positive.
func RateLimitPause(r Result, maxPause time.Duration) time.Duration {
	d := DefaultRetryAfter
	if r.HasRetryAfter && r.RetryAfter > 0 {
		d = r.RetryAfter
	}
	return min(d, maxPause)
}

// Backoff computes retry delays with exponential growth and equal jitter:
//
//	limit = min(Cap, Base × 3^(attempt-1))
//	wait  = limit/2 + random(0, limit/2)
//
// so a wait is never shorter than half its limit (no hot retries) and retries
// from many rows spread out instead of arriving together.
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a uniform value in [0, n). Nil means math/rand/v2.
	// Tests inject a fixed function to make waits deterministic.
	Rand func(n int64) int64
}

// Limit returns min(Cap, Base × 3^(attempt-1)). attempt < 1 is treated as 1.
func (b Backoff) Limit(attempt int) time.Duration {
	limit := b.Base
	for i := 1; i < attempt && limit < b.Cap; i++ {
		limit *= 3
	}
	return min(limit, b.Cap)
}

// Wait returns the jittered delay before the retry that follows attempt
// number attempt (1 = the first try just failed).
func (b Backoff) Wait(attempt int) time.Duration {
	limit := b.Limit(attempt)
	half := limit / 2
	rnd := b.Rand
	if rnd == nil {
		rnd = rand.Int64N
	}
	// random(0, limit/2) inclusive of both ends.
	return half + time.Duration(rnd(int64(limit-half)+1))
}
