package retry

import (
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	resp := func(code int) Result { return Result{Transport: TransportResponded, StatusCode: code} }

	tests := []struct {
		name string
		in   Result
		want Class
	}{
		{"202 accepted", resp(202), ClassSuccess},
		{"200 ok", resp(200), ClassSuccess},
		{"299", resp(299), ClassSuccess},
		{"500", resp(500), ClassTransient},
		{"502", resp(502), ClassTransient},
		{"503", resp(503), ClassTransient},
		{"599", resp(599), ClassTransient},
		{"408 request timeout", resp(408), ClassTransient},
		{"connection refused / never sent", Result{Transport: TransportNotSent}, ClassTransient},
		{"timeout after write", Result{Transport: TransportUnknown}, ClassUnknown},
		{"429", resp(429), ClassRateLimited},
		{"301", resp(301), ClassConfig},
		{"302", resp(302), ClassConfig},
		{"308", resp(308), ClassConfig},
		{"401", resp(401), ClassConfig},
		{"403", resp(403), ClassConfig},
		{"404", resp(404), ClassConfig},
		{"400", resp(400), ClassPermanent},
		{"409", resp(409), ClassPermanent},
		{"413", resp(413), ClassPermanent},
		{"422", resp(422), ClassPermanent},
		{"499", resp(499), ClassPermanent},
		{"1xx as final answer", resp(103), ClassUnknown},
		{"out-of-range code", resp(600), ClassUnknown},
		{"zero code", resp(0), ClassUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.in); got != tt.want {
				t.Fatalf("Classify = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestClassProperties(t *testing.T) {
	tests := []struct {
		class       Class
		usesAttempt bool
		channelWide bool
		outcome     string
	}{
		{ClassSuccess, false, false, "success"},
		{ClassTransient, true, false, "transient"},
		{ClassUnknown, true, false, "unknown"},
		{ClassRateLimited, false, true, "channel_wide"},
		{ClassConfig, false, true, "channel_wide"},
		{ClassPermanent, true, false, "permanent"},
	}
	for _, tt := range tests {
		t.Run(tt.class.String(), func(t *testing.T) {
			if got := tt.class.UsesAttempt(); got != tt.usesAttempt {
				t.Errorf("UsesAttempt = %v, want %v", got, tt.usesAttempt)
			}
			if got := tt.class.ChannelWide(); got != tt.channelWide {
				t.Errorf("ChannelWide = %v, want %v", got, tt.channelWide)
			}
			if got := tt.class.Outcome(); got != tt.outcome {
				t.Errorf("Outcome = %q, want %q", got, tt.outcome)
			}
		})
	}
}

func TestDecide(t *testing.T) {
	const max = 6
	tests := []struct {
		name     string
		class    Class
		attempts int
		want     Action
	}{
		{"success on first try", ClassSuccess, 1, ActionMarkSent},
		{"success on last try", ClassSuccess, max, ActionMarkSent},
		{"permanent on first try", ClassPermanent, 1, ActionFail},
		{"transient with attempts left", ClassTransient, 1, ActionRetry},
		{"transient on second to last", ClassTransient, max - 1, ActionRetry},
		{"transient on last attempt", ClassTransient, max, ActionFail},
		{"unknown with attempts left", ClassUnknown, 3, ActionRetry},
		{"unknown on last attempt", ClassUnknown, max, ActionFail},
		{"429 never fails the row", ClassRateLimited, max, ActionRequeue},
		{"config error never fails the row", ClassConfig, max, ActionRequeue},
		{"invalid class fails safe", Class(0), 1, ActionFail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Decide(tt.class, tt.attempts, max); got != tt.want {
				t.Fatalf("Decide = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestExhaustedOnReclaim(t *testing.T) {
	tests := []struct {
		attempts, max int
		want          bool
	}{
		{1, 6, false},
		{5, 6, false},
		{6, 6, true},
		{1, 1, true},
	}
	for _, tt := range tests {
		if got := ExhaustedOnReclaim(tt.attempts, tt.max); got != tt.want {
			t.Errorf("ExhaustedOnReclaim(%d, %d) = %v, want %v", tt.attempts, tt.max, got, tt.want)
		}
	}
}

func TestRateLimitPause(t *testing.T) {
	const cap = 5 * time.Minute
	tests := []struct {
		name string
		in   Result
		want time.Duration
	}{
		{"header absent uses default", Result{StatusCode: 429}, 30 * time.Second},
		{"header honoured", Result{StatusCode: 429, RetryAfter: 45 * time.Second, HasRetryAfter: true}, 45 * time.Second},
		{"header capped", Result{StatusCode: 429, RetryAfter: time.Hour, HasRetryAfter: true}, cap},
		{"zero header uses default", Result{StatusCode: 429, RetryAfter: 0, HasRetryAfter: true}, 30 * time.Second},
		{"negative header uses default", Result{StatusCode: 429, RetryAfter: -time.Second, HasRetryAfter: true}, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RateLimitPause(tt.in, cap); got != tt.want {
				t.Fatalf("RateLimitPause = %s, want %s", got, tt.want)
			}
		})
	}
	if got := RateLimitPause(Result{}, 10*time.Second); got != 10*time.Second {
		t.Fatalf("default must also respect the cap, got %s", got)
	}
}

func defaultBackoff(rnd func(int64) int64) Backoff {
	return Backoff{Base: 10 * time.Second, Cap: 10 * time.Minute, Rand: rnd}
}

func TestBackoffLimit(t *testing.T) {
	b := defaultBackoff(nil)
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 10 * time.Second}, // treated as 1
		{1, 10 * time.Second},
		{2, 30 * time.Second},
		{3, 90 * time.Second},
		{4, 270 * time.Second},
		{5, 600 * time.Second}, // 810 s capped
		{6, 600 * time.Second},
		{1000, 600 * time.Second}, // no overflow
	}
	for _, tt := range tests {
		if got := b.Limit(tt.attempt); got != tt.want {
			t.Errorf("Limit(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffWaitBounds(t *testing.T) {
	lowest := defaultBackoff(func(int64) int64 { return 0 })
	highest := defaultBackoff(func(n int64) int64 { return n - 1 })

	var minTotal, maxTotal time.Duration
	for attempt := 1; attempt <= 5; attempt++ {
		limit := lowest.Limit(attempt)
		lo, hi := lowest.Wait(attempt), highest.Wait(attempt)
		if lo != limit/2 {
			t.Errorf("attempt %d: lowest wait %s, want %s", attempt, lo, limit/2)
		}
		if hi != limit {
			t.Errorf("attempt %d: highest wait %s, want %s", attempt, hi, limit)
		}
		minTotal += lo
		maxTotal += hi
	}
	// D-010: the retry window is about 8–17 minutes.
	if minTotal != 500*time.Second || maxTotal != 1000*time.Second {
		t.Fatalf("retry window %s–%s, want 8m20s–16m40s", minTotal, maxTotal)
	}
}

func TestBackoffWaitUsesInjectedRandomness(t *testing.T) {
	var gotN int64
	b := defaultBackoff(func(n int64) int64 { gotN = n; return 7 })
	if got := b.Wait(1); got != 5*time.Second+7 {
		t.Fatalf("Wait = %s, want 5s+7ns", got)
	}
	if gotN != int64(5*time.Second)+1 {
		t.Fatalf("random range n = %d, want limit/2+1", gotN)
	}
}

func TestBackoffDefaultRandomStaysInRange(t *testing.T) {
	b := defaultBackoff(nil)
	for i := 0; i < 1000; i++ {
		for attempt := 1; attempt <= 6; attempt++ {
			limit := b.Limit(attempt)
			if w := b.Wait(attempt); w < limit/2 || w > limit {
				t.Fatalf("attempt %d: wait %s outside [%s, %s]", attempt, w, limit/2, limit)
			}
		}
	}
}
