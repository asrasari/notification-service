package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/provider"
	"github.com/asrasari/notification-service/internal/retry"
)

// --- fakes ---

type fakeStore struct {
	mu        sync.Mutex
	claims    [][]notification.Priority
	claimedAt []time.Time
	completes []postgres.Completion
	// next returns the row for a claim, or nil when nothing is due.
	next        func(order []notification.Priority) *notification.Notification
	completeErr error
}

func (s *fakeStore) Claim(_ context.Context, _ notification.Channel, order []notification.Priority) (postgres.ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims = append(s.claims, append([]notification.Priority(nil), order...))
	s.claimedAt = append(s.claimedAt, time.Now())
	if s.next == nil {
		return postgres.ClaimResult{}, nil
	}
	return postgres.ClaimResult{Claimed: s.next(order)}, nil
}

func (s *fakeStore) Complete(_ context.Context, c postgres.Completion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completes = append(s.completes, c)
	return s.completeErr
}

func (s *fakeStore) snapshot() ([][]notification.Priority, []time.Time, []postgres.Completion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claims, s.claimedAt, s.completes
}

type fakeSender struct {
	mu     sync.Mutex
	sentAt []time.Time
	ctxs   []context.Context
	msgs   []provider.Message
	reply  func(ctx context.Context) provider.Response
}

func (f *fakeSender) Send(ctx context.Context, m provider.Message) provider.Response {
	f.mu.Lock()
	f.sentAt = append(f.sentAt, time.Now())
	f.ctxs = append(f.ctxs, ctx)
	f.msgs = append(f.msgs, m)
	reply := f.reply
	f.mu.Unlock()
	if reply == nil {
		return accepted()
	}
	return reply(ctx)
}

func (f *fakeSender) times() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.sentAt...)
}

type fakeBreaker struct {
	mu       sync.Mutex
	allowErr error
	dones    []error
}

func (b *fakeBreaker) Allow() (func(error), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.allowErr != nil {
		return nil, b.allowErr
	}
	return func(err error) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.dones = append(b.dones, err)
	}, nil
}

func (b *fakeBreaker) State() string { return "closed" }

func (b *fakeBreaker) results() []error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]error(nil), b.dones...)
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// --- helpers ---

func accepted() provider.Response {
	return provider.Response{
		Result:    retry.Result{Transport: retry.TransportResponded, StatusCode: 202},
		MessageID: "m-1",
		Duration:  20 * time.Millisecond,
	}
}

func status(code int) provider.Response {
	return provider.Response{
		Result:   retry.Result{Transport: retry.TransportResponded, StatusCode: code},
		Err:      fmt.Errorf("provider answered HTTP %d", code),
		Duration: 5 * time.Millisecond,
	}
}

func row(p notification.Priority, attempts int) *notification.Notification {
	token := uuid.New()
	corr := "corr-42"
	return &notification.Notification{
		ID: uuid.New(), Channel: notification.ChannelSMS, Priority: p,
		Recipient: "+905550000000", Content: "very secret text",
		Status: notification.StatusProcessing, Attempts: attempts, MaxAttempts: 6,
		LeaseToken: &token, CorrelationID: &corr,
	}
}

// endless always has a row due.
func endless(order []notification.Priority) *notification.Notification { return row(order[0], 1) }

// rows hands out n rows, then reports an empty queue.
func rows(n int, attempts int) func([]notification.Priority) *notification.Notification {
	left := n
	return func(order []notification.Priority) *notification.Notification {
		if left == 0 {
			return nil
		}
		left--
		return row(order[0], attempts)
	}
}

var testCfg = Config{
	PollInterval:       200 * time.Millisecond,
	Concurrency:        50,
	Weights:            [3]int{70, 20, 10},
	Backoff:            retry.Backoff{Base: 10 * time.Second, Cap: 10 * time.Minute, Rand: func(int64) int64 { return 0 }},
	RetryAfterCap:      5 * time.Minute,
	BreakerOpenTimeout: 30 * time.Second,
}

type harness struct {
	w       *Worker
	store   *fakeStore
	sender  *fakeSender
	breaker *fakeBreaker
	logs    *syncBuffer
	events  *[]AttemptEvent
}

func newHarness(cfg Config, limiter Limiter) *harness {
	h := &harness{store: &fakeStore{}, sender: &fakeSender{}, breaker: &fakeBreaker{}, logs: &syncBuffer{}}
	var (
		mu     sync.Mutex
		events []AttemptEvent
	)
	h.events = &events
	cfg.OnAttempt = func(e AttemptEvent) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	logger := slog.New(logging.NewContextHandler(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if limiter == nil {
		limiter = rate.NewLimiter(rate.Inf, 1)
	}
	h.w = New(cfg, h.store, h.sender, logger, []ChannelDeps{{Channel: notification.ChannelSMS, Breaker: h.breaker, Limiter: limiter}})
	return h
}

// runFor runs the worker for d of fake time, then stops it and waits.
func (h *harness) runFor(d time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		h.w.Run(ctx, context.Background())
		close(stopped)
	}()
	time.Sleep(d)
	cancel()
	<-stopped
}

// --- dispatcher tests ---

func TestBreakerOpenMeansNoClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.breaker.allowErr = errors.New("circuit breaker is open")
		h.store.next = endless
		h.runFor(5 * time.Second)
		if claims, _, _ := h.store.snapshot(); len(claims) != 0 {
			t.Fatalf("%d claims while the breaker was open", len(claims))
		}
	})
}

func TestPausedChannelMeansNoClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(1, 1)
		start := time.Now()
		h.w.channels[0].pause(2 * time.Second)
		if st := h.w.Status()["sms"]; !st.Paused {
			t.Fatalf("status = %+v, want paused", st)
		}
		h.runFor(3 * time.Second)
		_, at, _ := h.store.snapshot()
		if len(at) == 0 {
			t.Fatal("no claim after the pause ended")
		}
		if first := at[0].Sub(start); first < 2*time.Second {
			t.Fatalf("first claim %v after start, want after the 2s pause", first)
		}
		if st := h.w.Status()["sms"]; st.Paused {
			t.Errorf("still paused after the pause ended")
		}
	})
}

func TestEmptyQueuePollsAndReportsNothingClaimed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.runFor(time.Second - time.Millisecond)
		claims, _, _ := h.store.snapshot()
		if len(claims) != 5 { // t = 0, 200, 400, 600, 800 ms
			t.Fatalf("%d claims in 1s with a 200ms poll interval, want 5", len(claims))
		}
		dones := h.breaker.results()
		if len(dones) != len(claims) {
			t.Fatalf("%d breaker results for %d claims", len(dones), len(claims))
		}
		for i, err := range dones {
			if !errors.Is(err, ErrNothingClaimed) {
				t.Errorf("result %d = %v, want ErrNothingClaimed (never nil)", i, err)
			}
		}
		if n := len(h.sender.times()); n != 0 {
			t.Errorf("%d sends from an empty queue", n)
		}
	})
}

func TestClaimsFollowWRR(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(20, 1)
		h.runFor(time.Second)
		claims, _, _ := h.store.snapshot()
		if len(claims) < 20 {
			t.Fatalf("%d claims, want at least 20", len(claims))
		}
		first := make([]notification.Priority, 20)
		for i := range first {
			first[i] = claims[i][0]
		}
		if got := letters(first); got != "HHNHHLHHNHHHNHHLHHNH" {
			t.Errorf("picked priorities = %s", got)
		}
		// Every claim also lists the fallback in strict order.
		for i, order := range claims[:20] {
			if len(order) != 3 {
				t.Fatalf("claim %d order = %v", i, order)
			}
		}
	})
}

// TestRateLimitEvenlySpaced is the D-007 synctest: endless work for 10 fake
// seconds gives at most 1001 sends, 10 ms apart.
func TestRateLimitEvenlySpaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, rate.NewLimiter(rate.Limit(100), 1))
		h.store.next = endless
		h.runFor(10 * time.Second)

		sent := h.sender.times()
		if len(sent) > 1001 || len(sent) < 990 {
			t.Fatalf("%d sends in 10s at 100/s, want 990–1001", len(sent))
		}
		for i := 1; i < len(sent); i++ {
			if gap := sent[i].Sub(sent[i-1]); gap < 10*time.Millisecond-time.Microsecond {
				t.Fatalf("send %d came %v after the previous one, want >= 10ms", i, gap)
			}
		}
	})
}

func TestConcurrencyIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testCfg
		cfg.Concurrency = 3
		h := newHarness(cfg, nil)
		h.store.next = endless
		release := make(chan struct{})
		h.sender.reply = func(context.Context) provider.Response { <-release; return accepted() }

		ctx, cancel := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		go func() { h.w.Run(ctx, context.Background()); close(stopped) }()
		time.Sleep(time.Second)
		if n := len(h.sender.times()); n != 3 {
			t.Fatalf("%d sends in flight, want 3 (WORKER_CONCURRENCY)", n)
		}
		cancel()
		close(release)
		<-stopped
	})
}

// --- sender tests ---

func TestOutcomes(t *testing.T) {
	tests := []struct {
		name       string
		attempts   int
		resp       provider.Response
		wantStatus notification.Status
		wantOut    notification.Outcome
		wantDelay  time.Duration
		refund     bool
		httpStatus int // 0 = NULL
		excluded   bool
		success    bool
		paused     bool
	}{
		{name: "202", attempts: 1, resp: accepted(), wantStatus: notification.StatusSent, wantOut: notification.OutcomeSuccess, httpStatus: 202, success: true},
		{name: "503 with attempts left", attempts: 1, resp: status(503), wantStatus: notification.StatusPending, wantOut: notification.OutcomeTransient, wantDelay: 5 * time.Second, httpStatus: 503},
		{name: "503 on the third attempt", attempts: 3, resp: status(503), wantStatus: notification.StatusPending, wantOut: notification.OutcomeTransient, wantDelay: 45 * time.Second, httpStatus: 503},
		{name: "503 on the last attempt", attempts: 6, resp: status(503), wantStatus: notification.StatusFailed, wantOut: notification.OutcomeTransient, httpStatus: 503},
		{name: "408", attempts: 1, resp: status(408), wantStatus: notification.StatusPending, wantOut: notification.OutcomeTransient, wantDelay: 5 * time.Second, httpStatus: 408},
		{name: "400", attempts: 1, resp: status(400), wantStatus: notification.StatusFailed, wantOut: notification.OutcomePermanent, httpStatus: 400, excluded: true},
		{name: "429 with Retry-After", attempts: 2, resp: provider.Response{
			Result: retry.Result{Transport: retry.TransportResponded, StatusCode: 429, RetryAfter: 3 * time.Second, HasRetryAfter: true},
			Err:    errors.New("provider answered HTTP 429"),
		}, wantStatus: notification.StatusPending, wantOut: notification.OutcomeChannelWide, wantDelay: 3 * time.Second, refund: true, httpStatus: 429, excluded: true, paused: true},
		{name: "429 over the cap", attempts: 1, resp: provider.Response{
			Result: retry.Result{Transport: retry.TransportResponded, StatusCode: 429, RetryAfter: time.Hour, HasRetryAfter: true},
			Err:    errors.New("provider answered HTTP 429"),
		}, wantStatus: notification.StatusPending, wantOut: notification.OutcomeChannelWide, wantDelay: 5 * time.Minute, refund: true, httpStatus: 429, excluded: true, paused: true},
		{name: "429 without Retry-After", attempts: 1, resp: status(429), wantStatus: notification.StatusPending, wantOut: notification.OutcomeChannelWide, wantDelay: 30 * time.Second, refund: true, httpStatus: 429, excluded: true, paused: true},
		{name: "404 is a config error", attempts: 1, resp: status(404), wantStatus: notification.StatusPending, wantOut: notification.OutcomeChannelWide, wantDelay: 30 * time.Second, refund: true, httpStatus: 404},
		{name: "timeout", attempts: 1, resp: provider.Response{
			Result: retry.Result{Transport: retry.TransportUnknown}, Err: errors.New("Post request: context deadline exceeded"),
		}, wantStatus: notification.StatusPending, wantOut: notification.OutcomeUnknown, wantDelay: 5 * time.Second},
		{name: "connection refused", attempts: 1, resp: provider.Response{
			Result: retry.Result{Transport: retry.TransportNotSent}, Err: errors.New("dial tcp: connection refused"),
		}, wantStatus: notification.StatusPending, wantOut: notification.OutcomeTransient, wantDelay: 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(testCfg, nil)
				h.store.next = rows(1, tt.attempts)
				h.sender.reply = func(context.Context) provider.Response { return tt.resp }
				h.runFor(100 * time.Millisecond)

				_, _, completes := h.store.snapshot()
				if len(completes) != 1 {
					t.Fatalf("%d completions, want 1", len(completes))
				}
				c := completes[0]
				if c.Status != tt.wantStatus || c.Attempt.Outcome != tt.wantOut || c.Delay != tt.wantDelay || c.RefundAttempt != tt.refund {
					t.Errorf("completion: status %s outcome %s delay %v refund %v; want %s %s %v %v",
						c.Status, c.Attempt.Outcome, c.Delay, c.RefundAttempt, tt.wantStatus, tt.wantOut, tt.wantDelay, tt.refund)
				}
				if c.Attempt.AttemptNo != tt.attempts {
					t.Errorf("attempt_no = %d, want %d", c.Attempt.AttemptNo, tt.attempts)
				}
				switch {
				case tt.httpStatus == 0 && c.Attempt.HTTPStatus != nil:
					t.Errorf("http_status = %d, want NULL", *c.Attempt.HTTPStatus)
				case tt.httpStatus != 0 && (c.Attempt.HTTPStatus == nil || *c.Attempt.HTTPStatus != tt.httpStatus):
					t.Errorf("http_status = %v, want %d", c.Attempt.HTTPStatus, tt.httpStatus)
				}
				if tt.success {
					if c.ProviderMessageID == nil || *c.ProviderMessageID != "m-1" || c.Attempt.Error != nil {
						t.Errorf("sent: message id %v, error %v", c.ProviderMessageID, c.Attempt.Error)
					}
				} else if c.Attempt.Error == nil {
					t.Errorf("no error text recorded")
				}

				dones := h.breaker.results()
				var sendResult error
				for _, d := range dones {
					if !errors.Is(d, ErrNothingClaimed) {
						sendResult = d
					}
				}
				if tt.success && sendResult != nil {
					t.Errorf("breaker got %v for a success, want nil", sendResult)
				}
				if !tt.success && (sendResult == nil || IsExcluded(sendResult) != tt.excluded) {
					t.Errorf("breaker got %v (excluded %v), want excluded %v", sendResult, IsExcluded(sendResult), tt.excluded)
				}
				if paused := h.w.channels[0].pausedFor() > 0; paused != tt.paused {
					t.Errorf("channel paused = %v, want %v", paused, tt.paused)
				}
				if len(*h.events) != 1 || (*h.events)[0].Status != tt.wantStatus {
					t.Errorf("OnAttempt events = %+v", *h.events)
				}
			})
		})
	}
}

func TestMessageAndLogs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(1, 1)
		h.runFor(100 * time.Millisecond)

		h.sender.mu.Lock()
		m, ctx := h.sender.msgs[0], h.sender.ctxs[0]
		h.sender.mu.Unlock()
		_, _, completes := h.store.snapshot()
		if m.IdempotencyKey != completes[0].ID.String() || m.To != "+905550000000" || m.Channel != "sms" || m.Content != "very secret text" {
			t.Errorf("message = %+v", m)
		}
		if id, ok := logging.CorrelationID(ctx); !ok || id != "corr-42" {
			t.Errorf("correlation id in the send context = %q, %v", id, ok)
		}

		logs := h.logs.String()
		for _, want := range []string{`"msg":"delivery attempt"`, `"correlation_id":"corr-42"`, `"notification_id":"` + completes[0].ID.String(), `"attempt":1`, `"outcome":"success"`, `"duration_ms":20`, `"channel":"sms"`} {
			if !strings.Contains(logs, want) {
				t.Errorf("logs lack %s:\n%s", want, logs)
			}
		}
		for _, pii := range []string{"+905550000000", "very secret text"} {
			if strings.Contains(logs, pii) {
				t.Errorf("logs contain %q (PII):\n%s", pii, logs)
			}
		}
	})
}

func TestStaleLeaseIsLoggedNotFatal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(2, 1)
		h.store.completeErr = postgres.ErrStaleLease
		h.runFor(100 * time.Millisecond)

		if n := len(h.sender.times()); n != 2 {
			t.Fatalf("%d sends, want 2 (the worker keeps going)", n)
		}
		if !strings.Contains(h.logs.String(), "the lease was lost") {
			t.Errorf("stale lease not logged:\n%s", h.logs.String())
		}
		if ev := *h.events; len(ev) != 2 || !ev[0].Stale {
			t.Errorf("events = %+v, want Stale", ev)
		}
		// The breaker still hears about the provider's answer.
		var successes int
		for _, d := range h.breaker.results() {
			if d == nil {
				successes++
			}
		}
		if successes != 2 {
			t.Errorf("breaker successes = %d, want 2", successes)
		}
	})
}

// TestShutdownLetsInFlightSendsFinish: cancelling the run context stops
// claiming but does not cancel a send under way (D-007).
func TestShutdownLetsInFlightSendsFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(1, 1)
		release := make(chan struct{})
		h.sender.reply = func(ctx context.Context) provider.Response {
			<-release
			if ctx.Err() != nil {
				t.Errorf("send context cancelled by shutdown: %v", ctx.Err())
			}
			return accepted()
		}

		ctx, cancel := context.WithCancel(context.Background())
		sendCtx, cancelSends := context.WithCancel(context.Background())
		defer cancelSends()
		stopped := make(chan struct{})
		go func() { h.w.Run(ctx, sendCtx); close(stopped) }()

		time.Sleep(time.Second)
		cancel()
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Run returned while a send was in flight")
		default:
		}
		claimsAtStop, _, _ := h.store.snapshot()

		time.Sleep(5 * time.Second)
		close(release)
		<-stopped

		claims, _, completes := h.store.snapshot()
		if len(claims) != len(claimsAtStop) {
			t.Errorf("claimed %d more rows after shutdown began", len(claims)-len(claimsAtStop))
		}
		if len(completes) != 1 || completes[0].Status != notification.StatusSent {
			t.Fatalf("completions = %+v, want the in-flight send written as sent", completes)
		}
	})
}

func TestResultIsWrittenEvenIfSendContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(testCfg, nil)
		h.store.next = rows(1, 1)
		sendCtx, cancelSends := context.WithCancel(context.Background())
		h.sender.reply = func(context.Context) provider.Response {
			cancelSends() // the shutdown timeout fires just as the 202 arrives
			return accepted()
		}
		ctx, cancel := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		go func() { h.w.Run(ctx, sendCtx); close(stopped) }()
		time.Sleep(100 * time.Millisecond)
		cancel()
		<-stopped

		if _, _, completes := h.store.snapshot(); len(completes) != 1 || completes[0].Status != notification.StatusSent {
			t.Fatalf("completions = %+v", completes)
		}
	})
}
