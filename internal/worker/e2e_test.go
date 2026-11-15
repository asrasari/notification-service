package worker_test

// End-to-end tests (D-017 layer 4): the real API handler and Postgres, the
// real worker, breaker, rate limiter and provider client, against an
// httptest provider. Real time with tiny settings; no synctest here.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"

	"github.com/asrasari/notification-service/internal/breaker"
	"github.com/asrasari/notification-service/internal/config"
	"github.com/asrasari/notification-service/internal/httpapi"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/provider"
	"github.com/asrasari/notification-service/internal/retry"
	"github.com/asrasari/notification-service/internal/testdb"
	"github.com/asrasari/notification-service/internal/worker"
)

func TestMain(m *testing.M) { testdb.Main(m) }

const (
	backoffBase   = 200 * time.Millisecond
	sendTimeout   = 300 * time.Millisecond
	retryAfterCap = 200 * time.Millisecond
)

// providerCall is what the fake provider saw.
type providerCall struct {
	key  string
	body map[string]string
}

// fakeProvider answers each call with the next reply; the last one repeats.
type fakeProvider struct {
	mu      sync.Mutex
	calls   []providerCall
	replies []func(w http.ResponseWriter)
	n       atomic.Int32
}

func (p *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	_ = json.NewDecoder(r.Body).Decode(&body)
	p.mu.Lock()
	p.calls = append(p.calls, providerCall{key: r.Header.Get("Idempotency-Key"), body: body})
	p.mu.Unlock()
	i := int(p.n.Add(1)) - 1
	p.replies[min(i, len(p.replies)-1)](w)
}

func reply(code int, headers ...string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		for i := 0; i+1 < len(headers); i += 2 {
			w.Header().Set(headers[i], headers[i+1])
		}
		w.WriteHeader(code)
		if code == http.StatusAccepted {
			_, _ = io.WriteString(w, `{"messageId":"provider-msg-1","status":"accepted","timestamp":"2026-11-15T10:00:00Z"}`)
		}
	}
}

func hang(w http.ResponseWriter) { time.Sleep(2 * sendTimeout); w.WriteHeader(http.StatusAccepted) }

type e2e struct {
	t        *testing.T
	api      *httptest.Server
	provider *fakeProvider
}

func start(t *testing.T, replies ...func(http.ResponseWriter)) *e2e {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.New(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger, err := logging.New(io.Discard, "error", "json")
	if err != nil {
		t.Fatal(err)
	}

	e := &e2e{t: t, provider: &fakeProvider{replies: replies}}
	e.api = httptest.NewServer(httpapi.NewHandler(logger, pool, postgres.NewStore(pool, 6)))
	t.Cleanup(e.api.Close)
	prov := httptest.NewServer(e.provider)
	t.Cleanup(prov.Close)

	cfg := worker.Config{
		PollInterval:       10 * time.Millisecond,
		Concurrency:        4,
		Weights:            [3]int{70, 20, 10},
		Backoff:            retry.Backoff{Base: backoffBase, Cap: time.Second, Rand: func(int64) int64 { return 0 }},
		RetryAfterCap:      retryAfterCap,
		BreakerOpenTimeout: 300 * time.Millisecond,
	}
	var deps []worker.ChannelDeps
	for _, ch := range notification.Channels {
		deps = append(deps, worker.ChannelDeps{
			Channel: ch,
			Breaker: breaker.New(config.Breaker{Window: 10 * time.Second, MinRequests: 20, FailureRatio: 0.5, OpenTimeout: time.Second},
				breaker.Options{Name: string(ch), IsExcluded: worker.IsExcluded}),
			Limiter: rate.NewLimiter(100, 1),
		})
	}
	w := worker.New(cfg, postgres.NewQueue(pool, 2*time.Second),
		provider.New(provider.Options{URL: prov.URL + "/hook", Timeout: sendTimeout, MaxConns: 4}), logger, deps)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx, context.Background()); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return e
}

func (e *e2e) create(body string) string {
	e.t.Helper()
	res, err := http.Post(e.api.URL+"/notifications", "application/json", strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	var n map[string]any
	_ = json.NewDecoder(res.Body).Decode(&n)
	if res.StatusCode != http.StatusCreated {
		e.t.Fatalf("create: %d %v", res.StatusCode, n)
	}
	return n["id"].(string)
}

type attemptJSON struct {
	AttemptNo  int       `json:"attempt_no"`
	StartedAt  time.Time `json:"started_at"`
	HTTPStatus *int      `json:"http_status"`
	Outcome    string    `json:"outcome"`
	Error      *string   `json:"error"`
}

type notificationJSON struct {
	Status            string        `json:"status"`
	Attempts          int           `json:"attempts"`
	ProviderMessageID *string       `json:"provider_message_id"`
	LastError         *string       `json:"last_error"`
	DeliveryAttempts  []attemptJSON `json:"delivery_attempts"`
}

func (e *e2e) get(id string) notificationJSON {
	e.t.Helper()
	res, err := http.Get(e.api.URL + "/notifications/" + id)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	var n notificationJSON
	if err := json.NewDecoder(res.Body).Decode(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// waitFor polls GET /notifications/{id} until the status is reached.
func (e *e2e) waitFor(id, status string) notificationJSON {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		n := e.get(id)
		if n.Status == status {
			return n
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("notification is %s with %+v after 15s, want %s", n.Status, n.DeliveryAttempts, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const smsBody = `{"recipient":"+905551234567","channel":"sms","content":"Your code is 1234"}`

func outcomes(n notificationJSON) []string {
	var out []string
	for _, a := range n.DeliveryAttempts {
		out = append(out, a.Outcome)
	}
	return out
}

func TestE2ESent(t *testing.T) {
	t.Parallel()
	e := start(t, reply(http.StatusAccepted))
	id := e.create(smsBody)

	n := e.waitFor(id, "sent")
	if n.Attempts != 1 || len(n.DeliveryAttempts) != 1 || n.DeliveryAttempts[0].Outcome != "success" ||
		*n.DeliveryAttempts[0].HTTPStatus != 202 || n.ProviderMessageID == nil || *n.ProviderMessageID != "provider-msg-1" {
		t.Fatalf("got %+v", n)
	}
	e.provider.mu.Lock()
	defer e.provider.mu.Unlock()
	call := e.provider.calls[0]
	if call.key != id || call.body["to"] != "+905551234567" || call.body["channel"] != "sms" || call.body["content"] != "Your code is 1234" {
		t.Errorf("provider got %+v", call)
	}
}

func TestE2ERetryThenSent(t *testing.T) {
	t.Parallel()
	e := start(t, reply(http.StatusInternalServerError), reply(http.StatusAccepted))
	id := e.create(smsBody)

	n := e.waitFor(id, "sent")
	if got := strings.Join(outcomes(n), ","); got != "transient,success" || n.Attempts != 2 {
		t.Fatalf("attempts %d, outcomes %s", n.Attempts, got)
	}
	a := n.DeliveryAttempts
	if *a[0].HTTPStatus != 500 || a[0].AttemptNo != 1 || a[1].AttemptNo != 2 {
		t.Errorf("attempts = %+v", a)
	}
	// Backoff after attempt 1 with no jitter: limit/2 = base/2.
	if gap := a[1].StartedAt.Sub(a[0].StartedAt); gap < backoffBase/2 {
		t.Errorf("retry came %v after the first try, want >= %v of backoff", gap, backoffBase/2)
	}
	e.provider.mu.Lock()
	defer e.provider.mu.Unlock()
	if len(e.provider.calls) != 2 || e.provider.calls[0].key != id || e.provider.calls[1].key != id {
		t.Errorf("provider calls = %+v, want two with Idempotency-Key %s", e.provider.calls, id)
	}
}

func TestE2EPermanentFails(t *testing.T) {
	t.Parallel()
	e := start(t, reply(http.StatusBadRequest))
	id := e.create(smsBody)

	n := e.waitFor(id, "failed")
	if got := strings.Join(outcomes(n), ","); got != "permanent" || n.LastError == nil {
		t.Fatalf("outcomes %s, last_error %v", got, n.LastError)
	}
}

func TestE2E429UsesNoAttempt(t *testing.T) {
	t.Parallel()
	e := start(t, reply(http.StatusTooManyRequests, "Retry-After", "1"), reply(http.StatusAccepted))
	id := e.create(smsBody)

	n := e.waitFor(id, "sent")
	if got := strings.Join(outcomes(n), ","); got != "channel_wide,success" {
		t.Fatalf("outcomes %s", got)
	}
	// The 429 gave its attempt back: the success is attempt 1 again.
	if n.Attempts != 1 || n.DeliveryAttempts[0].AttemptNo != 1 || n.DeliveryAttempts[1].AttemptNo != 1 {
		t.Errorf("attempts %d, history %+v", n.Attempts, n.DeliveryAttempts)
	}
	// Retry-After 1s capped at RETRY_AFTER_CAP.
	if gap := n.DeliveryAttempts[1].StartedAt.Sub(n.DeliveryAttempts[0].StartedAt); gap < retryAfterCap {
		t.Errorf("resent %v after the 429, want >= %v", gap, retryAfterCap)
	}
}

func TestE2ETimeoutIsUnknownThenSent(t *testing.T) {
	t.Parallel()
	e := start(t, hang, reply(http.StatusAccepted))
	id := e.create(smsBody)

	n := e.waitFor(id, "sent")
	if got := strings.Join(outcomes(n), ","); got != "unknown,success" || n.Attempts != 2 {
		t.Fatalf("attempts %d, outcomes %s", n.Attempts, got)
	}
	if n.DeliveryAttempts[0].HTTPStatus != nil {
		t.Errorf("timeout attempt has http_status %d", *n.DeliveryAttempts[0].HTTPStatus)
	}
}
