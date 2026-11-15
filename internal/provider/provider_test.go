package provider

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/asrasari/notification-service/internal/retry"
)

var msg = Message{IdempotencyKey: "0192f0c4-1111-7000-8000-000000000001", To: "+905551234567", Channel: "sms", Content: "Hello"}

func newClient(url string, timeout time.Duration) *Client {
	return New(Options{URL: url, Timeout: timeout, MaxConns: 4})
}

func TestSendRequestShape(t *testing.T) {
	var got struct {
		method, ctype, key string
		body               map[string]string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.method, got.ctype, got.key = r.Method, r.Header.Get("Content-Type"), r.Header.Get("Idempotency-Key")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"m-1","status":"accepted","timestamp":"2026-11-15T10:00:00Z"}`)
	}))
	defer srv.Close()

	res := newClient(srv.URL, time.Second).Send(context.Background(), msg)
	if res.Err != nil || res.MessageIDErr != nil {
		t.Fatalf("Err %v, MessageIDErr %v", res.Err, res.MessageIDErr)
	}
	if res.MessageID != "m-1" || res.Result.StatusCode != 202 || retry.Classify(res.Result) != retry.ClassSuccess {
		t.Errorf("got %+v", res)
	}
	if got.method != "POST" || got.ctype != "application/json" || got.key != msg.IdempotencyKey {
		t.Errorf("request: method %s, content-type %s, Idempotency-Key %q", got.method, got.ctype, got.key)
	}
	want := map[string]string{"to": msg.To, "channel": "sms", "content": "Hello"}
	if diff := cmp.Diff(want, got.body); diff != "" {
		t.Errorf("body (-want +got):\n%s", diff)
	}
	if res.Duration <= 0 {
		t.Errorf("Duration = %v", res.Duration)
	}
}

func TestSendStatuses(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		header        map[string]string
		body          string
		class         retry.Class
		retryAfter    time.Duration
		hasRetryAfter bool
		messageID     string
		messageIDErr  bool
	}{
		{name: "202 without body", status: 202, class: retry.ClassSuccess, messageIDErr: true},
		{name: "202 malformed body", status: 202, body: "accepted!", class: retry.ClassSuccess, messageIDErr: true},
		{name: "202 empty messageId", status: 202, body: `{"messageId":""}`, class: retry.ClassSuccess, messageIDErr: true},
		{name: "200 with messageId", status: 200, body: `{"messageId":"abc"}`, class: retry.ClassSuccess, messageID: "abc"},
		{name: "500", status: 500, class: retry.ClassTransient},
		{name: "503", status: 503, class: retry.ClassTransient},
		{name: "408", status: 408, class: retry.ClassTransient},
		{name: "429 without Retry-After", status: 429, class: retry.ClassRateLimited},
		{name: "429 with seconds", status: 429, header: map[string]string{"Retry-After": "7"}, class: retry.ClassRateLimited, retryAfter: 7 * time.Second, hasRetryAfter: true},
		{name: "429 with garbage", status: 429, header: map[string]string{"Retry-After": "soon"}, class: retry.ClassRateLimited},
		{name: "400", status: 400, class: retry.ClassPermanent},
		{name: "422", status: 422, class: retry.ClassPermanent},
		{name: "401", status: 401, class: retry.ClassConfig},
		{name: "404", status: 404, class: retry.ClassConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tt.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			res := newClient(srv.URL, time.Second).Send(context.Background(), msg)
			if c := retry.Classify(res.Result); c != tt.class {
				t.Errorf("class = %v, want %v", c, tt.class)
			}
			if res.Result.Transport != retry.TransportResponded || res.Result.StatusCode != tt.status {
				t.Errorf("result = %+v", res.Result)
			}
			if res.Result.RetryAfter != tt.retryAfter || res.Result.HasRetryAfter != tt.hasRetryAfter {
				t.Errorf("retry-after = %v/%v, want %v/%v", res.Result.RetryAfter, res.Result.HasRetryAfter, tt.retryAfter, tt.hasRetryAfter)
			}
			success := tt.class == retry.ClassSuccess
			if (res.Err == nil) != success {
				t.Errorf("Err = %v", res.Err)
			}
			if res.MessageID != tt.messageID || (res.MessageIDErr != nil) != tt.messageIDErr {
				t.Errorf("messageId %q, MessageIDErr %v", res.MessageID, res.MessageIDErr)
			}
		})
	}
}

func TestSend429WithHTTPDate(t *testing.T) {
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", now.Add(90*time.Second).Format(http.TimeFormat))
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New(Options{URL: srv.URL, Timeout: time.Second, Now: func() time.Time { return now }})
	res := c.Send(context.Background(), msg)
	if !res.Result.HasRetryAfter || res.Result.RetryAfter != 90*time.Second {
		t.Fatalf("retry-after = %v/%v, want 90s", res.Result.RetryAfter, res.Result.HasRetryAfter)
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			followed.Store(true)
			w.WriteHeader(http.StatusAccepted)
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	res := newClient(srv.URL, time.Second).Send(context.Background(), msg)
	if followed.Load() {
		t.Fatal("the redirect was followed")
	}
	if res.Result.StatusCode != 307 || retry.Classify(res.Result) != retry.ClassConfig {
		t.Fatalf("got %+v, want 307 classified as config", res.Result)
	}
}

func TestSendTimeoutIsUnknown(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	res := newClient(srv.URL+"/secret-token", 50*time.Millisecond).Send(context.Background(), msg)
	if res.Result.Transport != retry.TransportUnknown || retry.Classify(res.Result) != retry.ClassUnknown {
		t.Fatalf("got %+v, want unknown", res.Result)
	}
	assertNoURL(t, res.Err, srv.URL)
}

func TestSendConnectionRefusedIsNotSent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "http://" + ln.Addr().String() + "/secret-token"
	if err := ln.Close(); err != nil { // nothing listens there any more
		t.Fatal(err)
	}

	res := newClient(addr, time.Second).Send(context.Background(), msg)
	if res.Result.Transport != retry.TransportNotSent || retry.Classify(res.Result) != retry.ClassTransient {
		t.Fatalf("got %+v (err %v), want not sent / transient", res.Result, res.Err)
	}
	assertNoURL(t, res.Err, addr)
}

func TestSendDNSFailureIsNotSent(t *testing.T) {
	// .invalid never resolves (RFC 6761).
	res := newClient("http://provider.invalid/x", 2*time.Second).Send(context.Background(), msg)
	if res.Result.Transport != retry.TransportNotSent {
		t.Fatalf("got %+v (err %v), want not sent", res.Result, res.Err)
	}
}

// TestSendCutConnectionIsUnknown covers a server that drops the connection
// after reading the request: the provider may have acted on it.
func TestSendCutConnectionIsUnknown(t *testing.T) {
	for name, reply := range map[string]string{
		"reset before any response": "",
		"cut mid-status-line":       "HTTP/1.1 20",
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.ReadAll(r.Body)
				conn, buf, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				_, _ = buf.WriteString(reply)
				_ = buf.Flush()
				_ = conn.Close() // the point is to cut the connection
			}))
			defer srv.Close()

			res := newClient(srv.URL, time.Second).Send(context.Background(), msg)
			if res.Result.Transport != retry.TransportUnknown {
				t.Fatalf("got %+v (err %v), want unknown", res.Result, res.Err)
			}
		})
	}
}

func TestSendContextCancelledBeforeConnIsNotSent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := newClient("http://127.0.0.1:1/", time.Second).Send(ctx, msg)
	if res.Result.Transport != retry.TransportNotSent {
		t.Fatalf("got %+v, want not sent", res.Result)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"", 0, false},
		{"0", 0, true},
		{"120", 2 * time.Minute, true},
		{" 5 ", 5 * time.Second, true},
		{"-1", 0, false},
		{"1.5", 0, false},
		{"99999999999999", 24 * time.Hour, true},
		{now.Add(time.Minute).Format(http.TimeFormat), time.Minute, true},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
		{"Sunday, 15-Nov-26 10:00:30 GMT", 30 * time.Second, true}, // RFC 850 form
		{"tomorrow", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseRetryAfter(tt.in, now)
		if got != tt.want || ok != tt.ok {
			t.Errorf("parseRetryAfter(%q) = %v, %v; want %v, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func assertNoURL(t *testing.T, err error, url string) {
	t.Helper()
	if err == nil {
		t.Fatal("Err is nil")
	}
	// The host may appear (a dial error names it); the URL, whose path is the
	// webhook.site secret, must not.
	if strings.Contains(err.Error(), url) || strings.Contains(err.Error(), "secret-token") {
		t.Errorf("error %q contains the provider URL", err)
	}
}
