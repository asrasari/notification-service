// Package provider is the HTTP client for the external notification provider
// (webhook.site in this case). It sends one message and turns whatever
// happened — a response, or a transport error — into plain values that
// retry.Classify understands (D-010, D-019). It makes no retry decisions.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asrasari/notification-service/internal/retry"
)

// maxBodyBytes caps how much of a response body is read. The body only
// matters for the best-effort messageId.
const maxBodyBytes = 64 << 10

// Message is one notification to send.
type Message struct {
	// IdempotencyKey is sent as the Idempotency-Key header: the notification
	// id, the same on every attempt (D-009).
	IdempotencyKey string
	To             string
	Channel        string
	Content        string
}

// Response is what one send produced.
type Response struct {
	// Result feeds retry.Classify.
	Result retry.Result
	// MessageID is the provider's messageId from a 2xx body, or "".
	MessageID string
	// MessageIDErr explains why a 2xx body had no usable messageId. The send
	// still succeeded; the caller only logs this (D-010).
	MessageIDErr error
	// Err describes a non-2xx answer or the transport error, for last_error
	// and the attempt history. It never contains the provider URL, which is a
	// bearer secret for webhook.site. Nil on 2xx.
	Err error
	// Duration is the time from starting the request to the end of reading the
	// response (or the error).
	Duration time.Duration
}

// Options configures the client.
type Options struct {
	URL     string
	Timeout time.Duration // PROVIDER_TIMEOUT, covers the whole exchange
	// MaxConns is how many connections to keep open to the provider; set it to
	// the total number of senders so busy channels reuse connections.
	MaxConns int
	// Now returns the current time; it is used to turn a Retry-After HTTP-date
	// into a duration. Nil means time.Now.
	Now func() time.Time
}

// Client sends messages to the provider. It is safe for concurrent use.
type Client struct {
	url  string
	http *http.Client
	now  func() time.Time
}

// New returns a Client. It never follows redirects: a 3xx is returned as
// is and classified as a configuration error (D-010).
func New(o Options) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if o.MaxConns > 0 {
		tr.MaxIdleConns = o.MaxConns
		tr.MaxIdleConnsPerHost = o.MaxConns
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &Client{
		url: o.URL,
		http: &http.Client{
			Transport: tr,
			Timeout:   o.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now: now,
	}
}

type requestBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type responseBody struct {
	MessageID *string `json:"messageId"`
}

// Send posts one message and reports what happened. It never returns an
// error of its own: every failure is described in the Response.
func (c *Client) Send(ctx context.Context, m Message) Response {
	start := time.Now()
	resp := c.send(ctx, m)
	resp.Duration = time.Since(start)
	return resp
}

func (c *Client) send(ctx context.Context, m Message) Response {
	body, err := json.Marshal(requestBody{To: m.To, Channel: m.Channel, Content: m.Content})
	if err != nil { // can't happen for three strings
		return Response{Result: retry.Result{Transport: retry.TransportNotSent}, Err: fmt.Errorf("encode request: %w", err)}
	}

	// A request can only have reached the provider if a connection was
	// obtained. Without one it was certainly never sent (dial, DNS or proxy
	// failure); with one, any error is an unknown outcome (D-019).
	var gotConn atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { gotConn.Store(true) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Response{Result: retry.Result{Transport: retry.TransportNotSent}, Err: fmt.Errorf("build request: %w", stripURL(err))}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Idempotency-Key", m.IdempotencyKey)

	res, err := c.http.Do(req)
	if err != nil {
		t := retry.TransportUnknown
		if !gotConn.Load() {
			t = retry.TransportNotSent
		}
		return Response{Result: retry.Result{Transport: t}, Err: stripURL(err)}
	}
	defer func() { _ = res.Body.Close() }() // the body was drained above; a close error changes nothing

	// The status is what counts; the body is read only for the messageId, so
	// a failure to read it doesn't change the outcome.
	data, readErr := io.ReadAll(io.LimitReader(res.Body, maxBodyBytes))
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxBodyBytes)) // let the connection be reused

	out := Response{Result: retry.Result{Transport: retry.TransportResponded, StatusCode: res.StatusCode}}
	if d, ok := parseRetryAfter(res.Header.Get("Retry-After"), c.now()); ok {
		out.Result.RetryAfter, out.Result.HasRetryAfter = d, true
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		out.Err = fmt.Errorf("provider answered HTTP %d", res.StatusCode)
		return out
	}
	if readErr != nil {
		out.MessageIDErr = fmt.Errorf("read response body: %w", stripURL(readErr))
		return out
	}
	out.MessageID, out.MessageIDErr = parseMessageID(data)
	return out
}

// maxMessageIDLen keeps a misbehaving provider from filling the column.
const maxMessageIDLen = 255

func parseMessageID(data []byte) (string, error) {
	var b responseBody
	if err := json.Unmarshal(data, &b); err != nil {
		return "", fmt.Errorf("response body is not the expected JSON: %w", err)
	}
	switch {
	case b.MessageID == nil:
		return "", errors.New("response body has no messageId")
	case *b.MessageID == "":
		return "", errors.New("response body has an empty messageId")
	case len(*b.MessageID) > maxMessageIDLen:
		return "", fmt.Errorf("messageId is longer than %d bytes", maxMessageIDLen)
	}
	return *b.MessageID, nil
}

// parseRetryAfter reads a Retry-After value: delay-seconds or an HTTP-date
// (RFC 9110 §10.2.3). A date in the past means "now" (0).
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		// Anything this large is far beyond RETRY_AFTER_CAP; avoid overflow.
		const maxSecs = int64(24 * time.Hour / time.Second)
		return time.Duration(min(secs, maxSecs)) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0), true
	}
	return 0, false
}

// stripURL removes the *url.Error wrapper, whose message contains the
// provider URL.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s request: %w", ue.Op, ue.Err)
	}
	return err
}
