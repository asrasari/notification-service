package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/asrasari/notification-service/internal/httpapi"
	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/postgres"
	"github.com/asrasari/notification-service/internal/testdb"
)

func TestMain(m *testing.M) { testdb.Main(m) }

// env is one API server on its own database.
type env struct {
	t    *testing.T
	srv  *httptest.Server
	pool *pgxpool.Pool
}

func newEnv(t *testing.T) *env {
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
	srv := httptest.NewServer(httpapi.NewHandler(logger, pool, postgres.NewStore(pool, 6)))
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, pool: pool}
}

// response is a finished HTTP exchange.
type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("body is not a JSON object: %v\n%s", err, r.body)
	}
	return m
}

// do sends a request. A non-empty body is sent as application/json unless
// contentType says otherwise ("-" sends no Content-Type).
func (e *env) do(method, path, body string, contentType ...string) response {
	e.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	ct := "application/json"
	if len(contentType) > 0 {
		ct = contentType[0]
	}
	if body != "" && ct != "-" {
		req.Header.Set("Content-Type", ct)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return response{status: resp.StatusCode, header: resp.Header, body: b}
}

// want fails unless the response has status, and for errors also checks the
// problem content type and type.
func want(t *testing.T, r response, status int, problemType string) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status = %d, want %d\n%s", r.status, status, r.body)
	}
	if problemType == "" {
		if ct := r.header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		return
	}
	if ct := r.header.Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	p := r.json(t)
	if p["type"] != problemType || p["status"] != float64(status) {
		t.Errorf("problem type %v status %v, want %s %d", p["type"], p["status"], problemType, status)
	}
	if p["correlation_id"] != r.header.Get(httpapi.CorrelationIDHeader) {
		t.Errorf("problem correlation_id %v, header %q", p["correlation_id"], r.header.Get(httpapi.CorrelationIDHeader))
	}
}

// fields returns the field paths of a problem's errors.
func fields(t *testing.T, r response) []string {
	t.Helper()
	var p struct {
		Errors []struct{ Field string } `json:"errors"`
	}
	if err := json.Unmarshal(r.body, &p); err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(p.Errors))
	for i, e := range p.Errors {
		out[i] = e.Field
	}
	return out
}

const sms = `{"recipient":"+905551234567","channel":"sms","content":"Hello"}`

func smsWithKey(key, content string) string {
	return fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":%q,"idempotency_key":%q}`, content, key)
}

func TestCreateNotification(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	r := e.do("POST", "/notifications", smsWithKey("k1", "Merhaba şube"))
	want(t, r, 201, "")
	n := r.json(t)
	if loc := r.header.Get("Location"); loc != "/notifications/"+n["id"].(string) {
		t.Errorf("Location = %q, id %v", loc, n["id"])
	}
	if n["status"] != "pending" || n["priority"] != "normal" || n["attempts"] != float64(0) || n["max_attempts"] != float64(6) {
		t.Errorf("unexpected body: %s", r.body)
	}
	if diff := cmp.Diff(map[string]any{"encoding": "ucs2", "segments": float64(1)}, n["sms"]); diff != "" {
		t.Errorf("sms (-want +got):\n%s", diff)
	}
	if n["next_attempt_at"] == nil || n["correlation_id"] != r.header.Get(httpapi.CorrelationIDHeader) {
		t.Errorf("next_attempt_at %v correlation_id %v", n["next_attempt_at"], n["correlation_id"])
	}
	for _, hidden := range []string{"request_hash", "lease_token", "delivery_attempts"} {
		if _, ok := n[hidden]; ok {
			t.Errorf("%s must not be in the create response", hidden)
		}
	}

	t.Run("idempotent repeat is 200 with the same notification", func(t *testing.T) {
		again := e.do("POST", "/notifications", smsWithKey("k1", "Merhaba şube"))
		want(t, again, 200, "")
		if again.json(t)["id"] != n["id"] || again.header.Get("Location") != "" {
			t.Errorf("repeat: %s", again.body)
		}
	})
	t.Run("same key with other content is 409", func(t *testing.T) {
		want(t, e.do("POST", "/notifications", smsWithKey("k1", "Other")), 409, "/problems/idempotency-conflict")
	})
	t.Run("email has no sms block", func(t *testing.T) {
		r := e.do("POST", "/notifications", `{"recipient":"ann@example.com","channel":"email","content":"Hi","priority":"high"}`)
		want(t, r, 201, "")
		if _, ok := r.json(t)["sms"]; ok {
			t.Error("email response has an sms block")
		}
	})
}

func TestCreateNotificationErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	tests := []struct {
		name        string
		body        string
		contentType string
		status      int
		problem     string
		fields      []string
	}{
		{"no content type", sms, "-", 415, "about:blank", nil},
		{"wrong content type", sms, "text/plain", 415, "about:blank", nil},
		{"content type with charset is fine", sms, "application/json; charset=utf-8", 201, "", nil},
		{"empty body", "", "application/json", 400, "/problems/malformed-request", nil},
		{"broken JSON", `{"recipient":`, "", 400, "/problems/malformed-request", nil},
		{"trailing data", sms + `{}`, "", 400, "/problems/malformed-request", nil},
		{"wrong type", `{"recipient":"+905551234567","channel":"sms","content":5}`, "", 400, "/problems/malformed-request", nil},
		{"array instead of object", `[]`, "", 400, "/problems/malformed-request", nil},
		{"unknown field", `{"recipient":"+905551234567","channel":"sms","content":"x","colour":"red"}`, "", 422, "/problems/validation-failed", []string{"colour"}},
		{"all rule violations at once", `{"channel":"sms","content":" ","priority":"urgent"}`, "", 422, "/problems/validation-failed", []string{"recipient", "content", "priority"}},
		{"scheduled_at is refused", `{"recipient":"+905551234567","channel":"sms","content":"x","scheduled_at":"2030-01-01T00:00:00Z"}`, "", 422, "/problems/validation-failed", []string{"scheduled_at"}},
		{"too many SMS segments", fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":%q}`, strings.Repeat("a", 1531)), "", 422, "/problems/validation-failed", []string{"content"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := []string{}
			if tt.contentType != "" {
				ct = append(ct, tt.contentType)
			}
			var r response
			if tt.body == "" {
				// An empty body still carries the content type.
				req, _ := http.NewRequest("POST", e.srv.URL+"/notifications", strings.NewReader(""))
				req.Header.Set("Content-Type", tt.contentType)
				resp, err := e.srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				r = response{resp.StatusCode, resp.Header, b}
			} else {
				r = e.do("POST", "/notifications", tt.body, ct...)
			}
			want(t, r, tt.status, tt.problem)
			if tt.fields != nil {
				if diff := cmp.Diff(tt.fields, fields(t, r)); diff != "" {
					t.Errorf("error fields (-want +got):\n%s", diff)
				}
			}
		})
	}

	t.Run("body over 10 MiB is 413", func(t *testing.T) {
		big := `{"recipient":"ann@example.com","channel":"email","content":"` + strings.Repeat("a", httpapi.MaxBodyBytes) + `"}`
		want(t, e.do("POST", "/notifications", big), 413, "about:blank")
	})
}

func TestGetNotification(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.do("POST", "/notifications", sms).json(t)["id"].(string)

	r := e.do("GET", "/notifications/"+id, "")
	want(t, r, 200, "")
	n := r.json(t)
	if n["id"] != id {
		t.Errorf("id = %v", n["id"])
	}
	if attempts, ok := n["delivery_attempts"].([]any); !ok || len(attempts) != 0 {
		t.Errorf("delivery_attempts = %v, want []", n["delivery_attempts"])
	}

	_, err := e.pool.Exec(context.Background(), `
		INSERT INTO delivery_attempts (id, notification_id, attempt_no, started_at, duration_ms, http_status, outcome, error)
		VALUES (gen_random_uuid(), $1, 1, now(), 12, 503, 'transient', 'provider returned 503')`, id)
	if err != nil {
		t.Fatal(err)
	}
	attempts := e.do("GET", "/notifications/"+id, "").json(t)["delivery_attempts"].([]any)
	if len(attempts) != 1 || attempts[0].(map[string]any)["outcome"] != "transient" || attempts[0].(map[string]any)["http_status"] != float64(503) {
		t.Errorf("delivery_attempts = %v", attempts)
	}

	want(t, e.do("GET", "/notifications/0196e9a0-0000-7000-8000-000000000000", ""), 404, "about:blank")
	want(t, e.do("GET", "/notifications/not-a-uuid", ""), 400, "/problems/malformed-request")
}

func TestListNotifications(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var created []string
	for i := range 5 {
		created = append(created, e.do("POST", "/notifications", smsWithKey(fmt.Sprint(i), "x")).json(t)["id"].(string))
	}
	e.do("POST", "/notifications", `{"recipient":"ann@example.com","channel":"email","content":"Hi"}`)

	t.Run("cursor pages newest first", func(t *testing.T) {
		var got []string
		path := "/notifications?channel=sms&limit=2"
		for range 5 {
			r := e.do("GET", path, "")
			want(t, r, 200, "")
			var page struct {
				Data       []struct{ ID string }
				NextCursor *string `json:"next_cursor"`
			}
			if err := json.Unmarshal(r.body, &page); err != nil {
				t.Fatal(err)
			}
			for _, n := range page.Data {
				got = append(got, n.ID)
			}
			if page.NextCursor == nil {
				break
			}
			path = "/notifications?channel=sms&limit=2&cursor=" + *page.NextCursor
		}
		wantIDs := []string{created[4], created[3], created[2], created[1], created[0]}
		if diff := cmp.Diff(wantIDs, got); diff != "" {
			t.Errorf("ids (-want +got):\n%s", diff)
		}
	})

	t.Run("filters combine", func(t *testing.T) {
		r := e.do("GET", "/notifications?status=pending&channel=email", "")
		want(t, r, 200, "")
		if data := r.json(t)["data"].([]any); len(data) != 1 {
			t.Errorf("got %d rows, want 1", len(data))
		}
	})

	tests := []struct {
		name    string
		query   string
		status  int
		problem string
		fields  []string
	}{
		{"unknown parameter", "?stauts=failed", 422, "/problems/validation-failed", []string{"stauts"}},
		{"repeated parameter", "?status=sent&status=failed", 422, "/problems/validation-failed", []string{"status"}},
		{"bad values are all reported", "?status=done&channel=fax&limit=0&created_from=yesterday&batch_id=1", 422, "/problems/validation-failed",
			[]string{"status", "channel", "created_from", "batch_id", "limit"}},
		{"limit over 200", "?limit=201", 422, "/problems/validation-failed", []string{"limit"}},
		{"bad cursor", "?cursor=bm9wZQ", 400, "/problems/malformed-request", nil},
		{"undecodable cursor", "?cursor=%%%", 400, "/problems/malformed-request", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := e.do("GET", "/notifications"+tt.query, "")
			want(t, r, tt.status, tt.problem)
			if tt.fields != nil {
				if diff := cmp.Diff(tt.fields, fields(t, r)); diff != "" {
					t.Errorf("error fields (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestCancelNotification(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.do("POST", "/notifications", sms).json(t)["id"].(string)

	r := e.do("POST", "/notifications/"+id+"/cancel", "")
	want(t, r, 200, "")
	if n := r.json(t); n["status"] != "cancelled" || n["next_attempt_at"] != nil {
		t.Errorf("cancelled: %s", r.body)
	}
	want(t, e.do("POST", "/notifications/"+id+"/cancel", ""), 200, "") // idempotent

	sent := e.do("POST", "/notifications", sms).json(t)["id"].(string)
	if _, err := e.pool.Exec(context.Background(), `UPDATE notifications SET status = 'sent' WHERE id = $1`, sent); err != nil {
		t.Fatal(err)
	}
	want(t, e.do("POST", "/notifications/"+sent+"/cancel", ""), 409, "/problems/invalid-state")
	want(t, e.do("POST", "/notifications/0196e9a0-0000-7000-8000-000000000000/cancel", ""), 404, "about:blank")
	want(t, e.do("POST", "/notifications/123/cancel", ""), 400, "/problems/malformed-request")
}

func TestBatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	body := `{"items":[` + smsWithKey("a", "x") + `,` + sms + `,` + smsWithKey("b", "y") + `]}`
	r := e.do("POST", "/notifications/batch", body)
	want(t, r, 201, "")
	var res struct {
		BatchID *string `json:"batch_id"`
		Items   []struct {
			Index     int
			ID        string
			Status    string
			Duplicate bool
			BatchID   *string `json:"batch_id"`
		}
	}
	if err := json.Unmarshal(r.body, &res); err != nil {
		t.Fatal(err)
	}
	if res.BatchID == nil || len(res.Items) != 3 {
		t.Fatalf("batch: %s", r.body)
	}
	for i, it := range res.Items {
		if it.Index != i || it.Duplicate || it.Status != "pending" || *it.BatchID != *res.BatchID {
			t.Errorf("item %d: %+v", i, it)
		}
	}
	batchID := *res.BatchID

	t.Run("replay is 200 with the original batch", func(t *testing.T) {
		r := e.do("POST", "/notifications/batch", `{"items":[`+smsWithKey("b", "y")+`,`+smsWithKey("a", "x")+`]}`)
		want(t, r, 200, "")
		m := r.json(t)
		items := m["items"].([]any)
		if m["batch_id"] != batchID || items[0].(map[string]any)["id"] != res.Items[2].ID || items[0].(map[string]any)["duplicate"] != true {
			t.Errorf("replay: %s", r.body)
		}
	})

	t.Run("errors", func(t *testing.T) {
		tests := []struct {
			name    string
			body    string
			status  int
			problem string
			fields  []string
		}{
			{"invalid items report their index", `{"items":[` + sms + `,{"channel":"sms","content":"x"},` + sms + `,{"recipient":"+905551234567","channel":"fax","content":"x"}]}`,
				422, "/problems/validation-failed", []string{"items[1].recipient", "items[3].channel"}},
			{"duplicate key within the batch", `{"items":[` + smsWithKey("d", "x") + `,` + smsWithKey("d", "x") + `]}`,
				422, "/problems/validation-failed", []string{"items[1].idempotency_key"}},
			{"unknown fields in items", `{"items":[{"recipient":"+905551234567","channel":"sms","content":"x","foo":1},` + sms + `,{"bar":2}]}`,
				422, "/problems/validation-failed", []string{"items[0].foo", "items[2].bar"}},
			{"unknown top-level field", `{"items":[` + sms + `],"extra":true}`, 422, "/problems/validation-failed", []string{"extra"}},
			{"empty batch", `{"items":[]}`, 422, "/problems/validation-failed", []string{"items"}},
			{"missing items", `{}`, 422, "/problems/validation-failed", []string{"items"}},
			{"wrong type in an item", `{"items":[` + sms + `,{"recipient":1}]}`, 400, "/problems/malformed-request", nil},
			{"key reused with other content", `{"items":[` + smsWithKey("new", "x") + `,` + smsWithKey("a", "changed") + `]}`,
				409, "/problems/idempotency-conflict", []string{"items[1].idempotency_key"}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				r := e.do("POST", "/notifications/batch", tt.body)
				want(t, r, tt.status, tt.problem)
				if tt.fields != nil {
					if diff := cmp.Diff(tt.fields, fields(t, r)); diff != "" {
						t.Errorf("error fields (-want +got):\n%s", diff)
					}
				}
			})
		}
		// The 409 batch stored nothing: "new" must still be free.
		want(t, e.do("POST", "/notifications", smsWithKey("new", "x")), 201, "")
	})

	t.Run("summary has every status", func(t *testing.T) {
		r := e.do("GET", "/batches/"+batchID, "")
		want(t, r, 200, "")
		want := map[string]any{"pending": float64(3), "processing": float64(0), "sent": float64(0), "failed": float64(0), "cancelled": float64(0)}
		m := r.json(t)
		if diff := cmp.Diff(want, m["counts"]); diff != "" || m["total"] != float64(3) {
			t.Errorf("counts (-want +got):\n%s total %v", diff, m["total"])
		}
	})

	t.Run("cancel", func(t *testing.T) {
		r := e.do("POST", "/batches/"+batchID+"/cancel", "")
		want(t, r, 200, "")
		if m := r.json(t); m["cancelled"] != float64(3) || m["batch_id"] != batchID {
			t.Errorf("cancel: %s", r.body)
		}
		r = e.do("POST", "/batches/"+batchID+"/cancel", "")
		want(t, r, 200, "")
		if r.json(t)["cancelled"] != float64(0) {
			t.Errorf("second cancel: %s", r.body)
		}
	})

	missing := "0196e9a0-0000-7000-8000-000000000000"
	want(t, e.do("GET", "/batches/"+missing, ""), 404, "about:blank")
	want(t, e.do("POST", "/batches/"+missing+"/cancel", ""), 404, "about:blank")
	want(t, e.do("GET", "/batches/xyz", ""), 400, "/problems/malformed-request")
}

func TestJSONFallbacks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	want(t, e.do("GET", "/nope", ""), 404, "about:blank")
	r := e.do("DELETE", "/notifications", "")
	want(t, r, 405, "about:blank")
	if allow := r.header.Get("Allow"); !strings.Contains(allow, "GET") || !strings.Contains(allow, "POST") {
		t.Errorf("Allow = %q, want GET and POST", allow)
	}
	want(t, e.do("PUT", "/health", ""), 405, "about:blank")
}
