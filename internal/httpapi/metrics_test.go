package httpapi_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAPIMetrics(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	single := `{"recipient":"+905551234567","channel":"sms","content":"hi","priority":"high","idempotency_key":"m-1"}`
	want(t, e.do("POST", "/notifications", single), 201, "")
	want(t, e.do("POST", "/notifications", single), 200, "") // idempotent replay: not counted
	batch := `{"items":[
		{"recipient":"+905551234567","channel":"sms","content":"hi","priority":"high","idempotency_key":"m-1"},
		{"recipient":"a@example.com","channel":"email","content":"hello","idempotency_key":"m-2"},
		{"recipient":"device-token-1","channel":"push","content":"ping","priority":"low"}]}`
	want(t, e.do("POST", "/notifications/batch", batch), 201, "")
	want(t, e.do("GET", "/notifications/0196e9a0-0000-7000-8000-000000000000", ""), 404, "about:blank")
	want(t, e.do("GET", "/no/such/path/0196e9a0-0000-7000-8000-000000000001", ""), 404, "about:blank")

	created := `
# HELP notifications_created_total Notifications created, by channel and priority. Idempotent replays are not counted.
# TYPE notifications_created_total counter
notifications_created_total{channel="email",priority="normal"} 1
notifications_created_total{channel="push",priority="low"} 1
notifications_created_total{channel="sms",priority="high"} 1
`
	if err := testutil.GatherAndCompare(e.reg, strings.NewReader(created), "notifications_created_total"); err != nil {
		t.Error(err)
	}

	got := requestCounts(t, e.reg)
	for series, n := range map[string]float64{
		"POST|POST /notifications|201":       1,
		"POST|POST /notifications|200":       1,
		"POST|POST /notifications/batch|201": 1,
		"GET|GET /notifications/{id}|404":    1,
		"GET|unmatched|404":                  1,
	} {
		if got[series] != n {
			t.Errorf("http_requests_total{%s} = %v, want %v (all: %v)", series, got[series], n, got)
		}
	}

	// GET /metrics itself, through the contract check (text/plain).
	r := e.do("GET", "/metrics", "")
	if r.status != 200 || !strings.HasPrefix(r.header.Get("Content-Type"), "text/plain") {
		t.Fatalf("GET /metrics = %d %s", r.status, r.header.Get("Content-Type"))
	}
	if !strings.Contains(string(r.body), "http_request_duration_seconds_bucket") || !strings.Contains(string(r.body), "go_goroutines") {
		t.Errorf("/metrics body lacks expected series")
	}
	assertSafeLabels(t, e.reg, "+905551234567", "a@example.com", "device-token-1")
}

func requestCounts(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "http_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			l := map[string]string{}
			for _, lp := range m.GetLabel() {
				l[lp.GetName()] = lp.GetValue()
			}
			out[l["method"]+"|"+l["route"]+"|"+l["status"]] = m.GetCounter().GetValue()
		}
	}
	return out
}

var uuidRE = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-`)

// assertSafeLabels checks that no label value anywhere is an ID, a raw
// path, a URL or one of the given recipients (D-015 label rule).
func assertSafeLabels(t *testing.T, reg *prometheus.Registry, recipients ...string) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				v := lp.GetValue()
				bad := uuidRE.MatchString(v) || strings.Contains(v, "://") || strings.HasPrefix(v, "/no/")
				for _, r := range recipients {
					bad = bad || strings.Contains(v, r)
				}
				if bad {
					t.Errorf("%s has an unsafe label %s=%q", mf.GetName(), lp.GetName(), v)
				}
			}
		}
	}
}
