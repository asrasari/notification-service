package httpapi_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestScheduledCreate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	at := time.Now().Add(time.Hour).Truncate(time.Second)
	// +03:00 in, UTC out.
	local := at.In(time.FixedZone("TRT", 3*3600)).Format(time.RFC3339)
	body := fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":"Reminder","idempotency_key":"sch-1","scheduled_at":%q}`, local)

	r := e.do("POST", "/notifications", body)
	want(t, r, 201, "")
	n := r.json(t)
	utc := at.UTC().Format(time.RFC3339)
	if n["scheduled_at"] != utc || n["next_attempt_at"] != utc || n["status"] != "pending" {
		t.Errorf("scheduled_at %v, next_attempt_at %v, status %v; want %s, pending", n["scheduled_at"], n["next_attempt_at"], n["status"], utc)
	}

	// Replay → 200; same key, other time → 409.
	want(t, e.do("POST", "/notifications", body), 200, "")
	other := fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":"Reminder","idempotency_key":"sch-1","scheduled_at":%q}`,
		at.Add(time.Minute).UTC().Format(time.RFC3339))
	want(t, e.do("POST", "/notifications", other), 409, "/problems/idempotency-conflict")

	// Cancellable while waiting.
	r = e.do("POST", "/notifications/"+n["id"].(string)+"/cancel", "")
	want(t, r, 200, "")
}

func TestScheduledWindow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	now := time.Now()
	for _, tt := range []struct {
		name string
		at   time.Time
		msg  string
	}{
		{"in the past", now.Add(-2 * time.Minute), "must be in the future"},
		{"more than 30 days ahead", now.Add(31 * 24 * time.Hour), "must be at most 30 days ahead"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := e.do("POST", "/notifications", fmt.Sprintf(`{"recipient":"+905551234567","channel":"sms","content":"x","scheduled_at":%q}`, tt.at.UTC().Format(time.RFC3339)))
			want(t, r, 422, "/problems/validation-failed")
			errs := r.json(t)["errors"].([]any)
			if got := errs[0].(map[string]any); got["field"] != "scheduled_at" || got["message"] != tt.msg {
				t.Errorf("errors = %v, want scheduled_at %q", errs, tt.msg)
			}
		})
	}
}

func TestScheduledBatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	body := fmt.Sprintf(`{"items":[
		{"recipient":"+905551234567","channel":"sms","content":"a","scheduled_at":%q},
		{"recipient":"+905551234567","channel":"sms","content":"b","scheduled_at":%q},
		{"recipient":"+905551234567","channel":"sms","content":"c"}]}`, future, past)

	r := e.do("POST", "/notifications/batch", body)
	want(t, r, 422, "/problems/validation-failed")
	if diff := cmp.Diff([]string{"items[1].scheduled_at"}, fields(t, r)); diff != "" {
		t.Errorf("fields (-want +got):\n%s", diff)
	}
	if got := e.do("GET", "/notifications", "").json(t)["data"].([]any); len(got) != 0 {
		t.Errorf("%d notifications stored, want none", len(got))
	}
}
