package httpapi_test

import (
	"context"
	"testing"
	"time"

	"github.com/asrasari/notification-service/internal/postgres"
)

// TestHealthWithHeartbeats runs /health against real heartbeat rows, with
// the contract check on every answer.
func TestHealthWithHeartbeats(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	hb := postgres.NewHeartbeats(e.pool)
	ctx := context.Background()

	check := func(step, wantStatus string, wantLive int) map[string]any {
		t.Helper()
		r := e.do("GET", "/health", "")
		want(t, r, 200, "")
		body := r.json(t)
		workers := body["checks"].(map[string]any)["workers"].(map[string]any)
		if body["status"] != wantStatus || workers["live"] != float64(wantLive) {
			t.Fatalf("%s: got %v, want status %s with %d live", step, body, wantStatus, wantLive)
		}
		return workers["channels"].(map[string]any)
	}

	check("no worker row", "degraded", 0)

	if _, err := e.pool.Exec(ctx, `INSERT INTO worker_heartbeats (worker_id, expires_at, state)
		VALUES ('crashed', now() - interval '1 second', '{"sms":{"breaker":"closed","paused":false}}')`); err != nil {
		t.Fatal(err)
	}
	check("stale row only", "degraded", 0)

	beat := func(state string) {
		t.Helper()
		if err := hb.Beat(ctx, "w-1", 5*time.Second, []byte(state)); err != nil {
			t.Fatal(err)
		}
	}
	beat(`{"sms":{"breaker":"closed","paused":false},"email":{"breaker":"closed","paused":false},"push":{"breaker":"closed","paused":false}}`)
	if ch := check("fresh and healthy", "ok", 1); len(ch) != 3 {
		t.Errorf("channels = %v", ch)
	}

	beat(`{"sms":{"breaker":"open","paused":false},"email":{"breaker":"closed","paused":false},"push":{"breaker":"closed","paused":false}}`)
	if ch := check("fresh, breaker open", "degraded", 1); ch["sms"].(map[string]any)["breaker"] != "open" {
		t.Errorf("channels = %v", ch)
	}

	beat(`{"sms":{"breaker":"closed","paused":false},"email":{"breaker":"closed","paused":true},"push":{"breaker":"closed","paused":false}}`)
	if ch := check("fresh, channel paused", "degraded", 1); ch["email"].(map[string]any)["paused"] != true {
		t.Errorf("channels = %v", ch)
	}
}
