package notification

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestCanTransition(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusProcessing}:    true,
		{StatusPending, StatusCancelled}:     true,
		{StatusProcessing, StatusSent}:       true,
		{StatusProcessing, StatusPending}:    true,
		{StatusProcessing, StatusFailed}:     true,
		{StatusProcessing, StatusProcessing}: true,
	}
	// Check every ordered pair, so any transition not in D-008 is proven forbidden.
	for _, from := range Statuses {
		for _, to := range Statuses {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
	if CanTransition("bogus", StatusPending) || CanTransition(StatusPending, "bogus") {
		t.Error("unknown statuses must never transition")
	}
}

func TestTerminal(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusPending, false},
		{StatusProcessing, false},
		{StatusSent, true},
		{StatusFailed, true},
		{StatusCancelled, true},
	}
	for _, tt := range tests {
		if got := tt.status.Terminal(); got != tt.want {
			t.Errorf("%s.Terminal() = %v, want %v", tt.status, got, tt.want)
		}
		if tt.want && len(transitions[tt.status]) > 0 {
			t.Errorf("terminal status %s has outgoing transitions", tt.status)
		}
	}
}

func TestParseStatus(t *testing.T) {
	for _, s := range Statuses {
		got, err := ParseStatus(string(s))
		if err != nil || got != s {
			t.Errorf("ParseStatus(%q) = %q, %v", s, got, err)
		}
	}
	for _, bad := range []string{"", "scheduled", "retrying", "delivered", "PENDING"} {
		if _, err := ParseStatus(bad); err == nil {
			t.Errorf("ParseStatus(%q) should fail", bad)
		}
	}
}

func TestParseChannel(t *testing.T) {
	for _, c := range Channels {
		got, err := ParseChannel(string(c))
		if err != nil || got != c {
			t.Errorf("ParseChannel(%q) = %q, %v", c, got, err)
		}
	}
	for _, bad := range []string{"", "SMS", "webhook", "mail"} {
		if _, err := ParseChannel(bad); err == nil {
			t.Errorf("ParseChannel(%q) should fail", bad)
		}
	}
}

func TestPriorityMapping(t *testing.T) {
	tests := []struct {
		text string
		db   int16
	}{
		{"high", 1},
		{"normal", 2},
		{"low", 3},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			p, err := ParsePriority(tt.text)
			if err != nil || int16(p) != tt.db {
				t.Fatalf("ParsePriority(%q) = %d, %v; want %d", tt.text, p, err, tt.db)
			}
			back, err := PriorityFromDB(tt.db)
			if err != nil || back != p || back.String() != tt.text {
				t.Fatalf("PriorityFromDB(%d) = %v, %v", tt.db, back, err)
			}
		})
	}
	if PriorityHigh >= PriorityNormal || PriorityNormal >= PriorityLow {
		t.Error("ascending smallint order must put high first")
	}
	for _, bad := range []string{"", "HIGH", "urgent", "1"} {
		if _, err := ParsePriority(bad); err == nil {
			t.Errorf("ParsePriority(%q) should fail", bad)
		}
	}
	for _, bad := range []int16{0, 4, -1} {
		if _, err := PriorityFromDB(bad); err == nil {
			t.Errorf("PriorityFromDB(%d) should fail", bad)
		}
	}
}

func TestPriorityJSON(t *testing.T) {
	b, err := json.Marshal(struct{ P Priority }{PriorityLow})
	if err != nil || string(b) != `{"P":"low"}` {
		t.Fatalf("marshal: %s, %v", b, err)
	}
	var v struct{ P Priority }
	if err := json.Unmarshal([]byte(`{"P":"high"}`), &v); err != nil || v.P != PriorityHigh {
		t.Fatalf("unmarshal: %v, %v", v.P, err)
	}
	if err := json.Unmarshal([]byte(`{"P":"urgent"}`), &v); err == nil {
		t.Fatal("unknown priority should fail to unmarshal")
	}
	if _, err := json.Marshal(struct{ P Priority }{Priority(9)}); err == nil {
		t.Fatal("invalid priority should fail to marshal")
	}
}

func TestRequestHash(t *testing.T) {
	at := time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC)
	base := HashInput{
		Recipient:   "+905551234567",
		Channel:     ChannelSMS,
		Content:     "Your message",
		Priority:    PriorityNormal,
		ScheduledAt: &at,
	}
	baseHash := RequestHash(base)
	if len(baseHash) != 32 {
		t.Fatalf("hash length %d, want 32", len(baseHash))
	}

	sameInstantOtherZone := at.In(time.FixedZone("TRT", 3*60*60))
	extraNanos := at.Add(999 * time.Nanosecond)

	tests := []struct {
		name     string
		modify   func(h *HashInput)
		wantSame bool
	}{
		{"identical input", func(h *HashInput) {}, true},
		{"same instant in another time zone", func(h *HashInput) { h.ScheduledAt = &sameInstantOtherZone }, true},
		{"sub-microsecond difference", func(h *HashInput) { h.ScheduledAt = &extraNanos }, true},
		{"different recipient", func(h *HashInput) { h.Recipient = "+905551234568" }, false},
		{"different channel", func(h *HashInput) { h.Channel = ChannelPush }, false},
		{"different content", func(h *HashInput) { h.Content = "Your message!" }, false},
		{"different priority", func(h *HashInput) { h.Priority = PriorityHigh }, false},
		{"not scheduled", func(h *HashInput) { h.ScheduledAt = nil }, false},
		{"different schedule", func(h *HashInput) { later := at.Add(time.Microsecond); h.ScheduledAt = &later }, false},
		{"field boundary shift", func(h *HashInput) {
			h.Recipient = base.Recipient + "Your"
			h.Content = " message"
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := base
			tt.modify(&in)
			same := bytes.Equal(RequestHash(in), baseHash)
			if same != tt.wantSame {
				t.Fatalf("hash equal = %v, want %v", same, tt.wantSame)
			}
		})
	}
}

func TestRequestHashEmptyFieldsDoNotCollide(t *testing.T) {
	a := RequestHash(HashInput{Recipient: "x", Content: ""})
	b := RequestHash(HashInput{Recipient: "", Content: "x"})
	if bytes.Equal(a, b) {
		t.Fatal("moving a value between fields must change the hash")
	}
}
