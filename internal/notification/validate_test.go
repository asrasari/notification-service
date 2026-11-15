package notification

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func ptr[T any](v T) *T { return &v }

func TestComputeSMSInfo(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    SMSInfo
	}{
		// The cases D-011 names.
		{"160 × a fits one GSM-7 SMS", strings.Repeat("a", 160), SMSInfo{EncodingGSM7, 1, 160}},
		{"161 × a needs two parts", strings.Repeat("a", 161), SMSInfo{EncodingGSM7, 2, 161}},
		{"€ is an extension character and counts 2", "10€", SMSInfo{EncodingGSM7, 1, 4}},
		{"Turkish ş forces UCS-2", "Merhaba, kargonuz şubede", SMSInfo{EncodingUCS2, 1, 24}},
		{"70 × ş fits one UCS-2 SMS", strings.Repeat("ş", 70), SMSInfo{EncodingUCS2, 1, 70}},
		{"71 × ş needs two parts", strings.Repeat("ş", 71), SMSInfo{EncodingUCS2, 2, 71}},
		{"typographic apostrophe forces UCS-2", "It’s here", SMSInfo{EncodingUCS2, 1, 9}},
		{"öüÖÜÇ are in the GSM-7 basic set", "öüÖÜÇ", SMSInfo{EncodingGSM7, 1, 5}},
		{"1530 × a is exactly 10 parts", strings.Repeat("a", 1530), SMSInfo{EncodingGSM7, 10, 1530}},
		{"1531 × a needs 11 parts", strings.Repeat("a", 1531), SMSInfo{EncodingGSM7, 11, 1531}},

		// Edges of the tables and of UTF-16.
		{"lowercase ç is not in GSM-7", "ç", SMSInfo{EncodingUCS2, 1, 1}},
		{"80 extension characters = 160 septets", strings.Repeat("€", 80), SMSInfo{EncodingGSM7, 1, 160}},
		{"81 extension characters = 162 septets", strings.Repeat("{", 81), SMSInfo{EncodingGSM7, 2, 162}},
		{"emoji counts 2 UTF-16 units", "hi 👋", SMSInfo{EncodingUCS2, 1, 5}},
		{"35 emoji = 70 units", strings.Repeat("👋", 35), SMSInfo{EncodingUCS2, 1, 70}},
		{"36 emoji = 72 units", strings.Repeat("👋", 36), SMSInfo{EncodingUCS2, 2, 72}},
		{"newline and carriage return are GSM-7", "a\r\nb", SMSInfo{EncodingGSM7, 1, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, ComputeSMSInfo(tt.content)); diff != "" {
				t.Errorf("ComputeSMSInfo (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateAccepts(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		want Draft
	}{
		{
			name: "sms with default priority",
			req:  Request{Recipient: "+905551234567", Channel: "sms", Content: "Hello"},
			want: Draft{Channel: ChannelSMS, Recipient: "+905551234567", Content: "Hello", Priority: PriorityNormal},
		},
		{
			name: "email with priority and key",
			req:  Request{Recipient: "ann@example.com", Channel: "email", Content: "Hi", Priority: ptr("high"), IdempotencyKey: ptr("k-1")},
			want: Draft{Channel: ChannelEmail, Recipient: "ann@example.com", Content: "Hi", Priority: PriorityHigh, IdempotencyKey: ptr("k-1")},
		},
		{
			name: "push token with low priority",
			req:  Request{Recipient: "dGVzdC10b2tlbg:APA91b", Channel: "push", Content: "Ping", Priority: ptr("low")},
			want: Draft{Channel: ChannelPush, Recipient: "dGVzdC10b2tlbg:APA91b", Content: "Ping", Priority: PriorityLow},
		},
		{
			name: "content is stored untrimmed",
			req:  Request{Recipient: "+905551234567", Channel: "sms", Content: "  padded  "},
			want: Draft{Channel: ChannelSMS, Recipient: "+905551234567", Content: "  padded  ", Priority: PriorityNormal},
		},
		{
			name: "shortest E.164 (8 digits)",
			req:  Request{Recipient: "+12345678", Channel: "sms", Content: "x"},
			want: Draft{Channel: ChannelSMS, Recipient: "+12345678", Content: "x", Priority: PriorityNormal},
		},
		{
			name: "10 SMS segments is allowed",
			req:  Request{Recipient: "+905551234567", Channel: "sms", Content: strings.Repeat("a", 1530)},
			want: Draft{Channel: ChannelSMS, Recipient: "+905551234567", Content: strings.Repeat("a", 1530), Priority: PriorityNormal},
		},
		{
			name: "255-character idempotency key",
			req:  Request{Recipient: "+905551234567", Channel: "sms", Content: "x", IdempotencyKey: ptr(strings.Repeat("ş", 255))},
			want: Draft{Channel: ChannelSMS, Recipient: "+905551234567", Content: "x", Priority: PriorityNormal, IdempotencyKey: ptr(strings.Repeat("ş", 255))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, errs := tt.req.Validate()
			if errs != nil {
				t.Fatalf("unexpected errors: %v", errs)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("draft (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateRejects(t *testing.T) {
	valid := func(mod func(*Request)) Request {
		r := Request{Recipient: "+905551234567", Channel: "sms", Content: "Hello"}
		mod(&r)
		return r
	}
	email := func(recipient string) Request {
		return Request{Recipient: recipient, Channel: "email", Content: "Hi"}
	}
	push := func(recipient, content string) Request {
		return Request{Recipient: recipient, Channel: "push", Content: content}
	}

	tests := []struct {
		name string
		req  Request
		want FieldErrors
	}{
		{"everything missing", Request{}, FieldErrors{
			{"channel", "is required"},
			{"recipient", "is required"},
			{"content", "is required"},
		}},
		{"unknown channel skips recipient and content rules", Request{Recipient: "x", Channel: "fax", Content: "y"}, FieldErrors{
			{"channel", "must be one of sms, email, push"},
		}},
		{"channel is case-sensitive", valid(func(r *Request) { r.Channel = "SMS" }), FieldErrors{
			{"channel", "must be one of sms, email, push"},
		}},
		{"unknown priority", valid(func(r *Request) { r.Priority = ptr("urgent") }), FieldErrors{
			{"priority", "must be one of high, normal, low"},
		}},
		{"empty priority is not the default", valid(func(r *Request) { r.Priority = ptr("") }), FieldErrors{
			{"priority", "must be one of high, normal, low"},
		}},
		{"whitespace-only content", valid(func(r *Request) { r.Content = " \t\n " }), FieldErrors{
			{"content", "must not be only whitespace"},
		}},
		{"invalid UTF-8 content", valid(func(r *Request) { r.Content = "a\xffb" }), FieldErrors{
			{"content", "must be valid UTF-8"},
		}},
		{"empty idempotency key", valid(func(r *Request) { r.IdempotencyKey = ptr("") }), FieldErrors{
			{"idempotency_key", "must be 1 to 255 characters, got 0"},
		}},
		{"256-character idempotency key", valid(func(r *Request) { r.IdempotencyKey = ptr(strings.Repeat("k", 256)) }), FieldErrors{
			{"idempotency_key", "must be 1 to 255 characters, got 256"},
		}},

		// SMS recipients: strict E.164.
		{"sms without plus", valid(func(r *Request) { r.Recipient = "905551234567" }), FieldErrors{
			{"recipient", "must be an E.164 phone number, e.g. +905551234567"},
		}},
		{"sms with spaces", valid(func(r *Request) { r.Recipient = "+90 555 123 45 67" }), FieldErrors{
			{"recipient", "must be an E.164 phone number, e.g. +905551234567"},
		}},
		{"sms starting with zero", valid(func(r *Request) { r.Recipient = "+05551234567" }), FieldErrors{
			{"recipient", "must be an E.164 phone number, e.g. +905551234567"},
		}},
		{"sms too short", valid(func(r *Request) { r.Recipient = "+1234567" }), FieldErrors{
			{"recipient", "must be an E.164 phone number, e.g. +905551234567"},
		}},
		{"sms too long", valid(func(r *Request) { r.Recipient = "+1234567890123456" }), FieldErrors{
			{"recipient", "must be an E.164 phone number, e.g. +905551234567"},
		}},
		{"sms over 10 segments", valid(func(r *Request) { r.Content = strings.Repeat("a", 1531) }), FieldErrors{
			{"content", "content needs 11 SMS segments (GSM-7); the maximum is 10"},
		}},
		{"sms over 10 UCS-2 segments", valid(func(r *Request) { r.Content = strings.Repeat("ş", 750) }), FieldErrors{
			{"content", "content needs 12 SMS segments (UCS-2); the maximum is 10"},
		}},

		// Email recipients: one bare address.
		{"email with display name", email("Ann <ann@example.com>"), FieldErrors{
			{"recipient", "must be a single email address, e.g. ann@example.com"},
		}},
		{"email with surrounding space", email(" ann@example.com"), FieldErrors{
			{"recipient", "must be a single email address, e.g. ann@example.com"},
		}},
		{"email without at", email("ann.example.com"), FieldErrors{
			{"recipient", "must be a single email address, e.g. ann@example.com"},
		}},
		{"two email addresses", email("a@example.com, b@example.com"), FieldErrors{
			{"recipient", "must be a single email address, e.g. ann@example.com"},
		}},
		{"email over 254 characters", email(strings.Repeat("a", 64) + "@" + strings.Repeat("b", 186) + ".com"), FieldErrors{
			{"recipient", "must be at most 254 characters"},
		}},
		{"email content over 64 KiB", Request{Recipient: "ann@example.com", Channel: "email", Content: strings.Repeat("a", 64<<10+1)}, FieldErrors{
			{"content", "must be at most 65536 bytes (64 KiB), got 65537"},
		}},

		// Push recipients and content.
		{"push token with space", push("abc def", "x"), FieldErrors{
			{"recipient", "must not contain whitespace or control characters"},
		}},
		{"push token with control character", push("abc\x00def", "x"), FieldErrors{
			{"recipient", "must not contain whitespace or control characters"},
		}},
		{"push token over 512 characters", push(strings.Repeat("t", 513), "x"), FieldErrors{
			{"recipient", "must be at most 512 characters"},
		}},
		{"push content over 1024 characters", push("tok", strings.Repeat("ş", 1025)), FieldErrors{
			{"content", "must be at most 1024 characters, got 1025"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := tt.req.Validate()
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("errors (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValidateBatch(t *testing.T) {
	ok := func(key string) Request {
		r := Request{Recipient: "+905551234567", Channel: "sms", Content: "Hello"}
		if key != "" {
			r.IdempotencyKey = ptr(key)
		}
		return r
	}

	tests := []struct {
		name  string
		items []Request
		want  FieldErrors
	}{
		{"empty batch", nil, FieldErrors{{"items", "must contain 1 to 1000 items, got 0"}}},
		{"1001 items", make([]Request, 1001), FieldErrors{{"items", "must contain 1 to 1000 items, got 1001"}}},
		{"every item error is reported with its index", []Request{ok("a"), {Channel: "sms", Content: "x"}, ok(""), {Recipient: "+905551234567", Channel: "fax", Content: "x"}}, FieldErrors{
			{"items[1].recipient", "is required"},
			{"items[3].channel", "must be one of sms, email, push"},
		}},
		{"duplicate keys within the batch", []Request{ok("a"), ok("b"), ok("a"), ok(""), ok(""), ok("a")}, FieldErrors{
			{"items[2].idempotency_key", "is the same as items[0].idempotency_key; keys must be unique within a batch"},
			{"items[5].idempotency_key", "is the same as items[0].idempotency_key; keys must be unique within a batch"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drafts, got := ValidateBatch(tt.items)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("errors (-want +got):\n%s", diff)
			}
			if drafts != nil {
				t.Errorf("drafts returned with errors: %v", drafts)
			}
		})
	}

	t.Run("1000 valid items", func(t *testing.T) {
		items := make([]Request, 1000)
		for i := range items {
			items[i] = ok("")
		}
		drafts, errs := ValidateBatch(items)
		if errs != nil || len(drafts) != 1000 {
			t.Fatalf("got %d drafts, errors %v", len(drafts), errs)
		}
	})
}

func TestDraftHashUsesDefaults(t *testing.T) {
	// D-009: the default priority is applied before hashing, so omitting
	// priority and sending "normal" are the same request.
	a, _ := Request{Recipient: "+905551234567", Channel: "sms", Content: "x"}.Validate()
	b, _ := Request{Recipient: "+905551234567", Channel: "sms", Content: "x", Priority: ptr("normal")}.Validate()
	if !cmp.Equal(a.Hash(), b.Hash()) {
		t.Error("omitted priority and \"normal\" hash differently")
	}
	c, _ := Request{Recipient: "+905551234567", Channel: "sms", Content: "x", Priority: ptr("high")}.Validate()
	if cmp.Equal(a.Hash(), c.Hash()) {
		t.Error("different priorities hash the same")
	}
}

func TestNextAttemptAt(t *testing.T) {
	at := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	for _, s := range Statuses {
		n := Notification{Status: s, AvailableAt: at}
		got := n.NextAttemptAt()
		if s == StatusPending {
			if got == nil || !got.Equal(at) {
				t.Errorf("pending: NextAttemptAt = %v, want %v", got, at)
			}
		} else if got != nil {
			t.Errorf("%s: NextAttemptAt = %v, want nil", s, got)
		}
	}
}

func TestValidateScheduledAt(t *testing.T) {
	// +03:00 with nanoseconds: stored as UTC, truncated to microseconds.
	in := time.Date(2026, 11, 20, 12, 0, 0, 123456789, time.FixedZone("TRT", 3*3600))
	r := Request{Recipient: "+905551234567", Channel: "sms", Content: "hi", ScheduledAt: &in}
	d, errs := r.Validate()
	if errs != nil {
		t.Fatalf("errors: %v", errs)
	}
	want := time.Date(2026, 11, 20, 9, 0, 0, 123456000, time.UTC)
	if d.ScheduledAt == nil || !d.ScheduledAt.Equal(want) || d.ScheduledAt.Location() != time.UTC {
		t.Fatalf("ScheduledAt = %v, want %v", d.ScheduledAt, want)
	}
	// The same instant written differently hashes the same.
	other := want.Add(789 * time.Nanosecond).In(time.FixedZone("X", -5*3600))
	r.ScheduledAt = &other
	d2, _ := r.Validate()
	if string(d.Hash()) != string(d2.Hash()) {
		t.Error("the same instant in another zone and with extra nanoseconds hashes differently")
	}
	// A different time is a different request.
	later := in.Add(time.Second)
	r.ScheduledAt = &later
	d3, _ := r.Validate()
	if string(d.Hash()) == string(d3.Hash()) {
		t.Error("a different scheduled_at hashes the same")
	}
}
