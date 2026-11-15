package notification

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Validation limits (D-011). They are part of the API contract and are
// documented in api/openapi.yaml, so they are constants, not configuration.
const (
	MaxIdempotencyKeyLen = 255      // characters
	MaxEmailLen          = 254      // characters, per RFC 5321 path limits
	MaxEmailContentBytes = 64 << 10 // 64 KiB
	MaxPushRecipientLen  = 512      // characters
	MaxPushContentLen    = 1024     // characters
	MaxBatchItems        = 1000
)

// FieldError is one broken rule, with the JSON path of the field that broke
// it, e.g. "items[3].recipient".
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// FieldErrors is every broken rule of one request. Validation reports all of
// them at once (D-011).
type FieldErrors []FieldError

func (fe FieldErrors) Error() string {
	parts := make([]string, len(fe))
	for i, e := range fe {
		parts[i] = e.Field + ": " + e.Message
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Request is a create request as decoded from JSON, before any rule is
// checked. Optional fields are pointers so "absent" and "empty" differ.
type Request struct {
	Recipient      string
	Channel        string
	Content        string
	Priority       *string
	IdempotencyKey *string
	ScheduledAt    *time.Time
}

// Draft is a validated request, ready to be stored: channel and priority are
// parsed and the default priority is applied. Fields hold exactly what the
// client sent; nothing is trimmed or reformatted (D-011, Q-002).
type Draft struct {
	Channel        Channel
	Recipient      string
	Content        string
	Priority       Priority
	IdempotencyKey *string
	ScheduledAt    *time.Time
}

// Hash returns the request hash over exactly what will be stored (D-009).
func (d Draft) Hash() []byte {
	return RequestHash(HashInput{
		Recipient:   d.Recipient,
		Channel:     d.Channel,
		Content:     d.Content,
		Priority:    d.Priority,
		ScheduledAt: d.ScheduledAt,
	})
}

// Validate checks one request against D-011. Field paths have no prefix.
func (r Request) Validate() (Draft, FieldErrors) {
	return r.validate("")
}

// ValidateBatch checks a batch: 1–MaxBatchItems items, each item valid, and
// no idempotency key used twice within the batch (D-009). Field paths are
// "items" or "items[i].field".
func ValidateBatch(items []Request) ([]Draft, FieldErrors) {
	if len(items) == 0 || len(items) > MaxBatchItems {
		return nil, FieldErrors{{Field: "items", Message: fmt.Sprintf("must contain 1 to %d items, got %d", MaxBatchItems, len(items))}}
	}
	drafts := make([]Draft, len(items))
	var errs FieldErrors
	firstUse := make(map[string]int, len(items))
	for i, item := range items {
		prefix := fmt.Sprintf("items[%d].", i)
		d, itemErrs := item.validate(prefix)
		errs = append(errs, itemErrs...)
		drafts[i] = d

		if item.IdempotencyKey == nil {
			continue
		}
		if j, seen := firstUse[*item.IdempotencyKey]; seen {
			errs = append(errs, FieldError{
				Field:   prefix + "idempotency_key",
				Message: fmt.Sprintf("is the same as items[%d].idempotency_key; keys must be unique within a batch", j),
			})
		} else {
			firstUse[*item.IdempotencyKey] = i
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return drafts, nil
}

func (r Request) validate(prefix string) (Draft, FieldErrors) {
	var errs FieldErrors
	add := func(field, format string, args ...any) {
		errs = append(errs, FieldError{Field: prefix + field, Message: fmt.Sprintf(format, args...)})
	}

	d := Draft{
		Recipient:      r.Recipient,
		Content:        r.Content,
		Priority:       PriorityNormal,
		IdempotencyKey: r.IdempotencyKey,
		ScheduledAt:    r.ScheduledAt,
	}

	channelOK := false
	switch {
	case r.Channel == "":
		add("channel", "is required")
	default:
		c, err := ParseChannel(r.Channel)
		if err != nil {
			add("channel", "must be one of sms, email, push")
		} else {
			d.Channel, channelOK = c, true
		}
	}

	if r.Recipient == "" {
		add("recipient", "is required")
	} else if channelOK {
		if msg := checkRecipient(d.Channel, r.Recipient); msg != "" {
			add("recipient", "%s", msg)
		}
	}

	switch {
	case r.Content == "":
		add("content", "is required")
	case !utf8.ValidString(r.Content):
		add("content", "must be valid UTF-8")
	case strings.TrimSpace(r.Content) == "":
		add("content", "must not be only whitespace")
	case channelOK:
		if msg := checkContent(d.Channel, r.Content); msg != "" {
			add("content", "%s", msg)
		}
	}

	if r.Priority != nil {
		p, err := ParsePriority(*r.Priority)
		if err != nil {
			add("priority", "must be one of high, normal, low")
		} else {
			d.Priority = p
		}
	}

	if r.IdempotencyKey != nil {
		if n := utf8.RuneCountInString(*r.IdempotencyKey); n < 1 || n > MaxIdempotencyKeyLen {
			add("idempotency_key", "must be 1 to %d characters, got %d", MaxIdempotencyKeyLen, n)
		}
	}

	// scheduled_at is stored in UTC at the database's microsecond precision,
	// so a replay hashes exactly like the stored row (D-021). Whether the time
	// is in the allowed window is decided at insert time on the database clock,
	// and only for rows that would be newly created.
	if r.ScheduledAt != nil {
		t := r.ScheduledAt.UTC().Truncate(time.Microsecond)
		d.ScheduledAt = &t
	}

	if len(errs) > 0 {
		return Draft{}, errs
	}
	return d, nil
}

// e164 is a strict E.164 number: "+", a non-zero digit, 8–15 digits in total.
var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

// checkRecipient returns a message when recipient is not valid for channel.
func checkRecipient(c Channel, recipient string) string {
	switch c {
	case ChannelSMS:
		if !e164.MatchString(recipient) {
			return "must be an E.164 phone number, e.g. +905551234567"
		}
	case ChannelEmail:
		if utf8.RuneCountInString(recipient) > MaxEmailLen {
			return fmt.Sprintf("must be at most %d characters", MaxEmailLen)
		}
		// The parsed address must equal the input, which rules out the
		// display-name form ("Ann <ann@example.com>") and surrounding spaces.
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Address != recipient {
			return "must be a single email address, e.g. ann@example.com"
		}
	case ChannelPush:
		if utf8.RuneCountInString(recipient) > MaxPushRecipientLen {
			return fmt.Sprintf("must be at most %d characters", MaxPushRecipientLen)
		}
		for _, r := range recipient {
			if unicode.IsSpace(r) || unicode.IsControl(r) {
				return "must not contain whitespace or control characters"
			}
		}
	}
	return ""
}

// checkContent returns a message when content is too large for channel.
func checkContent(c Channel, content string) string {
	switch c {
	case ChannelSMS:
		info := ComputeSMSInfo(content)
		if info.Segments > MaxSMSSegments {
			name := "GSM-7"
			if info.Encoding == EncodingUCS2 {
				name = "UCS-2"
			}
			return fmt.Sprintf("content needs %d SMS segments (%s); the maximum is %d", info.Segments, name, MaxSMSSegments)
		}
	case ChannelEmail:
		if len(content) > MaxEmailContentBytes {
			return fmt.Sprintf("must be at most %d bytes (64 KiB), got %d", MaxEmailContentBytes, len(content))
		}
	case ChannelPush:
		if n := utf8.RuneCountInString(content); n > MaxPushContentLen {
			return fmt.Sprintf("must be at most %d characters, got %d", MaxPushContentLen, n)
		}
	}
	return ""
}
