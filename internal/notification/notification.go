// Package notification holds the core types and rules: statuses and their
// allowed transitions (D-008), channels, priorities and the request hash used
// for idempotency (D-009). It imports none of the service's other packages.
package notification

import "fmt"

// Status is one of the five stored statuses (D-008). "Scheduled" and
// "retrying" are not statuses: they are pending rows told apart by data.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent" // the provider accepted the message (202); not "delivered"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Statuses lists every stored status, in lifecycle order.
var Statuses = []Status{StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled}

// ParseStatus accepts exactly the five stored values.
func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return st, nil
	}
	return "", fmt.Errorf("unknown status %q", s)
}

// Terminal reports whether nothing may change the status any more.
func (s Status) Terminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// transitions is the complete list of allowed moves (D-008). Anything not
// listed is forbidden.
var transitions = map[Status][]Status{
	// A worker claims the row and takes a lease; or the API cancels it.
	StatusPending: {StatusProcessing, StatusCancelled},
	// 202 → sent; retryable with attempts left → pending; permanent or out of
	// attempts → failed; an expired lease is reclaimed directly with a new
	// lease token → processing.
	StatusProcessing: {StatusSent, StatusPending, StatusFailed, StatusProcessing},
}

// CanTransition reports whether a row may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// Channel is a delivery channel.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every channel. Each one gets its own claim loop and rate limit.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel accepts exactly "sms", "email" or "push".
func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	}
	return "", fmt.Errorf("unknown channel %q", s)
}

// Priority is stored as a smallint so that ORDER BY priority puts high first
// (D-013). The API uses the text form.
type Priority int16

const (
	PriorityHigh   Priority = 1
	PriorityNormal Priority = 2
	PriorityLow    Priority = 3
)

// ParsePriority maps the API text form to a Priority.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	}
	return 0, fmt.Errorf("unknown priority %q", s)
}

// PriorityFromDB maps the stored smallint back to a Priority.
func PriorityFromDB(v int16) (Priority, error) {
	p := Priority(v)
	if !p.Valid() {
		return 0, fmt.Errorf("invalid stored priority %d", v)
	}
	return p, nil
}

// Valid reports whether p is one of the three priorities.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityNormal || p == PriorityLow
}

// String returns the API text form: "high", "normal" or "low".
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("Priority(%d)", int16(p))
}

// MarshalText makes Priority encode as its text form in JSON.
func (p Priority) MarshalText() ([]byte, error) {
	if !p.Valid() {
		return nil, fmt.Errorf("invalid priority %d", int16(p))
	}
	return []byte(p.String()), nil
}

// UnmarshalText parses the text form.
func (p *Priority) UnmarshalText(b []byte) error {
	v, err := ParsePriority(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}
