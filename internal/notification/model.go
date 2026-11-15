package notification

import (
	"time"

	"github.com/google/uuid"
)

// Notification is a stored notification. It is also the queue entry (D-005).
// RequestHash and LeaseToken are internal and never leave the service
// (D-016 §8).
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	Channel           Channel
	Recipient         string
	Content           string
	Priority          Priority
	Status            Status
	IdempotencyKey    *string
	RequestHash       []byte
	CorrelationID     *string
	ScheduledAt       *time.Time
	AvailableAt       time.Time
	Attempts          int
	MaxAttempts       int
	LeaseToken        *uuid.UUID
	LastError         *string
	ProviderMessageID *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// NextAttemptAt is when a pending row becomes claimable: now for a new row,
// later for a scheduled or retrying one. It is nil in every other status,
// because available_at then means a lease expiry or nothing (D-008).
func (n Notification) NextAttemptAt() *time.Time {
	if n.Status != StatusPending {
		return nil
	}
	t := n.AvailableAt
	return &t
}

// Outcome classifies one delivery attempt (D-010 §5).
type Outcome string

const (
	OutcomeSuccess     Outcome = "success"
	OutcomeTransient   Outcome = "transient"
	OutcomePermanent   Outcome = "permanent"
	OutcomeUnknown     Outcome = "unknown"
	OutcomeChannelWide Outcome = "channel_wide"
)

// Attempt is one row of a notification's delivery history.
type Attempt struct {
	AttemptNo  int
	StartedAt  time.Time
	DurationMS *int // nil when unknown, e.g. for a reclaimed lease
	HTTPStatus *int // nil when no response arrived or the code was outside 100–599
	Outcome    Outcome
	Error      *string
}

// BatchSummary is a batch's status: counts of its notifications by status
// (D-008 rule 5). Counts holds every status, zeros included.
type BatchSummary struct {
	BatchID uuid.UUID
	Total   int
	Counts  map[Status]int
}
