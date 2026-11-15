package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

// notificationJSON is the Notification schema (D-016 §8). Nullable fields are
// always present (as null); sms appears only for the sms channel and
// delivery_attempts only on GET /notifications/{id}.
type notificationJSON struct {
	ID                uuid.UUID             `json:"id"`
	BatchID           *uuid.UUID            `json:"batch_id"`
	Channel           notification.Channel  `json:"channel"`
	Recipient         string                `json:"recipient"`
	Content           string                `json:"content"`
	Priority          notification.Priority `json:"priority"`
	IdempotencyKey    *string               `json:"idempotency_key"`
	CorrelationID     *string               `json:"correlation_id"`
	Status            notification.Status   `json:"status"`
	ScheduledAt       *time.Time            `json:"scheduled_at"`
	Attempts          int                   `json:"attempts"`
	MaxAttempts       int                   `json:"max_attempts"`
	NextAttemptAt     *time.Time            `json:"next_attempt_at"`
	LastError         *string               `json:"last_error"`
	ProviderMessageID *string               `json:"provider_message_id"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
	SMS               *notification.SMSInfo `json:"sms,omitempty"`
	DeliveryAttempts  *[]attemptJSON        `json:"delivery_attempts,omitempty"`
}

type attemptJSON struct {
	AttemptNo  int                  `json:"attempt_no"`
	StartedAt  time.Time            `json:"started_at"`
	DurationMS *int                 `json:"duration_ms"`
	HTTPStatus *int                 `json:"http_status"`
	Outcome    notification.Outcome `json:"outcome"`
	Error      *string              `json:"error"`
}

func toJSON(n notification.Notification) notificationJSON {
	out := notificationJSON{
		ID:                n.ID,
		BatchID:           n.BatchID,
		Channel:           n.Channel,
		Recipient:         n.Recipient,
		Content:           n.Content,
		Priority:          n.Priority,
		IdempotencyKey:    n.IdempotencyKey,
		CorrelationID:     n.CorrelationID,
		Status:            n.Status,
		ScheduledAt:       n.ScheduledAt,
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		NextAttemptAt:     n.NextAttemptAt(),
		LastError:         n.LastError,
		ProviderMessageID: n.ProviderMessageID,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
	}
	// Computed when the response is built; not stored (D-011).
	if n.Channel == notification.ChannelSMS {
		info := notification.ComputeSMSInfo(n.Content)
		out.SMS = &info
	}
	return out
}

func toJSONWithAttempts(n notification.Notification, attempts []notification.Attempt) notificationJSON {
	out := toJSON(n)
	list := make([]attemptJSON, len(attempts))
	for i, a := range attempts {
		list[i] = attemptJSON{
			AttemptNo:  a.AttemptNo,
			StartedAt:  a.StartedAt,
			DurationMS: a.DurationMS,
			HTTPStatus: a.HTTPStatus,
			Outcome:    a.Outcome,
			Error:      a.Error,
		}
	}
	out.DeliveryAttempts = &list
	return out
}

type listJSON struct {
	Data       []notificationJSON `json:"data"`
	NextCursor *string            `json:"next_cursor"`
}

type batchItemJSON struct {
	Index     int                 `json:"index"`
	ID        uuid.UUID           `json:"id"`
	Status    notification.Status `json:"status"`
	Duplicate bool                `json:"duplicate"`
	BatchID   *uuid.UUID          `json:"batch_id"`
}

type batchCreateJSON struct {
	BatchID *uuid.UUID      `json:"batch_id"`
	Items   []batchItemJSON `json:"items"`
}

func toBatchCreateJSON(res postgres.BatchResult) batchCreateJSON {
	out := batchCreateJSON{BatchID: res.BatchID, Items: make([]batchItemJSON, len(res.Items))}
	for i, it := range res.Items {
		out.Items[i] = batchItemJSON{Index: i, ID: it.ID, Status: it.Status, Duplicate: it.Duplicate, BatchID: it.BatchID}
	}
	return out
}

type batchSummaryJSON struct {
	BatchID uuid.UUID                   `json:"batch_id"`
	Total   int                         `json:"total"`
	Counts  map[notification.Status]int `json:"counts"`
}

type batchCancelJSON struct {
	BatchID   uuid.UUID `json:"batch_id"`
	Cancelled int       `json:"cancelled"`
}
