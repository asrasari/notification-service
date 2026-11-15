package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/metrics"
	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

// Store is the storage the API needs; *postgres.Store implements it.
type Store interface {
	Create(ctx context.Context, d notification.Draft) (notification.Notification, bool, error)
	CreateBatch(ctx context.Context, drafts []notification.Draft) (postgres.BatchResult, error)
	Get(ctx context.Context, id uuid.UUID) (notification.Notification, []notification.Attempt, error)
	List(ctx context.Context, f postgres.ListFilter) ([]notification.Notification, *postgres.Cursor, error)
	BatchSummary(ctx context.Context, batchID uuid.UUID) (notification.BatchSummary, error)
	Cancel(ctx context.Context, id uuid.UUID) (notification.Notification, error)
	CancelBatch(ctx context.Context, batchID uuid.UUID) (int, error)
}

type handlers struct {
	logger  *slog.Logger
	store   Store
	metrics *metrics.API
}

// createNotification handles POST /notifications: 201 with Location for a
// new notification, 200 with the stored one for an idempotent repeat, 409
// when the key was used with other content (D-009, D-016 §4).
func (a *handlers) createNotification(w http.ResponseWriter, r *http.Request) {
	body, derr := readJSONBody(w, r)
	if derr != nil {
		writeDecodeError(w, r, derr)
		return
	}
	var in createRequestJSON
	if derr := decodeStrict(body, &in, ""); derr != nil {
		writeDecodeError(w, r, derr)
		return
	}
	draft, errs := in.request().Validate()
	if errs != nil {
		validationFailed(w, r, errs)
		return
	}

	n, created, err := a.store.Create(r.Context(), draft)
	var sched *postgres.ScheduleError
	switch {
	case errors.As(err, &sched):
		validationFailed(w, r, notification.FieldErrors{{Field: "scheduled_at", Message: scheduleMessage(sched.TooFar)}})
		return
	case errors.Is(err, postgres.ErrIdempotencyConflict):
		writeProblem(w, r, problem{
			Type:   typeIdempotencyConflict,
			Title:  "Idempotency key reused",
			Status: http.StatusConflict,
			Detail: "This idempotency_key was already used for a different notification.",
			Errors: []notification.FieldError{{Field: "idempotency_key", Message: "was already used with different content"}},
		})
		return
	case err != nil:
		internalError(a.logger, w, r, err)
		return
	}
	if !created {
		writeJSON(w, http.StatusOK, toJSON(n))
		return
	}
	a.metrics.NotificationCreated(string(n.Channel), n.Priority.String())
	w.Header().Set("Location", "/notifications/"+n.ID.String())
	writeJSON(w, http.StatusCreated, toJSON(n))
}

// createBatch handles POST /notifications/batch (D-016 §6): all-or-nothing;
// 201 when at least one item is new, 200 for a full replay.
func (a *handlers) createBatch(w http.ResponseWriter, r *http.Request) {
	body, derr := readJSONBody(w, r)
	if derr != nil {
		writeDecodeError(w, r, derr)
		return
	}
	reqs, derr := decodeBatch(body)
	if derr != nil {
		writeDecodeError(w, r, derr)
		return
	}
	drafts, errs := notification.ValidateBatch(reqs)
	if errs != nil {
		validationFailed(w, r, errs)
		return
	}

	res, err := a.store.CreateBatch(r.Context(), drafts)
	var (
		conflict *postgres.BatchConflictError
		sched    *postgres.BatchScheduleError
	)
	switch {
	case errors.As(err, &sched):
		fields := make(notification.FieldErrors, len(sched.Items))
		for i, it := range sched.Items {
			fields[i] = notification.FieldError{Field: fmt.Sprintf("items[%d].scheduled_at", it.Index), Message: scheduleMessage(it.TooFar)}
		}
		validationFailed(w, r, fields)
		return
	case errors.As(err, &conflict):
		fields := make([]notification.FieldError, len(conflict.Indexes))
		for i, idx := range conflict.Indexes {
			fields[i] = notification.FieldError{Field: fmt.Sprintf("items[%d].idempotency_key", idx), Message: "was already used with different content"}
		}
		writeProblem(w, r, problem{
			Type:   typeIdempotencyConflict,
			Title:  "Idempotency key reused",
			Status: http.StatusConflict,
			Detail: "Some idempotency keys were already used for different notifications; nothing was stored.",
			Errors: fields,
		})
		return
	case err != nil:
		internalError(a.logger, w, r, err)
		return
	}
	// Count new rows only: duplicates are idempotent replays (D-015).
	for i, it := range res.Items {
		if !it.Duplicate {
			a.metrics.NotificationCreated(string(drafts[i].Channel), drafts[i].Priority.String())
		}
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toBatchCreateJSON(res))
}

// getNotification handles GET /notifications/{id}, with attempt history.
func (a *handlers) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, attempts, err := a.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		notFound(w, r, "No notification has this id.")
	case err != nil:
		internalError(a.logger, w, r, err)
	default:
		writeJSON(w, http.StatusOK, toJSONWithAttempts(n, attempts))
	}
}

// listNotifications handles GET /notifications (D-014).
func (a *handlers) listNotifications(w http.ResponseWriter, r *http.Request) {
	// r.URL.Query() silently drops pairs it can't decode; a broken query
	// string is unreadable input, so it is a 400.
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		badRequest(w, r, "The query string is not valid URL encoding.")
		return
	}
	filter, errs, err := parseListQuery(q)
	switch {
	case errors.Is(err, errBadCursor):
		badRequest(w, r, err.Error())
		return
	case errs != nil:
		validationFailed(w, r, errs)
		return
	}
	list, next, err := a.store.List(r.Context(), filter)
	if err != nil {
		internalError(a.logger, w, r, err)
		return
	}
	out := listJSON{Data: make([]notificationJSON, len(list))}
	for i, n := range list {
		out.Data[i] = toJSON(n)
	}
	if next != nil {
		c := encodeCursor(*next)
		out.NextCursor = &c
	}
	writeJSON(w, http.StatusOK, out)
}

// cancelNotification handles POST /notifications/{id}/cancel (D-016 §5).
func (a *handlers) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := a.store.Cancel(r.Context(), id)
	var invalid *postgres.InvalidStateError
	switch {
	case errors.As(err, &invalid):
		writeProblem(w, r, problem{
			Type:   typeInvalidState,
			Title:  "Notification can't be cancelled",
			Status: http.StatusConflict,
			Detail: fmt.Sprintf("The notification is %s; only pending notifications can be cancelled.", invalid.Status),
		})
	case errors.Is(err, postgres.ErrNotFound):
		notFound(w, r, "No notification has this id.")
	case err != nil:
		internalError(a.logger, w, r, err)
	default:
		writeJSON(w, http.StatusOK, toJSON(n))
	}
}

// getBatch handles GET /batches/{id}: counts for all five statuses.
func (a *handlers) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	sum, err := a.store.BatchSummary(r.Context(), id)
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		notFound(w, r, "No batch has this id.")
	case err != nil:
		internalError(a.logger, w, r, err)
	default:
		writeJSON(w, http.StatusOK, batchSummaryJSON{BatchID: sum.BatchID, Total: sum.Total, Counts: sum.Counts})
	}
}

// cancelBatch handles POST /batches/{id}/cancel: cancels every pending row.
func (a *handlers) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := a.store.CancelBatch(r.Context(), id)
	switch {
	case errors.Is(err, postgres.ErrNotFound):
		notFound(w, r, "No batch has this id.")
	case err != nil:
		internalError(a.logger, w, r, err)
	default:
		writeJSON(w, http.StatusOK, batchCancelJSON{BatchID: id, Cancelled: n})
	}
}

// scheduleMessage explains a scheduled_at outside the window (D-021).
func scheduleMessage(tooFar bool) string {
	if tooFar {
		return "must be at most 30 days ahead"
	}
	return "must be in the future"
}

// pathUUID reads the {id} path value; a non-UUID is a 400 (D-016 §3).
func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := parseUUID(r.PathValue("id"))
	if err != nil {
		badRequest(w, r, "The id in the path must be a UUID.")
		return uuid.UUID{}, false
	}
	return id, true
}
