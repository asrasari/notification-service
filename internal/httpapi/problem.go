package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/asrasari/notification-service/internal/logging"
	"github.com/asrasari/notification-service/internal/notification"
)

// Problem types (D-016 §2). They are stable identifiers listed in
// api/openapi.yaml; plain HTTP errors use about:blank.
const (
	typeBlank               = "about:blank"
	typeMalformedRequest    = "/problems/malformed-request"
	typeValidationFailed    = "/problems/validation-failed"
	typeIdempotencyConflict = "/problems/idempotency-conflict"
	typeInvalidState        = "/problems/invalid-state"
)

const problemContentType = "application/problem+json"

// problem is an RFC 9457 Problem Details body.
type problem struct {
	Type          string                    `json:"type"`
	Title         string                    `json:"title"`
	Status        int                       `json:"status"`
	Detail        string                    `json:"detail,omitempty"`
	CorrelationID string                    `json:"correlation_id,omitempty"`
	Errors        []notification.FieldError `json:"errors,omitempty"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, p problem) {
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	if p.Type == "" {
		p.Type = typeBlank
	}
	p.CorrelationID, _ = logging.CorrelationID(r.Context())
	w.Header().Set("Content-Type", problemContentType)
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func badRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, r, problem{Type: typeMalformedRequest, Title: "Malformed request", Status: http.StatusBadRequest, Detail: detail})
}

func validationFailed(w http.ResponseWriter, r *http.Request, errs []notification.FieldError) {
	writeProblem(w, r, problem{
		Type:   typeValidationFailed,
		Title:  "Validation failed",
		Status: http.StatusUnprocessableEntity,
		Detail: "The request breaks one or more rules; see errors.",
		Errors: errs,
	})
}

func notFound(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, r, problem{Status: http.StatusNotFound, Detail: detail})
}

// internalError logs err and answers 500 without exposing it (D-016 §2).
func internalError(logger *slog.Logger, w http.ResponseWriter, r *http.Request, err error) {
	logger.ErrorContext(r.Context(), "request failed", "route", r.Pattern, "error", err)
	writeProblem(w, r, problem{Status: http.StatusInternalServerError, Detail: "An internal error occurred. Quote the correlation_id when reporting it."})
}
