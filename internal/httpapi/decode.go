package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/asrasari/notification-service/internal/notification"
)

// MaxBodyBytes caps every request body (D-011). A full batch of 1000 maximal
// emails would not fit, but a realistic one does with a wide margin.
const MaxBodyBytes = 10 << 20 // 10 MiB

// decodeError is a failed request body, already mapped to its HTTP answer
// (D-016 §3).
type decodeError struct {
	status int
	detail string
	fields []notification.FieldError // only for 422
}

func (e *decodeError) Error() string { return e.detail }

// writeDecodeError answers with the problem for err.
func writeDecodeError(w http.ResponseWriter, r *http.Request, err *decodeError) {
	switch err.status {
	case http.StatusUnprocessableEntity:
		validationFailed(w, r, err.fields)
	case http.StatusBadRequest:
		badRequest(w, r, err.detail)
	default:
		writeProblem(w, r, problem{Status: err.status, Detail: err.detail})
	}
}

// readJSONBody checks the content type, then reads the body up to
// MaxBodyBytes. 415 without application/json, 413 when it is too large.
func readJSONBody(w http.ResponseWriter, r *http.Request) ([]byte, *decodeError) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return nil, &decodeError{status: http.StatusUnsupportedMediaType, detail: "Content-Type must be application/json."}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &decodeError{status: http.StatusRequestEntityTooLarge, detail: fmt.Sprintf("The request body is larger than %d bytes (10 MiB).", MaxBodyBytes)}
		}
		return nil, &decodeError{status: http.StatusBadRequest, detail: "The request body could not be read."}
	}
	return body, nil
}

// decodeStrict decodes exactly one JSON value into v. Unknown fields are a
// 422 naming the field (prefixed with path); bad syntax, wrong types,
// trailing data and an empty body are a 400 (D-011, D-016 §3).
func decodeStrict(data []byte, v any, path string) *decodeError {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return mapJSONError(err, path)
	}
	// Anything but EOF after the value is trailing data.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "unexpected data after the JSON value")}
	}
	return nil
}

func mapJSONError(err error, path string) *decodeError {
	var (
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		timeErr   *time.ParseError
	)
	switch {
	case errors.Is(err, io.EOF):
		return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "the body is empty; expected a JSON object")}
	case errors.Is(err, io.ErrUnexpectedEOF):
		return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "the JSON is incomplete")}
	case errors.As(err, &syntaxErr):
		return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, fmt.Sprintf("invalid JSON at byte %d: %v", syntaxErr.Offset, syntaxErr))}
	case errors.As(err, &typeErr):
		if typeErr.Field == "" {
			return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "expected a JSON object, got "+typeErr.Value)}
		}
		return &decodeError{status: http.StatusBadRequest, detail: fmt.Sprintf("%s: expected %s, got %s", joinPath(path, typeErr.Field), jsonTypeName(typeErr.Type.String()), typeErr.Value)}
	case errors.As(err, &timeErr):
		return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "a time is not in RFC 3339 format")}
	}
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		field := joinPath(path, strings.Trim(name, `"`))
		return &decodeError{status: http.StatusUnprocessableEntity, detail: "unknown field " + field, fields: []notification.FieldError{{Field: field, Message: "is not a known field"}}}
	}
	return &decodeError{status: http.StatusBadRequest, detail: prefixed(path, "invalid JSON")}
}

// jsonTypeName turns a Go type from a decode error into the JSON word.
func jsonTypeName(goType string) string {
	switch {
	case strings.Contains(goType, "string"), strings.Contains(goType, "Time"):
		return "a string"
	case strings.Contains(goType, "int"), strings.Contains(goType, "float"):
		return "a number"
	case strings.Contains(goType, "bool"):
		return "a boolean"
	case strings.HasPrefix(goType, "[]"):
		return "an array"
	}
	return "an object"
}

func joinPath(prefix, field string) string {
	if prefix == "" {
		return field
	}
	return prefix + "." + field
}

func prefixed(path, msg string) string {
	if path == "" {
		return msg
	}
	return path + ": " + msg
}

// createRequestJSON is the body of POST /notifications and each batch item.
type createRequestJSON struct {
	Recipient      string     `json:"recipient"`
	Channel        string     `json:"channel"`
	Content        string     `json:"content"`
	Priority       *string    `json:"priority"`
	IdempotencyKey *string    `json:"idempotency_key"`
	ScheduledAt    *time.Time `json:"scheduled_at"` // refused by validation until M6
}

func (c createRequestJSON) request() notification.Request {
	return notification.Request{
		Recipient:      c.Recipient,
		Channel:        c.Channel,
		Content:        c.Content,
		Priority:       c.Priority,
		IdempotencyKey: c.IdempotencyKey,
		ScheduledAt:    c.ScheduledAt,
	}
}

// batchRequestJSON is the body of POST /notifications/batch. Items stay raw
// so each one is decoded on its own and errors carry its index.
type batchRequestJSON struct {
	Items []json.RawMessage `json:"items"`
}

// decodeBatch decodes the batch envelope and every item. Unknown fields are
// collected across all items into one 422.
func decodeBatch(data []byte) ([]notification.Request, *decodeError) {
	var env batchRequestJSON
	if err := decodeStrict(data, &env, ""); err != nil {
		return nil, err
	}
	reqs := make([]notification.Request, len(env.Items))
	var unknown []notification.FieldError
	for i, raw := range env.Items {
		var item createRequestJSON
		if err := decodeStrict(raw, &item, fmt.Sprintf("items[%d]", i)); err != nil {
			if err.status != http.StatusUnprocessableEntity {
				return nil, err
			}
			unknown = append(unknown, err.fields...)
			continue
		}
		reqs[i] = item.request()
	}
	if len(unknown) > 0 {
		return nil, &decodeError{status: http.StatusUnprocessableEntity, fields: unknown}
	}
	return reqs, nil
}
