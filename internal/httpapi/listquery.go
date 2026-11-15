package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/asrasari/notification-service/internal/notification"
	"github.com/asrasari/notification-service/internal/postgres"
)

// List limits (D-014).
const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// listParams are the only query parameters GET /notifications accepts.
var listParams = []string{"status", "channel", "created_from", "created_to", "batch_id", "limit", "cursor"}

// errBadCursor marks a cursor that can't be decoded: a 400, not a 422 (D-014).
var errBadCursor = errors.New("cursor is invalid; use the next_cursor value of a previous page")

// parseListQuery turns the query string into a filter. Rule violations are
// returned as field errors (422, D-016 §9); a bad cursor as errBadCursor.
func parseListQuery(q url.Values) (postgres.ListFilter, []notification.FieldError, error) {
	f := postgres.ListFilter{Limit: defaultListLimit}
	var errs []notification.FieldError
	add := func(field, msg string) {
		errs = append(errs, notification.FieldError{Field: field, Message: msg})
	}

	// Report unknown and repeated parameters in a stable order.
	names := make([]string, 0, len(q))
	for name := range q {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		switch {
		case !slices.Contains(listParams, name):
			add(name, "is not a known query parameter")
		case len(q[name]) > 1:
			add(name, "must be given at most once")
		}
	}
	if len(errs) > 0 {
		return f, errs, nil
	}

	if v, ok := single(q, "status"); ok {
		st, err := notification.ParseStatus(v)
		if err != nil {
			add("status", "must be one of pending, processing, sent, failed, cancelled")
		} else {
			f.Status = &st
		}
	}
	if v, ok := single(q, "channel"); ok {
		ch, err := notification.ParseChannel(v)
		if err != nil {
			add("channel", "must be one of sms, email, push")
		} else {
			f.Channel = &ch
		}
	}
	for _, p := range []struct {
		name string
		dst  **time.Time
	}{{"created_from", &f.CreatedFrom}, {"created_to", &f.CreatedTo}} {
		if v, ok := single(q, p.name); ok {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				add(p.name, "must be an RFC 3339 time, e.g. 2026-11-15T10:00:00Z")
			} else {
				t = t.UTC()
				*p.dst = &t
			}
		}
	}
	// The range is [created_from, created_to); an empty or reversed one is a
	// client mistake, not an empty page (R-M2).
	if f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo) {
		add("created_to", "must be after created_from")
	}
	if v, ok := single(q, "batch_id"); ok {
		id, err := parseUUID(v)
		if err != nil {
			add("batch_id", "must be a UUID")
		} else {
			f.BatchID = &id
		}
	}
	if v, ok := single(q, "limit"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			add("limit", fmt.Sprintf("must be an integer from 1 to %d", maxListLimit))
		} else {
			f.Limit = n
		}
	}
	if len(errs) > 0 {
		return f, errs, nil
	}

	if v, ok := single(q, "cursor"); ok {
		c, err := decodeCursor(v)
		if err != nil {
			return f, nil, errBadCursor
		}
		f.After = &c
	}
	return f, nil, nil
}

func single(q url.Values, name string) (string, bool) {
	vs, ok := q[name]
	if !ok {
		return "", false
	}
	return vs[0], true
}

// parseUUID accepts only the canonical 36-character form.
func parseUUID(s string) (uuid.UUID, error) {
	if len(s) != 36 {
		return uuid.UUID{}, errors.New("not a canonical UUID")
	}
	return uuid.Parse(s)
}

// cursorJSON is the versioned content of a cursor. The version lets the
// format change later while old cursors fail cleanly with a 400.
type cursorJSON struct {
	V         int       `json:"v"`
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

const cursorVersion = 1

// encodeCursor makes the opaque next_cursor string: base64url of a small
// versioned JSON (D-014).
func encodeCursor(c postgres.Cursor) string {
	b, _ := json.Marshal(cursorJSON{V: cursorVersion, CreatedAt: c.CreatedAt.UTC(), ID: c.ID}) // can't fail for these types
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (postgres.Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return postgres.Cursor{}, err
	}
	var c cursorJSON
	if derr := decodeStrict(b, &c, ""); derr != nil {
		return postgres.Cursor{}, derr
	}
	if c.V != cursorVersion || c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return postgres.Cursor{}, errors.New("unsupported cursor")
	}
	return postgres.Cursor{CreatedAt: c.CreatedAt, ID: c.ID}, nil
}
