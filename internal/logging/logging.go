// Package logging builds the service's slog logger and carries the
// correlation ID through context.Context (D-015).
//
// Code logs with logger.InfoContext(ctx, …); the handler returned by New adds
// correlation_id from ctx to every record, so nobody passes the ID by hand.
//
// PII rule: never log recipient or content.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

// CorrelationIDKey is the attribute name used in every log record.
const CorrelationIDKey = "correlation_id"

type ctxKey struct{}

// WithCorrelationID returns a copy of ctx that carries id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the ID stored in ctx, if any.
func CorrelationID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKey{}).(string)
	return id, ok && id != ""
}

// New returns a logger writing to w. format is "json" or "text"; level is
// "debug", "info", "warn" or "error".
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("logging: invalid level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("logging: invalid format %q (want json or text)", format)
	}
	return slog.New(NewContextHandler(h)), nil
}

// ContextHandler wraps another slog.Handler and adds correlation_id from the
// record's context.
type ContextHandler struct {
	inner slog.Handler
}

// NewContextHandler wraps inner.
func NewContextHandler(inner slog.Handler) *ContextHandler {
	return &ContextHandler{inner: inner}
}

// Enabled reports whether the wrapped handler handles level.
func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle adds correlation_id (when ctx has one) and passes the record on.
func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id, ok := CorrelationID(ctx); ok {
		r = r.Clone()
		r.AddAttrs(slog.String(CorrelationIDKey, id))
	}
	return h.inner.Handle(ctx, r)
}

// WithAttrs keeps the wrapper in place for derived loggers.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup keeps the wrapper in place for derived loggers. Note that, as with
// any attribute added in Handle, correlation_id then appears inside the group.
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{inner: h.inner.WithGroup(name)}
}
