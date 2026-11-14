package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
	}
	return m
}

func TestCorrelationIDRoundTrip(t *testing.T) {
	ctx := context.Background()
	if _, ok := CorrelationID(ctx); ok {
		t.Fatal("empty context should have no correlation ID")
	}
	ctx = WithCorrelationID(ctx, "abc-123")
	id, ok := CorrelationID(ctx)
	if !ok || id != "abc-123" {
		t.Fatalf("got %q, %v", id, ok)
	}
	if _, ok := CorrelationID(WithCorrelationID(context.Background(), "")); ok {
		t.Fatal("an empty ID should count as absent")
	}
}

func TestHandlerAddsCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(&buf, "info", "json")
	if err != nil {
		t.Fatal(err)
	}

	ctx := WithCorrelationID(context.Background(), "req-1")
	logger.With("component", "test").InfoContext(ctx, "hello", "notification_id", "n-1")

	m := decode(t, &buf)
	if m[CorrelationIDKey] != "req-1" {
		t.Errorf("correlation_id = %v, want req-1", m[CorrelationIDKey])
	}
	if m["component"] != "test" || m["notification_id"] != "n-1" {
		t.Errorf("other attributes lost: %v", m)
	}
}

func TestHandlerWithoutCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(&buf, "info", "json")
	if err != nil {
		t.Fatal(err)
	}
	logger.InfoContext(context.Background(), "hello")
	if _, ok := decode(t, &buf)[CorrelationIDKey]; ok {
		t.Error("correlation_id should be absent when the context has none")
	}
}

func TestNewFormatsAndLevels(t *testing.T) {
	tests := []struct {
		name          string
		level, format string
		wantErr       bool
	}{
		{"json info", "info", "json", false},
		{"text debug", "debug", "text", false},
		{"warn", "warn", "json", false},
		{"error", "error", "json", false},
		{"bad level", "verbose", "json", true},
		{"bad format", "info", "xml", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(&bytes.Buffer{}, tt.level, tt.format)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTextFormatAndLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(&buf, "warn", "text")
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithCorrelationID(context.Background(), "req-2")
	logger.InfoContext(ctx, "dropped")
	logger.WarnContext(ctx, "kept")

	out := buf.String()
	if strings.Contains(out, "dropped") {
		t.Error("info record should be filtered at warn level")
	}
	if !strings.Contains(out, "msg=kept") || !strings.Contains(out, "correlation_id=req-2") {
		t.Errorf("unexpected text output: %q", out)
	}
}
