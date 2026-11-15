package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// HeartbeatStore records the worker's heartbeat: postgres.Heartbeats
// implements it.
type HeartbeatStore interface {
	Beat(ctx context.Context, workerID string, interval time.Duration, state []byte) error
	Remove(ctx context.Context, workerID string) error
}

// heartbeatWriteTimeout bounds one heartbeat statement.
const heartbeatWriteTimeout = 2 * time.Second

// RunHeartbeat writes the worker's heartbeat row now and every interval
// until ctx is cancelled, then deletes the row (D-015 continued). state is
// read on every beat. A failed write is logged and never stops the worker.
func RunHeartbeat(ctx context.Context, store HeartbeatStore, workerID string, interval time.Duration,
	state func() map[string]ChannelStatus, logger *slog.Logger) {
	beat := func() {
		data, err := json.Marshal(state())
		if err != nil { // a map of strings and bools can't fail
			logger.ErrorContext(ctx, "heartbeat state encoding failed", "error", err)
			return
		}
		bctx, cancel := context.WithTimeout(ctx, heartbeatWriteTimeout)
		defer cancel()
		if err := store.Beat(bctx, workerID, interval, data); err != nil && ctx.Err() == nil {
			logger.WarnContext(ctx, "heartbeat write failed", "worker_id", workerID, "error", err)
		}
	}

	beat()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			beat()
		case <-ctx.Done():
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), heartbeatWriteTimeout)
			defer cancel()
			if err := store.Remove(rctx, workerID); err != nil {
				logger.Warn("heartbeat row could not be removed; it expires on its own", "worker_id", workerID, "error", err)
			}
			return
		}
	}
}
