-- +goose Up

-- One row per running worker process (D-015 continued). The worker upserts
-- its row every HEARTBEAT_INTERVAL with the DB clock; a row whose expires_at
-- has passed belongs to a worker that stopped beating. The API's /health
-- reads it from M4 on.
CREATE TABLE worker_heartbeats (
    worker_id    text        PRIMARY KEY,   -- hostname + short random suffix
    started_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,      -- last_seen_at + 3 × HEARTBEAT_INTERVAL
    state        jsonb       NOT NULL       -- per channel: {"breaker": "...", "paused": bool}
);

-- +goose Down
DROP TABLE worker_heartbeats;
