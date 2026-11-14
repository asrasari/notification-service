-- +goose Up

-- notifications is both the record of every message and the work queue
-- (D-005). A row is claimable when status is pending or processing and
-- available_at <= now(): available_at holds the creation time, the scheduled
-- time, the retry time or the lease expiry, depending on the state (D-013).
CREATE TABLE notifications (
    id                  uuid        PRIMARY KEY,           -- UUIDv7, generated in Go
    batch_id            uuid,
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           text        NOT NULL,
    content             text        NOT NULL,
    priority            smallint    NOT NULL CHECK (priority IN (1, 2, 3)), -- 1 high, 2 normal, 3 low
    status              text        NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending', 'processing', 'sent', 'failed', 'cancelled')),
    idempotency_key     text        CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
    request_hash        bytea       NOT NULL CHECK (octet_length(request_hash) = 32), -- SHA-256 (D-009)
    scheduled_at        timestamptz,
    available_at        timestamptz NOT NULL DEFAULT now(),
    attempts            integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts        integer     NOT NULL CHECK (max_attempts >= 1),
    lease_token         uuid,
    last_error          text,
    provider_message_id text,
    correlation_id      text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (attempts <= max_attempts)
);

-- Duplicate-creation protection (D-009). Inserts use
-- ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING.
CREATE UNIQUE INDEX notifications_idempotency_key_key
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The claim query's index (D-005, D-013). Partial, so it only covers rows still
-- waiting to be sent and stays small however much history accumulates.
CREATE INDEX notifications_claim_idx
    ON notifications (channel, priority, available_at)
    WHERE status IN ('pending', 'processing');

-- Batch status lookups (D-008: batch status = counts grouped by status).
CREATE INDEX notifications_batch_id_idx
    ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

-- One row per delivery attempt (D-010), written in the same transaction as the
-- notification's status change.
CREATE TABLE delivery_attempts (
    id              uuid        PRIMARY KEY,               -- UUIDv7, generated in Go
    notification_id uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      integer     NOT NULL CHECK (attempt_no >= 1),
    started_at      timestamptz NOT NULL,
    duration_ms     integer     CHECK (duration_ms >= 0),   -- null when unknown (e.g. a reclaimed lease)
    http_status     smallint    CHECK (http_status BETWEEN 100 AND 599),
    outcome         text        NOT NULL
                                CHECK (outcome IN ('success', 'transient', 'permanent', 'unknown', 'channel_wide')),
    error           text
);

-- Attempt history for GET /notifications/{id}. Not unique: a channel-wide
-- outcome gives the attempt back (D-010), so an attempt number can repeat.
CREATE INDEX delivery_attempts_notification_idx
    ON delivery_attempts (notification_id, attempt_no);

-- +goose Down
DROP TABLE delivery_attempts;
DROP TABLE notifications;
