-- +goose Up

-- GET /notifications pages newest first with keyset pagination (D-014):
-- ORDER BY created_at DESC, id DESC and WHERE (created_at, id) < ($1, $2).
-- Postgres scans these ascending indexes backwards for the DESC order.
CREATE INDEX notifications_created_idx
    ON notifications (created_at, id);

-- The same order within one status, for ?status=… (the most common filter).
-- No channel index: three values, low selectivity, and it would cost a write
-- on every status change (D-014).
CREATE INDEX notifications_status_created_idx
    ON notifications (status, created_at, id);

-- +goose Down
DROP INDEX notifications_status_created_idx;
DROP INDEX notifications_created_idx;
