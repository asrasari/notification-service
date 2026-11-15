# Design decisions

Code comments and commit messages refer to decisions as `D-0xx`; this page lists each one in its current form. R-M1…R-M5, R-CASE and Q-0xx in commit messages were review rounds and builder questions; their outcomes are folded into the decisions below.

### D-001 — Scope: a polished core first
- **Core first:** every required feature (API, processing, delivery and retry, observability, deliverables) is built with proper tests and written reasoning.
- **Failure handling is core:** the `failed` status and the retry design need it.
- **Bonuses came after the core:** scheduled sends (D-021) and CI (D-022).
- **Why:** correctness, tests and reasoning matter more than breadth.

### D-002 — Go module path
- `github.com/asrasari/notification-service`, matching the repository, so imports resolve without rewrites.

### D-003 — Go version
- **`go.mod`:** says `go 1.27.0` with no `toolchain` line. It declares the language version we test against, without forcing a patch-level download.
- **Image:** the Docker build stage pins `golang:1.27.4`, so builds are reproducible.
- **Hard minimum:** `testing/synctest` (Go 1.25+).

### D-004 — Process layout: one codebase, two processes
- **Two entrypoints, one image:** `cmd/api` and `cmd/worker` share `internal/` and are built into one Docker image. Compose runs it as `migrate` (once), `api` and `worker`.
- **One worker replica:** inside it, each channel has its own dispatcher and pool of senders.
- **Polling:** the worker polls for due rows every 200 ms when idle.
- **Shutdown:** both shut down gracefully on SIGTERM.
- **Why:** the API scales with traffic, the worker with the rate limit, and a worker crash can't take the API down.

### D-005 — PostgreSQL is the datastore and the queue
- **One table:** the `notifications` table is also the queue. There is no Redis, Kafka or RabbitMQ.
- **Claiming:** rows are claimed with `SELECT … FOR UPDATE SKIP LOCKED` in a short transaction that commits before the provider is called. No transaction is open during the HTTP call.
- **Small and swappable:** a partial index covers only unfinished rows, and the queue sits behind an interface.
- **Hand-rolled:** we wrote the queue instead of using River and similar libraries, because they bring their own retry logic.
- **Why:** one row means no dual write and no outbox, and 300 msg/s is easy for Postgres.

### D-006 — Priority: weighted and work-conserving
- **Weights:** 70/20/10 (high/normal/low) per channel, configurable as `PRIORITY_WEIGHTS`. Each claim picks a priority with smooth weighted round-robin (`HHNHHLHHNH`).
- **Fallback:** if the picked priority has nothing due, the claim falls back to the others, high first, so no slot is wasted.
- **Who decides:** the client sets `priority` (default `normal`).
- **Why:** strict priority starves `low`, and aging breaks index use. Weighted keeps `high` fast while everything moves.

### D-007 — Rate limiter and the per-channel loop
- **Token bucket:** one per channel (`golang.org/x/time/rate`), burst 1, paced at **99 %** of `RATE_LIMIT_PER_CHANNEL`. The limiter controls when sends start, but claim and network time shift arrivals by a few ms; the 1 % headroom keeps the provider's view at or under the limit.
- **One dispatcher per channel**, in this order: free sender → 429 pause → breaker → token → priority pick → claim **one** row → hand off.
  - Nothing due → report "nothing claimed" to the breaker and sleep.
  - Claim errors back off from the poll interval up to 5 s.
- **Claim query:** one transaction probes each priority with `… LIMIT 1 FOR UPDATE SKIP LOCKED` (Postgres forbids `FOR UPDATE` with `UNION`).
- **Shutdown:** in-flight sends get their own context, cancelled only after `SHUTDOWN_TIMEOUT`.
- **Sizing:** `WORKER_CONCURRENCY=50` (Little's law: 100/s × ~0.5 s latency).

### D-008 — Status state machine
- **Statuses:** exactly `pending`, `processing`, `sent`, `failed` and `cancelled`; the last three are final.
- **Writes:** every transition is a conditional `UPDATE` on the expected status. A worker writes its result only with its own `lease_token`. An expired lease is reclaimed directly (`processing → processing`) with a new token.
- **Cancel:** allowed only from `pending`; an already cancelled row returns 200, other statuses return 409.
- **Derived, not stored:** "scheduled" and "retrying" are `pending` rows told apart by data, and a batch's status is the count of its rows per status.

### D-009 — Idempotency
- **On creation:** an optional `idempotency_key` (≤ 255 chars) with a partial unique index, inserted with `ON CONFLICT DO NOTHING`.
  - A SHA-256 of the normalized request (after defaults and UTC/µs rounding of `scheduled_at`) decides: same hash → 200 with the stored row, different → 409.
  - Batches are all-or-nothing. A duplicate key within one batch is a 422.
- **On delivery:** every provider request carries `Idempotency-Key: <notification id>`, the same on every attempt.
- **Delivery rules:** delivery is at least once. The rate-limit token is taken before the claim, the lease is longer than the provider timeout, and lease times use the DB clock.

### D-010 — Retry and failure handling
- **Classification:**

  | Provider answer | Class | Uses an attempt? |
  |---|---|---|
  | 2xx | sent | – |
  | 5xx, 408, connection never opened | transient (retry) | yes |
  | timeout or reset after sending, any code outside 100–599 | unknown (retry, same key) | yes |
  | 429 | pause the channel for `Retry-After` (default 30 s, capped at 5 min) | no |
  | 3xx, 401, 403, 404 | config error, requeued after `BREAKER_OPEN_TIMEOUT` | no |
  | other 4xx | failed | yes |

- **Attempts:** counted at claim time, and given back for channel-wide outcomes. An expired lease with no attempts left is failed, with an `unknown` attempt recorded.
- **Backoff:** exponential with equal jitter, `limit = min(10 min, 10 s × 3^(n-1))`, wait `limit/2 + rand(0, limit/2)`, on the DB clock.
- **Circuit breaker per channel:** gobreaker v2 two-step. Rolling 10 s window (1 s buckets); it trips at ≥ 20 counted results with ≥ 50 % failures; 30 s open; one probe. Permanent errors, 429s and empty claims are excluded.
- **Attempt history:** one `delivery_attempts` row per attempt, written in the same transaction as the status. `last_error` is cleared when a row becomes `sent`.

### D-011 — Content validation
- **Strict JSON:** unknown fields are a 422. The body is capped at 10 MiB (413), and values are stored exactly as sent.
- **Recipients:** sms needs E.164; email a single bare address (≤ 254 chars); push a token of 1–512 chars with no whitespace.
- **Content:**
  - required, valid UTF-8, not only whitespace;
  - SMS at most 10 segments, with GSM-7 vs UCS-2 detected and segments counted (160/153 vs 70/67) and returned as `sms: {encoding, segments}`;
  - email ≤ 64 KiB, push ≤ 1024 chars.
- **Errors:** every rule violation is listed with its field path (e.g. `items[2].recipient`).

### D-012 — HTTP router and server hardening
- **Router:** stdlib `http.ServeMux` with method and `{id}` patterns.
- **JSON everywhere:** unknown paths return a JSON 404, and the mux's 405 is turned into JSON with the `Allow` header kept.
- **Timeouts:** read-header 5 s, read 30 s, write 30 s, idle 120 s.
- **Body cap:** request bodies are capped with `http.MaxBytesReader`.

### D-013 — DB access, migrations, schema
- **SQL:** pgx v5 with hand-written SQL, no ORM. Batch inserts use one `unnest` statement. Filter values are only ever `$n` parameters.
- **Migrations:** goose v3, embedded, run by `cmd/migrate`. Every migration has a Down section, and a test runs up → down → up.
- **Schema:** UUIDv7 ids; `timestamptz` in UTC; `text` + `CHECK` instead of enums; priority as a `smallint`.
- **One "claimable from" column:** `available_at` is the creation time, scheduled time, retry time or lease expiry, so the claim is one index condition.

### D-014 — Pagination and list filters
- **Order and cursor:** newest first on `(created_at, id)` with an opaque, versioned keyset cursor (base64url JSON). A bad cursor is a 400.
- **Limit:** 1–200, default 50.
- **Filters:** `status`, `channel`, `batch_id`, `created_from` (inclusive), `created_to` (exclusive). A reversed range is a 422.
- **Strict parameters:** unknown or repeated query parameters are a 422.
- **Why:** offset pagination skips or repeats rows under inserts and slows down with depth.

### D-015 — Logging, metrics and health
- **Logs:** `log/slog` JSON. The correlation ID (`X-Correlation-ID`, validated, or generated) is stored on each row and restored in the worker. Recipients and content are never logged.
- **Metrics:** Prometheus, one registry per process with its own `/metrics`. Labels come only from small fixed sets.
  - The API has HTTP metrics, created notifications, and queue depth and oldest-due age read at scrape time.
  - The worker has attempts, results, send/claim/latency histograms, breaker state, pause and busy senders, fed through hooks so `worker/` doesn't import Prometheus.
- **Health:**
  - The worker writes a heartbeat row every 5 s.
  - API `/health`: `down` (503) if the DB doesn't answer; `degraded` (200) with no live worker or with a breaker/pause reported; otherwise `ok`.
  - Worker `/health` is liveness only, with no DB check.

### D-016 — API contract
- **Spec:** a hand-written OpenAPI 3.0.3 file (`api/openapi.yaml`), served with Swagger UI at `/docs`. Contract tests validate every response in the HTTP tests, and every operation in the spec is routed.
- **Errors:** RFC 9457 problem details with stable `type` values.
  - 400 unreadable input, 415 wrong content type, 413 too large;
  - 422 rule violations, 409 idempotency or state conflicts.
- **Endpoints:** create (201 + `Location`, 200 on replay), batch (201/200, per-item results under `items`), get with attempt history, list, cancel (idempotent), batch status as counts, batch cancel.
- **Deadlocks:** batch rows are inserted sorted by idempotency key, so concurrent overlapping batches can't deadlock.

### D-017 — Testing strategy
- **Four layers:** pure unit tests; SQL integration against real Postgres; HTTP API with the real handler; end to end with a fake provider.
- **Postgres via testcontainers-go:** one container per package, and each test gets a fresh database copied from a migrated template. `KEEP_TEST_DB=1` keeps it for debugging.
- **Commands:** `make test` = `go test -race ./...`; `-short` skips Docker tests.
- **Fake time:** timing logic (limiter, breaker, backoff) uses `testing/synctest`. Worker tests use fakes, not gobreaker.

### D-018 — Configuration
- **Mechanism:** environment variables only, loaded once into a typed struct (`caarlos0/env`) and validated at startup, failing fast with every problem listed. For example, the lease must be longer than the provider timeout.
- **Defaults:** every setting has one except `DATABASE_URL`. The worker binary requires `PROVIDER_URL`; under compose it defaults to the built-in fake provider (D-020).

### D-019 — Folder layout by responsibility
- **Packages:** `notification`, `retry`, `postgres`, `httpapi`, `worker`, `breaker`, `provider`, `config`, `logging`, `metrics`.
- **Imports point inward:** `notification` and `retry` import nothing of ours.
- **Consumers declare interfaces:** e.g. `worker` declares the `Store`, `Breaker` and `Limiter` interfaces it uses.
- **Thin `main`:** it calls `run()`, which owns the wiring and the ordered shutdown.
- **Transport errors:** the provider client reports "never sent" only when no connection was obtained; anything uncertain is `unknown`.

### D-020 — Shipping: compose, fake provider, load test, README
- **Compose:**
  - only one service builds the image;
  - Postgres isn't published to the host;
  - host ports are overridable (`API_PORT`, `WORKER_PORT`);
  - a built-in fake provider is the default, so `docker compose up` works with no setup. A `PROVIDER_URL` (e.g. webhook.site) overrides it, and the worker logs the provider host.
- **`make load`:** runs an isolated compose project that sends 3,000 notifications. It passes only if the fake provider's peak in any 1-second window is ≤ the limit on every channel. It also prints p50/p95 latency per priority.
- **Why:** free webhook.site URLs take seconds per call and are rate-limited, so they can't show 100/s. The proof is measured at the receiving side.

### D-021 — Scheduled notifications
- **Field:** optional `scheduled_at` (RFC 3339 with an offset) on single and batch items, stored in UTC to the microsecond. It becomes `available_at` and is part of the request hash.
- **Window:** a new notification's time must be from `now() − 1 min` to `now() + 30 days` (DB clock). The check is part of the `INSERT`, so it only applies to rows that would be new.
  - New and outside the window → 422.
  - A replay of an accepted request → 200/409 as usual, even after its time.
- **Rejected:** "past → send now" (could send a reminder after the event) and "reject the past in validation" (breaks idempotent retries).

### D-022 — Continuous integration
- **Workflow:** GitHub Actions on pushes to `main` and on pull requests, with read-only permissions and outdated runs cancelled.
- **Jobs:**
  - lint: golangci-lint, pinned; the same version runs locally with `make lint`;
  - test: `make test` on a clean runner;
  - build: `docker build .`.
- **Not in CI:** `make load`, because shared runners add more timing jitter than the 1 % headroom allows.

### D-023 — This index
- Decision IDs appear in code comments and commit history; this page gives each one in its current form so every reference leads somewhere.
