.PHONY: build test up down

# Build all binaries (api, worker, migrate) into ./bin.
build:
	go build -o bin/ ./cmd/...

# Run every test. Integration tests run only when TEST_DATABASE_URL is set.
test:
	go test ./...

# Build the image and start the whole stack.
up:
	docker compose up --build

# Stop the stack (data in the Postgres volume is kept).
down:
	docker compose down
