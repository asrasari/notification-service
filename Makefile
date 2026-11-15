.PHONY: build test test-short up down load

# testcontainers needs two variables when Colima is the active Docker context
# (D-017): DOCKER_HOST points at Colima's socket, and the socket override is
# the path inside the VM, which the Ryuk reaper container mounts.
# Values already set in the environment win.
ifeq ($(shell docker context show 2>/dev/null),colima)
DOCKER_HOST ?= $(shell docker context inspect colima --format '{{.Endpoints.docker.Host}}')
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
export DOCKER_HOST TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
endif

# Build all binaries (api, worker, migrate) into ./bin.
build:
	go build -o bin/ ./cmd/...

# Run every test with the race detector. Integration tests start their own
# Postgres container, so this needs only a running Docker.
test:
	go test -race ./...

# Unit tests only: skips every test that needs Docker.
test-short:
	go test -short ./...

# Build the image and start the whole stack.
up:
	docker compose up --build

# Stop the stack (data in the Postgres volume is kept).
down:
	docker compose down

# Load test in an isolated compose project (own database, no host ports,
# never reads .env): 3,000 notifications against a fake provider, then
# throughput, peak per second and latency per priority. Always tears down.
load:
	./loadtest/run.sh
