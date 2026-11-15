.PHONY: help up down logs test test-short lint load build

# testcontainers needs two variables when Colima is the active Docker context
# (D-017): DOCKER_HOST points at Colima's socket, and the socket override is
# the path inside the VM, which the Ryuk reaper container mounts.
# Values already set in the environment win.
ifeq ($(shell docker context show 2>/dev/null),colima)
DOCKER_HOST ?= $(shell docker context inspect colima --format '{{.Endpoints.docker.Host}}')
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
export DOCKER_HOST TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
endif

help: ## Show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-11s %s\n", $$1, $$2}'

up: ## Build and start the stack in the background (built-in fake provider unless PROVIDER_URL is set)
	docker compose up --build -d
	@echo "API: http://localhost:$${API_PORT:-8080}  (docs: /docs)   worker: http://localhost:$${WORKER_PORT:-8081}/health"

down: ## Stop the stack (the database volume is kept; add -v by hand to wipe it)
	docker compose down

logs: ## Follow the api and worker logs
	docker compose logs -f api worker

test: ## Run every test with the race detector (needs Docker for the Postgres tests)
	go test -race ./...

test-short: ## Run the unit tests only (no Docker)
	go test -short ./...

# The linter version is pinned here only; CI runs `make lint` too.
GOLANGCI_LINT_VERSION := v2.15.0

lint: ## Run golangci-lint (the CI version) in Docker
	docker run --rm -v "$(CURDIR)":/src -w /src \
		-v notification-service-golangci-cache:/root/.cache \
		-v notification-service-gomod-cache:/go/pkg/mod \
		golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run ./...

load: ## Load test: 3,000 notifications against a fake provider in an isolated stack
	./loadtest/run.sh

build: ## Build the binaries into ./bin
	go build -o bin/ ./cmd/...
