#!/bin/sh
# make load: build the image, start the isolated load-test stack, run the
# load test in a container, print its result, and always tear the stack
# down with its volume, even when the test fails or is interrupted.
set -u
cd "$(dirname "$0")"
compose() { docker compose -f docker-compose.yml -p notification-loadtest "$@"; }

cleanup() { compose down -v --remove-orphans >/dev/null 2>&1; }
trap cleanup EXIT INT TERM

echo "Building the image and starting the load-test stack (own project, own database, no host ports)..."
compose up -d --build --quiet-pull api worker fakeprovider >/dev/null 2>&1 || {
  echo "could not start the load-test stack:" >&2
  compose up -d --build api worker fakeprovider >&2
  exit 1
}
compose run --rm --no-deps loadtest
status=$?
if [ $status -ne 0 ]; then
  echo "--- worker logs (last 20 lines)" >&2
  compose logs --tail 20 worker >&2
fi
exit $status
