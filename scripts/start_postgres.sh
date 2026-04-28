#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CINDER_PG_CONTAINER:-cinder-postgres}"
PORT="${CINDER_PG_PORT:-55432}"
PASSWORD="${CINDER_PG_PASSWORD:-cinder}"
DATABASE="${CINDER_PG_DATABASE:-cinder}"
USER="${CINDER_PG_USER:-cinder}"

if podman ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "postgres already running on 127.0.0.1:${PORT}"
  exit 0
fi

if podman ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  podman rm "${CONTAINER_NAME}" >/dev/null
fi

podman run -d \
  --name "${CONTAINER_NAME}" \
  -e POSTGRES_USER="${USER}" \
  -e POSTGRES_PASSWORD="${PASSWORD}" \
  -e POSTGRES_DB="${DATABASE}" \
  -p "127.0.0.1:${PORT}:5432" \
  docker.io/library/postgres:16-alpine >/dev/null

echo "postgres started on 127.0.0.1:${PORT}"

