#!/bin/sh
set -eu

: "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME to the task-owned fixture value}"
: "${RABBITSTREAM_USER:?set RABBITSTREAM_USER}"
: "${RABBITSTREAM_PASSWORD:?set RABBITSTREAM_PASSWORD}"
: "${RABBITSTREAM_ERLANG_COOKIE:?set RABBITSTREAM_ERLANG_COOKIE}"

docker compose -f standalone-compose.yaml down --volumes
