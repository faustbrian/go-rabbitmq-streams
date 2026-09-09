#!/bin/sh
set -eu

: "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME to a task-owned value}"
: "${RABBITSTREAM_USER:?set RABBITSTREAM_USER}"
: "${RABBITSTREAM_PASSWORD:?set RABBITSTREAM_PASSWORD}"
: "${RABBITSTREAM_ERLANG_COOKIE:?set RABBITSTREAM_ERLANG_COOKIE}"

docker compose up -d --wait

docker compose exec -T rabbit1 rabbitmqctl await_online_nodes 3
docker compose exec -T rabbit1 rabbitmq-diagnostics -q cluster_status
