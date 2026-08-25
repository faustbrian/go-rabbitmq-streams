#!/bin/sh
set -eu

: "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME to a task-owned value}"
: "${RABBITSTREAM_USER:?set RABBITSTREAM_USER}"
: "${RABBITSTREAM_PASSWORD:?set RABBITSTREAM_PASSWORD}"
: "${RABBITSTREAM_ERLANG_COOKIE:?set RABBITSTREAM_ERLANG_COOKIE}"

docker compose -f standalone-compose.yaml up -d --wait
status="$(curl --silent --show-error \
    --retry 10 \
    --retry-all-errors \
    --retry-delay 1 \
    --output /dev/null \
    --write-out '%{http_code}' \
    --header 'Content-Type: application/json' \
    --request POST \
    --data '{"name":"rabbitstream","listen":"0.0.0.0:15552","upstream":"rabbit:5552"}' \
    http://127.0.0.1:18474/proxies)"
case "${status}" in
    201 | 409) ;;
    *)
        printf 'unexpected Toxiproxy create status: %s\n' "${status}" >&2
        exit 1
        ;;
esac
