#!/bin/sh
set -eu

: "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME to a task-owned value}"
: "${RABBITSTREAM_USER:?set RABBITSTREAM_USER}"
: "${RABBITSTREAM_PASSWORD:?set RABBITSTREAM_PASSWORD}"
: "${RABBITSTREAM_ERLANG_COOKIE:?set RABBITSTREAM_ERLANG_COOKIE}"
: "${RABBITSTREAM_RESTRICTED_USER:?set RABBITSTREAM_RESTRICTED_USER}"
: "${RABBITSTREAM_RESTRICTED_PASSWORD:?set RABBITSTREAM_RESTRICTED_PASSWORD}"
: "${RABBITSTREAM_TLS_RUNTIME:?set RABBITSTREAM_TLS_RUNTIME to a new task-owned directory}"

if [ -e "$RABBITSTREAM_TLS_RUNTIME" ]; then
    echo "TLS runtime path already exists" >&2
    exit 1
fi
mkdir -m 700 "$RABBITSTREAM_TLS_RUNTIME"

docker compose -f tls-compose.yaml up -d --wait
docker compose -f tls-compose.yaml exec -T rabbit-tls \
    rabbitmqctl add_user "$RABBITSTREAM_RESTRICTED_USER" "$RABBITSTREAM_RESTRICTED_PASSWORD"
docker compose -f tls-compose.yaml exec -T rabbit-tls \
    rabbitmqctl set_permissions -p / "$RABBITSTREAM_RESTRICTED_USER" \
    '^$' '^codex-rabbitstream-allowed-.*$' '^codex-rabbitstream-allowed-.*$'
docker compose -f tls-compose.yaml cp certgen:/certs/ca.pem "$RABBITSTREAM_TLS_RUNTIME/ca.pem"
docker compose -f tls-compose.yaml cp certgen:/certs/client.pem "$RABBITSTREAM_TLS_RUNTIME/client.pem"
docker compose -f tls-compose.yaml cp certgen:/certs/client-key.pem "$RABBITSTREAM_TLS_RUNTIME/client-key.pem"
docker compose -f tls-compose.yaml cp certgen:/certs/untrusted-client.pem "$RABBITSTREAM_TLS_RUNTIME/untrusted-client.pem"
docker compose -f tls-compose.yaml cp certgen:/certs/untrusted-client-key.pem "$RABBITSTREAM_TLS_RUNTIME/untrusted-client-key.pem"
chmod 600 "$RABBITSTREAM_TLS_RUNTIME/client-key.pem" \
    "$RABBITSTREAM_TLS_RUNTIME/untrusted-client-key.pem"
