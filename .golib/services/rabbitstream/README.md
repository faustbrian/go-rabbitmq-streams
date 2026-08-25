# RabbitMQ Streams integration fixture

The standalone fixture routes `localhost:15552` through a pinned Toxiproxy
container to a single RabbitMQ node and exposes its control API only at
`127.0.0.1:18474`. This makes connection loss and recovery deterministic
without detaching Docker's published-port forwarding:

```sh
export COMPOSE_PROJECT_NAME=codex-rabbitstream-single
export RABBITSTREAM_USER=rabbitstream-test
export RABBITSTREAM_PASSWORD="$(openssl rand -hex 24)"
export RABBITSTREAM_ERLANG_COOKIE="$(openssl rand -hex 32)"

./standalone-setup.sh
```

Use `RABBITSTREAM_TEST_PROXY_API=http://127.0.0.1:18474` and
`RABBITSTREAM_TEST_PROXY_NAME=rabbitstream` for the network-interruption test.
The broker restart container is `${COMPOSE_PROJECT_NAME}-rabbit-1`. Remove the
exact fixture with `standalone-teardown.sh`.

The clustered fixture starts on the RabbitMQ 4.3.4 multi-platform index digest
recorded in the root module source lock. The first integration test performs a
node-by-node rolling upgrade to the pinned 4.3.5 digest, verifies each node
version and online stream membership, and confirms publication between steps.
The mixed-version phase also proves that producer session establishment rotates
across the configured endpoints when a broker is reachable but its stream
metadata path is temporarily unavailable. Later cluster tests therefore run
against 4.3.5. Repeated gates skip the transition when the same task-owned
fixture is already fully upgraded. The fixture exposes the three Streams
listeners at `localhost:15561`, `localhost:15562`, and `localhost:15563`.

Set task-owned credentials and a unique Compose project name, start the three
services; the pinned classic peer-discovery configuration forms the cluster:

```sh
export COMPOSE_PROJECT_NAME=codex-rabbitstream-cluster
export RABBITSTREAM_USER=rabbitstream-test
export RABBITSTREAM_PASSWORD="$(openssl rand -hex 24)"
export RABBITSTREAM_ERLANG_COOKIE="$(openssl rand -hex 32)"

./setup.sh
```

Run the integration-tagged tests with the three endpoints and credentials in
the environment. Remove only this task-owned project when evidence is complete:

```sh
./teardown.sh
```

The separate `tls-compose.yaml` fixture exposes an mTLS-only Streams listener
at `localhost:15571`. Its setup script also creates a restricted user that can
read and write only streams whose names start with
`codex-rabbitstream-allowed-`. Supply task-owned values for
`RABBITSTREAM_RESTRICTED_USER`, `RABBITSTREAM_RESTRICTED_PASSWORD`, and
`RABBITSTREAM_TLS_RUNTIME` in addition to the variables above; the shared
`RABBITSTREAM_ERLANG_COOKIE` requirement also applies. The runtime
directory receives only the CA certificate, client certificate, and client
private key. Remove the exact Compose project with `tls-teardown.sh` and then
remove the exact runtime directory when the evidence run finishes.
