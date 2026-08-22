# rabbitstream/rabbitmq

This nested module adapts the pinned RabbitMQ-supported Go Streams client to the
stable policy types in the root [`rabbitstream`](..) module. It owns protocol
resources while keeping low-level client types out of ordinary public APIs.

The dependency remains nested because the selected client brings its own
dependency graph, including OpenTelemetry. Importing the root policy module
does not require it.

## API

- `OpenProducer` opens a bounded reconnecting transport and returns a root
  `*rabbitstream.Producer` for one stream or Super Stream.
- `OpenConsumer` opens one stream or the discovered backing streams and returns
  a root `*rabbitstream.Consumer` with explicit offset and failure policy.
- `NewReplayer` creates isolated exact-range replay using fresh environments;
  it never uses a live consumer name or stores offsets.
- `NewInspector` creates read-only topology, retained-range, offset, lag, and
  dependency-health inspection using fresh bounded environments. Its
  `StoredOffset` query avoids range snapshots when only durable consumer
  progress is required.

All constructors validate policy before connecting. Credential providers are
resolved for each new environment so reconnect and inspection can observe
rotation. Callers own the returned lifecycle and must close producers and
consumers.

## Quick start

See the root [producer and consumer guide](../README.md). Production TLS is the
default; plaintext is accepted only through the explicit development helper.
Streams and Super Streams must already exist.

```go
producer, err := rabbitmq.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{
    Stream: "tracking.events",
})
```

```go
consumer, err := rabbitmq.OpenConsumer(ctx, connection, rabbitstream.ConsumerConfig{
    Stream:       "tracking.events",
    ConsumerName: "tracking-projector-v1",
    Start:        rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored},
})
```

## Adapter behavior

- publisher confirmations map to confirmed, rejected, or ambiguous root
  results;
- connection loss invalidates a session and uses bounded,
  cancellation-aware recovery;
- one named producer plus explicit publishing IDs enables RabbitMQ's
  stream-scoped deduplication, not end-to-end exactly once;
- Super Stream publishing discovers backing streams, checks the expected count,
  and uses the selected client-compatible hash routing strategy;
- consumers keep each backing stream sequential while the root policy bounds
  parallelism across partitions;
- named offsets are stored only after successful root handler policy;
- replay checks the exact retained first and last message offsets before
  opening an isolated cursor;
- inspection does not treat committed chunk IDs as exact last offsets;
- unsupported or oversized wire metadata fails validation after conversion;
- all incoming payload and metadata bytes are copied.

The adapter never creates, changes, or deletes production topology.

## Wire contract

The adapter uses one AMQP 1.0 data section, standard message properties,
application properties, and the reserved
`x-rabbitstream-routing-key` annotation. Only string and binary annotation or
application-property values are accepted. See the root
[interoperability contract](../docs/interoperability.md).

## Failure and lifecycle tradeoffs

The selected client's protocol implementation is reused, but its unbounded HA
helper is not the policy boundary. This adapter owns finite reconnection,
context cancellation, safe error categories, pending-confirmation ambiguity,
and deterministic resource closure.

When a session fails after send, every pending confirmation is completed as
ambiguous. A new session may accept later calls, but the adapter does not retry
an already accepted message invisibly. Consumer reads reconnect after
connection-class failures; permanent authorization, offset, and partition
failures are returned.

Fresh environments used by replay and inspection avoid retaining a stale
locator. They are closed after each bounded operation. Replay cursor goroutines
have explicit terminal channels and close ownership.

## Security

The root connection policy enforces verified TLS 1.2 or newer and rejects
`InsecureSkipVerify`. Custom roots and mTLS are passed through a cloned
`tls.Config`. Broker errors are preserved for `errors.Is` and `errors.As`, but
rendered root errors remain category-only. Applications must not log unwrapped
causes without redaction.

Use least-privilege RabbitMQ users for publishing, consuming/offset storage,
and inspection. See the [operations guide](../docs/operations.md).

## Testing and interoperability boundary

Real-broker claims require the pinned single-node, TLS, restricted-user, and
three-node environments in [`integration`](integration). Tests cover
confirmation, duplicate IDs, restart, connection interruption, offset restart,
backlog recovery, retention, replay, Super Streams, topology changes, leader
and replica failure, endpoint rotation, and credential rotation.

The existence of these fixtures is not itself passing evidence. Use the
repository gates and record the exact RabbitMQ image, client, Go, OS, and
architecture for an interoperability claim.

Direct Laravel/PHP Streams compatibility is not claimed because the pinned
RabbitMQ support baseline has no supported PHP Streams client.

## FAQ

### Why is the upstream client not exposed?

Raw options can bypass bounds, lifecycle, TLS, and failure policy. New
capabilities require an explicit reviewed root contract.

### Does reconnect retry an ambiguous publish?

No. The original call receives an ambiguous result; application policy decides
whether and how to reconcile or retry it.

### Does inspection mutate broker state?

No. It uses read-only metadata and temporary consumers required to snapshot an
exact last message offset. It creates no stream and stores no offset.

### Who shuts down OpenTelemetry?

The application. This adapter does not own a telemetry provider.

## Release notes

See [CHANGELOG.md](CHANGELOG.md).
