# rabbitstream

`rabbitstream` is the policy layer for durable RabbitMQ Streams and Super
Streams workloads. It owns bounded messages, publishing, consumption, replay,
inspection, errors, lifecycle, and observations. It does not implement the
RabbitMQ Streams protocol.

This module is intentionally separate from [`queue`](../queue):

| Use `rabbitstream` | Use `queue` |
| --- | --- |
| retained event histories | jobs and commands |
| independent consumer progress | competing workers |
| replay and backlog catch-up | process-and-remove delivery |
| partitioned event ingestion | delayed or retried work |
| Kafka-replacement event distribution | Laravel/Horizon-like workloads |

The API is pre-v1. Production adoption requires the affected release gates and
environment-specific capacity evidence; passing unit tests alone is not a
capacity or availability claim.

## Modules

- `rabbitstream` contains vendor-neutral policy types and transport seams.
- [`rabbitstream/rabbitmq`](rabbitmq) adapts the pinned RabbitMQ-supported Go
  Streams client.
- [`rabbitstream/otel`](otel) provides optional bounded OpenTelemetry metrics
  and W3C Trace Context propagation.

The root module has no RabbitMQ client, OpenTelemetry, queue, outbox,
event-sourcing, database, schema, or service-framework dependency.

## API reference

| Surface | Purpose |
| --- | --- |
| `ConnectionConfig`, `Endpoint`, `SecurityConfig`, `CredentialProvider` | owned TLS, authentication, endpoint rotation, timeout, heartbeat, and reconnect policy |
| `Limits`, `Message`, `MetadataEntry`, `ValidateBatch` | bounded opaque payloads, ordered metadata, ownership, and batch validation |
| `ProducerConfig`, `ProducerPolicy`, `Producer` | synchronous, batch, and bounded asynchronous confirmed publishing |
| `DeliveryResult`, `PublishOutcome`, `BatchPublishError` | per-message certainty and explicit partial-batch failure |
| `ConsumerConfig`, `ConsumerPolicy`, `Consumer` | named at-least-once consumption, partition sequencing, pause, drain, and offset policy |
| `BatchPolicy`, `RetryPolicy`, `FailureStrategy`, `FailurePublisher` | bounded batching and explicit stop, retry, retry-stream, dead-letter, or delegated failure behavior |
| `StartPosition`, `OffsetStartKind` | stored, beginning, end, explicit-offset, or timestamp subscription position |
| `ReplayRequest`, `RetainedRange`, `Replayer` | exact-range isolated replay with checkpoints and side-effect visibility |
| `InspectionRequest`, `InspectionResult`, `Inspector` | read-only topology, retained offsets, stored offsets, lag, and dependency health |
| `OperationError`, stable `Err*` values | safe operation and error categories for `errors.Is` and `errors.As` |
| `Observation`, `Observer` | bounded low-cardinality lifecycle and delivery signals |
| `ProducerTransport`, `ConsumerTransport`, `ReplaySource` | narrow implementation seams for reviewed adapters, not general raw-client escape hatches |

Public Go documentation on each identifier defines exact invariants and zero
values. The sections below explain cross-operation behavior and adoption.

## Five-minute producer

RabbitMQ topology is operator-owned and must exist before the application
starts. The zero security value means verified TLS with TLS 1.2 or newer.

```go
connection := rabbitstream.ConnectionConfig{
    Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
    VirtualHost: "/",
    Credentials: rabbitstream.StaticCredentials(
        os.Getenv("RABBITMQ_STREAM_USER"),
        []byte(os.Getenv("RABBITMQ_STREAM_PASSWORD")),
    ),
    Security: rabbitstream.SecurityConfig{
        TLS: &tls.Config{ServerName: "rabbitmq.internal"},
    },
}

producer, err := rabbitmq.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{
    Stream: "tracking.events",
    Policy: rabbitstream.ProducerPolicy{
        MaxOutstanding: 256,
    },
})
if err != nil {
    return err
}
defer producer.Close(context.Background())

result, err := producer.Publish(ctx, rabbitstream.Message{
    Stream:        "tracking.events",
    ContentType:   "application/octet-stream",
    MessageID:     eventID,
    CorrelationID: shipmentID,
    Timestamp:     time.Now().UTC(),
    Payload:       encoded,
})
if err != nil {
    if result.State == rabbitstream.DeliveryAmbiguous {
        // The broker may have persisted the message. Reconcile or retry with a
        // stable producer name and publishing ID; do not call this a non-send.
    }
    return err
}
```

`PublishBatch` validates the complete bounded batch before sending, publishes
in input order, and returns one result per input. `PublishAsync` retains the
message and admits at most `Limits.MaxBufferedMessages` operations.

## Five-minute consumer

```go
consumer, err := rabbitmq.OpenConsumer(ctx, connection, rabbitstream.ConsumerConfig{
    Stream:       "tracking.events",
    ConsumerName: "tracking-projector-v1",
    Start:        rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored},
    Policy: rabbitstream.ConsumerPolicy{
        MaxConcurrency:           1,
        HandlerTimeout:           30 * time.Second,
        OffsetStoreEveryMessages: 1,
        FailureStrategy:          rabbitstream.FailureStop,
    },
})
if err != nil {
    return err
}
defer consumer.Close(context.Background())

return consumer.Run(ctx, func(handlerCtx context.Context, message rabbitstream.Message) error {
    if err := applyTrackingEvent(handlerCtx, message.Payload); err != nil {
        return err
    }
    // The consumer stores message.Offset only after this handler succeeds.
    return nil
})
```

`RunBatch` preserves partition boundaries and stores only the last offset after
the whole batch succeeds. `Pause` stops new transport reads; it does not cancel
an active handler. `Close` cancels an active run and drains within the caller
and configured close deadlines.

## Super Stream producer and consumer

Super Streams are the horizontal-scaling abstraction. Every message requires
a routing key. Hash routing is compatible with the selected Go client's
Murmur3 strategy and is stable only while the ordered backing-stream topology
is unchanged.

```go
producer, err := rabbitmq.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{
    SuperStream:        "tracking.events",
    RoutingStrategy:    rabbitstream.RoutingHash,
    ExpectedPartitions: 12,
})
// ...
result, err := producer.Publish(ctx, rabbitstream.Message{
    SuperStream: "tracking.events",
    RoutingKey:  shipmentID,
    Payload:     encoded,
})
```

```go
consumer, err := rabbitmq.OpenConsumer(ctx, connection, rabbitstream.ConsumerConfig{
    SuperStream:  "tracking.events",
    ConsumerName: "tracking-projector-v1",
    Start:        rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored},
    Policy:       rabbitstream.ConsumerPolicy{MaxConcurrency: 12},
})
```

Ordering is per backing stream, never global across a Super Stream. Increasing
or reordering partitions can change key placement. Review topology before any
change when per-aggregate order matters.

## Replay

Replay uses fresh cursors, never stores offsets, and requires an exact retained
range before invoking application code. Super Stream replay is deliberately
one backing stream at a time and pins the approved ordered topology.

```go
replayer, err := rabbitmq.NewReplayer(connection, rabbitstream.DefaultLimits())
if err != nil {
    return err
}
end := uint64(250_000)
request := rabbitstream.ReplayRequest{
    Stream:           "tracking.events",
    Start:            rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit, Offset: 200_000},
    EndOffset:        &end,
    AllowSideEffects: false,
}
retained, err := replayer.Inspect(ctx, request)
if err != nil {
    return err
}
log.Printf("retained offsets %d..%d", retained.FirstOffset, retained.LastOffset)
return replayer.Run(ctx, request, func(ctx context.Context, delivery rabbitstream.ReplayDelivery) error {
    return verifyEvent(ctx, delivery.Message)
})
```

An explicit start before the first retained offset is `ErrRetentionGap`. A
requested end beyond the last retained offset, early cursor termination,
changed Super Stream topology, or incomplete range is `ErrReplayRange` or
`ErrPartitionUnavailable`. Timestamp replay is supported but an exact caller
range still requires retained-range inspection because the broker can clamp a
timestamp subscription.

## Delivery and duplication contract

| Boundary | Contract |
| --- | --- |
| before transport acceptance | cancellation, validation, closure, and local send failure are `DeliveryNotSent` |
| confirmed by broker | `DeliveryConfirmed`; this is not confirmation of downstream processing |
| definitive broker rejection | `DeliveryRejected` |
| sent but confirmation missing | `DeliveryAmbiguous`; the message may be retained |
| named producer plus publishing ID | broker deduplication for that producer name and stream; not end-to-end exactly once |
| handler success | offset storage is attempted afterward |
| handler, panic, cancellation, or offset-store failure | progress is not silently advanced |
| retry/dead-letter publication | separate from source offset storage; duplicates and loss windows remain |

There is no atomic transaction spanning a RabbitMQ Stream offset and a
database, HTTP request, queue, or target stream publish.

## Failure strategies

`ConsumerPolicy.FailureStrategy` is explicit:

- `FailureStop` returns the handler failure without advancing the offset.
- `FailureRetry` retries in-process within `RetryPolicy`, then stops.
- `FailureRetryStream` publishes an owned failure record to `RetryStream`.
- `FailureDeadLetter` publishes to `DeadLetterStream`.

Applications delegate policy by selecting `FailureStop`, observing the returned
error, and deciding whether and when to start a new consumer run.

Failure-stream publication preserves source stream, partition, offset,
attempt, and safe category in reserved application properties. The source
offset advances only after the failure publish is confirmed.

## Bounds and ownership

`DefaultLimits` bounds stream and routing names, payloads, individual and
aggregate metadata, batches, and asynchronous buffers. Connection, producer,
consumer, handler, retry, confirmation, and close policies have finite defaults
and reject negative or excessive values.

Synchronous calls borrow message bytes only for the call. `Publish` copies
before handing data to a transport, `PublishAsync` retains before returning,
and handlers receive owned delivery bytes. Call `Message.Retain` before keeping
a message beyond a custom synchronous boundary.

Every opened producer and consumer owns its transport and must be closed.
Credential providers return fresh owned snapshots so reconnection can observe
rotation. No API accepts unrestricted raw client options.

## Errors

Use `errors.Is` with stable sentinels such as `ErrInvalidConfiguration`,
`ErrValidation`, `ErrClosed`, `ErrCanceled`, `ErrTimeout`, `ErrAuthentication`,
`ErrAuthorization`, `ErrConnection`, `ErrStreamUnavailable`,
`ErrPartitionUnavailable`, `ErrBrokerRejected`, `ErrMessageTooLarge`,
`ErrPublishAmbiguous`, `ErrConfirmation`, `ErrRetentionGap`, `ErrReplayRange`,
`ErrOffset`, `ErrHandler`, and `ErrFatal`.

Use `errors.As` for `*OperationError` or `*BatchPublishError`. Rendered
`OperationError` values contain only operation and stable category; preserved
causes are for programmatic classification and must not be logged without
application-owned redaction.

## Security

- verified TLS is the default and TLS 1.2 is the minimum;
- `InsecureSkipVerify` is rejected;
- custom roots and mutual TLS use a caller-owned `tls.Config`, which is cloned;
- plaintext requires `DevelopmentPlaintextSecurity` and must never carry
  production credentials or traffic;
- credentials, payloads, routing keys, arbitrary metadata, and low-level error
  text are excluded from stable observations;
- grant only the RabbitMQ permissions required for the selected streams,
  Super Stream metadata, publishing, consuming, and offset tracking.

See the [operations guide](docs/operations.md) for permissions, readiness,
rotation, rollout, recovery, capacity planning, and troubleshooting.

## Observability and inspection

`Observer` receives bounded scalar lifecycle signals. Implementations must not
block indefinitely; panics are contained. Stream names, routing keys, message
IDs, offsets as dimensions, payloads, headers, credentials, and raw errors are
not included. Signals cover connection and reconnect state, publish outcomes,
consumer delivery and handler outcomes, in-process retries, retry/dead-letter
publication outcomes, offset and lag progress, replay, and producer/consumer
shutdown duration. The optional [OpenTelemetry adapter](otel) maps them to fixed
instruments.

`rabbitmq.NewInspector` uses fresh bounded connections for read-only stream,
partition, retained-range, stored-offset, and lag snapshots. `Health` is
dependency health, not process liveness. A temporary RabbitMQ outage must not
trigger an automatic process restart.

## Adoption and tradeoffs

Adopt `rabbitstream` for retained, independently replayable event histories.
Keep `queue` for jobs. Keep domain envelopes, schemas, databases, outboxes, and
service lifecycle in application or dedicated adapter modules.

Before production adoption:

1. provision streams, Super Streams, retention, replication, TLS identities,
   and permissions outside the application;
2. select stable routing and consumer identities and document ownership;
3. decide how ambiguous publishes and handler side effects are reconciled;
4. validate the actual cluster, payload distribution, confirmation latency,
   handler capacity, backlog catch-up, and failure recovery;
5. set alerts and execute the runbook;
6. pass the affected repository release gates.

The policy adds safety and honest failure states at the cost of explicit
configuration, finite throughput limits, and no generic messaging API.

## Further documentation

- [broker, client, package, operator, and application guarantees](docs/guarantees.md)
- [operations and capacity runbook](docs/operations.md)
- [Laravel/PHP and language-neutral wire contract](docs/interoperability.md)
- [Kafka semantic mapping and migration guide](docs/kafka-mapping.md)
- [pinned primary-source baseline](specification/sources.lock.json)

## FAQ

### Is this a queue abstraction?

No. Streams retain histories and consumers own independent progress. Use
`queue` for competing job workers and ACK/NACK processing.

### Is a confirmed publish exactly once?

No. It proves broker confirmation. Network ambiguity, application retries,
consumer crashes, and external side effects can create duplicates.

### Can two producer instances share a deduplication name?

No. Treat the name as one stream-scoped producer identity and keep publishing
IDs monotonic across restarts. Concurrent owners undermine that contract.

### Can replay advance the live consumer?

No. Replay uses no live consumer name and stores no offsets.

### Does the package create topology?

No. Stream and Super Stream creation, retention, replication, and mutation are
operator responsibilities.

### Is direct Laravel/PHP Streams interoperability supported?

Not as a release claim. RabbitMQ has no supported PHP Streams client in the
pinned source baseline. See [interoperability status](docs/interoperability.md).

## Release notes

See [CHANGELOG.md](CHANGELOG.md).
