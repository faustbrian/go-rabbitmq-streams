# rabbitstream

[![CI](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-rabbitmq-streams.svg)](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-rabbitmq-streams?sort=semver)](https://github.com/faustbrian/go-rabbitmq-streams/releases)
[![Go](https://img.shields.io/badge/go-1.26.6-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`rabbitstream` is a policy layer for durable RabbitMQ Streams and Super Streams
workloads. It provides bounded publishing, consumption, replay, inspection,
failure handling, lifecycle, and observations without implementing the
RabbitMQ Streams protocol.

| Use `rabbitstream` | Use [`queue`](https://github.com/faustbrian/go-queue) |
| --- | --- |
| retained event histories | jobs and commands |
| independent consumer progress | competing workers |
| replay and backlog catch-up | process-and-remove delivery |
| partitioned event ingestion | delayed or retried work |

## Install

```sh
go get github.com/faustbrian/go-rabbitmq-streams
go get github.com/faustbrian/go-rabbitmq-streams/rabbitmq
```

The root module defines vendor-neutral policy and transport seams. The
[`rabbitmq`](rabbitmq) module adapts the supported RabbitMQ Go Streams client,
and [`otel`](otel) provides optional OpenTelemetry metrics and W3C Trace
Context propagation.

## Producer

Topology is operator-owned and must exist before the application starts. The
zero security value uses verified TLS 1.2 or newer.

```go
connection := rabbitstream.ConnectionConfig{
    Endpoints: []rabbitstream.Endpoint{{
        Host: "rabbitmq.internal",
        Port: 5551,
    }},
    VirtualHost: "/",
    Credentials: rabbitstream.StaticCredentials(
        os.Getenv("RABBITMQ_STREAM_USER"),
        []byte(os.Getenv("RABBITMQ_STREAM_PASSWORD")),
    ),
    Security: rabbitstream.SecurityConfig{
        TLS: &tls.Config{ServerName: "rabbitmq.internal"},
    },
}

producer, err := rabbitmq.OpenProducer(
    ctx,
    connection,
    rabbitstream.ProducerConfig{
        Stream: "tracking.events",
        Policy: rabbitstream.ProducerPolicy{MaxOutstanding: 256},
    },
)
if err != nil {
    return err
}
defer producer.Close(context.Background())

result, err := producer.Publish(ctx, rabbitstream.Message{
    Stream:        "tracking.events",
    MessageID:     eventID,
    CorrelationID: shipmentID,
    Payload:       encoded,
})
if err != nil {
    if result.State == rabbitstream.DeliveryAmbiguous {
        // The broker may have persisted the message. Reconcile before retrying.
    }
    return err
}
```

## Consumer

```go
consumer, err := rabbitmq.OpenConsumer(
    ctx,
    connection,
    rabbitstream.ConsumerConfig{
        Stream:       "tracking.events",
        ConsumerName: "tracking-projector-v1",
        Start: rabbitstream.StartPosition{
            Kind: rabbitstream.OffsetStartStored,
        },
        Policy: rabbitstream.ConsumerPolicy{
            MaxConcurrency:  1,
            HandlerTimeout:  30 * time.Second,
            FailureStrategy: rabbitstream.FailureStop,
        },
    },
)
if err != nil {
    return err
}
defer consumer.Close(context.Background())

return consumer.Run(ctx, func(
    handlerCtx context.Context,
    message rabbitstream.Message,
) error {
    return applyTrackingEvent(handlerCtx, message.Payload)
})
```

The consumer stores progress only after successful handling. External side
effects and RabbitMQ offsets do not share a transaction, so handlers must be
idempotent or reconcile duplicates.

## Guarantees

- Publisher confirmations do not prove downstream processing.
- A lost confirmation is ambiguous, not a definite non-send.
- Consumption is at least once; crashes can redeliver completed work.
- Ordering is per stream or Super Stream partition, never global.
- Replay uses independent cursors and does not advance live consumers.
- Retry and dead-letter publication complete before the source offset advances.

Read the [documentation index](docs/README.md) before production adoption. It
covers API contracts, delivery guarantees, operations, capacity validation,
interoperability, and Kafka migration.

## Compatibility

The public API follows semantic versioning. Pin an exact version for every
imported module and upgrade them as one reviewed set. Production capacity and
availability depend on the actual RabbitMQ cluster, topology, payloads,
handlers, and deployment environment.

## License

MIT. See [LICENSE](LICENSE).
