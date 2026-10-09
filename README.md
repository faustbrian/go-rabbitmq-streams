# rabbitstream

[![CI](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-rabbitmq-streams/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-rabbitmq-streams/v2.svg)](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/v2)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-rabbitmq-streams?sort=semver)](https://github.com/faustbrian/go-rabbitmq-streams/releases)
[![Go](https://img.shields.io/badge/go-1.27.2-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`rabbitstream` is a policy layer for durable RabbitMQ Streams and Super Streams
workloads. It provides bounded publishing, consumption, replay, inspection,
failure handling, lifecycle, and observations without implementing the
RabbitMQ Streams protocol.

The root policy library targets v2 and requires Go 1.27.2. Modules are released
independently from main. Existing v1 adapters retain root-v1 type identities;
they cannot be mixed with root-v2 configurations, producers, or consumers.
See the [migration guide](docs/migration.md) for the supported cohorts.

For the shared construction, ownership, lifecycle, and integration vocabulary,
see the versioned [v1.4.0 Golib ecosystem
index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Integration and data movement family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

| Use `rabbitstream` | Use [`queue`](https://github.com/faustbrian/go-queue) |
| --- | --- |
| retained event histories | jobs and commands |
| independent consumer progress | competing workers |
| replay and backlog catch-up | process-and-remove delivery |
| partitioned event ingestion | delayed or retried work |

## Install

```sh
go get github.com/faustbrian/go-rabbitmq-streams/v2@v2
```

Install only the modules an application imports. The root module defines
vendor-neutral policy and transport seams. The
[`adapters/rabbitmq`](adapters/rabbitmq/README.md) module adapts the supported
RabbitMQ Go Streams client, and [`adapters/otel`](adapters/otel/README.md)
provides optional OpenTelemetry metrics and W3C Trace Context propagation.
Select an adapter that explicitly supports the root major used by the
application. The published v1 OpenTelemetry adapter and facade remain on the
root-v1 cohort; their release is not implied by a root-v2 release. Select
RabbitMQ adapter/facade v2 with root v2. Their publication is a separate
delivery boundary. See the
[migration guide](docs/migration.md).

| Package | Use |
| --- | --- |
| `github.com/faustbrian/go-rabbitmq-streams/v2` | Define bounded messages, delivery policy, producer and consumer contracts, replay, and inspection. |
| `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2` | Open RabbitMQ Streams protocol resources through the bounded, attributed private client. |
| `github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2` | Use the deprecated compatibility facade within the root-v2/adapter-v2 cohort. |
| `github.com/faustbrian/go-rabbitmq-streams/adapters/otel` | Translate observations to caller-owned OpenTelemetry metrics and propagate W3C Trace Context. |

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
defer producer.Shutdown(context.Background())

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
defer consumer.Shutdown(context.Background())

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

Root producer and consumer values own bounded background work after successful
construction and require a bounded `Shutdown` call. Each caller independently
bounds its wait with context and `CloseTimeout`. Wait expiry is not cleanup
completion: one owner continues joining admitted work and closing transport,
and later callers can observe the actual shared result. The deprecated
context-taking `Close` method delegates to the same contract. This unreleased
change requires the next major release; trusted callbacks and contextless
supplier operations must still return. Stop new publications, cancel
and join consumer runs, close consumers, then close producers. The RabbitMQ
adapter owns the protocol connections and sessions it opens. The OpenTelemetry
adapter starts no goroutines, owns no provider or exporter, and exposes no
`Close` or `Shutdown`; callers flush and shut down their providers after all
stream clients stop emitting observations.

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

The [compiler-checked root examples](example_test.go) demonstrate producer and
consumer construction. The RabbitMQ and OpenTelemetry modules provide their
own [transport examples](adapters/rabbitmq/example_test.go) and
[telemetry example](adapters/otel/example_test.go).

## Compatibility

The public API follows semantic versioning. Pin an exact version for every
imported module and upgrade them as one reviewed set. Production capacity and
availability depend on the actual RabbitMQ cluster, topology, payloads,
handlers, and deployment environment.

## License

MIT. See [LICENSE](LICENSE).
