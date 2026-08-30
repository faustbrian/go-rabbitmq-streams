# Documentation

`rabbitstream` provides vendor-neutral policy for RabbitMQ Streams and Super
Streams, with separate RabbitMQ and OpenTelemetry adapter modules.

## Choosing a package

- Use the root module for message, delivery, retry, replay, inspection, and
  lifecycle contracts.
- Use [`rabbitmq`](../rabbitmq) for the supported RabbitMQ Go Streams client.
- Use [`otel`](../otel) for bounded metrics and W3C Trace Context propagation.
- Use [`queue`](https://github.com/faustbrian/go-queue) instead for competing
  job workers and process-and-remove delivery.

## Contracts

- [Delivery guarantees and responsibility boundaries](guarantees.md)
- [Language-neutral interoperability](interoperability.md)
- [Kafka semantic mapping and migration](kafka-mapping.md)
- [Pinned RabbitMQ adapter sources](../rabbitmq/specification/sources.lock.json)

## Operations

- [Provisioning, capacity, rollout, recovery, and troubleshooting](operations.md)

## API reference

- [Root package](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams)
- [RabbitMQ adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/rabbitmq)
- [OpenTelemetry adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/otel)

## Module documentation

- [RabbitMQ adapter documentation](../rabbitmq/docs/README.md)
- [OpenTelemetry adapter documentation](../otel/docs/README.md)
