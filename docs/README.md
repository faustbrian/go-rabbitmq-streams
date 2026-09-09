# Documentation

`rabbitstream` provides vendor-neutral policy for RabbitMQ Streams and Super
Streams, with separate RabbitMQ and OpenTelemetry adapter modules.

- [Root overview and canonical installation](../README.md)
- [Compiler-checked root examples](../example_test.go)
- [Compatibility and release tags](../COMPATIBILITY.md)

## Choosing a package

- Use the root module for message, delivery, retry, replay, inspection, and
  lifecycle contracts.
- Use [`adapters/rabbitmq`](../adapters/rabbitmq) for the supported RabbitMQ Go Streams client.
- Use [`adapters/otel`](../adapters/otel) for bounded metrics and W3C Trace Context propagation.
- Use [`queue`](https://github.com/faustbrian/go-queue) instead for competing
  job workers and process-and-remove delivery.

## Contracts

- [Delivery guarantees and responsibility boundaries](guarantees.md)
- [Language-neutral interoperability](interoperability.md)
- [Kafka semantic mapping and migration](kafka-mapping.md)
- [Lifecycle and adapter-path migration](migration.md)
- [Pinned RabbitMQ adapter sources](../adapters/rabbitmq/specification/sources.lock.json)

## Operations

- [Provisioning, capacity, rollout, recovery, and troubleshooting](operations.md)

## API reference

- [Root package](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams)
- [RabbitMQ adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq)
- [OpenTelemetry adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/otel)

## Module documentation

- [RabbitMQ adapter overview and installation](../adapters/rabbitmq/README.md)
- [RabbitMQ adapter documentation](../adapters/rabbitmq/docs/README.md)
- [RabbitMQ compiler-checked examples](../adapters/rabbitmq/example_test.go)
- [RabbitMQ changelog](../adapters/rabbitmq/CHANGELOG.md)
- [OpenTelemetry adapter overview and installation](../adapters/otel/README.md)
- [OpenTelemetry adapter documentation](../adapters/otel/docs/README.md)
- [OpenTelemetry compiler-checked example](../adapters/otel/example_test.go)
- [OpenTelemetry changelog](../adapters/otel/CHANGELOG.md)

## Support and maintenance

- [Operations, performance, troubleshooting, and FAQ](operations.md)
- [Support](../SUPPORT.md)
- [Private vulnerability reporting](../SECURITY.md)
- [Root changelog](../CHANGELOG.md)
- [Contributing](../CONTRIBUTING.md)
- [License](../LICENSE)
