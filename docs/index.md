# rabbitstream documentation map

[Repository README](../README.md) · [Current Golib portal](https://github.com/faustbrian/go-library-tools/blob/main/docs/ecosystem/README.md) · [Published ecosystem snapshot](https://github.com/faustbrian/go-library-tools/blob/v1.8.4/docs/ecosystem/README.md)

This map links package-owned adoption and operational guidance. Start with
the repository README for purpose, installation, and ownership boundaries.
Published compatibility sets are frozen multi-package baselines; they do not
certify every current main revision or a deployed application.

## Module boundaries

Import only the modules needed by your application. A deprecated facade,
preview adapter, or internal harness is not interchangeable with a stable
root API; follow its own README and migration guidance before composing it.

| Module | Status | Owner detail |
| --- | --- | --- |
| `github.com/faustbrian/go-rabbitmq-streams/v2` | stable | [README](../README.md) |
| `github.com/faustbrian/go-rabbitmq-streams/otel` | deprecated | [README](../otel/README.md) |
| `github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2` | deprecated | [README](../rabbitmq/README.md) |
| `github.com/faustbrian/go-rabbitmq-streams/adapters/otel` | stable | [README](../adapters/otel/README.md) |
| `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2` | stable | [README](../adapters/rabbitmq/README.md) |

## Choose and adopt

- [Documentation](README.md).
- [Guarantees and responsibility boundaries](guarantees.md).
- [Language-neutral interoperability](interoperability.md).
- [Kafka to RabbitMQ Streams mapping](kafka-mapping.md).
- [Lifecycle and adapter-path migration](migration.md).

## Operate and evaluate boundaries

- [Operations and capacity runbook](operations.md).
- [Security model](security-model.md).

## Owned adapters, examples, and harnesses

- [Rabbitstream OpenTelemetry adapter](../adapters/otel/README.md).
- [Documentation](../adapters/otel/docs/README.md).
- [Rabbitstream RabbitMQ adapter](../adapters/rabbitmq/README.md).
- [Documentation](../adapters/rabbitmq/docs/README.md).
- [RabbitMQ Streams integration fixture](../adapters/rabbitmq/integration/README.md).
- [Rabbitstream OpenTelemetry compatibility facade](../otel/README.md).
- [Documentation](../otel/docs/README.md).
- [Rabbitstream RabbitMQ compatibility facade](../rabbitmq/README.md).
- [Documentation](../rabbitmq/docs/README.md).
- [RabbitMQ Streams integration fixture](../rabbitmq/integration/README.md).

## Maintainer, specification, and evidence references

- [RabbitMQ Streams specification conformance](../adapters/rabbitmq/specification/README.md).
- [Source documentation audit](source-documentation-audit.md).

## Related next steps

[Contributor setup and checks](../CONTRIBUTING.md), [Compatibility policy](../COMPATIBILITY.md), [Release history](../CHANGELOG.md), [Private vulnerability reporting](../SECURITY.md), [Support](../SUPPORT.md).
