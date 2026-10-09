# Compatibility policy

The adapter follows semantic versioning under `rabbitmq/v<version>`.
The v2 facade consumes the canonical adapter-v2 and root-v2 cohort; it does
not preserve v1 nominal type identities across the major boundary.
Compatibility includes exported Go APIs, error classification, AMQP 1.0
mapping, RabbitMQ Streams confirmation and offset behavior, start-position and
replay semantics, Super Stream routing, transport security, and documented
defaults.

Observable protocol choices are stable entries in the
[specification decision register](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/adapters/rabbitmq/docs/specification-decisions.md). A changed
decision requires compatibility, wire-format, provider-evidence, and migration
review even when the earlier behavior was undocumented.
