# Compatibility policy

The adapter follows semantic versioning under `rabbitmq/v<version>`.
Compatibility includes exported Go APIs, error classification, AMQP 1.0
mapping, RabbitMQ Streams confirmation and offset behavior, start-position and
replay semantics, Super Stream routing, transport security, and documented
defaults.

Observable protocol choices are stable entries in the
[specification decision register](docs/specification-decisions.md). A changed
decision requires compatibility, wire-format, provider-evidence, and migration
review even when the earlier behavior was undocumented.
