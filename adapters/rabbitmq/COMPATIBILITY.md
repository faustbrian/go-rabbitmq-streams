# Compatibility policy

The adapter follows semantic versioning under
`adapters/rabbitmq/v<version>`.
Compatibility includes exported Go APIs, error classification, AMQP 1.0
mapping, RabbitMQ Streams confirmation and offset behavior, start-position and
replay semantics, Super Stream routing, transport security, and documented
defaults.

Changed protocol behavior requires compatibility, wire-format,
provider-evidence, and migration review even when the earlier behavior was
undocumented.
