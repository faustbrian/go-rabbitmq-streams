# Changelog

## Unreleased

### Added

- establish the pinned RabbitMQ Streams source baseline, Kafka semantic mapping,
  bounded connection policy, owned byte-message model, and stable safe error
  categories for the new stream-specific client policy
- document producer, consumer, Super Stream, replay, delivery, security,
  interoperability, migration, capacity, rollout, and failure-recovery policy,
  with compiling five-minute root API examples
- expose bounded retry, retry/dead-letter publication, and producer/consumer
  shutdown observations for production operations
- validate direct and Super Stream consumer delivery shapes independently from
  producer message shapes
- reject aggregate-invalid publish batches before allocating per-message
  result state
- record the independent outbox, CloudEvents, event-sourcing, and service
  adapter decisions without coupling those domains to the core module
