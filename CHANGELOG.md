# Changelog

## Unreleased

### Changed

- Track the pinned documentation-tool lockfile so clean CI checkouts install
  the exact validated cspell dependency.

- Reconcile standalone dependency checksums against deterministic current
  module archives so CI, local verification, and release consumers resolve
  identical content.

- Harden standalone documentation validation with deterministic spelling and
  link checks, package-specific documentation gates, and repository-local
  contributor guidance.

### Documentation

- Correct stale package, standalone, and authoritative-source links in public
  documentation.

## 1.0.0 - 2026-08-25

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-rabbitmq-streams` identity while preserving its documented API and behavior.

### Fixed

- link the module README to the repository documentation portal

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
