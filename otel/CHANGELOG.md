# Changelog

All notable changes to this module are documented here.

## Unreleased

### Changed

- Pin the core RabbitMQ Streams module to a published pseudo-version so this
  adapter resolves outside the repository workspace.

### Added

- caller-owned OpenTelemetry metrics for stable payload-free RabbitMQ Streams
  lifecycle observations
- bounded, ownership-safe W3C Trace Context injection and extraction without
  baggage or global OpenTelemetry state
- accept validated Super Stream deliveries carrying both logical and backing
  stream identity during trace-context extraction
- closed error-category dimensions, panic isolation, race coverage, fuzz
  targets, examples, and allocation benchmarks
- metrics for handler retries, retry/dead-letter publication outcomes, exact
  inspected stream progress, and producer/consumer shutdown duration
