# Changelog

All notable changes to this module are documented here.

## Unreleased

## 1.0.0 - 2026-08-25

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-rabbitmq-streams/otel` identity while preserving its documented API and behavior.

### Fixed

- link the module README to the repository documentation portal

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
