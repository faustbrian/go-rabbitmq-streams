# Changelog

All notable changes to this module are documented here.

## Unreleased

### Changed

- Advance module verification and ecosystem navigation to the final
  checksum-verified `go-library-tools` v1.4.0 release.
- Reconcile the root module archive checksum with its public Go proxy and
  checksum-database identity.
- Publish schema-v2 cohesion metadata, versioned ecosystem navigation, and the
  repository-local cohesion validation entry point for this module.
- Use the repository's pinned shared `golib` contract for module verification.

### Documentation

- Move detailed module guidance behind a concise README and documentation index.
- Link adoption and operations guidance to the standalone repository docs.

## 1.0.0 - 2026-08-25

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-rabbitmq-streams/otel` identity while preserving its documented API and behavior.

### Fixed

- Link the module README to package-owned documentation.

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
