# Changelog

All notable changes to this module are documented here.

## Unreleased

### Changed

- Select root module v1.1.1 for the deprecated compatibility facade.
  Public types and runtime behavior remain unchanged.

The v1.1.0 facade entry below is release preparation. Canonical adapter
v1.1.0 selection follows its public release; keep this facade unpublished
until that dependency selection is complete.

The canonical v1.1.0 dependency is now publicly available and selected,
completing that publication-order prerequisite.

## 1.1.0 - 2026-10-06

### Changed

- Require Go 1.27.0 rather than the previous 1.26.6 minimum. Upgrade
  the toolchain before adopting this release.
- Select the canonical otel adapter v1.1.0 while preserving the
  released facade types and legacy instrumentation scope.

### Changed

- Align the deprecated adapter's resolved OpenTelemetry metric SDK
  with API 1.47.0 while preserving its legacy instrumentation scope.

- Earlier preparation updated the deprecated adapter's API, metric,
  and trace graph to 1.47.0 while retaining SDK 1.45.0 and its legacy
  scope. The final SDK 1.47.0 alignment above supersedes that retained
  SDK version.

- Earlier preparation updated the deprecated adapter graph to 1.45.0;
  the final 1.47.0 graph supersedes that intermediate selection.

## 1.0.1 - 2026-09-09

### Deprecated

- Delegate the released module to
  `github.com/faustbrian/go-rabbitmq-streams/adapters/otel` while preserving
  public type identities, errors, instrumentation scope, and behavior.

### Changed

- Advance module verification and ecosystem navigation to the final
  checksum-verified `go-library-tools` v1.4.0 release.
- Reconcile the root module archive checksum with its public Go proxy and
  checksum-database identity.
- Publish schema-v2 cohesion metadata, versioned ecosystem navigation, and the
  repository-local cohesion validation entry point for this module.
- Use the repository's pinned shared `golib` contract for module verification.

### Documentation

- Add the package map, explicit no-shutdown and provider-ownership boundary,
  direct operational FAQ navigation, and live support and security routes.

- Correct the README's immutable v1.4.0 ecosystem navigation and link the
  Integration and data movement family guidance.
- State the exact supported Go release and link the executable example and
  parent support policy from the module entry point.
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
