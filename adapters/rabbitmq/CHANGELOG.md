# Changelog

## Unreleased

### Fixed

- Reject explicit replay starts outside the native signed offset range before
  acquiring a broker environment, preserving the maximum representable offset.

- Apply configured decoded-delivery byte and metadata-count limits before native
  adapter copies and key sorting in live consumption and replay. This does not
  bound allocations already made by the upstream frame decoder.

- Close partially opened native environments and sessions before returning an
  opening failure or retrying. Successful opens still transfer ownership to
  the caller; cancellation retains its original classification and priority.

## 1.1.0 - 2026-10-06

### Changed

- Require Go 1.27.0 rather than the previous 1.26.6 minimum. Upgrade
  the toolchain before adopting this release.

### Changed

- Update the RabbitMQ client's indirect system dependency to
  golang.org/x/sys 0.48.0 without changing the client version.

- Update the indirect OpenTelemetry API, metric, and trace dependency
  graph to 1.47.0 while retaining the RabbitMQ Streams client version.

- Refresh the RabbitMQ client's resolved OpenTelemetry dependencies
  to 1.45.0, together with its logging and system dependency graph.

## 1.0.1 - 2026-09-09

### Added

- Own the RabbitMQ Streams specification decisions, pinned authorities,
  provider evidence, and conformance matrix at the canonical adapter path; see
  the [decision register](docs/specification-decisions.md). This relocation
  records the unchanged decisions `RABBITMQ-STREAM-DEC-001`,
  `RABBITMQ-STREAM-DEC-002`, `RABBITMQ-STREAM-DEC-003`,
  `RABBITMQ-STREAM-DEC-004`, `RABBITMQ-STREAM-DEC-005`, and
  `RABBITMQ-STREAM-DEC-006` under their canonical module owner.

  - RABBITMQ-STREAM-DEC-001 sha256:88ec8ed74586d613ca70f65c217f39590e93f2ec97fc910e70c1e9e078d4e708
  - RABBITMQ-STREAM-DEC-002 sha256:69c2f86ad42bb6eafb0f1e01590028439fc419a036cea4a1fa5dbc31fb98dbc9
  - RABBITMQ-STREAM-DEC-003 sha256:135bc6886da49a139e1bbc4f7cb9bf5605ba08666975775f735f585c2cb577a3
  - RABBITMQ-STREAM-DEC-004 sha256:f5e642784c9bebe140bcafda7e1bad7edb222815abb548b0dde7ee821efb01a1
  - RABBITMQ-STREAM-DEC-005 sha256:1d3c5b46bd1b816404ce843dc6a6ac7226347521b3d482c8be9c5886714d764d
  - RABBITMQ-STREAM-DEC-006 sha256:322cf0cea567657029841e14785a5fac096888b98e0522d84559575a27f6635b

## 1.0.0 - 2026-09-09

### Added

- Publish the target-oriented RabbitMQ Streams client adapter successor with
  the released connection, recovery, inspection, replay, and wire behavior.
