# Changelog

## Unreleased

### Changed

- Use the repository's pinned shared `golib` contract for module verification.

### Fixed

- Run recovery and rolling-upgrade checks against the shared fixture's
  task-owned dynamic ports and generated Compose resources.

### Documentation

- Add a module documentation index for direct navigation.
- Link lifecycle and operations guidance to the standalone repository docs and
  remove the superseded timestamped local benchmark report.

## 1.0.1 - 2026-08-26

### Fixed

- Preserve the configured RPC timeout for established environments and
  sessions without overlapping slow connection attempts.

## 1.0.0 - 2026-08-25

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-rabbitmq-streams/rabbitmq` identity while preserving its documented API and behavior.

### Fixed

- Accept both immediate connection rejection and deadline expiry from the
  upstream client when invalid TLS identities are correctly rejected.
- Make broker-restart recovery tests use their full bounded deadline instead
  of failing after three otherwise retryable ambiguous confirmations.
- Assert that broker-stored offset zero remains a valid initial consumer
  position under strict mutation testing.
- Preserve explicit nil-context rejection coverage while keeping the strict
  static-analysis contract green through a line-local test exception.
- Make the concurrent consumer reconnect test enforce emitted signal presence,
  uniqueness, and causal reconnect-before-ready ordering without assuming that
  concurrent loss and reconnect goroutines are scheduled in one fixed order.
- Link the module README to package-owned documentation.

### Added

- add a bounded stored-offset-only inspector query that does not open an
  unrelated end-of-stream consumer
- add the initial RabbitMQ-supported Go client transport for confirmed
  single-stream publishing, bounded reconnect recovery, and explicit ambiguous
  outcomes after accepted sends, including connection-ready observations after
  producer and consumer recovery and whole-session endpoint rotation when a
  broker accepts a connection but cannot establish the requested stream session
- document the adapter API, reconnect and lifecycle policy, wire contract,
  security boundary, operational evidence, and interoperability limitations
- add equivalent real-broker raw-client, policy-wrapper, TLS, and bounded
  confirmation-window performance benchmarks
- reject invalid, duplicate, or over-limit broker-reported Super Stream
  topology before opening partition producers or consumers
- prove a node-by-node RabbitMQ 4.3.4 to 4.3.5 rolling cluster upgrade and add
  steady-state idle and repeated lifecycle resource evidence
