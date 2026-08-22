# Changelog

## Unreleased

### Changed

- pin the core RabbitMQ Streams module to a published pseudo-version so this
  adapter resolves outside the repository workspace

### Fixed

- Make the concurrent consumer reconnect test enforce emitted signal presence,
  uniqueness, and causal reconnect-before-ready ordering without assuming that
  concurrent loss and reconnect goroutines are scheduled in one fixed order.
- link the module README to the repository documentation portal

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
