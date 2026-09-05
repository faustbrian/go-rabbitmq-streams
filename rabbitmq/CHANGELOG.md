# Changelog

## Unreleased

### Changed

- Advance module verification and ecosystem navigation to the final
  checksum-verified `go-library-tools` v1.4.0 release.
- Reconcile the root module archive checksum with its public Go proxy and
  checksum-database identity.
- Publish schema-v2 cohesion metadata, versioned ecosystem navigation, and the
  repository-local cohesion validation entry point for this module.
- Govern the adapter's RabbitMQ Streams protocol, wire mapping, confirmation,
  offset, replay, Super Stream, and transport-security choices through the
  [specification decision register](docs/specification-decisions.md):
  `RABBITMQ-STREAM-DEC-001 sha256:82351d241102d65c13bd391ad564b5bccd4483197919ecd337116b288ea77fd3`,
  `RABBITMQ-STREAM-DEC-002 sha256:faf727d2bbb98e5f944924b295de5848b5f6a5b41c6e241ebe7caf8e6e94da8a`,
  `RABBITMQ-STREAM-DEC-003 sha256:42dfa5d274b3e421be3328a4b447d322723853ecccc239090c329cbf924711dd`,
  `RABBITMQ-STREAM-DEC-004 sha256:1068e2cb1d86639527e0bb8263f248ce21eb68ffb8064ab5dba8f50248152478`,
  `RABBITMQ-STREAM-DEC-005 sha256:74f82ab2d380c57560d2cc24b824433c8a0d46ac24664733b12c143aa3a3d61a`, and
  `RABBITMQ-STREAM-DEC-006 sha256:2d67c02f9756315064aa352b361029c2ebd62610c4e7dd1e3fea31ab6653752f`.
- Use the repository's pinned shared `golib` contract for module verification.

### Fixed

- Run recovery and rolling-upgrade checks against the shared fixture's
  task-owned dynamic ports and generated Compose resources.
- Retry bounded leader-failure probes when reconnection fails before transport
  admission, while retaining ambiguous-delivery classification.
- Preserve the complete connection deadline across endpoint rotation so one
  broker's RPC timeout cannot prematurely end failover to a live broker.

### Documentation

- Correct the README's immutable v1.4.0 ecosystem navigation and link the
  Integration and data movement family guidance.
- Add canonical installation guidance, the exact supported Go release, and
  direct compatibility and support navigation to the module entry point.
- Link compile-checked producer and consumer examples that exercise the public
  adapter lifecycle from open through publish or handler execution.
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
