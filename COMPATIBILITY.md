# Compatibility Policy

Each releasable directory is an independent Go module and follows semantic
versioning. Root releases use `v<version>` tags. Canonical OpenTelemetry adapter
releases use `adapters/otel/v<version>`, and canonical RabbitMQ transport
releases use `adapters/rabbitmq/v<version>`. The released `otel` and
`rabbitmq` modules retain their directory-prefixed tags as deprecated
compatibility facades. They preserve their released public type identities,
errors, instrumentation scope where applicable, and runtime behavior while
delegating to the canonical modules. See the [migration guide](docs/migration.md).
A directory prefix is never added to the root module's tag.

The root's next release is v2 with the required `/v2` module suffix and a
Go 1.27.2 minimum. Source remains at the repository root on main; major releases
use Git tags, not version-specific directories or branches. Existing v1 adapter
modules retain their root-v1 dependency cohort until explicitly migrated and
released. Root-v1 and root-v2 public types are distinct and must not be mixed.
The RabbitMQ transport's broker and client support matrix is documented
in [`adapters/rabbitmq/COMPATIBILITY.md`](adapters/rabbitmq/COMPATIBILITY.md);
OpenTelemetry support and limitations are documented in
[`adapters/otel/docs/reference.md`](adapters/otel/docs/reference.md).

Before `v1`, minor releases MAY contain reviewed breaking changes, but every
break MUST be documented with migration guidance. Patch releases MUST remain
backward compatible. At and after `v1`, incompatible exported API or documented
behavior changes require a new major version.

Compatibility includes exported Go APIs, error classification, serialization,
protocol behavior, persistence schemas, environment variables, command output,
resource ownership, ordering, retry/idempotency semantics, and documented
defaults. A compile-compatible change can still be behaviorally breaking.

Specification-backed modules MUST NOT diverge from their declared standards.
Ambiguities require documented decisions and stable tests. Deprecated APIs
follow [`DEPRECATION.md`](DEPRECATION.md).
