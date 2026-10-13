# Contributing

## Before Editing

1. Read [`AGENTS.md`](AGENTS.md) and the affected module's docs.
2. Run the focused standalone Go tests for the affected package.
3. Identify owned dependencies and reverse dependants in `modules.json`.
4. Preserve unrelated work and generated/corpus provenance.

## Changes

Keep commits focused and conventional. Update every affected changelog with
the behavior and migration impact. Public API changes require compatibility
evidence and documentation. Specification behavior requires a decision record,
fixture coverage, and interoperability evidence.

New direct dependencies and dependency updates must follow the
[dependency governance policy](AGENTS.md#dependencies-and-supply-chain). Package-local
update bots are forbidden; the root policy owns every module and action update.

Specification-backed changes must follow the
[specification governance contract](AGENTS.md#design), update
the affected stable decision entries, and complete the Specification Decisions
section of the pull request template. An unresolved interpretation or stale
source pin is release-blocking; peer behavior cannot silently select policy.

RabbitMQ Streams protocol, wire mapping, confirmation, offset, replay,
security, and Super Stream changes must update the canonical adapter's
[specification decision register](adapters/rabbitmq/docs/specification-decisions.md).

Explicit full mutation qualification must finish with zero surviving viable
mutants; mutation is not required for every ordinary edit.

Do not add package-local workflows, permanent replacements, machine-specific
paths, bypass flags, broad mutation exclusions, or aggregate quality metrics
that hide a failing package.

## Verification

Use Go 1.27.2 for verification. Public module minimums and the distinct
root/adapter/facade major identities remain unchanged. From the root, run
standalone untagged tests without sibling repositories or building the verifier
release from source:

```bash
GOWORK=off go test ./... -count=1 -timeout=3m
```

Run the same command from an affected nested module for its own tests. Do not
mix root-v1 telemetry types with root-v2 policy or RabbitMQ adapter/facade types.
No goal ledger is a contributor prerequisite.

`make check` and `make ci` use the checksum-qualified public CLI and bounded
`--local --all` verification, including deterministic Go tests, saved fuzz seeds
and the named races below. Committed-range CI additionally selects short active
fuzz smoke for affected targets; base-free Make commands do not explore fuzz
inputs. The RabbitMQ adapter still declares the
`rabbitstream` service and `integration` tag; manifest-driven checks require
its Docker-backed broker fixture. Root, telemetry adapter and both compatibility
facades declare no services or test tags. Direct untagged Go tests are not
equivalent to live-broker qualification.

Two selected races run once with a combined three-minute declared budget.
The root assertion distinguishes caller timeout from actual completed cleanup;
the adapter assertion requires close to cancel and join reconnect opening and
close the late resource exactly once:

```bash
GOWORK=off go test -race . -run '^TestShutdownWaitTimeoutDoesNotPublishCleanupCompletion$' -count=1 -timeout=90s
(cd adapters/rabbitmq && GOWORK=off go test -race . -run '^TestTransportCloseCancelsAndJoinsReconnectOpening$' -count=1 -timeout=90s)
```

Normal tests execute saved fuzz seeds, not active exploration. For an affected
boundary, choose one target and package; optional local mutation is package-scoped:

```bash
GOWORK=off go test . -run '^$' -fuzz '^FuzzMessageValidationAndOwnership$' -fuzztime=2s -timeout=2m
golib mutation --module . --package .
```

Agent-run Go commands must use a task-owned disposable `GOCACHE` and remove it
after the bounded run.

Shared PR, main and scheduled CI use the bounded route. `make check-full` and
the explicit `release_dry_run` workflow input retain full assurance, including
mutation, coverage, race, API/security checks, adapter conformance and all ten
fuzz targets. Eight retain 100-iteration budgets and the two private bounded
wire decoders retain 1,000 iterations, each with a two-minute timeout; the
three benchmark operations retain 100ms budgets and 20-minute limits. Adapter
conformance retains its selected wire, confirmation, offset, replay, topology,
security and cancellation assertions with a ten-minute limit. Live broker/TLS
and topology changes require their actual fixtures and service evidence, not
just the named fake-transport races.

`make specification-check` is offline; `make specification-check-online` and
full qualification retain online source authority. For a deliberate release,
run `golib release check --module .` and `golib release dry-run --module .`,
selecting the actual nested directory instead of `.` when appropriate. Rehearsal
does not publish, and one module's release does not release its adapters or
facades. Report unavailable or failing commands without claiming release
readiness from ordinary verification.

## Adding A Module

Follow [repository structure policy](AGENTS.md#repository-structure). New modules
require an explicit purpose, ownership boundary, dependency review, package
catalog entry, full quality gates, documentation, changelog, license, security
policy, compatibility plan, and release dry-run.

## Source Documentation

Install the checksum-pinned public verifier declared in `.golib.yaml`.
`make source-docs-check` validates objective Go comment, marker, link and
generated-producer structure; `make check`, `make check-full` and `make ci`
require it before their existing checks. Shared CI runs the same strict
contract and includes its result in Required. This is structural enforcement,
not a substitute for source-truth review or behavioral verification.
