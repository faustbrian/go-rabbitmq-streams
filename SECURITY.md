# Security Policy

## Reporting

Report suspected vulnerabilities through this repository's
[private security advisory](https://github.com/faustbrian/go-rabbitmq-streams/security/advisories/new)
form. Do not open a public issue containing exploit details, credentials,
private fixtures, or affected deployment information.

Include the affected module and version, impact, reproduction, preconditions,
and any suggested mitigation. Reports are acknowledged as soon as practical;
timelines depend on severity and verification.

## Supported Versions

Root delivery-admission and shutdown corrections target v2, not a v1 backport.
Root-v1 users must migrate to root v2 and compatible adapters to obtain those
corrections once the releases are published. Publication and consumer
verification remain separate from source availability on main.

Unchanged OpenTelemetry modules retain their published root-v1 cohort; this
does not certify root-v1 remediation. RabbitMQ adapter/facade remediation is
released separately and must not be inferred from a root release. Support and
major-cohort compatibility are documented in
[`COMPATIBILITY.md`](COMPATIBILITY.md).

## Security Gates

Releases require isolated tests, race and hostile-input checks, exact coverage
and mutation results, `govulncheck`, secret scanning, license verification,
SBOM generation, provenance validation, and clean-consumer resolution. A
missing scanner or unavailable service is a failed gate, not a warning.

Security fixes MUST include a regression test that does not publish weaponized
details or real secrets. Credentials MUST be redacted from logs and evidence.

## Repository Assurance

The versioned [root security model](docs/security-model.md) records root-owned
controls and residual collaborator responsibilities. It does not certify a
protocol implementation, broker, or older public release.

The repository [safety and concurrency policy](AGENTS.md#safety-and-concurrency)
and [supply-chain policy](AGENTS.md#dependencies-and-supply-chain) define shared
trust boundaries and release requirements. Package-specific security guidance
refines those rules for its owned boundary.
