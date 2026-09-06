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

The root, OpenTelemetry, and RabbitMQ modules each have a published stable v1
line. The latest v1 patch release for each module receives security fixes unless
announced otherwise. Fixes land on the default branch before the affected
module is released independently. Support windows are documented in
[`COMPATIBILITY.md`](COMPATIBILITY.md).

## Security Gates

Releases require isolated tests, race and hostile-input checks, exact coverage
and mutation results, `govulncheck`, secret scanning, license verification,
SBOM generation, provenance validation, and clean-consumer resolution. A
missing scanner or unavailable service is a failed gate, not a warning.

Security fixes MUST include a regression test that does not publish weaponized
details or real secrets. Credentials MUST be redacted from logs and evidence.

## Repository Assurance

The repository [safety and concurrency policy](AGENTS.md#safety-and-concurrency)
and [supply-chain policy](AGENTS.md#dependencies-and-supply-chain) define shared
trust boundaries and release requirements. Package-specific security guidance
refines those rules for its owned boundary.
