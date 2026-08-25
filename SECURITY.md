# Security Policy

## Reporting

Report suspected vulnerabilities privately through the GitHub security
advisory for `faustbrian/golib`. Do not open a public issue containing exploit
details, credentials, private fixtures, or affected deployment information.

Include the affected module and version, impact, reproduction, preconditions,
and any suggested mitigation. Reports are acknowledged as soon as practical;
timelines depend on severity and verification.

## Supported Versions

Until modules reach `v1`, only the latest released minor line receives security
fixes. After `v1`, support windows are documented per module and in
[`COMPATIBILITY.md`](COMPATIBILITY.md).

## Security Gates

Releases require isolated tests, race and hostile-input checks, exact coverage
and mutation results, `govulncheck`, secret scanning, license verification,
SBOM generation, provenance validation, and clean-consumer resolution. A
missing scanner or unavailable service is a failed gate, not a warning.

Security fixes MUST include a regression test that does not publish weaponized
details or real secrets. Credentials MUST be redacted from logs and evidence.

## Repository Assurance

The repository-wide [threat model](docs/security/threat-model.md),
[security matrix](docs/security/security-matrix.md), and
[residual-risk register](docs/security/residual-risks.md) define shared trust
boundaries and open release risks. Package-specific threat models refine those
rules for their owned boundary; they do not replace the repository model.
