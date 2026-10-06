# Changelog

## Unreleased

## 1.1.0 - 2026-10-06

### Changed

- Require Go 1.27.0 rather than the previous 1.26.6 minimum. Upgrade
  the toolchain before adopting this release.

### Changed

- Align the OpenTelemetry metric SDK and its resolved SDK dependencies
  with API 1.47.0 while keeping providers and shutdown caller-owned.

- Earlier preparation updated the OpenTelemetry API, metric, and trace
  graph to 1.47.0 while retaining SDK 1.45.0. The final SDK 1.47.0
  alignment above supersedes that retained SDK version.

- Earlier preparation updated metric and trace dependencies to 1.45.0
  with caller-owned providers; the final 1.47.0 graph supersedes it.

## 1.0.0 - 2026-09-09

### Added

- Publish the target-oriented OpenTelemetry adapter successor with the released
  observation, propagation, bounds, ownership, and error behavior.
