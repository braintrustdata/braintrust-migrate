# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added

- N/A

### Changed

- N/A

### Fixed

- N/A

## [0.3.0] - 2026-04-07

### Added

- Project-level migration concurrency is now fully wired through a single execution path, with regression coverage for orchestrator behavior, progress reporting, and configuration.
- Added a shared streaming flush threshold for logs, experiment events, and dataset events via `MIGRATION_EVENTS_FLUSH_MAX_ROWS`.

### Changed

- Simplified project migration execution so concurrent and sequential project runs share the same orchestration path.
- Unified buffered event streaming configuration across logs, experiment events, and dataset events, including CLI and environment-variable plumbing.
- Updated installation and release documentation for the `0.3.0` release.

### Fixed

- Restored compatibility with mocked and lightweight client configs when reading streaming flush settings.
- Improved streaming progress reporting so page-level counters reflect committed inserts separately from buffered rows.

## [0.2.0] - 2026-04-06

### Added

- SDK-backed `logs3` writers for high-volume logs, experiment events, and dataset events.
- Buffered BTQL event streaming with grouped dataset and experiment fetches for higher-throughput migrations.
- Additional regression coverage for SDK-backed streaming, retry behavior, UTF-8 batching, and resume flows.

### Changed

- Switched high-volume event migration from direct insert endpoints to buffered SDK-backed writers.
- Updated BTQL request metadata handling and removed the `query_source` request/state field.
- Refined release and streaming documentation to match the new `0.2.0` migration behavior.

### Fixed

- Made byte-based batching account for actual UTF-8 payload size instead of character count.
- Preserved compatibility with mocked and lightweight client configs used across the test suite.
- Brought integration and unit tests in line with buffered SDK streaming and the current retry API.

## [0.1.0] - 2026-03-02

### Added

- Initial public release of the Braintrust migration CLI with orchestrated resource migrators and test coverage.
- Support for migrating experiments, datasets, logs, ACLs, and experiment tags between Braintrust environments.
- Opt-in group member user mapping via email matching during ACL migration.
- Pagination support for resource listing, `created_before` filtering, and release/tag-based publishing workflows.
- Opt-in live smoke and E2E validation coverage for concurrency-sensitive migration paths.

### Changed

- Refactored migration internals to use the current API surface instead of the older Braintrust API SDK.
- Improved migration resilience around temporary-directory creation, client/config plumbing, and concurrent resource orchestration.
- Hardened CI and release automation with versioned release workflow guidance and pinned GitHub Actions.

### Fixed

- Dry-run project discovery is now read-only and no longer creates destination projects.
- File output now consistently uses UTF-8 encoding.
- Restored DAG scheduler and orchestrator compatibility helpers expected by the test suite.
