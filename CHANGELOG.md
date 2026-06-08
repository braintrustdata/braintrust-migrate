# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added

- N/A

### Changed

- Skip bundle-backed code functions during migration. A code function's compiled bundle is produced by the push/eval build pipeline and is not exposed by the API, so it can't be recreated in the destination — migrating it produces a broken function. These are now skipped (recorded with `skip_reason="code_bundle_not_migratable"` and logged with name/slug so they can be re-pushed manually). Inline code functions (which carry their source) and all other function types continue to migrate normally.

### Fixed

- N/A

## [0.4.1] - 2026-06-08

### Fixed

- Resolve `org_id` for attachment operations from a project (`GET /v1/project`, captured opportunistically whenever the client lists or creates a project) instead of `GET /ping`, which 404s on the SaaS REST API. Fixes a 0.4.0 regression where oversize-field attachment spilling failed on SaaS with "Unable to determine org_id for attachment operations" (also repairs the `copy_attachments` and organization-scoped ACL paths, which shared the same dependency). Verified end-to-end against SaaS.

## [0.4.0] - 2026-06-08

### Added

- Automatic spilling of oversized event fields into JSON attachments: when a row exceeds the ~20MB per-span logging upload limit, the largest of `input`, `output`, `metadata`, `expected`, `error`, and `span_attributes` are uploaded to the destination as `braintrust_attachment` references so the span migrates successfully instead of failing on `/logs3/overflow`. Applies to logs, experiment events, and dataset events; always on, no configuration required.
- End-to-end and unit coverage for oversize-field spilling, including a full `LogsMigrator` run over a >20MB event.

### Changed

- Folded per-event size measurement into a single pass in the streaming insert path, reusing the result for both oversize detection and byte accounting (removes a redundant serialization).
- Tracked spilled-field counts in streaming checkpoint state and migration reports.

### Fixed

- Experiment migration now remaps `parameters_id` (a foreign key to the prompts table) to the destination prompt id, or drops it together with `parameters_version` when the prompt was not migrated, preventing foreign-key violations introduced by recent OpenAPI schema additions.
- Migrated `tags` for datasets, now that it is part of the dataset create schema.

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
