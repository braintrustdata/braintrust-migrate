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
