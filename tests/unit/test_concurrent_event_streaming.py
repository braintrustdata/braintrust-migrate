"""Unit tests for concurrent dataset/experiment event streaming."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from braintrust_migrate.resources.datasets import DatasetMigrator
from braintrust_migrate.resources.experiments import ExperimentMigrator
from braintrust_migrate.resources.base import MigrationResult
import braintrust_migrate.resources.datasets as datasets_module
import braintrust_migrate.resources.experiments as experiments_module


# ---------------------------------------------------------------------------
# Stub client that tracks concurrent event streams
# ---------------------------------------------------------------------------


class _ConcurrencyTrackingClient:
    """Stub client that measures concurrent event stream activity."""

    def __init__(
        self,
        *,
        btql_pages_per_resource: dict[str, list[list[dict[str, Any]]]] | None = None,
    ) -> None:
        self._pages = btql_pages_per_resource or {}
        self._page_idx: dict[str, int] = {}
        self.inserts: list[dict[str, Any]] = []
        self.btql_queries: list[str] = []

        # Concurrency tracking
        self._in_flight = 0
        self.max_in_flight = 0
        self._lock = asyncio.Lock()

        # Expose migration_config for max_concurrent_resources
        self.migration_config = Mock()
        self.migration_config.max_concurrent_resources = 3
        self.migration_config.copy_attachments = False
        self.migration_config.insert_max_request_bytes = 6 * 1024 * 1024
        self.migration_config.insert_request_headroom_ratio = 0.75
        self.migration_config.events_fetch_group_size = 25

    async def with_retry(
        self,
        _name: str,
        coro_func,
        *,
        non_retryable_statuses: set[int] | None = None,
    ):
        _ = non_retryable_statuses
        res = coro_func()
        if hasattr(res, "__await__"):
            return await res
        return res

    async def raw_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        timeout: float | None = None,
    ) -> Any:
        _ = params
        _ = timeout

        if method.upper() == "POST" and path == "/btql":
            query = json.get("query", "") if json else ""
            self.btql_queries.append(query)
            async with self._lock:
                self._in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self._in_flight)

            await asyncio.sleep(0.01)  # simulate latency

            # Determine which resource this BTQL query is for by parsing the query.
            resource_id = None
            matching_ids = [rid for rid in self._pages if rid in query]

            async with self._lock:
                self._in_flight -= 1

            if matching_ids:
                combined_events: list[dict[str, Any]] = []
                for resource_id in matching_ids:
                    idx = self._page_idx.get(resource_id, 0)
                    pages = self._pages[resource_id]
                    if idx < len(pages):
                        self._page_idx[resource_id] = idx + 1
                        combined_events.extend(pages[idx])
                combined_events.sort(key=lambda event: event["_pagination_key"])
                last_pk = combined_events[-1]["_pagination_key"] if combined_events else None
                return {
                    "data": combined_events,
                    "btql_last_pagination_key": last_pk,
                }
            return {"data": []}

        if method.upper() == "POST" and "/insert" in path:
            self.inserts.append(json or {})
            events = (json or {}).get("events", [])
            return {"row_ids": [e.get("id", "") for e in events]}

        if method.upper() == "POST":
            # Dataset/experiment creation
            name = (json or {}).get("name", "unnamed")
            return {"id": f"dest-{name}", "name": name}

        if method.upper() == "GET":
            return {"objects": []}

        raise AssertionError(f"Unexpected request: {method} {path}")


# ---------------------------------------------------------------------------
# Dataset concurrent streaming tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentDatasetStreaming:

    async def test_multiple_datasets_streamed_in_grouped_btql_fetch(
        self, tmp_path: Path
    ):
        """Dataset events should be fetched via grouped BTQL queries."""
        # Set up 3 datasets, each with one page of events.
        pages = {
            "ds1": [[{"id": "e1", "dataset_id": "ds1", "_pagination_key": "pk1", "_xact_id": "1"}]],
            "ds2": [[{"id": "e2", "dataset_id": "ds2", "_pagination_key": "pk2", "_xact_id": "2"}]],
            "ds3": [[{"id": "e3", "dataset_id": "ds3", "_pagination_key": "pk3", "_xact_id": "3"}]],
        }
        source = _ConcurrencyTrackingClient(btql_pages_per_resource=pages)
        dest = _ConcurrencyTrackingClient()
        original_writer = datasets_module.SDKDatasetWriter

        class _FakeSDKDatasetWriter:
            def __init__(self, dest_client: _ConcurrencyTrackingClient, dataset_id: str) -> None:
                self._dest_client = dest_client
                self._dataset_id = dataset_id

            async def write_rows(self, rows: list[dict[str, Any]]) -> None:
                self._dest_client.inserts.append(
                    {
                        "dataset_id": self._dataset_id,
                        "events": [dict(row) for row in rows],
                    }
                )

        datasets_module.SDKDatasetWriter = _FakeSDKDatasetWriter

        try:
            migrator = DatasetMigrator(
                source,  # type: ignore[arg-type]
                dest,  # type: ignore[arg-type]
                tmp_path,
                events_fetch_limit=100,
                events_use_seen_db=False,
            )

            results = [
                MigrationResult(success=True, source_id="ds1", dest_id="dest-ds1"),
                MigrationResult(success=True, source_id="ds2", dest_id="dest-ds2"),
                MigrationResult(success=True, source_id="ds3", dest_id="dest-ds3"),
            ]

            await migrator._migrate_records_for_datasets(results)

            assert len(dest.inserts) == 3
            assert source.max_in_flight >= 1
            assert len(source.btql_queries) == 2
            assert "dataset('ds1', 'ds2', 'ds3') spans" in source.btql_queries[0]
        finally:
            datasets_module.SDKDatasetWriter = original_writer


# ---------------------------------------------------------------------------
# Experiment concurrent streaming tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentExperimentStreaming:

    async def test_multiple_experiments_streamed_in_grouped_btql_fetch(
        self, tmp_path: Path
    ):
        """Experiment events should be fetched via grouped BTQL queries."""
        pages = {
            "exp1": [[{"id": "e1", "experiment_id": "exp1", "_pagination_key": "pk1", "_xact_id": "1"}]],
            "exp2": [[{"id": "e2", "experiment_id": "exp2", "_pagination_key": "pk2", "_xact_id": "2"}]],
            "exp3": [[{"id": "e3", "experiment_id": "exp3", "_pagination_key": "pk3", "_xact_id": "3"}]],
        }
        source = _ConcurrencyTrackingClient(btql_pages_per_resource=pages)
        dest = _ConcurrencyTrackingClient()
        original_writer = experiments_module.SDKExperimentWriter

        class _FakeSDKExperimentWriter:
            def __init__(self, dest_client: _ConcurrencyTrackingClient, experiment_id: str) -> None:
                self._dest_client = dest_client
                self._experiment_id = experiment_id

            async def write_rows(self, rows: list[dict[str, Any]]) -> None:
                self._dest_client.inserts.append(
                    {
                        "experiment_id": self._experiment_id,
                        "events": [dict(row) for row in rows],
                    }
                )

        experiments_module.SDKExperimentWriter = _FakeSDKExperimentWriter

        try:
            migrator = ExperimentMigrator(
                source,  # type: ignore[arg-type]
                dest,  # type: ignore[arg-type]
                tmp_path,
                events_fetch_limit=100,
                events_use_seen_db=False,
            )

            results = [
                MigrationResult(success=True, source_id="exp1", dest_id="dest-exp1"),
                MigrationResult(success=True, source_id="exp2", dest_id="dest-exp2"),
                MigrationResult(success=True, source_id="exp3", dest_id="dest-exp3"),
            ]

            await migrator._migrate_events_for_experiments(results)

            assert len(dest.inserts) == 3
            assert len(source.btql_queries) == 2
            assert "experiment('exp1', 'exp2', 'exp3') spans" in source.btql_queries[0]
        finally:
            experiments_module.SDKExperimentWriter = original_writer
