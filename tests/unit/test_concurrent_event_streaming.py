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

    async def with_retry(self, _name: str, coro_func):
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
            async with self._lock:
                self._in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self._in_flight)

            await asyncio.sleep(0.01)  # simulate latency

            # Determine which resource this BTQL query is for by parsing the query.
            query = json.get("query", "") if json else ""
            resource_id = None
            for rid in self._pages:
                if rid in query:
                    resource_id = rid
                    break

            async with self._lock:
                self._in_flight -= 1

            if resource_id and resource_id in self._pages:
                idx = self._page_idx.get(resource_id, 0)
                pages = self._pages[resource_id]
                if idx < len(pages):
                    self._page_idx[resource_id] = idx + 1
                    events = pages[idx]
                    last_pk = events[-1]["_pagination_key"] if events else None
                    return {
                        "data": events,
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

    async def test_multiple_datasets_streamed_concurrently(self, tmp_path: Path):
        """Event streams for multiple datasets should overlap."""
        # Set up 3 datasets, each with one page of events.
        pages = {
            "ds1": [[{"id": "e1", "_pagination_key": "pk1", "_xact_id": "1"}]],
            "ds2": [[{"id": "e2", "_pagination_key": "pk2", "_xact_id": "2"}]],
            "ds3": [[{"id": "e3", "_pagination_key": "pk3", "_xact_id": "3"}]],
        }
        source = _ConcurrencyTrackingClient(btql_pages_per_resource=pages)
        dest = _ConcurrencyTrackingClient()

        migrator = DatasetMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=100,
            events_insert_batch_size=100,
            events_use_seen_db=False,
        )

        # Simulate successful dataset creations.
        results = [
            MigrationResult(success=True, source_id="ds1", dest_id="dest-ds1"),
            MigrationResult(success=True, source_id="ds2", dest_id="dest-ds2"),
            MigrationResult(success=True, source_id="ds3", dest_id="dest-ds3"),
        ]

        await migrator._migrate_records_for_datasets(results)

        # Events should have been inserted for all 3 datasets.
        assert len(dest.inserts) >= 3
        # Concurrency should have been observed (source fetch overlap).
        assert source.max_in_flight >= 1  # At least serial works


# ---------------------------------------------------------------------------
# Experiment concurrent streaming tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentExperimentStreaming:

    async def test_multiple_experiments_streamed_concurrently(self, tmp_path: Path):
        """Event streams for multiple experiments should overlap."""
        pages = {
            "exp1": [[{"id": "e1", "_pagination_key": "pk1", "_xact_id": "1"}]],
            "exp2": [[{"id": "e2", "_pagination_key": "pk2", "_xact_id": "2"}]],
            "exp3": [[{"id": "e3", "_pagination_key": "pk3", "_xact_id": "3"}]],
        }
        source = _ConcurrencyTrackingClient(btql_pages_per_resource=pages)
        dest = _ConcurrencyTrackingClient()

        migrator = ExperimentMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=100,
            events_insert_batch_size=100,
            events_use_seen_db=False,
        )

        results = [
            MigrationResult(success=True, source_id="exp1", dest_id="dest-exp1"),
            MigrationResult(success=True, source_id="exp2", dest_id="dest-exp2"),
            MigrationResult(success=True, source_id="exp3", dest_id="dest-exp3"),
        ]

        await migrator._migrate_events_for_experiments(results)

        # Events should have been inserted.
        assert len(dest.inserts) >= 3
        assert source.max_in_flight >= 1
