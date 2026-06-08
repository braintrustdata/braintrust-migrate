"""Regression test: a single dataset event hitting HTTP 413 must invoke the
oversize-event diagnostic without crashing.

Previously `_on_single_413` referenced an unbound `source_dataset_id`, so a
single-event 413 raised `NameError` (aborting the migration and never writing
the diagnostic) instead of writing the summary and propagating the 413.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

import braintrust_migrate.resources.datasets as datasets_module
from braintrust_migrate.config import MigrationConfig
from braintrust_migrate.resources.datasets import DatasetMigrator


class _SourceStub:
    def __init__(self, page: list[dict[str, Any]]) -> None:
        self._page = page

    async def with_retry(self, _op, coro_func, *, non_retryable_statuses=None):
        _ = non_retryable_statuses
        res = coro_func()
        return await res if hasattr(res, "__await__") else res

    async def raw_request(self, method, path, *, params=None, json=None, timeout=None):
        _ = params, timeout
        assert method.lower() == "post" and path == "/btql"
        q = json["query"]
        # First page returns the event; subsequent (paginated) pages are empty.
        if "_pagination_key >" in q:
            return {"data": []}
        return {"data": self._page}


class _DestStub:
    migration_config = MigrationConfig(events_flush_max_rows=1)

    async def with_retry(self, _op, coro_func, *, non_retryable_statuses=None):
        _ = non_retryable_statuses
        res = coro_func()
        return await res if hasattr(res, "__await__") else res


class _FakeWriter413:
    """SDK writer stub whose insert always raises an HTTP 413."""

    def __init__(self, dest_client: _DestStub, dataset_id: str) -> None:
        self._dataset_id = dataset_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        req = httpx.Request("POST", "https://dest.example/logs3")
        raise httpx.HTTPStatusError(
            "payload too large",
            request=req,
            response=httpx.Response(413, request=req),
        )


@pytest.mark.asyncio
async def test_single_dataset_event_413_writes_summary_and_propagates(
    tmp_path: Path,
) -> None:
    page = [
        {
            "id": "evt-a",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
            "input": {"big": "x" * 100},
        }
    ]
    source = _SourceStub(page)
    dest = _DestStub()

    original_writer = datasets_module.SDKDatasetWriter
    datasets_module.SDKDatasetWriter = _FakeWriter413
    try:
        migrator = DatasetMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=1,
            events_use_seen_db=False,
        )

        # The 413 is re-raised after the diagnostic is written. Crucially it must
        # be the HTTPStatusError, NOT a NameError from an unbound variable.
        with pytest.raises(httpx.HTTPStatusError):
            await migrator._migrate_dataset_records(  # type: ignore[attr-defined]
                "source-dataset-id", "dest-dataset-id"
            )

        # The oversize diagnostic was written (proves _on_single_413 ran cleanly),
        # with the correct per-resource dest id resolved from the event.
        summary_path = tmp_path / "dataset_events" / "oversize_dataset_event_evt-a.json"
        assert summary_path.exists()
        import json

        summary = json.loads(summary_path.read_text())
        assert summary["dest_dataset_id"] == "dest-dataset-id"
        assert summary["event_id"] == "evt-a"
    finally:
        datasets_module.SDKDatasetWriter = original_writer
