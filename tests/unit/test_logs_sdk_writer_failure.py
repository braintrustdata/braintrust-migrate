from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import braintrust_migrate.resources.logs as logs_module
from braintrust_migrate.resources.logs import LogsMigrator


class _SourceClient:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = pages

    async def with_retry(
        self,
        _operation_name: str,
        coro_func,
        *,
        non_retryable_statuses: set[int] | None = None,
    ):
        _ = non_retryable_statuses
        return await coro_func()

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
        assert method.lower() == "post"
        assert path == "/btql"
        assert json is not None
        assert isinstance(json.get("query"), str)
        if self._pages:
            return {"data": self._pages.pop(0)}
        return {"data": []}


class _DestClient:
    async def with_retry(
        self,
        _operation_name: str,
        coro_func,
        *,
        non_retryable_statuses: set[int] | None = None,
    ):
        _ = non_retryable_statuses
        return await coro_func()


class _FailingSDKProjectLogsWriter:
    def __init__(self, _dest_client: _DestClient, _project_id: str) -> None:
        self.calls = 0

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        _ = rows
        raise RuntimeError("sdk flush failed")


@pytest.mark.asyncio
async def test_logs_migrator_propagates_sdk_writer_failure(tmp_path: Path) -> None:
    page_events = [
        {
            "id": "e1",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        }
    ]
    source = _SourceClient([page_events])
    dest = _DestClient()

    original_writer = logs_module.SDKProjectLogsWriter
    logs_module.SDKProjectLogsWriter = _FailingSDKProjectLogsWriter
    try:
        migrator = LogsMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            page_limit=10,
            insert_batch_size=10,
            use_version_snapshot=False,
            use_seen_db=False,
        )
        migrator.set_destination_project_id("dest-project-id")

        with pytest.raises(RuntimeError, match="sdk flush failed"):
            await migrator.migrate_all("source-project-id")
    finally:
        logs_module.SDKProjectLogsWriter = original_writer
