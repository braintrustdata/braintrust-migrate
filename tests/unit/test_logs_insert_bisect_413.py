from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

from braintrust_migrate.resources.logs import LogsMigrator


class _Dest413Client:
    def __init__(self, *, max_events_per_insert: int) -> None:
        self.max_events_per_insert = max_events_per_insert
        self.inserts: list[int] = []

    async def with_retry(self, _operation_name: str, coro_func):
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
        if path.endswith("/insert"):
            assert json is not None
            events = json.get("events", [])
            if len(events) > self.max_events_per_insert:
                req = httpx.Request(
                    "POST", "https://api.braintrust.dev/v1/project_logs/x/insert"
                )
                resp = httpx.Response(413, request=req)
                raise httpx.HTTPStatusError("413", request=req, response=resp)
            self.inserts.append(len(events))
            return {"row_ids": [e.get("id", "") for e in events]}
        raise AssertionError(f"Unexpected path: {path}")


class _SourceClient:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = pages

    async def with_retry(self, _operation_name: str, coro_func):
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


@pytest.mark.asyncio
async def test_logs_insert_bisects_on_413(tmp_path: Path) -> None:
    TOTAL_EVENTS = 25
    MAX_PER_INSERT = 10
    # One page with 25 events; initial insert batch is 25 and will 413.
    page_events = [
        {
            "id": f"e{i}",
            "_pagination_key": f"p{i:05d}",
            "_xact_id": str(100 - i),
            "created": "2023-01-01T00:00:00Z",
        }
        for i in range(TOTAL_EVENTS)
    ]
    source = _SourceClient([page_events])
    dest = _Dest413Client(max_events_per_insert=MAX_PER_INSERT)

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=50,
        insert_batch_size=50,  # force single large insert attempt
        use_version_snapshot=False,
        use_seen_db=False,
    )
    migrator.set_destination_project_id("dest-project-id")

    result = await migrator.migrate_all("source-project-id")
    assert result["migrated"] == TOTAL_EVENTS
    # Should have split at least once (multiple insert calls)
    assert len(dest.inserts) > 1
    # All individual inserts should be <= 10
    assert all(n <= MAX_PER_INSERT for n in dest.inserts)
