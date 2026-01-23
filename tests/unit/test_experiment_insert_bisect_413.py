from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

from braintrust_migrate.resources.experiments import ExperimentMigrator


class _SourceClient:
    def __init__(self, btql_pages: list[list[dict[str, Any]]]) -> None:
        self.btql_pages = btql_pages

    async def with_retry(self, _operation_name: str, coro_func):
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
        assert method.lower() == "post"
        if path == "/btql":
            assert json is not None and isinstance(json.get("query"), str)
            if not self.btql_pages:
                return {"data": []}
            return {"data": self.btql_pages.pop(0)}
        raise AssertionError(f"Unexpected path: {path}")


class _Dest413Client:
    def __init__(self, *, max_events_per_insert: int) -> None:
        self.max_events_per_insert = max_events_per_insert
        self.successful_inserts: list[int] = []

    async def with_retry(self, _operation_name: str, coro_func):
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
        assert method.lower() == "post"
        if path.endswith("/insert"):
            assert json is not None
            events = json.get("events", [])
            if len(events) > self.max_events_per_insert:
                req = httpx.Request(
                    "POST", "https://api.braintrust.dev/v1/experiment/x/insert"
                )
                resp = httpx.Response(413, request=req)
                raise httpx.HTTPStatusError("413", request=req, response=resp)
            self.successful_inserts.append(len(events))
            return {"row_ids": [e.get("id", "") for e in events]}
        raise AssertionError(f"Unexpected path: {path}")


@pytest.mark.asyncio
async def test_experiment_insert_bisects_on_413(tmp_path: Path) -> None:
    TOTAL_EVENTS = 25
    MAX_PER_INSERT = 10
    exp_id = "exp-source"

    page1 = [
        {
            "id": f"e{i}",
            "_pagination_key": f"p{i:05d}",
            "_xact_id": str(100 - i),
            "created": "2023-01-01T00:00:00Z",
        }
        for i in range(TOTAL_EVENTS)
    ]
    source = _SourceClient([page1])
    dest = _Dest413Client(max_events_per_insert=MAX_PER_INSERT)

    migrator = ExperimentMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        events_fetch_limit=50,
        events_insert_batch_size=50,
        events_use_version_snapshot=False,
        events_use_seen_db=False,
    )

    # call internal streaming method to avoid mocking experiment list/create plumbing
    await migrator._migrate_experiment_events_streaming(exp_id, "exp-dest")
    assert sum(dest.successful_inserts) == TOTAL_EVENTS
    assert all(n <= MAX_PER_INSERT for n in dest.successful_inserts)
