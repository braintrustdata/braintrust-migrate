from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.resources.logs import LogsMigrator


class _StubClient:
    def __init__(
        self, *, page1: list[dict[str, Any]], page2: list[dict[str, Any]]
    ) -> None:
        self._page1 = page1
        self._page2 = page2
        self.inserts: list[list[dict[str, Any]]] = []
        self.fail_on_insert_call: int | None = None
        self._insert_calls = 0

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
            q = json.get("query")
            assert isinstance(q, str)
            if "WHERE _pagination_key > 'p1'" in q:
                return {"data": self._page2}
            if "WHERE _pagination_key > 'p2'" in q:
                return {"data": []}
            return {"data": self._page1}

        if path.endswith("/insert"):
            self._insert_calls += 1
            if self.fail_on_insert_call == self._insert_calls:
                raise RuntimeError("simulated insert failure")
            assert json is not None
            events = json.get("events", [])
            self.inserts.append(events)
            return {"row_ids": [e.get("id", "") for e in events]}

        raise AssertionError(f"Unexpected path: {path}")


@pytest.mark.asyncio
async def test_logs_migrator_resume_after_insert_failure(tmp_path: Path) -> None:
    page1 = [
        {
            "id": "a",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        }
    ]
    page2 = [
        {
            "id": "b",
            "_pagination_key": "p2",
            "_xact_id": "9",
            "created": "2023-01-01T00:00:01Z",
        }
    ]

    source = _StubClient(page1=page1, page2=page2)
    dest = _StubClient(page1=page1, page2=page2)
    dest.fail_on_insert_call = 2  # fail on second insert (page2) during first run

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=1,
        insert_batch_size=10,
        use_version_snapshot=True,
        use_seen_db=True,
    )
    migrator.set_destination_project_id("dest-project-id")

    with pytest.raises(RuntimeError):
        await migrator.migrate_all("source-project-id")

    # First run inserted only "a"
    inserted_first = [e["id"] for batch in dest.inserts for e in batch]
    assert inserted_first == ["a"]

    # Second run should resume and insert only "b" (not reinsert "a")
    dest.fail_on_insert_call = None
    await migrator.migrate_all("source-project-id")
    inserted_all = [e["id"] for batch in dest.inserts for e in batch]
    assert inserted_all == ["a", "b"]
