from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.resources.logs import LogsMigrator


class _StubClient:
    def __init__(self, *, btql_pages: list[list[dict[str, Any]]] | None = None) -> None:
        self._btql_pages = btql_pages or []
        self.inserts: list[dict[str, Any]] = []

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
        assert method in {"POST", "post"}
        _ = params
        _ = timeout

        if path == "/btql":
            assert json is not None
            assert isinstance(json.get("query"), str)
            if not self._btql_pages:
                return {"data": []}
            return {"data": self._btql_pages.pop(0)}

        if path.endswith("/insert"):
            assert json is not None
            self.inserts.append(json)
            events = json.get("events", [])
            # mimic API response shape
            return {"row_ids": [e.get("id", "") for e in events]}

        raise AssertionError(f"Unexpected path: {path}")


@pytest.mark.asyncio
async def test_logs_migrator_skips_duplicate_ids_across_pages(tmp_path: Path) -> None:
    expected_inserted = 2

    # Main page 1
    page1 = [
        {
            "id": "a",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "_pagination_key": "p2",
            "_xact_id": "9",
            "created": "2023-01-01T00:00:01Z",
        },
    ]

    # Main page 2 contains an older version of "a" (should be skipped)
    page2 = [
        {
            "id": "a",
            "_pagination_key": "p3",
            "_xact_id": "8",
            "created": "2022-12-31T23:59:59Z",
        }
    ]

    source = _StubClient(btql_pages=[page1, page2])
    dest = _StubClient()

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=1,
        insert_batch_size=2,
        use_version_snapshot=False,
        use_seen_db=True,
    )
    migrator.set_destination_project_id("dest-project-id")

    result = await migrator.migrate_all("source-project-id")
    assert result["streaming"] is True
    assert result["version"] is None
    assert result["migrated"] == expected_inserted  # a + b inserted once
    assert result["skipped"] == 1  # older duplicate 'a' skipped on page 2

    # Ensure destination insert saw only two ids total
    inserted_ids: list[str] = []
    for call in dest.inserts:
        inserted_ids.extend([e["id"] for e in call["events"]])
    assert inserted_ids == ["a", "b"]

    # Ensure checkpoint exists
    assert (tmp_path / "logs_streaming_state.json").exists()
