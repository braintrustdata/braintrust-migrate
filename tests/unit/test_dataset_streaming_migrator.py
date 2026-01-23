from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.resources.datasets import DatasetMigrator


class _StubClient:
    def __init__(self, *, btql_pages: list[list[dict[str, Any]]] | None = None) -> None:
        self._btql_pages = btql_pages or []
        self.inserts: list[dict[str, Any]] = []

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
            if self._btql_pages:
                return {"data": self._btql_pages.pop(0)}
            return {"data": []}

        if path.endswith("/insert"):
            assert json is not None
            self.inserts.append(json)
            events = json.get("events", [])
            return {"row_ids": [e.get("id", "") for e in events]}

        raise AssertionError(f"Unexpected path: {path}")


@pytest.mark.asyncio
async def test_dataset_streaming_skips_deleted_and_duplicates(tmp_path: Path) -> None:
    # Page 1 includes one deleted event "d"
    page1 = [
        {
            "id": "a",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        },
        {"id": "d", "_pagination_key": "p1.5", "_xact_id": "9", "_object_delete": True},
    ]

    # Page 2 has duplicate older version of "a" (should be skipped)
    page2 = [
        {
            "id": "a",
            "_pagination_key": "p2",
            "_xact_id": "8",
            "created": "2022-12-31T23:59:59Z",
        }
    ]

    source = _StubClient(btql_pages=[page1, page2])
    dest = _StubClient()

    migrator = DatasetMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        events_fetch_limit=1,
        events_insert_batch_size=2,
        events_use_version_snapshot=False,
        events_use_seen_db=True,
    )

    await migrator._migrate_dataset_records("source-dataset-id", "dest-dataset-id")  # type: ignore[attr-defined]

    inserted_ids: list[str] = []
    for call in dest.inserts:
        inserted_ids.extend([e["id"] for e in call["events"]])
    assert inserted_ids == ["a"]

    # Checkpoint exists
    assert (tmp_path / "dataset_events" / "source-dataset-id_state.json").exists()
