from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import braintrust_migrate.resources.experiments as experiments_module
from braintrust_migrate.resources.experiments import ExperimentMigrator


class _StubClient:
    def __init__(self, *, btql_pages: list[list[dict[str, Any]]] | None = None) -> None:
        self._btql_pages = btql_pages or []
        self.inserts: list[dict[str, Any]] = []
        self.fail_insert_once: bool = False

    async def with_retry(
        self,
        _operation_name: str,
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
        assert method.lower() == "post"

        if path == "/btql":
            assert json is not None and isinstance(json.get("query"), str)
            if self._btql_pages:
                return {"data": self._btql_pages.pop(0)}
            return {"data": []}

        raise AssertionError(f"Unexpected path: {path}")


class _FakeSDKExperimentWriter:
    def __init__(self, dest_client: _StubClient, experiment_id: str) -> None:
        self._dest_client = dest_client
        self._experiment_id = experiment_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._dest_client.inserts.append(
            {
                "experiment_id": self._experiment_id,
                "events": [dict(row) for row in rows],
            }
        )


class _PaginationAwareStubClient(_StubClient):
    def __init__(self) -> None:
        super().__init__(btql_pages=None)
        self.btql_queries: list[str] = []

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

        if path != "/btql":
            raise AssertionError(f"Unexpected path: {path}")

        assert json is not None and isinstance(json.get("query"), str)
        query = json["query"]
        self.btql_queries.append(query)

        if "_pagination_key > 'p2'" in query:
            return {"data": []}
        if "_pagination_key > 'p1'" in query:
            return {
                "data": [
                    {
                        "id": "b",
                        "experiment_id": "source-exp-id",
                        "_pagination_key": "p2",
                        "_xact_id": "2",
                        "created": "2023-01-01T00:00:01Z",
                    }
                ]
            }
        return {
            "data": [
                {
                    "id": "a",
                    "experiment_id": "source-exp-id",
                    "_pagination_key": "p1",
                    "_xact_id": "1",
                    "created": "2023-01-01T00:00:00Z",
                }
            ]
        }


@pytest.mark.asyncio
async def test_experiment_streaming_skips_deleted_and_duplicates(
    tmp_path: Path,
) -> None:
    # Page 1 includes one deleted event "d"
    page1 = [
        {
            "id": "a",
            "experiment_id": "source-exp-id",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        },
        {
            "id": "d",
            "experiment_id": "source-exp-id",
            "_pagination_key": "p1.5",
            "_xact_id": "9",
            "_object_delete": True,
        },
    ]

    # Page 2 has duplicate older version of "a" (should be skipped)
    page2 = [
        {
            "id": "a",
            "experiment_id": "source-exp-id",
            "_pagination_key": "p2",
            "_xact_id": "8",
            "created": "2022-12-31T23:59:59Z",
        }
    ]

    source = _StubClient(btql_pages=[page1, page2])
    dest = _StubClient()
    original_writer = experiments_module.SDKExperimentWriter
    experiments_module.SDKExperimentWriter = _FakeSDKExperimentWriter

    try:
        migrator = ExperimentMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=1,
            events_use_seen_db=True,
        )

        await migrator._migrate_experiment_events(  # type: ignore[attr-defined]
            "source-exp-id", "dest-exp-id"
        )

        inserted_ids: list[str] = []
        for call in dest.inserts:
            inserted_ids.extend([e["id"] for e in call["events"]])
        assert inserted_ids == ["a"]
        assert len(dest.inserts) == 1
        assert all(call["experiment_id"] == "dest-exp-id" for call in dest.inserts)
        assert (tmp_path / "experiment_events" / "source-exp-id_state.json").exists()
    finally:
        experiments_module.SDKExperimentWriter = original_writer


@pytest.mark.asyncio
async def test_experiment_streaming_advances_btql_pagination_before_flush(
    tmp_path: Path,
) -> None:
    source = _PaginationAwareStubClient()
    dest = _StubClient()
    original_writer = experiments_module.SDKExperimentWriter
    experiments_module.SDKExperimentWriter = _FakeSDKExperimentWriter

    try:
        migrator = ExperimentMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=100000,
            events_use_seen_db=False,
        )
        migrator._sdk_flush_max_rows = 100

        await migrator._migrate_experiment_events(  # type: ignore[attr-defined]
            "source-exp-id", "dest-exp-id"
        )

        inserted_ids: list[str] = []
        for call in dest.inserts:
            inserted_ids.extend([e["id"] for e in call["events"]])

        assert inserted_ids == ["a", "b"]
        assert any("_pagination_key > 'p1'" in query for query in source.btql_queries)
        assert len(source.btql_queries) == 3
    finally:
        experiments_module.SDKExperimentWriter = original_writer
