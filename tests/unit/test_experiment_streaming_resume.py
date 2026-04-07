from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import braintrust_migrate.resources.experiments as experiments_module
from braintrust_migrate.resources.experiments import ExperimentMigrator


class _StubClient:
    def __init__(
        self,
        *,
        page1: list[dict[str, Any]] | None = None,
        page2: list[dict[str, Any]] | None = None,
    ) -> None:
        self._page1 = page1 or []
        self._page2 = page2 or []
        self.inserts: list[list[dict[str, Any]]] = []
        self.fail_on_insert_call: int | None = None
        self._insert_calls = 0

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
            q = json.get("query")
            assert isinstance(q, str)
            # Native BTQL syntax uses `filter:` instead of SQL `WHERE`.
            if "_pagination_key > 'p1'" in q:
                return {"data": self._page2}
            if "_pagination_key > 'p2'" in q:
                return {"data": []}
            return {"data": self._page1}

        raise AssertionError(f"Unexpected path: {path}")


class _FakeSDKExperimentWriter:
    def __init__(self, dest_client: _StubClient, experiment_id: str) -> None:
        self._dest_client = dest_client
        self._experiment_id = experiment_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._dest_client._insert_calls += 1
        if self._dest_client.fail_on_insert_call == self._dest_client._insert_calls:
            raise RuntimeError("simulated insert failure")
        self._dest_client.inserts.append([dict(row) for row in rows])


@pytest.mark.asyncio
async def test_experiment_streaming_resume_after_insert_failure(tmp_path: Path) -> None:
    # Page 1 (will insert "a")
    page1 = [
        {
            "id": "a",
            "experiment_id": "source-exp-id",
            "_pagination_key": "p1",
            "_xact_id": "10",
            "created": "2023-01-01T00:00:00Z",
        }
    ]
    # Page 2 (will insert "b", but first run fails before insert completes)
    page2 = [
        {
            "id": "b",
            "experiment_id": "source-exp-id",
            "_pagination_key": "p2",
            "_xact_id": "9",
            "created": "2023-01-01T00:00:01Z",
        }
    ]

    source = _StubClient(page1=page1, page2=page2)
    dest = _StubClient()
    dest.fail_on_insert_call = 2  # fail on second insert during first run
    original_writer = experiments_module.SDKExperimentWriter
    original_flush_max_rows = experiments_module.ExperimentMigrator.SDK_FLUSH_MAX_ROWS
    experiments_module.SDKExperimentWriter = _FakeSDKExperimentWriter
    experiments_module.ExperimentMigrator.SDK_FLUSH_MAX_ROWS = 1

    try:
        migrator = ExperimentMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            events_fetch_limit=1,
            events_use_seen_db=True,
        )

        with pytest.raises(RuntimeError):
            await migrator._migrate_experiment_events(  # type: ignore[attr-defined]
                "source-exp-id", "dest-exp-id"
            )

        inserted_first = [e["id"] for batch in dest.inserts for e in batch]
        assert inserted_first == ["a"]

        dest.fail_on_insert_call = None
        await migrator._migrate_experiment_events(  # type: ignore[attr-defined]
            "source-exp-id", "dest-exp-id"
        )
        inserted_all = [e["id"] for batch in dest.inserts for e in batch]
        assert inserted_all == ["a", "b"]
    finally:
        experiments_module.SDKExperimentWriter = original_writer
        experiments_module.ExperimentMigrator.SDK_FLUSH_MAX_ROWS = original_flush_max_rows
