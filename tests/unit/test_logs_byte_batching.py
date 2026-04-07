from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.batching import approx_json_bytes
from braintrust_migrate.config import MigrationConfig
import braintrust_migrate.resources.logs as logs_module
from braintrust_migrate.resources.logs import LogsMigrator


class _SourceFetchClient:
    def __init__(self, events: list[dict[str, Any]], mig_cfg: MigrationConfig) -> None:
        self._events = events
        self._calls = 0
        self.migration_config = mig_cfg

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
        self, method: str, path: str, *, json: Any = None, **kwargs: Any
    ) -> Any:
        _ = kwargs
        assert method.upper() == "POST"
        assert path == "/btql"
        self._calls += 1
        if self._calls == 1:
            return {"data": self._events}
        return {"data": []}


class _DestInsertClient:
    def __init__(self, mig_cfg: MigrationConfig) -> None:
        self.migration_config = mig_cfg
        self.insert_calls: list[list[str]] = []

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
        self, method: str, path: str, *, json: Any = None, **kwargs: Any
    ) -> Any:
        _ = method, path, json, kwargs
        raise AssertionError("SDK-backed logs migration should not use raw_request")


class _FakeSDKProjectLogsWriter:
    def __init__(self, dest_client: _DestInsertClient, project_id: str) -> None:
        self._dest_client = dest_client
        self._project_id = project_id

    async def write_rows(self, rows: list[dict[str, Any]]) -> None:
        ids: list[str] = []
        for e in rows:
            if not isinstance(e, dict):
                continue
            v = e.get("id")
            if isinstance(v, str):
                ids.append(v)
        self._dest_client.insert_calls.append(ids)


@pytest.mark.asyncio
async def test_logs_migrator_buffers_rows_before_sdk_flush(tmp_path: Path) -> None:
    EXPECTED_MIGRATED = 2
    # Make events large enough that 2 events exceed the byte cap, but 1 fits.
    raw_events = [
        {
            "id": "a",
            "_pagination_key": "p1",
            "input": "x" * 800,
            "created": "2020-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "_pagination_key": "p2",
            "input": "y" * 800,
            "created": "2020-01-01T00:00:01Z",
        },
    ]

    one = approx_json_bytes({"events": [raw_events[0]]})
    two = approx_json_bytes({"events": raw_events})
    assert one < two

    # Target cap just above one-event payload. The logs migrator should still buffer
    # both rows together and let the SDK handle any downstream request splitting.
    max_req = one + 25

    cfg = MigrationConfig(
        insert_max_request_bytes=max_req,
        insert_request_headroom_ratio=1.0,
    )

    source = _SourceFetchClient(raw_events, cfg)
    dest = _DestInsertClient(cfg)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(logs_module, "SDKProjectLogsWriter", _FakeSDKProjectLogsWriter)

    try:
        migrator = LogsMigrator(
            source,  # type: ignore[arg-type]
            dest,  # type: ignore[arg-type]
            tmp_path,
            page_limit=10,
            insert_batch_size=10_000,  # ensure count does not cause splitting
            use_version_snapshot=False,
            use_seen_db=False,
            progress_hook=None,
        )
        migrator.set_destination_project_id("proj-dest")
        res = await migrator.migrate_all("proj-source")
    finally:
        monkeypatch.undo()

    assert res["migrated"] == EXPECTED_MIGRATED
    assert migrator._stream_state.inserted_bytes > 0
    assert len(dest.insert_calls) == 1
    flattened = [x for call in dest.insert_calls for x in call]
    assert flattened == ["a", "b"]
