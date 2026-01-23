from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.config import MigrationConfig
from braintrust_migrate.resources.logs import LogsMigrator


class _SourceBtqlClient:
    def __init__(
        self, pages: list[list[dict[str, Any]]], mig_cfg: MigrationConfig
    ) -> None:
        self._pages = pages
        self.migration_config = mig_cfg
        self._calls = 0

    async def with_retry(self, _operation_name: str, coro_func):
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
        # We don't fully parse SQL/BTQL, but we do validate that the migrator is using
        # sorted + offset-based pagination (filter on the last _pagination_key), not cursor.
        q = (json or {}).get("query")
        assert isinstance(q, str)
        assert "SELECT *" in q
        assert "FROM project_logs('proj-source', shape => 'spans')" in q
        assert "ORDER BY _pagination_key" in q
        assert "LIMIT" in q
        assert "cursor:" not in q  # cursor pagination doesn't work with sort
        self._calls += 1
        PAGE_ONE = 1
        PAGE_TWO = 2
        # Page 1 should have no WHERE filter (start of stream).
        if self._calls == PAGE_ONE:
            assert "WHERE _pagination_key >" not in q
        # Page 2 should filter on the last row of page 1 (pk=2).
        if self._calls == PAGE_TWO:
            assert "WHERE _pagination_key > '2'" in q

        if self._pages:
            return {"data": self._pages.pop(0)}
        return {"data": []}


class _SourceBtqlClient500ThenOK:
    def __init__(
        self, pages: list[list[dict[str, Any]]], mig_cfg: MigrationConfig
    ) -> None:
        self._pages = pages
        self.migration_config = mig_cfg
        self.queries: list[str] = []

    async def with_retry(self, _operation_name: str, coro_func):
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
        q = (json or {}).get("query")
        assert isinstance(q, str)
        self.queries.append(q)

        # Simulate the backend error you saw: LIMIT 1000 fails, smaller LIMIT succeeds.
        if "LIMIT 1000" in q:
            import httpx

            req = httpx.Request("POST", "https://api.braintrust.dev/btql")
            resp = httpx.Response(
                500,
                request=req,
                text='{"Code":"InternalServerError","Path":"/btql","Service":"api"}',
            )
            raise httpx.HTTPStatusError(
                "InternalServerError", request=req, response=resp
            )

        if self._pages:
            return {"data": self._pages.pop(0)}
        return {"data": []}


class _DestInsertClient:
    def __init__(self, mig_cfg: MigrationConfig) -> None:
        self.migration_config = mig_cfg
        self.inserted_ids: list[str] = []

        class _Views:
            async def create(self, **kwargs: Any) -> Any:
                _ = kwargs
                return {"id": "v1"}

        class _ClientObj:
            def __init__(self) -> None:
                self.views = _Views()

        self.client = _ClientObj()

    async def with_retry(self, _operation_name: str, coro_func):
        res = coro_func()
        if hasattr(res, "__await__"):
            return await res
        return res

    async def raw_request(
        self, method: str, path: str, *, json: Any = None, **kwargs: Any
    ) -> Any:
        _ = kwargs
        assert method.upper() == "POST"
        assert "/v1/project_logs/" in path
        assert path.endswith("/insert")
        events = (json or {}).get("events", [])
        for e in events:
            if isinstance(e, dict) and isinstance(e.get("id"), str):
                self.inserted_ids.append(e["id"])
        return {"row_ids": self.inserted_ids}


class _SourceBtqlClientCreatedAfter:
    def __init__(
        self, pages: list[list[dict[str, Any]]], mig_cfg: MigrationConfig
    ) -> None:
        self._pages = pages
        self.migration_config = mig_cfg
        self.queries: list[str] = []
        self._fetch_calls = 0
        self._preflight_calls = 0

    async def with_retry(self, _operation_name: str, coro_func):
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
        q = (json or {}).get("query")
        assert isinstance(q, str)
        self.queries.append(q)

        # Preflight query selects only _pagination_key.
        if "SELECT _pagination_key" in q:
            self._preflight_calls += 1
            assert "FROM project_logs('proj-source', shape => 'spans')" in q
            assert "WHERE created >= '2020-01-02T00:00:00Z'" in q
            assert "ORDER BY _pagination_key ASC" in q
            assert "LIMIT 1" in q
            # Start at pk=2 (row "b")
            return {"data": [{"_pagination_key": "2"}]}

        # Fetch queries select full rows.
        assert "SELECT *" in q
        assert "FROM project_logs('proj-source', shape => 'spans')" in q
        assert "ORDER BY _pagination_key" in q
        assert "cursor:" not in q

        self._fetch_calls += 1
        if self._fetch_calls == 1:
            assert (
                "WHERE created >= '2020-01-02T00:00:00Z' AND _pagination_key >= '2'"
                in q
            )
        if self._fetch_calls == 2:
            assert (
                "WHERE created >= '2020-01-02T00:00:00Z' AND _pagination_key > '2'" in q
            )

        if self._pages:
            return {"data": self._pages.pop(0)}
        return {"data": []}


@pytest.mark.asyncio
async def test_logs_btql_sorted_fetch_inserts_in_created_order(tmp_path: Path) -> None:
    EXPECTED_MIGRATED = 3
    # Two pages of data already in created-asc order (as BTQL sort would produce).
    pages = [
        [
            {"id": "a", "created": "2020-01-01T00:00:00Z", "_pagination_key": "1"},
            {"id": "b", "created": "2020-01-02T00:00:00Z", "_pagination_key": "2"},
        ],
        [
            {"id": "c", "created": "2020-01-03T00:00:00Z", "_pagination_key": "3"},
        ],
    ]
    source_cfg = MigrationConfig()
    dest_cfg = MigrationConfig()

    source = _SourceBtqlClient(pages, source_cfg)
    dest = _DestInsertClient(dest_cfg)

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=2,
        insert_batch_size=10,
        use_version_snapshot=False,
        use_seen_db=False,
        progress_hook=None,
    )
    migrator.set_destination_project_id("proj-dest")
    res = await migrator.migrate_all("proj-source")

    assert res["migrated"] == EXPECTED_MIGRATED
    assert dest.inserted_ids == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_logs_btql_fetch_retries_smaller_limit_on_500(tmp_path: Path) -> None:
    pages = [
        [
            {"id": "a", "created": "2020-01-01T00:00:00Z", "_pagination_key": "1"},
        ],
    ]
    source_cfg = MigrationConfig()
    dest_cfg = MigrationConfig()

    source = _SourceBtqlClient500ThenOK(pages, source_cfg)
    dest = _DestInsertClient(dest_cfg)

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=1000,  # will 500, then retry with smaller limit
        insert_batch_size=10,
        use_version_snapshot=False,
        use_seen_db=False,
        progress_hook=None,
    )
    migrator.set_destination_project_id("proj-dest")
    res = await migrator.migrate_all("proj-source")

    assert res["migrated"] == 1
    assert dest.inserted_ids == ["a"]
    # Verify we tried LIMIT 1000, then retried with a smaller limit (500 is first).
    combined = "\n".join(source.queries)
    assert "LIMIT 1000" in combined
    assert "LIMIT 500" in combined


@pytest.mark.asyncio
async def test_logs_created_after_uses_preflight_and_inclusive_start_pk(
    tmp_path: Path,
) -> None:
    # created_after=2020-01-02 (date-only) should canonicalize to midnight UTC.
    source_cfg = MigrationConfig(created_after="2020-01-02")
    dest_cfg = MigrationConfig(created_after="2020-01-02")

    pages = [
        [
            {"id": "b", "created": "2020-01-02T00:00:00Z", "_pagination_key": "2"},
        ],
        [
            {"id": "c", "created": "2020-01-03T00:00:00Z", "_pagination_key": "3"},
        ],
    ]

    source = _SourceBtqlClientCreatedAfter(pages, source_cfg)
    dest = _DestInsertClient(dest_cfg)

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=1,
        insert_batch_size=10,
        use_version_snapshot=False,
        use_seen_db=False,
        progress_hook=None,
    )
    migrator.set_destination_project_id("proj-dest")
    res = await migrator.migrate_all("proj-source")

    assert res["migrated"] == 2
    assert dest.inserted_ids == ["b", "c"]


@pytest.mark.asyncio
async def test_logs_created_after_mismatch_with_checkpoint_errors(
    tmp_path: Path,
) -> None:
    source_cfg = MigrationConfig(created_after="2020-01-02T00:00:00Z")
    dest_cfg = MigrationConfig(created_after="2020-01-02T00:00:00Z")

    # Write a checkpoint state with a different created_after.
    (tmp_path / "logs_streaming_state.json").write_text(
        '{"created_after":"2020-01-01T00:00:00Z"}'
    )

    source = _SourceBtqlClient([], source_cfg)
    dest = _DestInsertClient(dest_cfg)

    migrator = LogsMigrator(
        source,  # type: ignore[arg-type]
        dest,  # type: ignore[arg-type]
        tmp_path,
        page_limit=1,
        insert_batch_size=10,
        use_version_snapshot=False,
        use_seen_db=False,
        progress_hook=None,
    )
    migrator.set_destination_project_id("proj-dest")

    with pytest.raises(ValueError, match="created_after mismatch"):
        await migrator.migrate_all("proj-source")
