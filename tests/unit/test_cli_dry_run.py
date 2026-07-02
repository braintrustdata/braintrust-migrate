"""Tests for dry-run behavior in CLI."""

from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import HttpUrl

from braintrust_migrate.cli import (
    _discover_projects_read_only,
    _load_project_name_mapping_override,
    _run_dry_run,
    _test_logs_dry_run_probe,
)
from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig


def _make_config(tmp_path: Path) -> Config:
    return Config(
        source=BraintrustOrgConfig(
            api_key="source-key",
            url=HttpUrl("https://source.braintrust.dev"),
        ),
        destination=BraintrustOrgConfig(
            api_key="dest-key",
            url=HttpUrl("https://dest.braintrust.dev"),
        ),
        migration=MigrationConfig(),
        state_dir=tmp_path,
        resources=["all"],
    )


@pytest.mark.asyncio
async def test_discover_projects_read_only_matches_by_name_without_creating() -> None:
    """Read-only discovery should map existing destination projects by name only."""
    source = Mock()
    dest = Mock()
    source.list_projects = AsyncMock(
        return_value=[
            {"id": "src-1", "name": "Project A"},
            {"id": "src-2", "name": "Project B"},
        ]
    )
    dest.list_projects = AsyncMock(return_value=[{"id": "dest-1", "name": "Project A"}])
    dest.create_project = AsyncMock()

    async def with_retry(_op_name, coro_func):
        result = coro_func()
        if hasattr(result, "__await__"):
            return await result
        return result

    source.with_retry = with_retry
    dest.with_retry = with_retry

    projects = await _discover_projects_read_only(source, dest)

    assert projects == [
        {
            "source_id": "src-1",
            "dest_id": "dest-1",
            "name": "Project A",
            "dest_name": "Project A",
            "dest_status": "exists",
        },
        {
            "source_id": "src-2",
            "dest_id": "",
            "name": "Project B",
            "dest_name": "Project B",
            "dest_status": "would create",
        },
    ]
    dest.create_project.assert_not_called()


@pytest.mark.asyncio
async def test_discover_projects_read_only_uses_project_name_mapping() -> None:
    """Read-only discovery should resolve mapped destination project names."""
    source = Mock()
    dest = Mock()
    source.list_projects = AsyncMock(
        return_value=[{"id": "src-1", "name": "Project A"}]
    )
    dest.list_projects = AsyncMock(return_value=[{"id": "dest-1", "name": "Project Z"}])
    dest.create_project = AsyncMock()

    async def with_retry(_op_name, coro_func):
        result = coro_func()
        if hasattr(result, "__await__"):
            return await result
        return result

    source.with_retry = with_retry
    dest.with_retry = with_retry

    projects = await _discover_projects_read_only(
        source,
        dest,
        project_name_mapping={"Project A": "Project Z"},
    )

    assert projects == [
        {
            "source_id": "src-1",
            "dest_id": "dest-1",
            "name": "Project A",
            "dest_name": "Project Z",
            "dest_status": "exists",
        }
    ]
    dest.create_project.assert_not_called()


@pytest.mark.asyncio
async def test_discover_projects_read_only_reports_mapped_destination_would_create() -> None:
    """Dry-run project discovery should report when a mapped destination is absent."""
    source = Mock()
    dest = Mock()
    source.list_projects = AsyncMock(
        return_value=[{"id": "src-1", "name": "Project A"}]
    )
    dest.list_projects = AsyncMock(return_value=[])
    dest.create_project = AsyncMock()

    async def with_retry(_op_name, coro_func):
        result = coro_func()
        if hasattr(result, "__await__"):
            return await result
        return result

    source.with_retry = with_retry
    dest.with_retry = with_retry

    projects = await _discover_projects_read_only(
        source,
        dest,
        project_name_mapping={"Project A": "Project Z"},
    )

    assert projects == [
        {
            "source_id": "src-1",
            "dest_id": "",
            "name": "Project A",
            "dest_name": "Project Z",
            "dest_status": "would create",
        }
    ]
    dest.create_project.assert_not_called()


def test_load_project_name_mapping_override_rejects_inline_and_file(
    tmp_path: Path,
) -> None:
    """CLI project-map override accepts either inline JSON or a file, not both."""
    project_map_file = tmp_path / "project-map.json"
    project_map_file.write_text('{"Project A":"Project Z"}')

    with pytest.raises(ValueError, match="Set only one"):
        _load_project_name_mapping_override(
            '{"Project A":"Project Z"}',
            project_map_file,
        )


@pytest.mark.asyncio
async def test_logs_dry_run_probe_uses_source_project_and_created_filters(
    tmp_path: Path,
) -> None:
    """Logs dry-run probe should be read-only and use source id plus time filters."""
    config = _make_config(tmp_path)
    config.resources = ["logs"]
    config.migration.created_after = "2026-01-01T00:00:00Z"
    config.migration.created_before = "2026-01-02T00:00:00Z"

    source = Mock()
    queries: list[str] = []

    async def raw_request(method, path, *, json=None, **_kwargs):
        assert method == "POST"
        assert path == "/btql"
        assert json is not None
        query = json["query"]
        queries.append(query)
        return {"data": [{"id": "span-1", "_pagination_key": "p1"}]}

    async def with_retry(_op_name, coro_func, **_kwargs):
        result = coro_func()
        if hasattr(result, "__await__"):
            return await result
        return result

    source.raw_request = raw_request
    source.with_retry = with_retry

    results = await _test_logs_dry_run_probe(
        source,
        [{"source_id": "src-project-id", "name": "Project A"}],
        config,
        Mock(),
        1,
    )

    assert results == {
        "Project A": {"status": "success", "matching_spans_visible": True}
    }
    assert len(queries) == 1
    assert "project_logs('src-project-id') spans" in queries[0]
    assert "created >= '2026-01-01T00:00:00Z'" in queries[0]
    assert "created < '2026-01-02T00:00:00Z'" in queries[0]
    assert "limit: 1" in queries[0]


@pytest.mark.asyncio
async def test_run_dry_run_does_not_create_destination_projects(tmp_path: Path) -> None:
    """Dry-run should not create missing destination projects."""
    config = _make_config(tmp_path)

    source_mock = Mock()
    dest_mock = Mock()

    source_mock.list_projects = AsyncMock(
        return_value=[{"id": "src-1", "name": "Project A"}]
    )
    dest_mock.list_projects = AsyncMock(return_value=[])
    dest_mock.create_project = AsyncMock()

    async def with_retry(_op_name, coro_func):
        result = coro_func()
        if hasattr(result, "__await__"):
            return await result
        return result

    source_mock.with_retry = with_retry
    dest_mock.with_retry = with_retry

    @asynccontextmanager
    async def mock_create_client_pair(_source_cfg, _dest_cfg, _migration_cfg):
        yield source_mock, dest_mock

    with (
        patch("braintrust_migrate.client.create_client_pair", mock_create_client_pair),
        patch("braintrust_migrate.cli._test_connectivity", new_callable=AsyncMock),
        patch(
            "braintrust_migrate.cli._test_resource_discovery",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            "braintrust_migrate.cli._test_logs_dry_run_probe",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch("braintrust_migrate.cli._display_dry_run_results"),
    ):
        await _run_dry_run(config)

    dest_mock.create_project.assert_not_called()
