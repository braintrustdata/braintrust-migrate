"""Tests for dry-run behavior in CLI."""

from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import HttpUrl

from braintrust_migrate.cli import _discover_projects_read_only, _run_dry_run
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
        {"source_id": "src-1", "dest_id": "dest-1", "name": "Project A"},
        {"source_id": "src-2", "dest_id": "", "name": "Project B"},
    ]
    dest.create_project.assert_not_called()


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
        patch("braintrust_migrate.cli._display_dry_run_results"),
    ):
        await _run_dry_run(config)

    dest_mock.create_project.assert_not_called()
