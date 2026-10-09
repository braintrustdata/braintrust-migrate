"""Tests for the unmigrated-environments warning."""

from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from braintrust_migrate.config import Config, MigrationConfig
from braintrust_migrate.environments import check_unmigrated_environments
from braintrust_migrate.orchestration import MigrationOrchestrator


async def _with_retry(_op_name, coro_func):
    return await coro_func()


def _source_client(raw_request: AsyncMock) -> Mock:
    client = Mock()
    client.with_retry = _with_retry
    client.raw_request = raw_request
    return client


@pytest.mark.asyncio
async def test_warns_with_active_environment_slugs() -> None:
    raw_request = AsyncMock(
        return_value={
            "objects": [
                {"id": "1", "slug": "staging", "name": "Staging"},
                {"id": "2", "slug": "production", "name": "Production"},
                {"id": "3", "slug": "old", "name": "Old", "deleted_at": "2026-01-01"},
            ]
        }
    )

    warning = await check_unmigrated_environments(_source_client(raw_request))

    raw_request.assert_awaited_once_with("GET", "/environment")
    assert warning is not None
    assert warning["type"] == "unmigrated_environments"
    assert warning["environments"] == ["production", "staging"]
    assert "production, staging" in warning["message"]


@pytest.mark.asyncio
async def test_no_warning_without_environments() -> None:
    raw_request = AsyncMock(return_value={"objects": []})

    assert await check_unmigrated_environments(_source_client(raw_request)) is None


@pytest.mark.asyncio
async def test_failed_check_does_not_raise() -> None:
    raw_request = AsyncMock(side_effect=RuntimeError("404 Not Found"))

    assert await check_unmigrated_environments(_source_client(raw_request)) is None


@pytest.mark.asyncio
async def test_unexpected_response_does_not_raise() -> None:
    raw_request = AsyncMock(return_value=["not", "a", "dict"])

    assert await check_unmigrated_environments(_source_client(raw_request)) is None


def _make_config(tmp_path: Path, resources: list[str]) -> Config:
    return Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        migration=MigrationConfig(),
        state_dir=tmp_path,
        resources=resources,
    )


async def _run_migrate_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resources: list[str]
) -> tuple[dict, AsyncMock]:
    orchestrator = MigrationOrchestrator(_make_config(tmp_path, resources))
    warning = {"type": "unmigrated_environments", "message": "envs not migrated"}
    check = AsyncMock(return_value=warning)

    @asynccontextmanager
    async def mock_create_client_pair(_source_cfg, _dest_cfg, _migration_cfg):
        yield Mock(), Mock()

    empty = {"resources": {}, "errors": []}
    monkeypatch.setattr(
        "braintrust_migrate.orchestration.create_client_pair", mock_create_client_pair
    )
    monkeypatch.setattr(
        "braintrust_migrate.orchestration.check_unmigrated_environments", check
    )
    monkeypatch.setattr(orchestrator, "_discover_projects", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        orchestrator, "_migrate_organization_resources", AsyncMock(return_value=empty)
    )
    monkeypatch.setattr(
        orchestrator,
        "_migrate_post_project_global_resources",
        AsyncMock(return_value=empty),
    )

    checkpoint_dir = tmp_path / "run"
    checkpoint_dir.mkdir()
    results = await orchestrator.migrate_all(checkpoint_dir=checkpoint_dir)
    return results, check


@pytest.mark.asyncio
async def test_migrate_all_surfaces_warning_in_results_and_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, check = await _run_migrate_all(tmp_path, monkeypatch, ["all"])

    check.assert_awaited_once()
    assert results["warnings"] == [
        {"type": "unmigrated_environments", "message": "envs not migrated"}
    ]
    # Advisory only: a warning must not mark the migration as failed.
    assert results["success"] is True
    summary_text = (tmp_path / "run" / "migration_summary.txt").read_text()
    assert "envs not migrated" in summary_text


@pytest.mark.asyncio
async def test_migrate_all_skips_check_when_prompts_and_functions_out_of_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results, check = await _run_migrate_all(tmp_path, monkeypatch, ["datasets"])

    check.assert_not_awaited()
    assert results["warnings"] == []
