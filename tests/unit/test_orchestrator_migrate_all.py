from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from braintrust_migrate.config import Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


def _make_config(tmp_path: Path, *, max_concurrent: int = 2) -> Config:
    return Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        migration=MigrationConfig(max_concurrent=max_concurrent),
        state_dir=tmp_path,
        resources=["all"],
    )


@pytest.mark.asyncio
async def test_migrate_all_runs_projects_concurrently_and_emits_hooks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orchestrator = MigrationOrchestrator(_make_config(tmp_path, max_concurrent=2))
    checkpoint_dir = tmp_path / "run"
    report_path = checkpoint_dir / "migration_report.json"
    projects = [
        {"source_id": "src-1", "dest_id": "dst-1", "name": "Project 1"},
        {"source_id": "src-2", "dest_id": "dst-2", "name": "Project 2"},
        {"source_id": "src-3", "dest_id": "dst-3", "name": "Project 3"},
    ]

    source_client = Mock()
    dest_client = Mock()

    @asynccontextmanager
    async def mock_create_client_pair(_source_cfg, _dest_cfg, _migration_cfg):
        yield source_client, dest_client

    monkeypatch.setattr(
        "braintrust_migrate.orchestration.create_client_pair", mock_create_client_pair
    )
    monkeypatch.setattr(orchestrator, "_discover_projects", AsyncMock(return_value=projects))
    monkeypatch.setattr(
        orchestrator,
        "_migrate_organization_resources",
        AsyncMock(
            return_value={
                "resources": {},
                "total_resources": 0,
                "migrated_resources": 0,
                "skipped_resources": 0,
                "failed_resources": 0,
                "errors": [],
            }
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "_migrate_post_project_global_resources",
        AsyncMock(
            return_value={
                "resources": {},
                "total_resources": 0,
                "migrated_resources": 0,
                "skipped_resources": 0,
                "failed_resources": 0,
                "errors": [],
            }
        ),
    )
    monkeypatch.setattr(orchestrator, "_generate_migration_report", lambda *_: report_path)

    lock = asyncio.Lock()
    in_flight = 0
    max_seen = 0
    started: list[tuple[str, int, int]] = []
    completed: list[tuple[str, int, int]] = []
    discovered: list[str] = []

    async def fake_migrate_project(
        project: dict[str, str],
        _source_client: Mock,
        _dest_client: Mock,
        _checkpoint_dir: Path,
        _global_id_mappings: dict[str, str],
        progress_factory=None,
        resource_callback=None,
    ) -> dict[str, object]:
        nonlocal in_flight, max_seen
        assert callable(progress_factory)
        assert callable(resource_callback)
        async with lock:
            in_flight += 1
            max_seen = max(max_seen, in_flight)
        await asyncio.sleep(0.02)
        resource_callback(
            "datasets",
            {"total": 1, "migrated": 1, "skipped": 0, "failed": 0, "errors": []},
        )
        async with lock:
            in_flight -= 1
        return {
            "project_id": project["dest_id"],
            "project_name": project["name"],
            "resources": {},
            "total_resources": 1,
            "migrated_resources": 1,
            "skipped_resources": 0,
            "failed_resources": 0,
            "errors": [],
        }

    monkeypatch.setattr(orchestrator, "_migrate_project", fake_migrate_project)

    results = await orchestrator.migrate_all(
        checkpoint_dir=checkpoint_dir,
        on_projects_discovered=lambda ps: discovered.extend([p["name"] for p in ps]),
        on_project_start=lambda project, index, total: started.append(
            (project["name"], index, total)
        ),
        on_project_complete=lambda project, _result, done, total: completed.append(
            (project["name"], done, total)
        ),
        progress_factory_factory=lambda _project: lambda _resource_name: lambda _update: None,
        resource_callback_factory=lambda _project: lambda _resource_name, _results: None,
    )

    assert discovered == [p["name"] for p in projects]
    assert len(started) == 3
    assert len(completed) == 3
    assert max_seen == 2
    assert results["summary"]["total_projects"] == 3
    assert results["summary"]["migrated_resources"] == 3
    assert results["report_path"] == str(report_path)
