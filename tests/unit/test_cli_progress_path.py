from __future__ import annotations

from pathlib import Path

import pytest
from rich.progress import Progress

from braintrust_migrate.cli import _run_migration_with_progress
from braintrust_migrate.config import Config, MigrationConfig


class _StubOrchestrator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.migrate_all_calls: list[dict] = []

    async def migrate_all(self, **kwargs):
        self.migrate_all_calls.append(kwargs)

        projects = [
            {"source_id": "src-1", "dest_id": "dst-1", "name": "Project A"},
            {"source_id": "src-2", "dest_id": "dst-2", "name": "Project B"},
        ]
        kwargs["on_projects_discovered"](projects)
        kwargs["on_organization_start"]()
        kwargs["on_organization_complete"]({})
        kwargs["on_project_start"](projects[0], 1, 2)
        progress_factory = kwargs["progress_factory_factory"](projects[0])
        resource_callback = kwargs["resource_callback_factory"](projects[0])
        progress_hook = progress_factory("logs")
        progress_hook(
            {
                "phase": "done",
                "resource": "logs",
                "fetched_total": 10,
                "inserted_total": 8,
                "inserted_bytes_total": 2_000_000_000,
            }
        )
        resource_callback(
            "logs",
            {"total": 1, "migrated": 1, "skipped": 0, "failed": 0, "errors": []},
        )
        kwargs["on_project_complete"](
            projects[0],
            {
                "project_id": "dst-1",
                "project_name": "Project A",
                "resources": {},
                "total_resources": 1,
                "migrated_resources": 1,
                "skipped_resources": 0,
                "failed_resources": 0,
                "errors": [],
            },
            1,
            2,
        )
        return {
            "summary": {
                "total_projects": 2,
                "total_resources": 1,
                "migrated_resources": 1,
                "skipped_resources": 0,
                "failed_resources": 0,
                "errors": [],
            },
            "projects": {
                "Project A": {
                    "project_id": "dst-1",
                    "project_name": "Project A",
                    "resources": {},
                    "total_resources": 1,
                    "migrated_resources": 1,
                    "skipped_resources": 0,
                    "failed_resources": 0,
                    "errors": [],
                }
            },
            "duration_seconds": 2.0,
            "success": True,
        }


@pytest.mark.asyncio
async def test_run_migration_with_progress_delegates_to_orchestrator(
    tmp_path: Path,
) -> None:
    config = Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        migration=MigrationConfig(),
        state_dir=tmp_path,
        resources=["all"],
    )
    orchestrator = _StubOrchestrator(config)

    with Progress() as progress:
        migration_task = progress.add_task("migrate", total=1)
        results = await _run_migration_with_progress(
            orchestrator,
            progress,
            migration_task,
            checkpoint_dir=tmp_path / "run",
        )

    assert len(orchestrator.migrate_all_calls) == 1
    call = orchestrator.migrate_all_calls[0]
    assert call["checkpoint_dir"] == tmp_path / "run"
    assert callable(call["on_projects_discovered"])
    assert callable(call["on_project_start"])
    assert callable(call["on_project_complete"])
    assert callable(call["progress_factory_factory"])
    assert callable(call["resource_callback_factory"])
    assert results["throughput"]["inserted_rows_total"] == 8
    assert results["throughput"]["inserted_gb_total"] == 2.0
    assert results["throughput"]["rows_per_sec"] == 4.0
