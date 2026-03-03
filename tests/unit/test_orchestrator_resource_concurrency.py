from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from braintrust_migrate.config import Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


class _DummyMigrator:
    observed_max_concurrent: int | None = None

    def __init__(
        self,
        source_client: Any,
        dest_client: Any,
        checkpoint_dir: Path,
        batch_size: int,
    ) -> None:
        self.source_client = source_client
        self.dest_client = dest_client
        self.checkpoint_dir = checkpoint_dir
        self.batch_size = batch_size
        self.state = type("State", (), {"id_mapping": {}})()

    def set_destination_project_id(self, _dest_project_id: str | None) -> None:
        return

    def update_id_mappings(self, _mappings: dict[str, str]) -> None:
        return

    async def populate_dependency_mappings(
        self,
        _source_client: Any,
        _dest_client: Any,
        _project_id: str | None,
        _shared_dependency_cache: dict[str, Any],
    ) -> None:
        return

    async def migrate_all(
        self, project_id: str | None = None, max_concurrent: int | None = None
    ) -> dict[str, Any]:
        _ = project_id
        _DummyMigrator.observed_max_concurrent = max_concurrent
        return {
            "resource_type": "dummy",
            "total": 0,
            "migrated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }


@pytest.mark.asyncio
async def test_org_resource_migration_passes_max_concurrent_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_max = 7
    source = Mock()
    dest = Mock()
    config = Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        migration=MigrationConfig(max_concurrent_resources=expected_max),
        state_dir=tmp_path,
        resources=["dummy"],
    )
    orchestrator = MigrationOrchestrator(config)

    monkeypatch.setattr(
        MigrationOrchestrator,
        "ORGANIZATION_SCOPED_RESOURCES",
        [("dummy", _DummyMigrator)],
    )

    _DummyMigrator.observed_max_concurrent = None
    await orchestrator._migrate_organization_resources(  # pyright: ignore[reportPrivateUsage]
        source, dest, tmp_path, {}
    )

    assert _DummyMigrator.observed_max_concurrent == expected_max
