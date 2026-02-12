"""Unit tests for concurrent migrate_batch (max_concurrent > 1)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from braintrust_migrate.resources.base import (
    MigrationResult,
    MigrationState,
    ResourceMigrator,
)


# ---------------------------------------------------------------------------
# Concrete test migrator
# ---------------------------------------------------------------------------


class _TestMigrator(ResourceMigrator[dict]):
    """Minimal concrete migrator for testing."""

    def __init__(self, checkpoint_dir: Path, *, delay: float = 0.01) -> None:
        source = Mock()
        dest = Mock()
        super().__init__(source, dest, checkpoint_dir, batch_size=100)
        self._delay = delay
        self._call_log: list[str] = []
        self._in_flight = 0
        self._max_in_flight = 0
        self._lock = asyncio.Lock()
        self._fail_ids: set[str] = set()

    @property
    def resource_name(self) -> str:
        return "TestResources"

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        return []

    async def migrate_resource(self, resource: dict) -> str:
        rid = resource["id"]
        async with self._lock:
            self._in_flight += 1
            self._max_in_flight = max(self._max_in_flight, self._in_flight)
            self._call_log.append(f"start:{rid}")

        await asyncio.sleep(self._delay)

        if rid in self._fail_ids:
            async with self._lock:
                self._in_flight -= 1
            raise RuntimeError(f"fail:{rid}")

        async with self._lock:
            self._in_flight -= 1
            self._call_log.append(f"end:{rid}")

        return f"dest-{rid}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestConcurrentMigrateBatch:

    async def test_sequential_when_max_concurrent_1(self, tmp_path: Path):
        """With max_concurrent=1, resources migrate one at a time."""
        m = _TestMigrator(tmp_path)
        resources = [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]

        results = await m.migrate_batch(resources, max_concurrent=1)

        assert len(results) == 2
        assert all(r.success for r in results)
        assert m._max_in_flight == 1

    async def test_concurrent_when_max_concurrent_gt_1(self, tmp_path: Path):
        """With max_concurrent > 1, resources should overlap."""
        m = _TestMigrator(tmp_path, delay=0.02)
        resources = [{"id": str(i), "name": f"R{i}"} for i in range(5)]

        results = await m.migrate_batch(resources, max_concurrent=5)

        assert len(results) == 5
        assert all(r.success for r in results)
        # With 5 concurrent and delay, should see >1 in-flight
        assert m._max_in_flight > 1

    async def test_concurrency_bounded_by_semaphore(self, tmp_path: Path):
        """Max in-flight should not exceed max_concurrent."""
        m = _TestMigrator(tmp_path, delay=0.02)
        resources = [{"id": str(i), "name": f"R{i}"} for i in range(10)]

        results = await m.migrate_batch(resources, max_concurrent=3)

        assert len(results) == 10
        assert all(r.success for r in results)
        assert m._max_in_flight <= 3

    async def test_state_updated_correctly_under_concurrency(self, tmp_path: Path):
        """id_mapping should contain all successful migrations."""
        m = _TestMigrator(tmp_path, delay=0.01)
        resources = [{"id": str(i), "name": f"R{i}"} for i in range(5)]

        await m.migrate_batch(resources, max_concurrent=5)

        for i in range(5):
            assert str(i) in m.state.id_mapping
            assert m.state.id_mapping[str(i)] == f"dest-{i}"
            assert str(i) in m.state.completed_ids

    async def test_failures_dont_block_others(self, tmp_path: Path):
        """A failing resource should not prevent others from completing."""
        m = _TestMigrator(tmp_path, delay=0.01)
        m._fail_ids = {"2"}
        resources = [{"id": str(i), "name": f"R{i}"} for i in range(5)]

        results = await m.migrate_batch(resources, max_concurrent=5)

        assert len(results) == 5
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 4
        assert len(failures) == 1
        assert failures[0].source_id == "2"
        assert "2" in m.state.failed_ids

    async def test_already_migrated_skipped(self, tmp_path: Path):
        """Resources already in id_mapping should be skipped."""
        m = _TestMigrator(tmp_path)
        m.state.id_mapping["existing"] = "dest-existing"
        resources = [
            {"id": "existing", "name": "Already"},
            {"id": "new", "name": "New"},
        ]

        results = await m.migrate_batch(resources, max_concurrent=3)

        assert len(results) == 2
        skipped = [r for r in results if r.skipped]
        created = [r for r in results if r.success and not r.skipped]
        assert len(skipped) == 1
        assert skipped[0].source_id == "existing"
        assert len(created) == 1
        assert created[0].source_id == "new"

    async def test_migrate_all_passes_max_concurrent(self, tmp_path: Path):
        """migrate_all should pass max_concurrent through to migrate_batch."""
        m = _TestMigrator(tmp_path, delay=0.02)

        # Inject resources via list_source_resources
        test_resources = [{"id": str(i), "name": f"R{i}"} for i in range(6)]

        async def mock_list(project_id=None):
            return test_resources

        m.list_source_resources = mock_list  # type: ignore[assignment]

        results = await m.migrate_all(project_id="proj", max_concurrent=3)

        assert results["total"] == 6
        assert results["migrated"] == 6
        assert m._max_in_flight <= 3
        assert m._max_in_flight > 1  # Should have had some concurrency
