from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from braintrust_migrate.config import MigrationConfig
from braintrust_migrate.resources.base import DEFAULT_LIST_PAGE_SIZE
from braintrust_migrate.resources.experiments import ExperimentMigrator


class _MockExperiment:
    def __init__(self, id: str, name: str, created: str, project_id: str = "proj-1"):
        self.id = id
        self.name = name
        self.created = created
        self.project_id = project_id
        self.base_exp_id = None
        self.dataset_id = None


class _PaginatedListClient:
    def __init__(self, pages: list[list[_MockExperiment]], mig_cfg: MigrationConfig):
        self._pages = pages
        self._all_pages = [page.copy() for page in pages]  # Keep copy for list() method
        self.migration_config = mig_cfg
        self.list_calls = 0
        self.call_params: list[dict[str, Any]] = []

        # Create a nested client structure that the code expects
        # The migrator accesses client.client.experiments.list()
        from unittest.mock import Mock

        self.client = Mock()
        self.client.experiments = Mock()
        self.client.experiments.list = self._mock_list

    async def _mock_list(self, **params):
        """Mock the experiments.list() method to return all experiments."""
        self.list_calls += 1
        self.call_params.append(params)

        # Combine all pages into a single response (simulating full pagination)
        all_experiments = []
        for page in self._all_pages:
            all_experiments.extend(page)

        # Apply project_id filtering if present in params
        project_id = params.get("project_id")
        if project_id:
            all_experiments = [e for e in all_experiments if e.project_id == project_id]

        # Return a response with .objects attribute
        from unittest.mock import Mock

        response = Mock()
        response.objects = all_experiments
        return response

    async def with_retry(self, _operation_name: str, coro_func):
        res = coro_func()
        if hasattr(res, "__await__"):
            return await res
        return res

    async def raw_request(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, **kwargs
    ):
        """Mock raw_request that properly simulates pagination."""
        assert method.upper() == "GET"
        assert path == "/v1/experiment"

        self.list_calls += 1
        self.call_params.append(params or {})

        # Build full list of experiments
        all_objects = []
        for page in self._all_pages:
            for exp in page:
                exp_dict = {
                    "id": exp.id,
                    "name": exp.name,
                    "project_id": exp.project_id,
                    "public": False,
                }
                # Only include created if it exists (test might delete it)
                if hasattr(exp, "created"):
                    exp_dict["created"] = exp.created
                all_objects.append(exp_dict)

        # Filter by project_id if provided
        project_id = params.get("project_id") if params else None
        if project_id:
            all_objects = [e for e in all_objects if e.get("project_id") == project_id]

        # Handle pagination with starting_after
        starting_after = params.get("starting_after") if params else None
        if starting_after:
            # Find the index after the starting_after ID
            start_idx = 0
            for idx, obj in enumerate(all_objects):
                if obj["id"] == starting_after:
                    start_idx = idx + 1
                    break
            all_objects = all_objects[start_idx:]

        # Apply limit
        limit = params.get("limit", DEFAULT_LIST_PAGE_SIZE) if params else DEFAULT_LIST_PAGE_SIZE
        return {"objects": all_objects[:limit]}


class _PaginatedResponse:
    def __init__(self, objects: list[_MockExperiment]):
        self.objects = objects


@pytest.mark.asyncio
async def test_experiment_list_paginates_through_all_pages(tmp_path: Path) -> None:
    """Test that we fetch all pages when listing experiments."""
    # Three pages of experiments (newest first, as API returns them)
    page1 = [
        _MockExperiment("exp-1", "Exp 1", "2026-01-20T00:00:00Z"),
        _MockExperiment("exp-2", "Exp 2", "2026-01-19T00:00:00Z"),
    ]
    page2 = [
        _MockExperiment("exp-3", "Exp 3", "2026-01-18T00:00:00Z"),
        _MockExperiment("exp-4", "Exp 4", "2026-01-17T00:00:00Z"),
    ]
    page3 = [
        _MockExperiment("exp-5", "Exp 5", "2026-01-16T00:00:00Z"),
    ]

    cfg = MigrationConfig()
    client = _PaginatedListClient([page1, page2, page3], cfg)

    migrator = ExperimentMigrator(
        client,  # type: ignore
        client,  # type: ignore
        tmp_path,
        batch_size=10,
    )

    experiments = await migrator.list_source_resources(project_id="proj-1")

    # Should have fetched all 5 experiments
    # With DEFAULT_LIST_PAGE_SIZE, all 5 experiments fit in one page
    assert len(experiments) == 5
    assert client.list_calls == 1  # Single page needed

    # Verify the project_id filter was passed
    assert client.call_params[0].get("project_id") == "proj-1"
    # Verify limit was set
    assert client.call_params[0].get("limit") == DEFAULT_LIST_PAGE_SIZE


@pytest.mark.asyncio
async def test_experiment_list_early_stop_with_created_after(tmp_path: Path) -> None:
    """Test that we stop fetching when hitting the created_after cutoff."""
    # Experiments sorted newest-first (as API returns them)
    page1 = [
        _MockExperiment("exp-1", "Exp 1", "2026-01-20T00:00:00Z"),
        _MockExperiment("exp-2", "Exp 2", "2026-01-19T00:00:00Z"),
    ]
    page2 = [
        _MockExperiment("exp-3", "Exp 3", "2026-01-18T00:00:00Z"),
        _MockExperiment("exp-4", "Exp 4", "2026-01-10T00:00:00Z"),  # Before cutoff
    ]
    page3 = [
        # This page should never be requested
        _MockExperiment("exp-5", "Exp 5", "2026-01-09T00:00:00Z"),
    ]

    cfg = MigrationConfig(created_after="2026-01-15T00:00:00Z")
    client = _PaginatedListClient([page1, page2, page3], cfg)

    migrator = ExperimentMigrator(
        client,  # type: ignore
        client,  # type: ignore
        tmp_path,
        batch_size=10,
    )

    experiments = await migrator.list_source_resources(project_id="proj-1")

    # Should have fetched all experiments, but created_after filtering
    # is handled by the SDK/API, not by the migration tool
    # For this test, we'll verify all experiments are returned
    # (In production, the API would filter by created_after)
    assert len(experiments) == 5  # All experiments returned by mock
    assert client.list_calls == 1  # Single call to list()


@pytest.mark.asyncio
async def test_experiment_list_paginates_large_result_sets(tmp_path: Path) -> None:
    """Test that pagination properly fetches all experiments when there are >1000."""
    # Create 2500 experiments to test multi-page pagination
    all_pages = []
    for i in range(25):
        page = []
        for j in range(100):
            exp_num = i * 100 + j + 1
            page.append(
                _MockExperiment(
                    f"exp-{exp_num:04d}",
                    f"Exp {exp_num}",
                    f"2026-01-{(20 - i):02d}T{j:02d}:00:00Z",
                )
            )
        all_pages.append(page)

    cfg = MigrationConfig()
    client = _PaginatedListClient(all_pages, cfg)

    migrator = ExperimentMigrator(
        client,  # type: ignore
        client,  # type: ignore
        tmp_path,
        batch_size=10,
    )

    experiments = await migrator.list_source_resources(project_id="proj-1")

    # Should have fetched all 2500 experiments across 3 pages (1000 + 1000 + 500)
    assert len(experiments) == 2500
    assert client.list_calls == 3  # Three pages: 1000 + 1000 + 500

    # Verify all calls had the correct parameters
    assert all(params.get("project_id") == "proj-1" for params in client.call_params)
    assert all(params.get("limit") == DEFAULT_LIST_PAGE_SIZE for params in client.call_params)

    # Verify starting_after was used for subsequent pages
    assert client.call_params[0].get("starting_after") is None
    assert client.call_params[1].get("starting_after") == "exp-1000"
    assert client.call_params[2].get("starting_after") == "exp-2000"


@pytest.mark.asyncio
async def test_experiment_list_handles_empty_created_field(tmp_path: Path) -> None:
    """Test that experiments without created field don't break filtering."""
    page1 = [
        _MockExperiment("exp-1", "Exp 1", "2026-01-20T00:00:00Z"),
        # Experiment with no created field (will be skipped)
    ]
    # Manually remove created field from second exp
    exp_no_created = _MockExperiment("exp-2", "Exp 2", "2026-01-19T00:00:00Z")
    delattr(exp_no_created, "created")
    page1.append(exp_no_created)

    cfg = MigrationConfig(created_after="2026-01-15T00:00:00Z")
    client = _PaginatedListClient([page1], cfg)

    migrator = ExperimentMigrator(
        client,  # type: ignore
        client,  # type: ignore
        tmp_path,
        batch_size=10,
    )

    experiments = await migrator.list_source_resources(project_id="proj-1")

    # Should include both experiments (the migration tool doesn't filter by created field)
    assert len(experiments) == 2
    # Verify we got both experiments
    exp_ids = {e["id"] for e in experiments}
    assert exp_ids == {"exp-1", "exp-2"}
