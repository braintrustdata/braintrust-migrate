"""Unit tests for the dependency-aware DAG scheduler in orchestration.py."""

import asyncio

import pytest

from braintrust_migrate.orchestration import (
    RESOURCE_TYPE_DEPENDENCIES,
    _run_dag_schedule,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tracker():
    """Return (run_fn, log) where log records (name, start_order, end_order)."""
    log: list[str] = []
    in_flight: set[str] = set()
    concurrency_snapshots: list[set[str]] = []
    order_counter = {"n": 0}
    lock = asyncio.Lock()

    async def run(name: str) -> None:
        async with lock:
            order_counter["n"] += 1
            in_flight.add(name)
            concurrency_snapshots.append(set(in_flight))
        await asyncio.sleep(0.01)  # simulate work
        async with lock:
            in_flight.discard(name)
            log.append(name)

    return run, log, concurrency_snapshots


# ---------------------------------------------------------------------------
# RESOURCE_TYPE_DEPENDENCIES graph validation
# ---------------------------------------------------------------------------


class TestResourceTypeDependencies:
    """Validate the static dependency graph is well-formed."""

    def test_all_project_resources_present(self):
        """Every project-scoped resource should have an entry."""
        expected = {
            "datasets",
            "project_tags",
            "span_iframes",
            "functions",
            "prompts",
            "project_scores",
            "experiments",
            "logs",
            "views",
            "project_automations",
        }
        assert set(RESOURCE_TYPE_DEPENDENCIES.keys()) == expected

    def test_no_self_dependencies(self):
        """No resource should depend on itself."""
        for name, deps in RESOURCE_TYPE_DEPENDENCIES.items():
            assert name not in deps, f"{name} depends on itself"

    def test_dependencies_refer_to_known_resources(self):
        """All dependency targets should be valid resource names."""
        all_names = set(RESOURCE_TYPE_DEPENDENCIES.keys())
        for name, deps in RESOURCE_TYPE_DEPENDENCIES.items():
            unknown = deps - all_names
            assert not unknown, f"{name} depends on unknown resources: {unknown}"

    def test_no_cycles(self):
        """The dependency graph must be a DAG (no cycles)."""
        visited: set[str] = set()
        path: set[str] = set()

        def dfs(node: str) -> None:
            if node in path:
                raise AssertionError(f"Cycle detected involving {node}")
            if node in visited:
                return
            path.add(node)
            for dep in RESOURCE_TYPE_DEPENDENCIES.get(node, set()):
                dfs(dep)
            path.discard(node)
            visited.add(node)

        for name in RESOURCE_TYPE_DEPENDENCIES:
            dfs(name)

    def test_tier0_resources_have_no_deps(self):
        """Datasets, project_tags, span_iframes should be independent."""
        for name in ("datasets", "project_tags", "span_iframes", "logs", "project_automations"):
            assert RESOURCE_TYPE_DEPENDENCIES[name] == set(), (
                f"{name} should have no dependencies"
            )

    def test_expected_dependency_edges(self):
        """Spot-check critical dependency relationships."""
        assert "datasets" in RESOURCE_TYPE_DEPENDENCIES["functions"]
        assert "datasets" in RESOURCE_TYPE_DEPENDENCIES["experiments"]
        assert "functions" in RESOURCE_TYPE_DEPENDENCIES["prompts"]
        assert "functions" in RESOURCE_TYPE_DEPENDENCIES["project_scores"]
        assert "datasets" in RESOURCE_TYPE_DEPENDENCIES["views"]
        assert "experiments" in RESOURCE_TYPE_DEPENDENCIES["views"]


# ---------------------------------------------------------------------------
# _run_dag_schedule tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRunDagSchedule:

    async def test_independent_resources_run_concurrently(self):
        """Resources with no mutual deps should run in the same tier."""
        run, log, snapshots = _make_tracker()

        await _run_dag_schedule(["datasets", "project_tags", "span_iframes"], run)

        assert set(log) == {"datasets", "project_tags", "span_iframes"}
        # At least one snapshot should show >1 resource in-flight simultaneously.
        assert any(len(s) > 1 for s in snapshots), (
            "Independent resources should overlap"
        )

    async def test_dependency_ordering_respected(self):
        """Dependent resources must start after their prerequisites."""
        completed_before_start: dict[str, set[str]] = {}
        completed: set[str] = set()
        lock = asyncio.Lock()

        async def run(name: str) -> None:
            async with lock:
                completed_before_start[name] = set(completed)
            await asyncio.sleep(0.01)
            async with lock:
                completed.add(name)

        # functions depends on datasets; prompts depends on functions
        await _run_dag_schedule(["datasets", "functions", "prompts"], run)

        assert "datasets" in completed_before_start["functions"]
        assert "functions" in completed_before_start["prompts"]

    async def test_failure_skips_dependents(self):
        """When a resource fails, its dependents should be skipped."""
        log: list[str] = []

        async def run(name: str) -> None:
            log.append(name)
            if name == "datasets":
                raise RuntimeError("datasets failed")
            await asyncio.sleep(0.01)

        # datasets fails -> functions depends on datasets -> should be skipped.
        # project_tags has no deps, should still run.
        with pytest.raises(RuntimeError, match="datasets failed"):
            await _run_dag_schedule(
                ["datasets", "project_tags", "functions", "experiments"],
                run,
            )

        assert "datasets" in log
        assert "project_tags" in log
        # functions and experiments both depend on datasets -- they should NOT have run.
        assert "functions" not in log
        assert "experiments" not in log

    async def test_empty_list(self):
        """Empty resource list should be a no-op."""
        run, log, _ = _make_tracker()
        await _run_dag_schedule([], run)
        assert log == []

    async def test_single_resource(self):
        """A single resource should complete normally."""
        run, log, _ = _make_tracker()
        await _run_dag_schedule(["logs"], run)
        assert log == ["logs"]

    async def test_subset_deps_filtered(self):
        """Dependencies not in the run set should be ignored."""
        # prompts depends on functions, but functions is not in the list.
        # prompts should still run (dependency is outside the run set).
        run, log, _ = _make_tracker()
        await _run_dag_schedule(["prompts", "project_tags"], run)
        assert set(log) == {"prompts", "project_tags"}

    async def test_full_graph_completes(self):
        """All project-scoped resources should eventually complete."""
        run, log, _ = _make_tracker()
        all_names = list(RESOURCE_TYPE_DEPENDENCIES.keys())
        await _run_dag_schedule(all_names, run)
        assert set(log) == set(all_names)

    async def test_views_waits_for_both_deps(self):
        """Views depends on both datasets and experiments."""
        completed_before_start: dict[str, set[str]] = {}
        completed: set[str] = set()
        lock = asyncio.Lock()

        async def run(name: str) -> None:
            async with lock:
                completed_before_start[name] = set(completed)
            await asyncio.sleep(0.01)
            async with lock:
                completed.add(name)

        await _run_dag_schedule(["datasets", "experiments", "views"], run)

        assert "datasets" in completed_before_start["views"]
        assert "experiments" in completed_before_start["views"]
