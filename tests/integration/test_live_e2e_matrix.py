"""Opt-in live end-to-end migration matrix against real orgs.

By default this test is skipped. Enable explicitly:

  MIGRATION_RUN_LIVE_E2E=true \
  MIGRATION_LIVE_ALLOW_WRITES=true \
  MIGRATION_LIVE_PROJECTS="Project A,Project B" \
  MIGRATION_LIVE_RESOURCES="datasets,experiments,logs" \
  MIGRATION_LIVE_CREATED_AFTER=2026-02-01 \
  MIGRATION_LIVE_CREATED_BEFORE=2026-03-01 \
  uv run pytest tests/integration/test_live_e2e_matrix.py -q

Environment loading:
- If `MIGRATION_LIVE_ENV_FILE` is set, this test loads that `.env` file.
- Otherwise it relies on the process environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from dotenv import load_dotenv

from braintrust_migrate.config import Config
from braintrust_migrate.orchestration import MigrationOrchestrator


def _bool_env(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _live_enabled() -> bool:
    return _bool_env("MIGRATION_RUN_LIVE_E2E", "false")


def _load_optional_env_file() -> None:
    env_file = os.getenv("MIGRATION_LIVE_ENV_FILE")
    if env_file:
        load_dotenv(env_file, override=False)


def _csv_env(name: str, default: str | None = None) -> list[str]:
    raw = os.getenv(name, default or "")
    return [part.strip() for part in raw.split(",") if part.strip()]


@dataclass(frozen=True)
class E2EScenario:
    name: str
    max_concurrent: int
    max_concurrent_resources: int
    max_concurrent_requests: int
    streaming_pipeline: bool


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _live_enabled(),
    reason="Set MIGRATION_RUN_LIVE_E2E=true to enable live E2E matrix",
)
@pytest.mark.parametrize(
    "scenario",
    [
        E2EScenario(
            name="concurrent_pipeline_on",
            max_concurrent=3,
            max_concurrent_resources=6,
            max_concurrent_requests=30,
            streaming_pipeline=True,
        ),
        E2EScenario(
            name="sequential_pipeline_off",
            max_concurrent=1,
            max_concurrent_resources=1,
            max_concurrent_requests=10,
            streaming_pipeline=False,
        ),
    ],
    ids=lambda s: s.name,
)
async def test_live_e2e_matrix(
    scenario: E2EScenario,
    tmp_path: Path,
) -> None:
    _load_optional_env_file()

    if not _bool_env("MIGRATION_LIVE_ALLOW_WRITES", "false"):
        pytest.skip("Set MIGRATION_LIVE_ALLOW_WRITES=true to allow live migration runs")

    project_names = _csv_env("MIGRATION_LIVE_PROJECTS")
    if not project_names:
        pytest.skip("Set MIGRATION_LIVE_PROJECTS to scope live E2E migration safely")

    resources = _csv_env("MIGRATION_LIVE_RESOURCES", "datasets,experiments,logs")
    created_after = os.getenv("MIGRATION_LIVE_CREATED_AFTER")
    created_before = os.getenv("MIGRATION_LIVE_CREATED_BEFORE")

    # Safety guard: require a bounded date range for high-volume streaming resources.
    if any(r in {"logs", "experiments", "datasets"} for r in resources):
        if not created_after or not created_before:
            pytest.skip(
                "For logs/experiments/datasets, set MIGRATION_LIVE_CREATED_AFTER and "
                "MIGRATION_LIVE_CREATED_BEFORE to bound volume."
            )

    config = Config.from_env()
    config.resources = resources
    config.project_names = project_names
    config.state_dir = tmp_path / scenario.name
    config.migration.max_concurrent = scenario.max_concurrent
    config.migration.max_concurrent_resources = scenario.max_concurrent_resources
    config.migration.max_concurrent_requests = scenario.max_concurrent_requests
    config.migration.streaming_pipeline = scenario.streaming_pipeline
    config.migration.created_after = created_after
    config.migration.created_before = created_before

    orchestrator = MigrationOrchestrator(config)
    results = await orchestrator.migrate_all()

    summary = results.get("summary", {})
    assert isinstance(summary, dict)
    assert summary.get("total_projects", 0) >= 1
    assert summary.get("failed_resources", 0) == 0
