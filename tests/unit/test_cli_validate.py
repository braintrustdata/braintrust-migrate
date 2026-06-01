from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from braintrust_migrate.cli import _parse_validation_resources, validate
from braintrust_migrate.config import Config, MigrationConfig

EXPECTED_LOGS_FETCH_LIMIT = 7


def _make_config(tmp_path: Path) -> Config:
    return Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        migration=MigrationConfig(),
        state_dir=tmp_path,
        resources=["all"],
    )


def test_parse_validation_resources_rejects_unsupported() -> None:
    with pytest.raises(ValueError, match="Resource validation currently supports only"):
        _parse_validation_resources("logs,datasets")


def test_validate_without_resources_only_checks_connectivity(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    with (
        patch("braintrust_migrate.cli.Config.from_env", return_value=config),
        patch("braintrust_migrate.cli._test_connectivity", new_callable=AsyncMock) as test_connectivity,
        patch(
            "braintrust_migrate.cli._run_resource_validation",
            new_callable=AsyncMock,
        ) as run_resource_validation,
    ):
        validate(resources=None, log_level="INFO")

    test_connectivity.assert_awaited_once()
    run_resource_validation.assert_not_awaited()


def test_validate_with_logs_resources_dispatches_validator(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    with (
        patch("braintrust_migrate.cli.Config.from_env", return_value=config),
        patch("braintrust_migrate.cli._test_connectivity", new_callable=AsyncMock),
        patch(
            "braintrust_migrate.cli._run_resource_validation",
            new_callable=AsyncMock,
            return_value={
                "logs": {
                    "resource": "logs",
                    "checked": 0,
                    "matched": 0,
                    "missing": 0,
                    "duplicate_destination": 0,
                    "unverifiable_source": 0,
                    "unverifiable_destination": 0,
                    "skipped_projects": 0,
                    "success": True,
                    "projects": [],
                }
            },
        ) as run_resource_validation,
    ):
        validate(
            resources="logs",
            projects="Project A",
            created_after="2026-01-01",
            created_before="2026-02-01",
            logs_fetch_limit=EXPECTED_LOGS_FETCH_LIMIT,
            log_level="INFO",
        )

    run_resource_validation.assert_awaited_once()
    validated_config = run_resource_validation.await_args.args[0]
    validation_resources = run_resource_validation.await_args.args[1]
    assert validation_resources == ["logs"]
    assert validated_config.project_names == ["Project A"]
    assert validated_config.migration.created_after == "2026-01-01T00:00:00Z"
    assert validated_config.migration.created_before == "2026-02-01T00:00:00Z"
    assert validated_config.migration.logs_fetch_limit == EXPECTED_LOGS_FETCH_LIMIT


def test_validate_with_unsupported_resource_exits_nonzero(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    with (
        patch("braintrust_migrate.cli.Config.from_env", return_value=config),
        patch("braintrust_migrate.cli._test_connectivity", new_callable=AsyncMock) as test_connectivity,
    ):
        with pytest.raises(SystemExit) as exc:
            validate(resources="all", log_level="INFO")

    assert exc.value.code == 1
    test_connectivity.assert_not_awaited()
