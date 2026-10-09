"""Unit tests for project settings migration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from braintrust_migrate import orchestration
from braintrust_migrate.config import Config
from braintrust_migrate.orchestration import MigrationOrchestrator
from braintrust_migrate.project_settings import (
    migrate_project_settings,
    plan_project_settings,
)

EXPECTED_SETTINGS_TOTAL = 2
SRC_FN = "src-fn-111"
DST_FN = "dst-fn-222"
SRC_EXP = "src-exp-333"
DST_EXP = "dst-exp-444"
ID_MAPPING = {SRC_FN: DST_FN, SRC_EXP: DST_EXP}

FULL_SOURCE_SETTINGS = {
    "comparison_key": "input.question",
    "baseline_experiment_id": SRC_EXP,
    "spanFieldOrder": [
        {"object_type": "project_logs", "column_id": "output", "position": "0"}
    ],
    "remote_eval_sources": [{"url": "https://evals.example.com", "name": "dev"}],
    "disable_realtime_queries": True,
    "default_preprocessor": {"type": "function", "id": SRC_FN, "version": "123"},
}


class TestPlanProjectSettings:
    def test_new_project_gets_all_settings_with_ids_remapped(self):
        plan = plan_project_settings(FULL_SOURCE_SETTINGS, None, ID_MAPPING)

        assert plan.settings == {
            **FULL_SOURCE_SETTINGS,
            "baseline_experiment_id": DST_EXP,
            "default_preprocessor": {"type": "function", "id": DST_FN},
        }
        assert sorted(plan.applied) == sorted(FULL_SOURCE_SETTINGS)
        assert plan.kept_existing == []
        assert plan.skipped == {}

    def test_existing_destination_settings_are_kept(self):
        dest = {"comparison_key": "input", "default_preprocessor": None}
        plan = plan_project_settings(
            {
                "comparison_key": "input.question",
                "default_preprocessor": {"type": "function", "id": SRC_FN},
            },
            dest,
            ID_MAPPING,
        )

        assert plan.settings["comparison_key"] == "input"
        assert plan.settings["default_preprocessor"] == {
            "type": "function",
            "id": DST_FN,
        }
        assert plan.kept_existing == ["comparison_key"]
        assert plan.applied == ["default_preprocessor"]

    def test_unmapped_references_are_skipped(self):
        plan = plan_project_settings(
            {
                "baseline_experiment_id": "unknown-exp",
                "default_preprocessor": {"type": "function", "id": "unknown-fn"},
            },
            None,
            ID_MAPPING,
        )

        assert plan.settings == {}
        assert not plan.has_changes
        assert plan.skipped == {
            "baseline_experiment_id": "baseline_experiment_not_migrated",
            "default_preprocessor": "preprocessor_function_not_migrated",
        }

    def test_global_preprocessor_is_copied(self):
        value = {"type": "global", "name": "thread", "function_type": "preprocessor"}
        plan = plan_project_settings({"default_preprocessor": value}, None, {})

        assert plan.settings == {"default_preprocessor": value}

    def test_unknown_settings_are_skipped_not_copied(self):
        plan = plan_project_settings({"some_future_setting": "abc"}, None, {})

        assert plan.settings == {}
        assert plan.skipped == {"some_future_setting": "unknown_setting"}

    def test_null_source_values_are_ignored(self):
        plan = plan_project_settings(
            {"comparison_key": None, "default_preprocessor": None}, {"x": 1}, {}
        )

        assert plan.settings == {"x": 1}
        assert not plan.has_changes
        assert plan.skipped == {}


def _dest_client(current_settings):
    client = Mock()
    client.raw_request = AsyncMock(
        side_effect=lambda method, path, **kw: (
            {"id": "dest-proj", "settings": current_settings}
            if method == "GET"
            else {"id": "dest-proj", "settings": kw["json"]["settings"]}
        )
    )

    async def run(_name, fn):
        return await fn()

    client.with_retry = AsyncMock(side_effect=run)
    return client


@pytest.mark.asyncio
class TestMigrateProjectSettings:
    async def test_patches_merged_settings(self):
        client = _dest_client({"comparison_key": "input"})

        results = await migrate_project_settings(
            source_settings={
                "comparison_key": "input.question",
                "default_preprocessor": {"type": "function", "id": SRC_FN},
            },
            dest_client=client,
            dest_project_id="dest-proj",
            id_mapping=ID_MAPPING,
            project_name="p",
        )

        patch = client.raw_request.call_args_list[-1]
        assert patch.args == ("PATCH", "/v1/project/dest-proj")
        assert patch.kwargs["json"] == {
            "settings": {
                "comparison_key": "input",
                "default_preprocessor": {"type": "function", "id": DST_FN},
            }
        }
        assert results["total"] == EXPECTED_SETTINGS_TOTAL
        assert results["migrated"] == 1
        assert results["skipped"] == 1
        assert results["skipped_details"] == [
            {
                "setting": "comparison_key",
                "source_id": "comparison_key",
                "name": None,
                "skip_reason": "dest_already_set",
            }
        ]

    async def test_skipped_settings_render_in_migration_report(self, tmp_path):
        """Regression: skipped settings crashed report generation (KeyError)."""
        results = await migrate_project_settings(
            source_settings={"comparison_key": "input.question"},
            dest_client=_dest_client({"comparison_key": "input"}),
            dest_project_id="dest-proj",
            id_mapping=ID_MAPPING,
            project_name="p",
        )
        orchestrator = MigrationOrchestrator(
            Config(
                source={"api_key": "src", "url": "https://api.braintrust.dev"},
                destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
                state_dir=tmp_path,
            )
        )
        run_results = {
            "start_time": "2026-01-01T00:00:00",
            "end_time": "2026-01-01T00:00:01",
            "duration_seconds": 1.0,
            "success": True,
            "summary": {
                "total_projects": 1,
                "total_resources": 1,
                "migrated_resources": 0,
                "skipped_resources": 1,
                "failed_resources": 0,
            },
            "organization_resources": {},
            "projects": {
                "p": {
                    "project_id": "dest-proj",
                    "resources": {"project_settings": results},
                    "total_resources": 1,
                    "migrated_resources": 0,
                    "skipped_resources": 1,
                    "failed_resources": 0,
                    "errors": [],
                }
            },
        }

        orchestrator._generate_migration_report(run_results, tmp_path)

        summary_text = (tmp_path / "migration_summary.txt").read_text()
        assert "project_settings: comparison_key" in summary_text

    async def test_no_patch_when_nothing_to_apply(self):
        client = _dest_client({"comparison_key": "input"})

        results = await migrate_project_settings(
            source_settings={"comparison_key": "input.question"},
            dest_client=client,
            dest_project_id="dest-proj",
            id_mapping={},
            project_name="p",
        )

        assert [c.args[0] for c in client.raw_request.call_args_list] == ["GET"]
        assert results["migrated"] == 0
        assert results["skipped"] == 1

    async def test_no_requests_when_source_has_no_settings(self):
        client = _dest_client(None)

        results = await migrate_project_settings(
            source_settings=None,
            dest_client=client,
            dest_project_id="dest-proj",
            id_mapping={},
            project_name="p",
        )

        client.raw_request.assert_not_called()
        assert results == {
            "total": 0,
            "migrated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    async def test_api_failure_is_reported_not_raised(self):
        client = Mock()

        async def boom(_name, _fn):
            raise RuntimeError("403 forbidden")

        client.with_retry = AsyncMock(side_effect=boom)

        results = await migrate_project_settings(
            source_settings={"comparison_key": "input.question"},
            dest_client=client,
            dest_project_id="dest-proj",
            id_mapping={},
            project_name="p",
        )

        assert results["failed"] == 1
        assert results["errors"][0]["resource_type"] == "project_settings"


def _make_config(tmp_path: Path, resources: list[str]) -> Config:
    return Config(
        source={"api_key": "src", "url": "https://api.braintrust.dev"},
        destination={"api_key": "dst", "url": "https://api.braintrust.dev"},
        state_dir=tmp_path,
        resources=resources,
    )


PROJECT = {
    "source_id": "src-proj",
    "dest_id": "dest-proj",
    "name": "Project A",
    "dest_name": "Project A",
    "settings": {"default_preprocessor": {"type": "function", "id": SRC_FN}},
}


@pytest.mark.asyncio
class TestOrchestratorProjectSettings:
    async def test_runs_after_resources_with_global_mappings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        fake = AsyncMock(
            return_value={
                "total": 1,
                "migrated": 1,
                "skipped": 0,
                "failed": 0,
                "errors": [],
            }
        )
        monkeypatch.setattr(orchestration, "migrate_project_settings", fake)
        orchestrator = MigrationOrchestrator(
            _make_config(tmp_path, ["project_settings"])
        )
        callback = Mock()

        results = await orchestrator._migrate_project(
            PROJECT,
            Mock(),
            Mock(),
            tmp_path,
            dict(ID_MAPPING),
            resource_callback=callback,
        )

        fake.assert_awaited_once()
        assert fake.await_args.kwargs["source_settings"] == PROJECT["settings"]
        assert fake.await_args.kwargs["dest_project_id"] == "dest-proj"
        assert fake.await_args.kwargs["id_mapping"][SRC_FN] == DST_FN
        assert results["resources"]["project_settings"]["migrated"] == 1
        assert results["migrated_resources"] == 1
        callback.assert_called_once_with(
            "project_settings", results["resources"]["project_settings"]
        )

    async def test_skipped_when_not_selected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        fake = AsyncMock()
        monkeypatch.setattr(orchestration, "migrate_project_settings", fake)
        orchestrator = MigrationOrchestrator(_make_config(tmp_path, ["span_iframes"]))
        monkeypatch.setattr(orchestrator, "PROJECT_SCOPED_RESOURCES", [])

        results = await orchestrator._migrate_project(
            PROJECT, Mock(), Mock(), tmp_path, {}
        )

        fake.assert_not_awaited()
        assert "project_settings" not in results["resources"]
