"""Wiring tests: orchestrator validation phase + console rendering."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import HttpUrl
from rich.console import Console

import braintrust_migrate.cli as cli_module
from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


class _OrchStub:
    """Serves /v1/<type> list pages and /btql counts via raw_request."""

    def __init__(
        self,
        *,
        lists: dict[str, list[dict[str, Any]]] | None = None,
        counts: dict[str, int] | None = None,
    ) -> None:
        self._lists = lists or {}
        self._counts = counts or {}

    async def with_retry(self, _op, coro_func, **kwargs):
        res = coro_func()
        return await res if hasattr(res, "__await__") else res

    async def raw_request(self, method, path, *, params=None, json=None, **kwargs):
        if method == "GET" and path.startswith("/v1/"):
            resource_type = path[len("/v1/") :] + "s"  # "dataset" -> "datasets"
            return {"objects": self._lists.get(resource_type, [])}
        if method == "POST" and path == "/btql":
            query = json["query"]
            for parent_id, n in self._counts.items():
                if f"'{parent_id}'" in query:
                    return {"data": [{"n": n}]}
            return {"data": [{"n": 0}]}
        raise AssertionError(f"unexpected request: {method} {path}")


def _config() -> Config:
    return Config(
        source=BraintrustOrgConfig(api_key="s", url=HttpUrl("https://src.dev")),
        destination=BraintrustOrgConfig(api_key="d", url=HttpUrl("https://dst.dev")),
        migration=MigrationConfig(validate_migration=True),
    )


@pytest.mark.asyncio
async def test_validate_project_aggregates_object_and_event_checks():
    orch = MigrationOrchestrator(_config())
    source = _OrchStub(
        lists={
            "datasets": [{"id": "sd1", "name": "D1"}],
            "prompts": [{"id": "sp1", "name": "P1"}, {"id": "sp2", "name": "P2"}],
        },
        counts={"sd1": 50, "sproj": 1000},
    )
    dest = _OrchStub(
        lists={
            "datasets": [{"id": "dd1", "name": "D1"}],
            "prompts": [{"id": "dp1", "name": "P1"}],  # P2 missing
        },
        counts={"dd1": 50, "dproj": 1000},
    )

    result = await orch._validate_project(
        source_client=source,
        dest_client=dest,
        source_project_id="sproj",
        dest_project_id="dproj",
        migrated_resource_types=["datasets", "prompts", "logs"],
        id_mapping={"sd1": "dd1"},
    )

    assert result["ok"] is False  # prompts P2 missing
    by_type = {r["resource_type"]: r for r in result["resources"]}
    assert set(by_type) == {"datasets", "prompts", "logs"}

    assert by_type["datasets"]["ok"] is True  # object + 50==50 events
    assert by_type["logs"]["ok"] is True  # 1000==1000 project events

    prompts = by_type["prompts"]
    assert prompts["ok"] is False
    obj_check = next(c for c in prompts["checks"] if c["kind"] == "object")
    assert obj_check["missing"] == ["P2"]


def test_display_results_renders_validation_failures(monkeypatch):
    rec = Console(record=True, width=200)
    monkeypatch.setattr(cli_module, "console", rec)

    results = {
        "summary": {
            "total_projects": 1,
            "total_resources": 3,
            "migrated_resources": 3,
            "skipped_resources": 0,
            "failed_resources": 0,
            "errors": [],
        },
        "projects": {
            "ProjA": {
                "total_resources": 3,
                "migrated_resources": 3,
                "skipped_resources": 0,
                "failed_resources": 0,
                "project_id": "p",
                "validation": {
                    "ok": False,
                    "resources": [
                        {"resource_type": "datasets", "ok": True, "checks": []},
                        {
                            "resource_type": "prompts",
                            "ok": False,
                            "checks": [
                                {
                                    "kind": "object",
                                    "scope": "prompts",
                                    "source_count": 2,
                                    "dest_count": 1,
                                    "ok": False,
                                    "missing": ["P2"],
                                }
                            ],
                        },
                        {
                            "resource_type": "logs",
                            "ok": False,
                            "checks": [
                                {
                                    "kind": "events",
                                    "scope": "logs/events",
                                    "source_count": 1000,
                                    "dest_count": 990,
                                    "ok": False,
                                }
                            ],
                        },
                    ],
                },
            }
        },
    }

    cli_module._display_results(results)
    out = rec.export_text()

    assert "Validation" in out
    assert "2 of 3 validated" in out  # 2 failing resources of 3
    assert "ProjA/prompts" in out
    assert "missing 1: P2" in out
    assert "logs/events: source=1000 dest=990" in out


def test_display_results_validation_all_ok(monkeypatch):
    rec = Console(record=True, width=200)
    monkeypatch.setattr(cli_module, "console", rec)
    results = {
        "summary": {
            "total_projects": 1,
            "total_resources": 1,
            "migrated_resources": 1,
            "skipped_resources": 0,
            "failed_resources": 0,
            "errors": [],
        },
        "projects": {
            "ProjA": {
                "total_resources": 1,
                "migrated_resources": 1,
                "skipped_resources": 0,
                "failed_resources": 0,
                "project_id": "p",
                "validation": {
                    "ok": True,
                    "resources": [
                        {"resource_type": "datasets", "ok": True, "checks": []}
                    ],
                },
            }
        },
    }
    cli_module._display_results(results)
    out = rec.export_text()
    assert "all 1 validated resource(s) match" in out


def test_config_from_env_validate_flag(monkeypatch):
    monkeypatch.setenv("BT_SOURCE_API_KEY", "s")
    monkeypatch.setenv("BT_DEST_API_KEY", "d")
    monkeypatch.setenv("MIGRATION_VALIDATE", "true")
    assert Config.from_env().migration.validate_migration is True
    monkeypatch.setenv("MIGRATION_VALIDATE", "false")
    assert Config.from_env().migration.validate_migration is False
