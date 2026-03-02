"""Integration tests for group member user mapping flow."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import HttpUrl

from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


@pytest.mark.asyncio
async def test_group_member_user_mapping_auto_invite_flow(tmp_path: Path) -> None:
    config = Config(
        source=BraintrustOrgConfig(
            api_key="source-key",
            url=cast(HttpUrl, HttpUrl("https://source.braintrust.dev")),
        ),
        destination=BraintrustOrgConfig(
            api_key="dest-key",
            url=cast(HttpUrl, HttpUrl("https://dest.braintrust.dev")),
        ),
        migration=MigrationConfig(
            batch_size=10,
            group_map_users=True,
            group_auto_invite_users=True,
        ),
        state_dir=tmp_path,
        resources=["groups"],
    )

    source_project = {"id": "src-proj-1", "name": "Project A"}
    source_group = {
        "id": "src-group-1",
        "name": "Analysts",
        "description": "Data analysts",
        "member_users": ["src-user-1"],
    }

    dest_user_lookup_calls = {"count": 0}
    created_group_payloads: list[dict] = []

    @asynccontextmanager
    async def mock_create_client_pair(_source_cfg, _dest_cfg, _migration_cfg):
        source_mock = Mock()
        dest_mock = Mock()

        source_mock.migration_config = config.migration
        dest_mock.migration_config = config.migration

        source_mock.list_projects = AsyncMock(return_value=[source_project])
        dest_mock.list_projects = AsyncMock(return_value=[])
        dest_mock.create_project = AsyncMock(
            return_value={"id": "dest-proj-1", "name": "Project A"}
        )

        async def with_retry(_operation_name, coro_func):
            result = coro_func()
            if hasattr(result, "__await__"):
                return await result
            return result

        source_mock.with_retry = with_retry
        dest_mock.with_retry = with_retry

        async def source_raw_request(method, path, **kwargs):
            if method == "GET" and path == "/v1/group":
                return {"objects": [source_group]}
            if method == "GET" and path == "/v1/user/src-user-1":
                return {"id": "src-user-1", "email": "user@example.com"}
            raise AssertionError(f"Unexpected source request: {method} {path} {kwargs}")

        async def dest_raw_request(method, path, **kwargs):
            if method == "GET" and path == "/v1/user":
                dest_user_lookup_calls["count"] += 1
                if dest_user_lookup_calls["count"] == 1:
                    return {"objects": []}
                return {"objects": [{"id": "dest-user-1", "email": "user@example.com"}]}
            if method == "PATCH" and path == "/v1/organization/members":
                return {"status": "success"}
            if method == "POST" and path == "/v1/group":
                payload = kwargs.get("json", {})
                created_group_payloads.append(payload)
                return {"id": "dest-group-1", "name": payload.get("name")}
            raise AssertionError(f"Unexpected destination request: {method} {path} {kwargs}")

        source_mock.raw_request = source_raw_request
        dest_mock.raw_request = dest_raw_request

        yield source_mock, dest_mock

    with patch(
        "braintrust_migrate.orchestration.create_client_pair",
        mock_create_client_pair,
    ):
        orchestrator = MigrationOrchestrator(config)
        results = await orchestrator.migrate_all()

    org_resources = results.get("organization_resources", {}).get("resources", {})
    group_results = org_resources.get("groups", {})

    assert group_results.get("migrated") == 1
    assert group_results.get("failed") == 0
    assert group_results.get("skipped") == 0
    assert created_group_payloads
    assert created_group_payloads[0].get("member_users") == ["dest-user-1"]


@pytest.mark.asyncio
async def test_group_member_users_skipped_when_mapping_disabled(tmp_path: Path) -> None:
    config = Config(
        source=BraintrustOrgConfig(
            api_key="source-key",
            url=cast(HttpUrl, HttpUrl("https://source.braintrust.dev")),
        ),
        destination=BraintrustOrgConfig(
            api_key="dest-key",
            url=cast(HttpUrl, HttpUrl("https://dest.braintrust.dev")),
        ),
        migration=MigrationConfig(
            batch_size=10,
            group_map_users=False,
        ),
        state_dir=tmp_path,
        resources=["groups"],
    )

    source_project = {"id": "src-proj-1", "name": "Project A"}
    source_group = {
        "id": "src-group-1",
        "name": "Analysts",
        "description": "Data analysts",
        "member_users": ["src-user-1"],
    }

    created_group_payloads: list[dict] = []

    @asynccontextmanager
    async def mock_create_client_pair(_source_cfg, _dest_cfg, _migration_cfg):
        source_mock = Mock()
        dest_mock = Mock()

        source_mock.migration_config = config.migration
        dest_mock.migration_config = config.migration

        source_mock.list_projects = AsyncMock(return_value=[source_project])
        dest_mock.list_projects = AsyncMock(return_value=[])
        dest_mock.create_project = AsyncMock(
            return_value={"id": "dest-proj-1", "name": "Project A"}
        )

        async def with_retry(_operation_name, coro_func):
            result = coro_func()
            if hasattr(result, "__await__"):
                return await result
            return result

        source_mock.with_retry = with_retry
        dest_mock.with_retry = with_retry

        async def source_raw_request(method, path, **kwargs):
            if method == "GET" and path == "/v1/group":
                return {"objects": [source_group]}
            raise AssertionError(f"Unexpected source request: {method} {path} {kwargs}")

        async def dest_raw_request(method, path, **kwargs):
            if method == "POST" and path == "/v1/group":
                payload = kwargs.get("json", {})
                created_group_payloads.append(payload)
                return {"id": "dest-group-1", "name": payload.get("name")}
            raise AssertionError(f"Unexpected destination request: {method} {path} {kwargs}")

        source_mock.raw_request = source_raw_request
        dest_mock.raw_request = dest_raw_request

        yield source_mock, dest_mock

    with patch(
        "braintrust_migrate.orchestration.create_client_pair",
        mock_create_client_pair,
    ):
        orchestrator = MigrationOrchestrator(config)
        results = await orchestrator.migrate_all()

    org_resources = results.get("organization_resources", {}).get("resources", {})
    group_results = org_resources.get("groups", {})

    assert group_results.get("migrated") == 1
    assert group_results.get("failed") == 0
    assert group_results.get("skipped") == 0
    assert created_group_payloads
    assert "member_users" not in created_group_payloads[0]
