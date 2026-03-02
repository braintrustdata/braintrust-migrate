"""Integration test for ACL user mapping flow with auto-invite."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import HttpUrl

from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


@pytest.mark.asyncio
async def test_acl_user_mapping_auto_invite_flow(tmp_path: Path) -> None:
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
            acl_map_users=True,
            acl_auto_invite_users=True,
        ),
        state_dir=tmp_path,
        resources=["acls"],
    )

    source_project = {"id": "src-proj-1", "name": "Project A"}
    source_acl = {
        "id": "src-acl-1",
        "object_type": "project",
        "object_id": "src-proj-1",
        "user_id": "src-user-1",
        "permission": "read",
    }

    dest_user_lookup_calls = {"count": 0}
    created_acl_payloads: list[dict] = []

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
            if method == "GET" and path == "/v1/acl/list_org":
                return [source_acl]
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
            if method == "POST" and path == "/v1/acl":
                payload = kwargs.get("json", {})
                created_acl_payloads.append(payload)
                return {"id": "dest-acl-1", "object_type": payload.get("object_type")}
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
    acl_results = org_resources.get("acls", {})

    assert acl_results.get("migrated") == 1
    assert acl_results.get("failed") == 0
    assert acl_results.get("skipped") == 0
    assert created_acl_payloads
    assert created_acl_payloads[0].get("user_id") == "dest-user-1"
