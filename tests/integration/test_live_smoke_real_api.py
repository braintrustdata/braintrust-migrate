"""Opt-in live smoke test against real Braintrust source/destination orgs.

This test is skipped by default. To run:

  MIGRATION_RUN_LIVE_SMOKE=true \
  BT_SOURCE_API_KEY=... \
  BT_DEST_API_KEY=... \
  [BT_SOURCE_URL=https://api.braintrust.dev] \
  [BT_DEST_URL=https://api.braintrust.dev] \
  uv run pytest tests/integration/test_live_smoke_real_api.py

It performs read-only checks only (no writes).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, cast

import pytest
from pydantic import HttpUrl

from braintrust_migrate.client import create_client_pair
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


def _bool_env(name: str, default: str = "false") -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _live_enabled() -> bool:
    return _bool_env("MIGRATION_RUN_LIVE_SMOKE", "false")


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _live_enabled(),
    reason="Set MIGRATION_RUN_LIVE_SMOKE=true to enable live smoke test",
)
async def test_live_api_smoke_read_only() -> None:
    concurrent_reads = 8
    source_api_key = os.getenv("BT_SOURCE_API_KEY")
    dest_api_key = os.getenv("BT_DEST_API_KEY")
    if not source_api_key or not dest_api_key:
        pytest.skip("BT_SOURCE_API_KEY and BT_DEST_API_KEY are required")

    source_url = os.getenv("BT_SOURCE_URL", "https://api.braintrust.dev")
    dest_url = os.getenv("BT_DEST_URL", "https://api.braintrust.dev")
    max_concurrent_requests = int(os.getenv("MIGRATION_MAX_CONCURRENT_REQUESTS", "20"))

    source_cfg = BraintrustOrgConfig(
        api_key=source_api_key,
        url=cast(HttpUrl, HttpUrl(source_url)),
    )
    dest_cfg = BraintrustOrgConfig(
        api_key=dest_api_key,
        url=cast(HttpUrl, HttpUrl(dest_url)),
    )
    migration_cfg = MigrationConfig(
        max_concurrent_requests=max_concurrent_requests,
        max_concurrent_resources=5,
    )

    async with create_client_pair(source_cfg, dest_cfg, migration_cfg) as (
        source_client,
        dest_client,
    ):
        source_projects = await source_client.list_projects(limit=3)
        dest_projects = await dest_client.list_projects(limit=3)

        assert isinstance(source_projects, list)
        assert isinstance(dest_projects, list)

        async def _source_read(i: int) -> Any:
            _ = i
            return await source_client.raw_request(
                "GET",
                "/v1/project",
                params={"limit": 1},
            )

        responses = await asyncio.gather(
            *[_source_read(i) for i in range(concurrent_reads)]
        )
        assert len(responses) == concurrent_reads
        for payload in responses:
            assert isinstance(payload, dict)
            assert "objects" in payload
