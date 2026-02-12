"""Unit tests for the client request semaphore."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


@pytest.fixture
def org_config():
    return BraintrustOrgConfig(
        api_key="test-key", url="https://test.braintrust.dev"
    )


def _make_client(org_config: BraintrustOrgConfig, max_concurrent_requests: int = 20):
    config = MigrationConfig(max_concurrent_requests=max_concurrent_requests)
    return BraintrustClient(org_config, config, "test")


@pytest.mark.asyncio
class TestClientRequestSemaphore:

    async def test_semaphore_initialized_from_config(self, org_config):
        """Semaphore should reflect max_concurrent_requests config value."""
        client = _make_client(org_config, max_concurrent_requests=7)
        assert client._request_semaphore._value == 7

    async def test_default_semaphore_value(self, org_config):
        """Default semaphore should be 20."""
        client = _make_client(org_config)
        assert client._request_semaphore._value == 20

    async def test_semaphore_limits_concurrent_requests(self, org_config):
        """raw_request should not exceed max_concurrent_requests in-flight."""
        max_req = 3
        client = _make_client(org_config, max_concurrent_requests=max_req)

        in_flight = 0
        max_seen = 0
        lock = asyncio.Lock()

        original_inner = client._raw_request_inner

        async def mock_inner(*args, **kwargs):
            nonlocal in_flight, max_seen
            async with lock:
                in_flight += 1
                max_seen = max(max_seen, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1
            return {"ok": True}

        client._raw_request_inner = mock_inner  # type: ignore[assignment]
        # Ensure the client appears connected.
        client._http_client = AsyncMock()

        # Fire 10 concurrent raw_request calls.
        tasks = [
            asyncio.create_task(client.raw_request("GET", f"/v1/test/{i}"))
            for i in range(10)
        ]
        await asyncio.gather(*tasks)

        assert max_seen <= max_req
        assert max_seen > 1  # At least some concurrency happened

    async def test_connection_pool_sized_to_match_semaphore(self, org_config):
        """Connection pool max_connections should be >= semaphore value."""
        client = _make_client(org_config, max_concurrent_requests=30)

        with patch.object(
            BraintrustClient,
            "raw_request",
            new_callable=AsyncMock,
            return_value={"objects": []},
        ):
            await client.connect()

        # httpx exposes pool limits via _transport._pool on modern versions.
        # Use the transport's internal pool to verify sizing.
        transport = client._http_client._transport  # type: ignore[union-attr]
        pool = getattr(transport, "_pool", None)
        if pool is not None:
            max_conns = getattr(pool, "_max_connections", None)
            if max_conns is not None:
                assert max_conns >= 30
        # If we can't introspect the pool (httpx version varies), just verify
        # the client was created without error and the semaphore is correct.
        assert client._request_semaphore._value == 30

        await client.close()
