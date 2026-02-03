"""Unit tests for the BraintrustClient wrapper."""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from braintrust_migrate.client import BraintrustClient, BraintrustConnectionError
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig

# Test constants
EXPECTED_HEALTH_CHECK_CALLS = 2  # Once during connect(), once explicitly
EXPECTED_TIMEOUT_SECONDS = 30.0


@pytest.fixture
def org_config():
    """Create a test organization configuration."""
    return BraintrustOrgConfig(
        api_key="test-api-key", url="https://test.braintrust.dev"
    )


@pytest.fixture
def migration_config():
    """Create a test migration configuration."""
    return MigrationConfig(
        batch_size=50,
        retry_attempts=3,
        retry_delay=1.0,
        max_concurrent=10,
        checkpoint_interval=50,
    )


@pytest.mark.asyncio
class TestBraintrustClient:
    """Test the BraintrustClient wrapper."""

    async def test_initialization(self, org_config, migration_config):
        """Test client initialization."""
        client = BraintrustClient(org_config, migration_config, "test-org")

        assert client.org_config == org_config
        assert client.migration_config == migration_config
        assert client.org_name == "test-org"
        assert client._http_client is None

    async def test_context_manager(self, org_config, migration_config):
        """Test async context manager behavior."""
        with patch.object(
            BraintrustClient,
            "raw_request",
            new_callable=AsyncMock,
            return_value={"objects": []},
        ):
            async with BraintrustClient(org_config, migration_config, "test-org") as c:
                assert isinstance(c._http_client, httpx.AsyncClient)
            # After context manager exit, client should be closed/reset.
            assert c._http_client is None

    async def test_health_check_success(self, org_config, migration_config):
        """Test successful health check."""
        with patch.object(
            BraintrustClient,
            "raw_request",
            new_callable=AsyncMock,
            return_value={"objects": []},
        ) as mock_raw_request:
            client = BraintrustClient(org_config, migration_config, "test-org")
            await client.connect()

            result = await client.health_check()

            assert result["status"] == "healthy"
            assert result["projects_accessible"] is True
            # Health check is called once during connect() and once explicitly
            assert mock_raw_request.call_count == EXPECTED_HEALTH_CHECK_CALLS

    async def test_health_check_failure(self, org_config, migration_config):
        """Test health check failure."""
        client = BraintrustClient(org_config, migration_config, "test-org")
        client._http_client = Mock(spec=httpx.AsyncClient)
        with patch.object(
            client, "raw_request", new=AsyncMock(side_effect=Exception("API Error"))
        ):
            with pytest.raises(BraintrustConnectionError):
                await client.health_check()

    async def test_http_client_configuration(self, org_config, migration_config):
        """Test HTTP client is properly configured."""
        with patch.object(
            BraintrustClient,
            "raw_request",
            new_callable=AsyncMock,
            return_value={"objects": []},
        ):
            client = BraintrustClient(org_config, migration_config, "test-org")
            await client.connect()

            # Check HTTP client configuration
            assert isinstance(client._http_client, httpx.AsyncClient)
            # httpx.Timeout doesn't have a 'total' attribute, check timeout property
            assert client._http_client.timeout.connect == EXPECTED_TIMEOUT_SECONDS
            # Note: httpx.AsyncClient doesn't expose limits as a public attribute
            # The limits are set during construction but not accessible for testing

    async def test_raw_request_not_connected(self, org_config, migration_config):
        """Test that raw_request raises error when not connected."""
        client = BraintrustClient(org_config, migration_config, "test-org")

        with pytest.raises(BraintrustConnectionError, match="Not connected"):
            await client.raw_request("GET", "/v1/project")

    async def test_with_retry(self, org_config, migration_config):
        """Test the with_retry method."""
        client = BraintrustClient(org_config, migration_config, "test-org")

        # Mock a successful operation
        async def mock_operation():
            return "success"

        result = await client.with_retry("test_operation", mock_operation)
        assert result == "success"
