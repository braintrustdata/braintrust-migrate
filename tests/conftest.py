"""Shared pytest fixtures for the migration tool tests."""

import inspect
from unittest.mock import AsyncMock, Mock

import pytest

from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


@pytest.fixture
def org_config() -> BraintrustOrgConfig:
    """Create a test organization configuration."""
    return BraintrustOrgConfig(api_key="test-api-key", url="https://test.braintrust.dev")


@pytest.fixture
def migration_config() -> MigrationConfig:
    """Create a test migration configuration."""
    return MigrationConfig(
        batch_size=50,
        retry_attempts=3,
        retry_delay=1.0,
        max_concurrent=10,
        checkpoint_interval=50,
    )


def _make_mock_client() -> Mock:
    client = Mock()

    async def _with_retry(_operation_name, coro_func):
        result = coro_func()
        if inspect.isawaitable(result):
            return await result
        return result

    client.with_retry = AsyncMock(side_effect=_with_retry)
    client.raw_request = AsyncMock(return_value={"objects": []})
    return client


@pytest.fixture
def mock_source_client() -> Mock:
    """Mock source `BraintrustClient`-like object."""
    return _make_mock_client()


@pytest.fixture
def mock_dest_client() -> Mock:
    """Mock destination `BraintrustClient`-like object."""
    return _make_mock_client()


@pytest.fixture
def temp_checkpoint_dir(tmp_path):
    """Create a temporary checkpoint directory for testing."""
    checkpoint_dir = tmp_path / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    return checkpoint_dir


# Test constants that can be reused across tests
TEST_PROJECT_ID = "test-project-123"
TEST_DEST_PROJECT_ID = "dest-project-456"
TEST_USER_ID = "test-user-789"
TEST_BATCH_SIZE = 10
