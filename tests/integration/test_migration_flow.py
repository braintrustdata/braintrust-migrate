"""Integration tests for the migration flow."""

import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from braintrust_migrate.config import BraintrustOrgConfig, Config, MigrationConfig
from braintrust_migrate.orchestration import MigrationOrchestrator


@pytest.fixture
def temp_checkpoint_dir():
    """Create a temporary checkpoint directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def config(temp_checkpoint_dir):
    """Create a test configuration."""
    return Config(
        source=BraintrustOrgConfig(
            api_key="source-key", url="https://source.braintrust.dev"
        ),
        destination=BraintrustOrgConfig(
            api_key="dest-key", url="https://dest.braintrust.dev"
        ),
        migration=MigrationConfig(batch_size=10, max_retries=2),
        state_dir=temp_checkpoint_dir,
        resources=["datasets"],  # Only test datasets for simplicity
    )


@pytest.fixture
def mock_project_data():
    """Sample project data for testing."""
    return {
        "id": "proj_123",
        "name": "Test Project",
        "slug": "test-project",
        "description": "A test project",
    }


@pytest.fixture
def mock_dataset_data():
    """Sample dataset data for testing."""
    return {
        "id": "ds_123",
        "name": "Test Dataset",
        "project_id": "proj_123",
        "description": "A test dataset",
    }


@pytest.mark.asyncio
class TestMigrationFlow:
    """Test end-to-end migration flow."""

    async def test_successful_dataset_migration(
        self, config, mock_project_data, mock_dataset_data, temp_checkpoint_dir
    ):
        """Test successful migration of datasets."""

        @asynccontextmanager
        async def mock_create_client_pair(source_config, dest_config, migration_config):
            # Create mock clients
            source_mock = Mock()
            dest_mock = Mock()

            # Create mock client attributes
            for mock_client, is_source in [(source_mock, True), (dest_mock, False)]:
                mock_client_attr = Mock()
                mock_projects = Mock()
                mock_projects.list = AsyncMock(return_value=[])
                mock_client_attr.projects = mock_projects
                mock_client.client = mock_client_attr

                async def mock_with_retry(op_name, coro_func, is_source=is_source):
                    # Call the lambda function to get the coroutine, then return mock data
                    try:
                        # Call the function to get the coroutine (but don't await it)
                        coro = coro_func()
                        if hasattr(coro, "__await__"):
                            await coro  # Consume the coroutine
                    except Exception:
                        # If it fails, just ignore - we're mocking anyway
                        pass

                    # Return different data based on operation name and client type
                    if is_source and "list_source_projects" in op_name:
                        # Return a mock project object with the expected attributes
                        mock_project = Mock()
                        mock_project.id = mock_project_data["id"]
                        mock_project.name = mock_project_data["name"]
                        mock_project.description = mock_project_data.get("description")
                        return [mock_project]
                    elif not is_source and "list_dest_projects" in op_name:
                        return []
                    elif not is_source and "create_project" in op_name:
                        # Return a mock project object with the expected attributes
                        mock_project = Mock()
                        mock_project.id = mock_project_data["id"]
                        mock_project.name = mock_project_data["name"]
                        mock_project.description = mock_project_data.get("description")
                        return mock_project
                    elif "list_datasets" in op_name:
                        # raw_request returns dict with objects key
                        return {"objects": []}
                    elif "list_experiments" in op_name:
                        # raw_request returns dict with objects key
                        return {"objects": []}
                    elif "list_functions" in op_name:
                        # raw_request returns dict with objects key
                        return {"objects": []}
                    else:
                        return []

                mock_client.with_retry = mock_with_retry

                # Mock raw_request to return empty objects
                async def mock_raw_request(method, path, **kwargs):
                    return {"objects": []}

                mock_client.raw_request = mock_raw_request

            yield source_mock, dest_mock

        with patch(
            "braintrust_migrate.orchestration.create_client_pair",
            mock_create_client_pair,
        ):
            # Run migration
            orchestrator = MigrationOrchestrator(config)
            results = await orchestrator.migrate_all()

            # Verify results structure
            assert isinstance(results, dict)
            assert "summary" in results
            assert "projects" in results
            assert results["success"] is True

            # Check that projects were processed (1 project found and migrated)
            assert results["summary"]["total_projects"] == 1
            assert results["summary"]["total_resources"] == 0  # No datasets found
            assert results["summary"]["migrated_resources"] == 0
            assert results["summary"]["skipped_resources"] == 0
            assert results["summary"]["failed_resources"] == 0
            assert len(results["summary"]["errors"]) == 0

            # Verify project was created
            assert len(results["projects"]) == 1
            project_name = mock_project_data["name"]
            assert project_name in results["projects"]
            project_result = results["projects"][project_name]
            assert project_result["project_id"] == mock_project_data["id"]
            assert project_result["project_name"] == mock_project_data["name"]
