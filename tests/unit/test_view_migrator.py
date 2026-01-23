"""Unit tests for ViewMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.views import ViewMigrator


@pytest.fixture
def sample_view():
    """Create a sample view dict."""
    return {
        "id": "view-123",
        "name": "Test View",
        "object_id": "project-456",
        "object_type": "project",
        "view_type": "table",
        "created": "2024-01-01T00:00:00Z",
        "view_data": {"columns": ["name", "id"]},
    }


@pytest.fixture
def experiment_view():
    """Create a view attached to an experiment."""
    return {
        "id": "view-exp-123",
        "name": "Experiment View",
        "object_id": "exp-789",
        "object_type": "experiment",
        "view_type": "table",
        "view_data": {"columns": ["score", "output"]},
    }


@pytest.mark.asyncio
class TestViewMigrator:
    """Test ViewMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "Views"

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_view,
    ):
        """Test successful listing of source views."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_view]}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        views = await migrator.list_source_resources()

        assert len(views) == 1
        assert views[0] == sample_view

    async def test_list_source_resources_with_project_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_view,
    ):
        """Test listing views filtered by project."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_view]}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        views = await migrator.list_source_resources(project_id="project-456")

        assert len(views) == 1

    async def test_migrate_resource_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_view,
    ):
        """Test successful migration of a view."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-view-789", "name": "Test View"}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_view)

        assert result == "new-view-789"

    async def test_migrate_resource_with_dest_project_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_view,
    ):
        """Test that project views use dest_project_id."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-view-789", "name": "Test View"}
        )

        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_view)

        assert result == "new-view-789"

    async def test_get_dependencies_project_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_view,
    ):
        """Test that project views have no extra dependencies."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(sample_view)

        # Project views don't add dependencies (project is handled separately)
        assert deps == []

    async def test_get_dependencies_experiment_view(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        experiment_view,
    ):
        """Test that experiment views depend on the experiment."""
        migrator = ViewMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        deps = await migrator.get_dependencies(experiment_view)

        assert deps == ["exp-789"]
