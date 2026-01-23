"""Unit tests for ProjectTagMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.project_tags import ProjectTagMigrator


@pytest.fixture
def sample_project_tag():
    """Create a sample project tag dict."""
    return {
        "id": "tag-123",
        "name": "Test Tag",
        "project_id": "project-456",
        "description": "A test tag",
        "color": "#FF0000",
        "created": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def minimal_project_tag():
    """Create a minimal project tag dict."""
    return {
        "id": "tag-min-456",
        "name": "Minimal Tag",
        "project_id": "project-456",
    }


@pytest.mark.asyncio
class TestProjectTagMigrator:
    """Test ProjectTagMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "ProjectTags"

    async def test_get_dependencies_empty(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test that project tags have no dependencies."""
        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(sample_project_tag)

        assert dependencies == []

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test successful listing of source project tags."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_tag]}
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        tags = await migrator.list_source_resources()

        assert len(tags) == 1
        assert tags[0] == sample_project_tag

    async def test_list_source_resources_with_project_filter(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test listing project tags with project filter."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_tag]}
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        tags = await migrator.list_source_resources(project_id="project-456")

        # Should filter client-side
        assert len(tags) == 1

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test listing project tags with raw API response."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_project_tag]}
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        tags = await migrator.list_source_resources()

        assert len(tags) == 1

    async def test_list_source_resources_direct_list(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test listing project tags with empty response."""
        mock_source_client.with_retry = AsyncMock(return_value={"objects": []})

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        tags = await migrator.list_source_resources()

        assert len(tags) == 0

    async def test_migrate_resource_success_full_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test successful migration of a project tag with all fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-tag-789", "name": "Test Tag"}
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(sample_project_tag)

        assert result == "new-tag-789"

    async def test_migrate_resource_success_minimal_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        minimal_project_tag,
    ):
        """Test migration of project tag with minimal fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-tag-min", "name": "Minimal Tag"}
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        result = await migrator.migrate_resource(minimal_project_tag)

        assert result == "new-tag-min"

    async def test_migrate_resource_no_project_mapping(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test migration fails without project mapping."""
        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = None  # No default project ID

        with pytest.raises(ValueError, match="No destination project mapping"):
            await migrator.migrate_resource(sample_project_tag)

    async def test_migrate_resource_creation_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_project_tag,
    ):
        """Test handling of creation errors during migration."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating tag")
        )

        migrator = ProjectTagMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.dest_project_id = "dest-project-999"

        with pytest.raises(Exception, match="Failed to migrate project tag"):
            await migrator.migrate_resource(sample_project_tag)
