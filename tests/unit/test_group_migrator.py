"""Unit tests for GroupMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.groups import GroupMigrator


@pytest.fixture
def group_with_inheritance():
    """Create a group dict that inherits from other groups."""
    return {
        "id": "group-child-123",
        "name": "Child Group",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A group that inherits from parent groups",
        "deleted_at": None,
        "member_groups": ["group-parent-1", "group-parent-2"],
        "member_users": ["user-123", "user-456"],
    }


@pytest.fixture
def group_without_inheritance():
    """Create a group dict without inheritance dependencies."""
    return {
        "id": "group-independent-456",
        "name": "Independent Group",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A group without inheritance",
        "deleted_at": None,
        "member_groups": None,
        "member_users": ["user-123"],
    }


@pytest.fixture
def sample_group():
    """Create a sample group dict."""
    return {
        "id": "group-123",
        "name": "Test Group",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A test group",
    }


@pytest.mark.asyncio
class TestGroupMigrator:
    """Test GroupMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "Groups"

    async def test_get_resource_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_group,
    ):
        """Test extracting resource ID from group."""
        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        resource_id = migrator.get_resource_id(sample_group)

        assert resource_id == "group-123"

    async def test_get_dependencies_with_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        group_with_inheritance,
    ):
        """Test that groups with member_groups return correct dependencies."""
        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(group_with_inheritance)

        assert dependencies == ["group-parent-1", "group-parent-2"]

    async def test_get_dependencies_without_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        group_without_inheritance,
    ):
        """Test that groups without member_groups return no dependencies."""
        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(group_without_inheritance)

        assert dependencies == []

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_group,
    ):
        """Test successful listing of source groups."""
        # Mock raw_request response (now returns dict with objects list)
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_group]}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        groups = await migrator.list_source_resources()

        assert len(groups) == 1
        assert groups[0] == sample_group
        mock_source_client.with_retry.assert_called_once()

    async def test_list_source_resources_empty(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test listing source groups when none exist."""
        mock_source_client.with_retry = AsyncMock(return_value={"objects": []})

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        groups = await migrator.list_source_resources()

        assert len(groups) == 0

    async def test_migrate_resource_success_full_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_group,
    ):
        """Test successful migration of a group with all fields."""
        # Mock raw_request for create
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-group-789", "name": "Test Group"}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.migrate_resource(sample_group)

        assert result == "new-group-789"
        mock_dest_client.with_retry.assert_called_once()

    async def test_migrate_resource_success_minimal_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test migration of group with minimal fields."""
        minimal_group = {
            "id": "min-group-123",
            "name": "Minimal Group",
        }

        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-group-min", "name": "Minimal Group"}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.migrate_resource(minimal_group)

        assert result == "new-group-min"

    async def test_migrate_resource_with_resolved_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        group_with_inheritance,
    ):
        """Test migration of group with resolved inheritance dependencies."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-child-group", "name": "Child Group"}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Pre-populate the ID mapping with resolved parent groups
        migrator.state.id_mapping["group-parent-1"] = "dest-parent-1"
        migrator.state.id_mapping["group-parent-2"] = "dest-parent-2"

        result = await migrator.migrate_resource(group_with_inheritance)

        assert result == "new-child-group"
        # Verify the create params included resolved member_groups
        call_args = mock_dest_client.with_retry.call_args
        assert call_args is not None

    async def test_migrate_resource_with_unresolved_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        group_with_inheritance,
    ):
        """Test migration of group with unresolved inheritance (warning logged)."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-child-group", "name": "Child Group"}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # Don't add any parent group mappings - they should log warnings

        result = await migrator.migrate_resource(group_with_inheritance)

        # Should still succeed, but with warnings logged
        assert result == "new-child-group"

    async def test_migrate_resource_with_member_users_skipped(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        group_with_inheritance,
    ):
        """Test that member_users are skipped during migration."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-group-no-users", "name": "Child Group"}
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.migrate_resource(group_with_inheritance)

        # Should succeed - member_users are logged and skipped
        assert result == "new-group-no-users"

    async def test_migrate_resource_creation_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_group,
    ):
        """Test handling of creation errors during migration."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating group")
        )

        migrator = GroupMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        with pytest.raises(Exception, match="API error creating group"):
            await migrator.migrate_resource(sample_group)
