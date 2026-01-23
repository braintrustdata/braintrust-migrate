"""Unit tests for RoleMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.roles import RoleMigrator


@pytest.fixture
def role_with_inheritance():
    """Create a role dict that inherits from other roles."""
    return {
        "id": "role-child-123",
        "name": "Child Role",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A role that inherits from parent roles",
        "deleted_at": None,
        "member_roles": ["role-parent-1", "role-parent-2"],
        "member_permissions": [{"permission": "read", "restrict_object_type": None}],
    }


@pytest.fixture
def role_without_inheritance():
    """Create a role dict without inheritance dependencies."""
    return {
        "id": "role-independent-456",
        "name": "Independent Role",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A role without inheritance",
        "deleted_at": None,
        "member_roles": None,
        "member_permissions": [{"permission": "read", "restrict_object_type": None}],
    }


@pytest.fixture
def sample_role():
    """Create a sample role dict."""
    return {
        "id": "role-123",
        "name": "Test Role",
        "org_id": "org-456",
        "user_id": "user-789",
        "created": "2024-01-01T00:00:00Z",
        "description": "A test role",
    }


@pytest.mark.asyncio
class TestRoleMigrator:
    """Test RoleMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "Roles"

    async def test_get_resource_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test extracting resource ID from role."""
        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        resource_id = migrator.get_resource_id(sample_role)

        assert resource_id == "role-123"

    async def test_get_dependencies_with_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        role_with_inheritance,
    ):
        """Test that roles with member_roles return correct dependencies."""
        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(role_with_inheritance)

        assert dependencies == ["role-parent-1", "role-parent-2"]

    async def test_get_dependencies_without_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        role_without_inheritance,
    ):
        """Test that roles without member_roles return no dependencies."""
        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(role_without_inheritance)

        assert dependencies == []

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test successful listing of source roles."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_role]}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        roles = await migrator.list_source_resources()

        assert len(roles) == 1
        assert roles[0] == sample_role
        mock_source_client.with_retry.assert_called_once()

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test listing source roles with raw API response."""
        mock_source_client.with_retry = AsyncMock(
            return_value={"objects": [sample_role]}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        roles = await migrator.list_source_resources()

        assert len(roles) == 1
        assert roles[0] == sample_role

    async def test_list_source_resources_direct_list(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test listing source roles with empty response."""
        mock_source_client.with_retry = AsyncMock(return_value={"objects": []})

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        roles = await migrator.list_source_resources()

        assert len(roles) == 0

    async def test_migrate_resource_success_full_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test successful migration of a role with all fields."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-role-789", "name": "Test Role"}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.migrate_resource(sample_role)

        assert result == "new-role-789"
        mock_dest_client.with_retry.assert_called_once()

    async def test_migrate_resource_success_minimal_fields(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test migration of role with minimal fields."""
        minimal_role = {
            "id": "min-role-123",
            "name": "Minimal Role",
        }

        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-role-min", "name": "Minimal Role"}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.migrate_resource(minimal_role)

        assert result == "new-role-min"

    async def test_migrate_resource_with_resolved_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        role_with_inheritance,
    ):
        """Test migration of role with resolved inheritance dependencies."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-child-role", "name": "Child Role"}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Pre-populate the ID mapping with resolved parent roles
        migrator.state.id_mapping["role-parent-1"] = "dest-parent-1"
        migrator.state.id_mapping["role-parent-2"] = "dest-parent-2"

        result = await migrator.migrate_resource(role_with_inheritance)

        assert result == "new-child-role"

    async def test_migrate_resource_with_unresolved_inheritance(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        role_with_inheritance,
    ):
        """Test migration of role with unresolved inheritance (warning logged)."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "new-child-role", "name": "Child Role"}
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        # Don't add any parent role mappings - they should log warnings

        result = await migrator.migrate_resource(role_with_inheritance)

        # Should still succeed, but with warnings logged
        assert result == "new-child-role"

    async def test_migrate_resource_creation_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_role,
    ):
        """Test handling of creation errors during migration."""
        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("API error creating role")
        )

        migrator = RoleMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        with pytest.raises(Exception, match="API error creating role"):
            await migrator.migrate_resource(sample_role)
