"""Unit tests for ACLMigrator."""

from unittest.mock import Mock

import pytest
from braintrust_api.types import ACL

from braintrust_migrate.resources.acls import ACLMigrator


@pytest.fixture
def acl_with_user():
    """Create an ACL with user_id."""
    acl = Mock(spec=ACL)
    acl.id = "acl-user-123"
    acl.object_type = "project"
    acl.object_id = "project-456"
    acl.user_id = "user-123"
    acl.group_id = None
    acl.permission = "read"
    acl.role_id = None
    acl.restrict_object_type = None
    acl.object_org_id = "org-456"
    acl.created = "2024-01-01T00:00:00Z"

    # Mock the to_dict method
    acl.to_dict.return_value = {
        "id": "acl-user-123",
        "object_type": "project",
        "object_id": "project-456",
        "user_id": "user-123",
        "group_id": None,
        "permission": "read",
        "role_id": None,
        "restrict_object_type": None,
        "object_org_id": "org-456",
        "created": "2024-01-01T00:00:00Z",
    }

    return acl


@pytest.fixture
def acl_with_role():
    """Create an ACL with role_id."""
    acl = Mock(spec=ACL)
    acl.id = "acl-role-123"
    acl.object_type = "project"
    acl.object_id = "project-456"
    acl.user_id = None
    acl.group_id = "group-789"
    acl.permission = None
    acl.role_id = "role-123"
    acl.restrict_object_type = None
    acl.object_org_id = "org-456"
    acl.created = "2024-01-01T00:00:00Z"

    # Mock the to_dict method
    acl.to_dict.return_value = {
        "id": "acl-role-123",
        "object_type": "project",
        "object_id": "project-456",
        "user_id": None,
        "group_id": "group-789",
        "permission": None,
        "role_id": "role-123",
        "restrict_object_type": None,
        "object_org_id": "org-456",
        "created": "2024-01-01T00:00:00Z",
    }

    return acl


@pytest.fixture
def acl_with_permission():
    """Create an ACL with direct permission."""
    acl = Mock(spec=ACL)
    acl.id = "acl-permission-123"
    acl.object_type = "dataset"
    acl.object_id = "dataset-456"
    acl.user_id = None
    acl.group_id = "group-789"
    acl.permission = "update"
    acl.role_id = None
    acl.restrict_object_type = None
    acl.object_org_id = "org-456"
    acl.created = "2024-01-01T00:00:00Z"

    # Mock the to_dict method
    acl.to_dict.return_value = {
        "id": "acl-permission-123",
        "object_type": "dataset",
        "object_id": "dataset-456",
        "user_id": None,
        "group_id": "group-789",
        "permission": "update",
        "role_id": None,
        "restrict_object_type": None,
        "object_org_id": "org-456",
        "created": "2024-01-01T00:00:00Z",
    }

    return acl


@pytest.mark.asyncio
class TestACLMigrator:
    """Test ACLMigrator functionality."""

    async def test_resource_name(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that resource_name returns correct value."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        assert migrator.resource_name == "ACLs"

    async def test_get_resource_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test extracting resource ID from ACL."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        resource_id = migrator.get_resource_id(sample_acl)

        assert resource_id == "acl-123"

    async def test_get_dependencies_with_all_deps(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_role,
    ):
        """Test that ACLs with object, group, and role dependencies return all."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(acl_with_role)

        # Should include object_id, group_id, and role_id
        assert set(dependencies) == {"project-456", "group-789", "role-123"}

    async def test_get_dependencies_with_permission_only(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_permission,
    ):
        """Test that ACLs with only object and group dependencies return those."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        dependencies = await migrator.get_dependencies(acl_with_permission)

        # Should include object_id and group_id, but not role_id
        assert set(dependencies) == {"dataset-456", "group-789"}

    async def test_list_source_resources_success(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test listing source ACLs returns empty list (not implemented)."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        acls = await migrator.list_source_resources()

        # ACL migration not fully implemented, should return empty list
        assert len(acls) == 0
        # Should not call with_retry since we return early
        mock_source_client.with_retry.assert_not_called()

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test listing source ACLs returns empty list (not implemented)."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        acls = await migrator.list_source_resources()

        # ACL migration not fully implemented, should return empty list
        assert len(acls) == 0

    async def test_list_source_resources_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that list_source_resources doesn't raise errors (returns empty list)."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Should not raise an exception, just return empty list
        acls = await migrator.list_source_resources()
        assert len(acls) == 0

    async def test_resource_exists_in_dest_found(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test finding existing equivalent ACL in destination."""
        # Mock existing ACL in destination
        dest_acl = Mock(spec=ACL)
        dest_acl.id = "dest-acl-123"
        dest_acl.object_type = "project"
        dest_acl.object_id = "dest-project-456"  # Mapped object ID
        dest_acl.group_id = "dest-group-789"  # Mapped group ID
        dest_acl.permission = "read"
        dest_acl.role_id = None
        dest_acl.restrict_object_type = None

        mock_dest_client.with_retry.return_value = [dest_acl]

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up ID mappings
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        result = await migrator.resource_exists_in_dest(sample_acl)

        assert result == "dest-acl-123"

    async def test_resource_exists_in_dest_not_found(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test ACL not found in destination."""
        mock_dest_client.with_retry.return_value = []

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.resource_exists_in_dest(sample_acl)

        assert result is None

    async def test_resource_exists_in_dest_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test error handling in resource_exists_in_dest."""
        mock_dest_client.with_retry.side_effect = Exception("API Error")

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        result = await migrator.resource_exists_in_dest(sample_acl)

        assert result is None

    async def test_migrate_resource_with_user_id_skipped(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_user,
    ):
        """Test that ACLs with user_id are skipped."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        with pytest.raises(Exception, match="has user_id and cannot be migrated"):
            await migrator.migrate_resource(acl_with_user)

    async def test_migrate_resource_success_with_permission(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_permission,
    ):
        """Test successful ACL migration with direct permission."""
        # Mock successful ACL creation
        created_acl = Mock(spec=ACL)
        created_acl.id = "dest-acl-permission-123"
        mock_dest_client.with_retry.return_value = created_acl

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up ID mappings
        migrator.state.id_mapping["dataset-456"] = "dest-dataset-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        result = await migrator.migrate_resource(acl_with_permission)

        assert result == "dest-acl-permission-123"
        mock_dest_client.with_retry.assert_called_once()

    async def test_migrate_resource_success_with_role(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_role,
    ):
        """Test successful ACL migration with role."""
        # Mock successful ACL creation
        created_acl = Mock(spec=ACL)
        created_acl.id = "dest-acl-role-123"
        mock_dest_client.with_retry.return_value = created_acl

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up ID mappings
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"
        migrator.state.id_mapping["role-123"] = "dest-role-123"

        result = await migrator.migrate_resource(acl_with_role)

        assert result == "dest-acl-role-123"
        mock_dest_client.with_retry.assert_called_once()

    async def test_migrate_resource_missing_object_dependency(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test error when object dependency cannot be resolved."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # No ID mapping set up for object_id

        with pytest.raises(Exception, match="Could not resolve object dependency"):
            await migrator.migrate_resource(sample_acl)

    async def test_migrate_resource_missing_group_dependency(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test error when group dependency cannot be resolved."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up object mapping but not group mapping
        migrator.state.id_mapping["project-456"] = "dest-project-456"

        with pytest.raises(Exception, match="Could not resolve group dependency"):
            await migrator.migrate_resource(sample_acl)

    async def test_migrate_resource_missing_role_dependency(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_role,
    ):
        """Test error when role dependency cannot be resolved."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up object and group mappings but not role mapping
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        with pytest.raises(Exception, match="Could not resolve role dependency"):
            await migrator.migrate_resource(acl_with_role)

    async def test_migrate_resource_creation_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test error handling during ACL creation."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up ID mappings
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        mock_dest_client.with_retry.side_effect = Exception("Creation failed")

        with pytest.raises(Exception, match="Creation failed"):
            await migrator.migrate_resource(sample_acl)

    async def test_acls_equivalent_true(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test _acls_equivalent returns True for equivalent ACLs."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Set up ID mappings
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        # Create equivalent destination ACL
        dest_acl = Mock(spec=ACL)
        dest_acl.object_type = "project"
        dest_acl.object_id = "dest-project-456"
        dest_acl.group_id = "dest-group-789"
        dest_acl.permission = "read"
        dest_acl.role_id = None
        dest_acl.restrict_object_type = None

        result = await migrator._acls_equivalent(sample_acl, dest_acl)

        assert result is True

    async def test_acls_equivalent_false_different_object_type(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test _acls_equivalent returns False for different object types."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        # Create ACL with different object_type
        dest_acl = Mock(spec=ACL)
        dest_acl.object_type = "dataset"  # Different from sample_acl's "project"

        result = await migrator._acls_equivalent(sample_acl, dest_acl)

        assert result is False
