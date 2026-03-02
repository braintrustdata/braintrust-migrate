"""Unit tests for ACLMigrator."""

from unittest.mock import AsyncMock

import pytest

from braintrust_migrate.resources.acls import ACLMigrator


@pytest.fixture
def acl_with_user():
    """Create an ACL dict with user_id."""
    return {
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


@pytest.fixture
def acl_with_role():
    """Create an ACL dict with role_id."""
    return {
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


@pytest.fixture
def acl_with_permission():
    """Create an ACL dict with direct permission."""
    return {
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


@pytest.fixture
def sample_acl():
    """Create a sample ACL dict."""
    return {
        "id": "acl-123",
        "object_type": "project",
        "object_id": "project-456",
        "group_id": "group-789",
        "permission": "read",
        "restrict_object_type": None,
    }


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
        migrator.ensure_dependency_mapping = AsyncMock(return_value=None)

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
        migrator.ensure_dependency_mapping = AsyncMock(return_value=None)

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
        migrator.ensure_dependency_mapping = AsyncMock(return_value=None)

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
        """Test listing source ACLs from list_org API."""
        mock_source_client.with_retry = AsyncMock(return_value=[sample_acl])
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        acls = await migrator.list_source_resources()

        assert len(acls) == 1
        assert acls[0]["id"] == "acl-123"
        mock_source_client.with_retry.assert_called_once()

    async def test_list_source_resources_async_iterator(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test listing source ACLs when API returns object wrapper."""
        mock_source_client.with_retry = AsyncMock(
            side_effect=[{"objects": [sample_acl]}, {"objects": []}]
        )
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        acls = await migrator.list_source_resources()

        assert len(acls) == 1
        assert acls[0]["id"] == "acl-123"

    async def test_list_source_resources_error(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test that list_source_resources raises upstream API errors."""
        mock_source_client.with_retry = AsyncMock(side_effect=Exception("boom"))
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        with pytest.raises(Exception, match="boom"):
            await migrator.list_source_resources()

    async def test_list_source_resources_stops_on_missing_cursor_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test pagination stops when the last ACL is missing an id cursor."""
        mock_source_client.with_retry = AsyncMock(
            return_value=[
                {
                    "object_type": "project",
                    "object_id": "project-456",
                    "group_id": "group-789",
                    "permission": "read",
                }
            ]
        )
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        acls = await migrator.list_source_resources()

        assert len(acls) == 1
        mock_source_client.with_retry.assert_called_once()

    async def test_migrate_resource_with_user_id_skipped(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_user,
    ):
        """Test that ACLs with user_id are skipped when mapping is disabled."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        with pytest.raises(Exception, match="reason=user_acl_mapping_disabled"):
            await migrator.migrate_resource(acl_with_user)

    async def test_migrate_resource_unsupported_object_type(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test unsupported ACL object types are rejected with explicit reason."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        acl = {
            "id": "acl-unsupported-1",
            "object_type": "prompt_session",
            "object_id": "prompt-session-1",
            "group_id": "group-789",
            "permission": "read",
        }

        with pytest.raises(Exception, match="reason=unsupported_object_type"):
            await migrator.migrate_resource(acl)

    async def test_migrate_resource_organization_object_uses_destination_org_id(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
    ):
        """Test organization ACLs resolve object_id to destination org id."""
        mock_dest_client.get_org_id = AsyncMock(return_value="dest-org-1")
        captured_payloads: list[dict] = []

        async def _with_retry(_operation_name, coro_func):
            result = coro_func()
            if hasattr(result, "__await__"):
                result = await result
            if isinstance(result, dict):
                captured_payloads.append(result)
            return result

        mock_dest_client.with_retry = AsyncMock(side_effect=_with_retry)
        mock_dest_client.raw_request = AsyncMock(
            return_value={"id": "dest-acl-org-1", "object_type": "organization"}
        )
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        acl = {
            "id": "acl-org-1",
            "object_type": "organization",
            "object_id": "src-org-1",
            "group_id": "group-789",
            "permission": "read",
        }
        migrator.state.id_mapping["group-789"] = "dest-group-789"

        result = await migrator.migrate_resource(acl)

        assert result == "dest-acl-org-1"
        mock_dest_client.get_org_id.assert_called_once()
        assert captured_payloads
        mock_dest_client.raw_request.assert_awaited_once()
        payload = mock_dest_client.raw_request.call_args.kwargs["json"]
        assert payload["object_id"] == "dest-org-1"

    async def test_migrate_resource_with_user_id_mapped_by_email(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_user,
    ):
        """Test ACL user_id mapping by source/destination email."""
        mock_source_client.migration_config.acl_map_users = True
        mock_source_client.migration_config.acl_auto_invite_users = False
        mock_source_client.with_retry = AsyncMock(
            return_value={"id": "user-123", "email": "analyst@example.com"}
        )
        mock_dest_client.with_retry = AsyncMock(
            side_effect=[
                {
                    "objects": [
                        {"id": "dest-user-999", "email": "analyst@example.com"}
                    ]
                },
                {"id": "dest-acl-user-123", "object_type": "project"},
            ]
        )

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["project-456"] = "dest-project-456"

        result = await migrator.migrate_resource(acl_with_user)

        assert result == "dest-acl-user-123"
        assert migrator.state.id_mapping["user-123"] == "dest-user-999"

    async def test_migrate_resource_with_user_id_auto_invite(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_user,
    ):
        """Test ACL user auto-invite fallback when destination user is missing."""
        mock_source_client.migration_config.acl_map_users = True
        mock_source_client.migration_config.acl_auto_invite_users = True
        mock_source_client.with_retry = AsyncMock(
            return_value={"id": "user-123", "email": "newuser@example.com"}
        )
        mock_dest_client.with_retry = AsyncMock(
            side_effect=[
                {"objects": []},  # initial lookup
                {"status": "success"},  # invite
                {"objects": [{"id": "dest-user-321", "email": "newuser@example.com"}]},  # lookup after invite
                {"id": "dest-acl-user-invite", "object_type": "project"},  # create acl
            ]
        )

        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["project-456"] = "dest-project-456"

        result = await migrator.migrate_resource(acl_with_user)

        assert result == "dest-acl-user-invite"
        assert migrator.state.id_mapping["user-123"] == "dest-user-321"

    async def test_migrate_resource_success_with_permission(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_permission,
    ):
        """Test successful ACL migration with direct permission."""
        # Mock successful ACL creation
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "dest-acl-permission-123", "object_type": "dataset"}
        )

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
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "dest-acl-role-123", "object_type": "project"}
        )

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

        with pytest.raises(Exception, match="reason=unresolved_object_dependency"):
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

        with pytest.raises(Exception, match="reason=unresolved_group_dependency"):
            await migrator.migrate_resource(sample_acl)

    async def test_migrate_resource_group_dependency_resolved_via_fallback(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        sample_acl,
    ):
        """Test unresolved group dependency can be resolved through fallback mapping."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "dest-acl-123", "object_type": "project"}
        )
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.ensure_dependency_mapping = AsyncMock(return_value="dest-group-789")

        result = await migrator.migrate_resource(sample_acl)

        assert result == "dest-acl-123"
        migrator.ensure_dependency_mapping.assert_called_once_with(
            "groups", "group-789", project_id=None
        )

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

        with pytest.raises(Exception, match="reason=unresolved_role_dependency"):
            await migrator.migrate_resource(acl_with_role)

    async def test_migrate_resource_role_dependency_resolved_via_fallback(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_role,
    ):
        """Test unresolved role dependency can be resolved through fallback mapping."""
        mock_dest_client.with_retry = AsyncMock(
            return_value={"id": "dest-acl-role-123", "object_type": "project"}
        )
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )
        migrator.state.id_mapping["project-456"] = "dest-project-456"
        migrator.state.id_mapping["group-789"] = "dest-group-789"
        migrator.ensure_dependency_mapping = AsyncMock(return_value="dest-role-123")

        result = await migrator.migrate_resource(acl_with_role)

        assert result == "dest-acl-role-123"
        migrator.ensure_dependency_mapping.assert_called_once_with(
            "roles", "role-123", project_id=None
        )

    async def test_migrate_batch_skips_unmigratable_user_acl(
        self,
        mock_source_client,
        mock_dest_client,
        temp_checkpoint_dir,
        acl_with_user,
    ):
        """Test batch migration marks user ACL as skipped when mapping disabled."""
        migrator = ACLMigrator(
            mock_source_client, mock_dest_client, temp_checkpoint_dir
        )

        results = await migrator.migrate_batch([acl_with_user])

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].skipped is True
        assert results[0].metadata.get("skip_reason") == "user_acl_mapping_disabled"

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

        mock_dest_client.with_retry = AsyncMock(
            side_effect=Exception("Creation failed")
        )

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

        # Create equivalent destination ACL dict
        dest_acl = {
            "object_type": "project",
            "object_id": "dest-project-456",
            "group_id": "dest-group-789",
            "permission": "read",
            "role_id": None,
            "restrict_object_type": None,
        }

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
        dest_acl = {
            "object_type": "dataset",  # Different from sample_acl's "project"
        }

        result = await migrator._acls_equivalent(sample_acl, dest_acl)

        assert result is False
