"""ACL migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class ACLMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust ACLs.

    Handles ACL migration including complex dependency resolution:
    - object_id references to various resource types (projects, experiments, datasets, etc.)
    - role_id references to roles
    - group_id references to groups

    Note: user_id references are handled specially since users are
    organization-specific and cannot be migrated between organizations.
    ACLs with user_id will be skipped with a warning.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "ACLs"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of resource IDs that this ACL depends on.

        ACLs can depend on:
        - object_id: The resource the ACL applies to
        - role_id: The role being granted (if not a direct permission)
        - group_id: The group receiving the ACL (if not a user)

        Args:
            resource: ACL dict to get dependencies for.

        Returns:
            List of resource IDs this ACL depends on.
        """
        dependencies = []

        # Add object dependency
        object_id = resource.get("object_id")
        if object_id:
            dependencies.append(object_id)
            self._logger.debug(
                "Found ACL object dependency",
                acl_id=resource.get("id"),
                object_type=resource.get("object_type"),
                object_id=object_id,
            )

        # Add role dependency if present
        role_id = resource.get("role_id")
        if role_id:
            dependencies.append(role_id)
            self._logger.debug(
                "Found ACL role dependency",
                acl_id=resource.get("id"),
                role_id=role_id,
            )

        # Add group dependency if present
        group_id = resource.get("group_id")
        if group_id:
            dependencies.append(group_id)
            self._logger.debug(
                "Found ACL group dependency",
                acl_id=resource.get("id"),
                group_id=group_id,
            )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all ACLs from the source organization.

        Args:
            project_id: Not used for ACLs (they are org-scoped).

        Returns:
            List of ACL dicts from the source organization.
        """
        try:
            # ACLs API requires object_id and object_type parameters
            # We can't list all ACLs at once, so return empty list for now
            # ACLs will need to be migrated per-object basis in a future implementation
            self._logger.warning(
                "ACL migration not fully implemented - ACLs API requires object_id and object_type"
            )
            return []

        except Exception as e:
            self._logger.error("Failed to list source ACLs", error=str(e))
            raise

    async def _acls_equivalent(self, source_acl: dict, dest_acl: dict) -> bool:
        """Check if two ACLs are equivalent after ID mapping.

        Args:
            source_acl: Source ACL dict.
            dest_acl: Destination ACL dict.

        Returns:
            True if ACLs are equivalent, False otherwise.
        """
        # Check object_type and object_id (with mapping)
        source_object_id = source_acl.get("object_id")
        mapped_object_id = (
            self.state.id_mapping.get(source_object_id) if source_object_id else None
        )
        object_match = source_acl.get("object_type") == dest_acl.get(
            "object_type"
        ) and mapped_object_id == dest_acl.get("object_id")

        if not object_match:
            return False

        # Check user_id or group_id (with mapping for groups)
        user_group_match = True
        source_user_id = source_acl.get("user_id")
        source_group_id = source_acl.get("group_id")
        if source_user_id:
            # Users can't be mapped, so this should not match
            user_group_match = False
        elif source_group_id:
            mapped_group_id = self.state.id_mapping.get(source_group_id)
            user_group_match = mapped_group_id == dest_acl.get("group_id")
        else:
            # Neither user_id nor group_id - this shouldn't happen
            user_group_match = False

        if not user_group_match:
            return False

        # Check permission or role_id (with mapping for roles)
        permission_role_match = True
        source_permission = source_acl.get("permission")
        source_role_id = source_acl.get("role_id")
        if source_permission:
            permission_role_match = source_permission == dest_acl.get("permission")
        elif source_role_id:
            mapped_role_id = self.state.id_mapping.get(source_role_id)
            permission_role_match = mapped_role_id == dest_acl.get("role_id")

        if not permission_role_match:
            return False

        # Check restrict_object_type
        return source_acl.get("restrict_object_type") == dest_acl.get(
            "restrict_object_type"
        )

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single ACL from source to destination using raw API.

        Args:
            resource: Source ACL dict to migrate.

        Returns:
            ID of the created ACL in destination.

        Raises:
            Exception: If migration fails.
        """
        resource_id = resource.get("id")
        object_id = resource.get("object_id")
        object_type = resource.get("object_type")

        # Skip ACLs with user_id since users can't be migrated
        user_id = resource.get("user_id")
        if user_id:
            self._logger.warning(
                "Skipping ACL with user_id - users are organization-specific",
                acl_id=resource_id,
                user_id=user_id,
                object_type=object_type,
                object_id=object_id,
            )
            raise Exception(f"ACL {resource_id} has user_id and cannot be migrated")

        self._logger.info(
            "Migrating ACL",
            source_id=resource_id,
            object_type=object_type,
            object_id=object_id,
        )

        # Create ACL in destination
        create_params = self.serialize_resource_for_insert(resource)

        # Resolve object_id dependency
        mapped_object_id = self.state.id_mapping.get(object_id) if object_id else None
        if mapped_object_id:
            create_params["object_id"] = mapped_object_id
            self._logger.debug(
                "Resolved ACL object dependency",
                acl_id=resource_id,
                source_object_id=object_id,
                dest_object_id=mapped_object_id,
            )
        else:
            self._logger.error(
                "Could not resolve ACL object dependency",
                acl_id=resource_id,
                object_type=object_type,
                object_id=object_id,
            )
            raise Exception(
                f"Could not resolve object dependency for ACL {resource_id}"
            )

        # Handle group_id with dependency resolution
        group_id = resource.get("group_id")
        if group_id:
            mapped_group_id = self.state.id_mapping.get(group_id)
            if mapped_group_id:
                create_params["group_id"] = mapped_group_id
                self._logger.debug(
                    "Resolved ACL group dependency",
                    acl_id=resource_id,
                    source_group_id=group_id,
                    dest_group_id=mapped_group_id,
                )
            else:
                self._logger.error(
                    "Could not resolve ACL group dependency",
                    acl_id=resource_id,
                    group_id=group_id,
                )
                raise Exception(
                    f"Could not resolve group dependency for ACL {resource_id}"
                )

        # Handle role_id with dependency resolution
        role_id = resource.get("role_id")
        if role_id:
            mapped_role_id = self.state.id_mapping.get(role_id)
            if mapped_role_id:
                create_params["role_id"] = mapped_role_id
                self._logger.debug(
                    "Resolved ACL role dependency",
                    acl_id=resource_id,
                    source_role_id=role_id,
                    dest_role_id=mapped_role_id,
                )
            else:
                self._logger.error(
                    "Could not resolve ACL role dependency",
                    acl_id=resource_id,
                    role_id=role_id,
                )
                raise Exception(
                    f"Could not resolve role dependency for ACL {resource_id}"
                )

        # Create ACL using raw API
        response = await self.dest_client.with_retry(
            "create_acl",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/acl",
                json=create_params,
            ),
        )

        dest_acl_id = response.get("id")
        if not dest_acl_id:
            raise ValueError(
                f"No ID returned when creating ACL for {object_type}/{object_id}"
            )

        self._logger.info(
            "Created ACL in destination",
            source_id=resource_id,
            dest_id=dest_acl_id,
            object_type=object_type,
        )

        return dest_acl_id
