"""Role migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class RoleMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust roles.

    Handles role migration including role inheritance dependencies.
    Roles can inherit from other roles via the member_roles field,
    so parent roles must be migrated before child roles.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Roles"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of role IDs that this role depends on.

        Roles can depend on other roles via the member_roles field.

        Args:
            resource: Role dict to get dependencies for.

        Returns:
            List of role IDs this role inherits from.
        """
        dependencies = []

        # Check if role has member_roles (inheritance)
        member_roles = resource.get("member_roles")
        if member_roles:
            for role_id in member_roles:
                dependencies.append(role_id)
                self._logger.debug(
                    "Found role inheritance dependency",
                    role_id=resource.get("id"),
                    role_name=resource.get("name"),
                    parent_role_id=role_id,
                )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all roles from the source organization using raw API.

        Args:
            project_id: Not used for roles (they are org-scoped).

        Returns:
            List of role dicts from the source organization.
        """
        try:
            # Roles are organization-scoped, not project-scoped
            # Use base class helper but without project_id
            return await self._list_resources_with_client(
                self.source_client, "roles", project_id=None
            )

        except Exception as e:
            self._logger.error("Failed to list source roles", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single role from source to destination using raw API.

        Args:
            resource: Source role dict to migrate.

        Returns:
            ID of the created role in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating role",
            source_id=resource.get("id"),
            name=resource.get("name"),
            org_id=resource.get("org_id"),
        )

        # Create role in destination
        create_params = self.serialize_resource_for_insert(resource)

        # Handle member_roles with dependency resolution
        member_roles = resource.get("member_roles")
        if member_roles:
            resolved_member_roles = []
            for role_id in member_roles:
                # Resolve role dependency to destination ID
                dest_role_id = self.state.id_mapping.get(role_id)
                if dest_role_id:
                    resolved_member_roles.append(dest_role_id)
                    self._logger.debug(
                        "Resolved role inheritance dependency",
                        role_id=resource.get("id"),
                        source_parent_role_id=role_id,
                        dest_parent_role_id=dest_role_id,
                    )
                else:
                    self._logger.warning(
                        "Could not resolve role inheritance dependency - parent role may not have been migrated",
                        role_id=resource.get("id"),
                        role_name=resource.get("name"),
                        source_parent_role_id=role_id,
                    )

            if resolved_member_roles:
                create_params["member_roles"] = resolved_member_roles

        # Create role using raw API
        response = await self.dest_client.with_retry(
            "create_role",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/role",
                json=create_params,
            ),
        )

        dest_role_id = response.get("id")
        if not dest_role_id:
            raise ValueError(
                f"No ID returned when creating role {resource.get('name')}"
            )

        self._logger.info(
            "Created role in destination",
            source_id=resource.get("id"),
            dest_id=dest_role_id,
            name=resource.get("name"),
        )

        return dest_role_id
