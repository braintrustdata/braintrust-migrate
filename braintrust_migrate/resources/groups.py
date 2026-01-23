"""Group migrator for Braintrust migration tool."""

from braintrust_migrate.resources.base import ResourceMigrator


class GroupMigrator(ResourceMigrator[dict]):
    """Migrator for Braintrust groups.

    Handles group migration including group inheritance dependencies.
    Groups can inherit from other groups via the member_groups field,
    so parent groups must be migrated before child groups.

    Note: member_users are excluded from migration since users are
    organization-specific and cannot be migrated between organizations.

    Uses raw API requests instead of SDK to avoid model dependencies.
    """

    @property
    def resource_name(self) -> str:
        """Human-readable name for this resource type."""
        return "Groups"

    async def get_dependencies(self, resource: dict) -> list[str]:
        """Get list of group IDs that this group depends on.

        Groups can depend on other groups via the member_groups field.

        Args:
            resource: Group dict to get dependencies for.

        Returns:
            List of group IDs this group inherits from.
        """
        dependencies = []

        # Check if group has member_groups (inheritance)
        member_groups = resource.get("member_groups")
        if member_groups:
            for group_id in member_groups:
                dependencies.append(group_id)
                self._logger.debug(
                    "Found group inheritance dependency",
                    group_id=resource.get("id"),
                    group_name=resource.get("name"),
                    parent_group_id=group_id,
                )

        return dependencies

    async def list_source_resources(self, project_id: str | None = None) -> list[dict]:
        """List all groups from the source organization using raw API.

        Args:
            project_id: Not used for groups (they are org-scoped).

        Returns:
            List of group dicts from the source organization.
        """
        try:
            # Groups are organization-scoped, not project-scoped
            # Use base class helper but without project_id
            return await self._list_resources_with_client(
                self.source_client, "groups", project_id=None
            )

        except Exception as e:
            self._logger.error("Failed to list source groups", error=str(e))
            raise

    async def migrate_resource(self, resource: dict) -> str:
        """Migrate a single group from source to destination using raw API.

        Args:
            resource: Source group dict to migrate.

        Returns:
            ID of the created group in destination.

        Raises:
            Exception: If migration fails.
        """
        self._logger.info(
            "Migrating group",
            source_id=resource.get("id"),
            name=resource.get("name"),
            org_id=resource.get("org_id"),
        )

        # Create group in destination using base class serialization
        create_params = self.serialize_resource_for_insert(resource)

        # Handle member_groups with dependency resolution
        member_groups = resource.get("member_groups")
        if member_groups:
            resolved_member_groups = []
            for group_id in member_groups:
                # Resolve group dependency to destination ID
                dest_group_id = self.state.id_mapping.get(group_id)
                if dest_group_id:
                    resolved_member_groups.append(dest_group_id)
                    self._logger.debug(
                        "Resolved group inheritance dependency",
                        group_id=resource.get("id"),
                        source_parent_group_id=group_id,
                        dest_parent_group_id=dest_group_id,
                    )
                else:
                    self._logger.warning(
                        "Could not resolve group inheritance dependency - parent group may not have been migrated",
                        group_id=resource.get("id"),
                        group_name=resource.get("name"),
                        source_parent_group_id=group_id,
                    )

            if resolved_member_groups:
                create_params["member_groups"] = resolved_member_groups

        # Note: member_users are intentionally excluded from migration
        # since users are organization-specific and cannot be migrated
        member_users = resource.get("member_users")
        if member_users:
            self._logger.info(
                "Skipping member_users migration - users are organization-specific",
                group_id=resource.get("id"),
                group_name=resource.get("name"),
                user_count=len(member_users),
            )
            # Remove member_users from create_params to avoid trying to migrate them
            create_params.pop("member_users", None)

        # Create group using raw API
        response = await self.dest_client.with_retry(
            "create_group",
            lambda create_params=create_params: self.dest_client.raw_request(
                "POST",
                "/v1/group",
                json=create_params,
            ),
        )

        dest_group_id = response.get("id")
        if not dest_group_id:
            raise ValueError(
                f"No ID returned when creating group {resource.get('name')}"
            )

        self._logger.info(
            "Created group in destination",
            source_id=resource.get("id"),
            dest_id=dest_group_id,
            name=resource.get("name"),
        )

        return dest_group_id
